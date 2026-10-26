//! The in-memory heat counters and the promotion queue.
//!
//! Every GET records a read here, including a cache hit. Counters are sharded
//! so concurrent GETs do not contend on one lock; a key enters the bounded
//! promotion queue once, when its count crosses `promote_reads` (T9). The
//! counters drain to the durable index in chunks, and both counters and
//! queued promotions expire once they leave the heat window (T21).

use std::{
    collections::{HashMap, VecDeque, hash_map::DefaultHasher},
    hash::{Hash as _, Hasher as _},
    mem,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant, SystemTime},
};

use crate::{
    _core::{bucket, object},
    index::HeatEntry,
};

/// The number of counter shards a read is spread across.
const SHARDS: usize = 16;

/// The promotion queue's bound, so a hot burst cannot grow it without limit.
const PROMOTE_QUEUE_CAPACITY: usize = 1024;

/// One shard of the heat counters behind its own lock: one map per bucket, so
/// a lookup by a borrowed `(bucket, key)` hashes the names without owning a
/// copy of either.
type Shard = Mutex<HashMap<bucket::Name, HashMap<object::Key, Counter>>>;

/// The in-memory heat counters and the bounded promotion queue.
///
/// A GET calls [`record_read`](Self::record_read) whether the bytes came from
/// memory or below it. Counters are sharded, so one read contends with a
/// fraction of the other reads. When a key's count reaches `promote_reads`,
/// the key enters the promotion queue exactly once for that crossing; reads
/// past the crossing do not enqueue it again until the counter is drained or
/// expires. [`expire`](Self::expire) drops counters and queued promotions
/// older than the heat window (T9, T21).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_core::{bucket, object};
/// use tinio_tier::Heat;
///
/// let heat = Heat::new(Duration::from_secs(3600), 3);
/// let data = bucket::name("data").expect("a valid bucket name");
/// let key = object::key("a.txt").expect("a valid object key");
/// heat.record_read(&data, &key);
/// assert_eq!(heat.score(&data, &key), 1);
/// ```
#[derive(Debug)]
pub struct Heat {
    window: Duration,
    promote_reads: u32,
    shards: Box<[Shard]>,
    queue: Mutex<VecDeque<Queued>>,
}

/// One key's reads inside the heat window.
#[derive(Debug)]
struct Counter {
    reads: u64,
    last: Instant,
}

/// One counter as [`Heat::snapshot`] saw it: the durable row plus the
/// monotonic read time [`Heat::forget`] matches against, so a read that
/// arrives between the two calls keeps its counter.
#[derive(Debug, Clone)]
pub(crate) struct Snapshot {
    /// The row the index absorbs.
    pub(crate) entry: HeatEntry,
    /// The counter's read time when the snapshot was taken.
    last: Instant,
}

/// One promotion candidate and the time it crossed the threshold.
#[derive(Debug)]
struct Queued {
    bucket: bucket::Name,
    key: object::Key,
    since: Instant,
}

/// Whether `last` is inside the heat window ending at `now`.
///
/// This is the one window-expiry rule: a read landing exactly on the window's
/// edge still counts, and only a strictly older read is expired.
fn live_counter(last: Instant, window: Duration, now: Instant) -> bool {
    now.saturating_duration_since(last) <= window
}

/// Count one read in `counter`, answering whether this read crossed the
/// promotion threshold.
fn bump(counter: &mut Counter, window: Duration, now: Instant, promote_reads: u32) -> bool {
    if !live_counter(counter.last, window, now) {
        counter.reads = 0;
    }
    counter.reads = counter.reads.saturating_add(1);
    counter.last = now;
    promote_reads > 0 && counter.reads == u64::from(promote_reads)
}

/// The one poison recovery the heat's locks take: a poisoned mutex still
/// yields its data, since a counter or a queue is no less usable for it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Heat {
    /// Counters that expire after `window`, with a key entering the promotion
    /// queue at `promote_reads`.
    ///
    /// A threshold of zero disables promotion while counters still accumulate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 2);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// assert_eq!(heat.score(&data, &key), 0);
    /// ```
    pub fn new(window: Duration, promote_reads: u32) -> Self {
        Self {
            window,
            promote_reads,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Record one read of `key`, whichever plane served it.
    ///
    /// The counter restarts when its last read is older than the window, so a
    /// read that has aged out does not count toward the next crossing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 3);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// heat.record_read(&data, &key);
    /// heat.record_read(&data, &key);
    /// assert_eq!(heat.score(&data, &key), 2);
    /// ```
    pub fn record_read(&self, bucket: &bucket::Name, key: &object::Key) {
        let now = Instant::now();
        let crossed = {
            let mut shard = lock(self.shard(bucket, key));
            if let Some(counters) = shard.get_mut(bucket) {
                if let Some(counter) = counters.get_mut(key) {
                    bump(counter, self.window, now, self.promote_reads)
                } else {
                    let counter = counters.entry(key.clone()).or_insert(Counter {
                        reads: 0,
                        last: now,
                    });
                    bump(counter, self.window, now, self.promote_reads)
                }
            } else {
                let counter = shard
                    .entry(bucket.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_insert(Counter {
                        reads: 0,
                        last: now,
                    });
                bump(counter, self.window, now, self.promote_reads)
            }
        };
        if crossed {
            let mut queue = lock(&self.queue);
            if queue.len() < PROMOTE_QUEUE_CAPACITY {
                queue.push_back(Queued {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    since: now,
                });
            }
        }
    }

    /// The reads recorded for `key` inside the window; zero once they age out.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 3);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// assert_eq!(heat.score(&data, &key), 0);
    /// heat.record_read(&data, &key);
    /// assert_eq!(heat.score(&data, &key), 1);
    /// ```
    pub fn score(&self, bucket: &bucket::Name, key: &object::Key) -> u64 {
        let now = Instant::now();
        let shard = lock(self.shard(bucket, key));
        shard
            .get(bucket)
            .and_then(|counters| counters.get(key))
            .filter(|counter| live_counter(counter.last, self.window, now))
            .map_or(0, |counter| counter.reads)
    }

    /// When `key` was last read inside the window, or `None` when no counter
    /// exists or the counter has aged past the window.
    ///
    /// A caller that decides whether an object is idle compares this against
    /// its own threshold: the counter's window (the heat window) is usually
    /// shorter than an idle threshold, so `None` here does not mean the object
    /// was never read, only that no read survives in memory.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 3);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// assert!(heat.last_read(&data, &key).is_none());
    /// heat.record_read(&data, &key);
    /// assert!(heat.last_read(&data, &key).is_some());
    /// ```
    pub fn last_read(&self, bucket: &bucket::Name, key: &object::Key) -> Option<Instant> {
        let now = Instant::now();
        let shard = lock(self.shard(bucket, key));
        shard
            .get(bucket)
            .and_then(|counters| counters.get(key))
            .filter(|counter| live_counter(counter.last, self.window, now))
            .map(|counter| counter.last)
    }

    /// Take every counter inside the window, in `(bucket, key)` order, for the
    /// index flush.
    ///
    /// One implementation with the flush: [`Heat::snapshot`] then
    /// [`Heat::forget`], so the window rule lives once. Each row carries the
    /// counter's own last-read time, not the drain time, so a counter that
    /// outlived a slower flush cadence is still aged correctly once it reaches
    /// the durable table. An out-of-window counter is not persisted as fresh
    /// and is left in the map for [`Heat::expire`] to remove — this drain does
    /// not take it. The promotion queue is not touched.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 2);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// heat.record_read(&data, &key);
    /// heat.record_read(&data, &key);
    /// let rows = heat.drain();
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].reads, 2);
    /// assert_eq!(heat.score(&data, &key), 0);
    /// ```
    pub fn drain(&self) -> Vec<HeatEntry> {
        let taken = self.snapshot();
        self.forget(&taken);
        taken.into_iter().map(|snapshot| snapshot.entry).collect()
    }

    /// The counters the index should absorb, in `(bucket, key)` order, without
    /// removing them.
    ///
    /// The two-phase pair with [`Heat::forget`] keeps a read visible while its
    /// batch is being written: the counters stay in memory until the index has
    /// accepted the rows, so a reader landing between the two calls still sees
    /// the read. A counter already older than the window is left to
    /// [`Heat::expire`].
    pub(crate) fn snapshot(&self) -> Vec<Snapshot> {
        // Sample the wall clock first, so the derived read time is never
        // shifted later than the counter it describes.
        let drained_at = SystemTime::now();
        let now = Instant::now();
        let mut rows = Vec::new();
        for shard in self.shards.iter() {
            let shard = lock(shard);
            for (bucket, counters) in shard.iter() {
                for (key, counter) in counters {
                    let age = now.saturating_duration_since(counter.last);
                    if age > self.window {
                        continue;
                    }
                    rows.push(Snapshot {
                        entry: HeatEntry {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            reads: counter.reads,
                            last_read: drained_at.checked_sub(age).unwrap_or(drained_at),
                        },
                        last: counter.last,
                    });
                }
            }
        }
        rows.sort_by(|a, b| {
            (&*a.entry.bucket, &*a.entry.key).cmp(&(&*b.entry.bucket, &*b.entry.key))
        });
        rows
    }

    /// Remove the counters `taken` holds, unless a read has touched one since
    /// the snapshot.
    ///
    /// A counter whose last read moved after the snapshot stays in place, but
    /// it loses the reads this batch absorbed: the index already stored that
    /// delta, so keeping the cumulative total would store it again on the next
    /// flush. The exception is a counter whose window elapsed in between: that
    /// new read reset it, so its whole count belongs to a fresh window and
    /// subtracting the absorbed total would drop it. A counter no read has
    /// touched since the snapshot is removed whole.
    pub(crate) fn forget(&self, taken: &[Snapshot]) {
        if taken.is_empty() {
            return;
        }
        let seen: HashMap<(&str, &str), (Instant, u64)> = taken
            .iter()
            .map(|snapshot| {
                (
                    (
                        snapshot.entry.bucket.as_ref().as_str(),
                        snapshot.entry.key.as_ref().as_str(),
                    ),
                    (snapshot.last, snapshot.entry.reads),
                )
            })
            .collect();
        for shard in self.shards.iter() {
            let mut shard = lock(shard);
            shard.retain(|bucket, counters| {
                counters.retain(|key, counter| {
                    match seen.get(&(bucket.as_ref().as_str(), key.as_ref().as_str())) {
                        // Unchanged: the batch carried every read.
                        Some((last, _)) if *last == counter.last => false,
                        Some((last, absorbed)) => {
                            // A newer read arrived: keep only the part the index
                            // has not stored yet — unless the counter's window
                            // elapsed in between, in which case that read reset
                            // the counter and the whole count is the new window's.
                            // A span past the window proves a reset; a chain of in-window reads can span it too, and absorb_heat_report's stale-row replacement keeps that case from double counting.
                            if counter.last.saturating_duration_since(*last) <= self.window {
                                counter.reads = counter.reads.saturating_sub(*absorbed);
                            }
                            true
                        }
                        // Another batch owns the counter.
                        None => true,
                    }
                });
                !counters.is_empty()
            });
        }
    }

    /// Take the keys that crossed the promotion threshold, oldest crossing
    /// first.
    ///
    /// The caller owns the returned candidates; a later call does not repeat
    /// them. The queue is bounded, so a crossing that arrives while it is full
    /// is dropped rather than growing memory without limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::from_secs(60), 2);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// heat.record_read(&data, &key);
    /// heat.record_read(&data, &key);
    /// assert_eq!(heat.promote_queue(), vec![(data, key)]);
    /// ```
    pub fn promote_queue(&self) -> Vec<(bucket::Name, object::Key)> {
        let mut queue = lock(&self.queue);
        let queued = mem::take(&mut *queue);
        queued
            .into_iter()
            .map(|entry| (entry.bucket, entry.key))
            .collect()
    }

    /// Put a key back into the promotion queue — the post-bind restore and a
    /// promotion deferred by the dwell both need the queue to reconsider it.
    ///
    /// The queue is bounded and deduplicated: a key already queued keeps its
    /// place, and a crossing that arrives while the queue is full is dropped.
    pub(crate) fn enqueue(&self, bucket: bucket::Name, key: object::Key) {
        let mut queue = lock(&self.queue);
        if queue.len() >= PROMOTE_QUEUE_CAPACITY {
            return;
        }
        if queue
            .iter()
            .any(|queued| queued.bucket == bucket && queued.key == key)
        {
            return;
        }
        queue.push_back(Queued {
            bucket,
            key,
            since: Instant::now(),
        });
    }

    /// Forget `key` entirely: drop its in-memory counter and any queued
    /// promotion for it.
    ///
    /// A mutation calls this so the reads recorded for the bytes it replaces
    /// cannot keep the new object in the promotion queue (T9). The durable row
    /// is [`Index::remove_heat`](crate::Index) beside it.
    pub(crate) fn forget_key(&self, bucket: &bucket::Name, key: &object::Key) {
        let mut shard = lock(self.shard(bucket, key));
        if let Some(counters) = shard.get_mut(bucket) {
            counters.remove(key);
            if counters.is_empty() {
                shard.remove(bucket);
            }
        }
        let mut queue = lock(&self.queue);
        queue.retain(|queued| &queued.bucket != bucket || &queued.key != key);
    }

    /// Drop the counters and queued promotions older than the heat window,
    /// answering how many counters were dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{thread, time::Duration};
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Heat;
    ///
    /// let heat = Heat::new(Duration::ZERO, 1);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// heat.record_read(&data, &key);
    /// thread::sleep(Duration::from_millis(1));
    /// assert_eq!(heat.expire(), 1);
    /// assert_eq!(heat.score(&data, &key), 0);
    /// ```
    pub fn expire(&self) -> usize {
        let now = Instant::now();
        let mut dropped = 0;
        for shard in self.shards.iter() {
            let mut shard = lock(shard);
            shard.retain(|_, counters| {
                counters.retain(|_, counter| {
                    let keep = live_counter(counter.last, self.window, now);
                    if !keep {
                        dropped += 1;
                    }
                    keep
                });
                !counters.is_empty()
            });
        }
        let mut queue = lock(&self.queue);
        queue.retain(|queued| live_counter(queued.since, self.window, now));
        dropped
    }

    fn shard(&self, bucket: &bucket::Name, key: &object::Key) -> &Shard {
        let mut hasher = DefaultHasher::new();
        bucket.hash(&mut hasher);
        key.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % self.shards.len()]
    }
}

#[cfg(test)]
mod tests {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use super::*;
    use crate::_core::{bucket, object};

    fn bucket() -> bucket::Name {
        bucket::name("data").expect("a valid bucket name")
    }

    fn key(name: &str) -> object::Key {
        object::key(name).expect("a valid object key")
    }

    #[test]
    fn the_promotion_queue_fires_at_threshold_once() {
        let heat = Heat::new(Duration::from_secs(60), 3);
        let data = bucket();
        let hot = key("a");

        for _ in 0..3 {
            heat.record_read(&data, &hot);
        }
        assert_eq!(heat.score(&data, &hot), 3);
        assert_eq!(
            heat.promote_queue(),
            vec![(data.clone(), hot.clone())],
            "the crossing enqueues the key once"
        );

        for _ in 0..5 {
            heat.record_read(&data, &hot);
        }
        assert_eq!(heat.score(&data, &hot), 8);
        assert!(
            heat.promote_queue().is_empty(),
            "reads past the crossing do not re-enqueue the key"
        );

        let second = key("b");
        for _ in 0..3 {
            heat.record_read(&data, &second);
        }
        assert_eq!(heat.promote_queue(), vec![(data.clone(), second.clone())]);

        let mut rows = heat.drain();
        rows.sort_by(|a, b| (&*a.bucket, &*a.key).cmp(&(&*b.bucket, &*b.key)));
        assert_eq!(rows.len(), 2, "the drain takes every counter");
        assert_eq!(rows[0].key, hot);
        assert_eq!(rows[0].reads, 8);
        assert_eq!(rows[1].key, second);
        assert_eq!(rows[1].reads, 3);
        assert_eq!(heat.score(&data, &hot), 0, "the drain empties the counters");

        for _ in 0..3 {
            heat.record_read(&data, &hot);
        }
        assert_eq!(
            heat.promote_queue(),
            vec![(data, hot)],
            "a fresh crossing after a drain fires again"
        );
    }

    /// A drained row keeps the counter's own read time, and a counter already
    /// outside the window is dropped rather than persisted as a fresh read.
    #[test]
    fn a_drain_keeps_each_counters_own_read_time() {
        let window = Duration::from_millis(300);
        let heat = Heat::new(window, 2);
        let data = bucket();
        let stale = key("stale");
        let kept = key("kept");

        heat.record_read(&data, &stale);
        // The stale counter must age past the window while the kept one stays
        // inside it.
        thread::sleep(Duration::from_millis(400));
        heat.record_read(&data, &kept);
        thread::sleep(Duration::from_millis(150));

        let drained_at = SystemTime::now();
        let rows = heat.drain();

        assert_eq!(rows.len(), 1, "the out-of-window counter is not persisted");
        assert_eq!(rows[0].key, kept);
        let age = drained_at
            .duration_since(rows[0].last_read)
            .expect("the row's read time is not in the future");
        assert!(
            age >= Duration::from_millis(100),
            "the row carries the counter's read time, not the drain time: {age:?}"
        );
        assert!(
            age < window,
            "the persisted row is still inside the window: {age:?}"
        );
        assert!(heat.drain().is_empty(), "the drain empties the counters");
    }

    /// A snapshot leaves the counters in place; forgetting them removes only
    /// the counters the snapshot saw, so a read that arrived in between is not
    /// lost.
    #[test]
    fn a_snapshot_keeps_the_counters_until_forgotten() {
        let heat = Heat::new(Duration::from_secs(60), 2);
        let data = bucket();
        let hot = key("hot");

        heat.record_read(&data, &hot);
        heat.record_read(&data, &hot);
        let taken = heat.snapshot();

        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].entry.reads, 2);
        assert!(
            heat.last_read(&data, &hot).is_some(),
            "the snapshot does not take the counter"
        );

        // A read between the snapshot and the forget moves the counter, so the
        // batch that carried the older count must not remove it — and it keeps
        // only the delta the index has not stored.
        heat.record_read(&data, &hot);
        heat.forget(&taken);
        assert!(
            heat.last_read(&data, &hot).is_some(),
            "a newer read survives the forget"
        );
        assert_eq!(
            heat.score(&data, &hot),
            1,
            "the absorbed delta is not counted again"
        );

        let next = heat.snapshot();
        assert_eq!(
            next[0].entry.reads, 1,
            "the next batch carries only the unabsorbed read"
        );
        heat.forget(&next);
        assert!(
            heat.last_read(&data, &hot).is_none(),
            "an unchanged counter is forgotten"
        );
    }

    #[test]
    fn expired_heat_is_dropped() {
        let window = Duration::from_millis(20);
        let heat = Heat::new(window, 2);
        let data = bucket();
        let stale = key("stale");

        heat.record_read(&data, &stale);
        heat.record_read(&data, &stale);
        assert_eq!(heat.score(&data, &stale), 2);
        assert_eq!(heat.promote_queue().len(), 1);

        let expired_at = Instant::now();
        while expired_at.elapsed() <= window {
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(heat.expire(), 1, "the stale counter is dropped");
        assert_eq!(heat.score(&data, &stale), 0);
        assert!(heat.drain().is_empty());
        assert!(
            heat.promote_queue().is_empty(),
            "a queued promotion older than the window is not promoted"
        );

        let fresh = key("fresh");
        heat.record_read(&data, &fresh);
        assert_eq!(heat.expire(), 0, "a fresh counter survives");
        assert_eq!(heat.score(&data, &fresh), 1);
    }
}
