//! The in-memory object cache and the byte budget it shares with the write
//! buffer.
//!
//! The cache holds whole objects and evicts least-recently-used entries under
//! a byte budget; the same budget is charged by the write buffer, so the two
//! planes partition one pool rather than each owning a private cap (T9).

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tinio_util::metrics::record_tier_cache_eviction;
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;

use crate::_core::{bucket, object};

/// The approximate budget charge for one cached [`object::Info`].
///
/// An entry stores its metadata behind an [`Arc`], so a hit hands out a
/// pointer rather than a deep copy; the budget still accounts for the metadata
/// it keeps alive. The exact heap size depends on the key, the tags and the
/// ACL, so this fixed estimate is a conservative allowance for the struct and
/// its short strings rather than a measured size.
const INFO_FOOTPRINT: u64 = 512;

/// The byte budget the object cache and the write buffer partition.
///
/// Both planes charge the same counter, so a byte admitted to one is
/// unavailable to the other (T9). A charge larger than the remaining capacity
/// is refused rather than queued, which keeps the admission decision
/// synchronous for the reader and the writer.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::Budget;
///
/// let budget = Budget::new(1024);
/// assert_eq!(budget.capacity(), 1024);
/// assert_eq!(budget.used(), 0);
/// assert!(budget.try_take(768));
/// assert_eq!(budget.available(), 256);
/// assert!(!budget.try_take(512));
/// budget.give_back(768);
/// assert_eq!(budget.available(), 1024);
/// ```
#[derive(Debug)]
pub struct Budget {
    capacity: u64,
    used: AtomicU64,
}

impl Budget {
    /// A budget of `capacity` bytes with nothing charged yet.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// let budget = Budget::new(4096);
    /// assert_eq!(budget.capacity(), 4096);
    /// assert_eq!(budget.used(), 0);
    /// ```
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            used: AtomicU64::new(0),
        }
    }

    /// The total number of bytes either plane may hold.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// assert_eq!(Budget::new(512).capacity(), 512);
    /// ```
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// The bytes currently charged by the cache and the write buffer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// let budget = Budget::new(512);
    /// budget.try_take(128);
    /// assert_eq!(budget.used(), 128);
    /// ```
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// The bytes either plane may still charge.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// let budget = Budget::new(512);
    /// budget.try_take(512);
    /// assert_eq!(budget.available(), 0);
    /// ```
    pub fn available(&self) -> u64 {
        self.capacity.saturating_sub(self.used())
    }

    /// Charge `bytes` to the budget, answering whether the charge was
    /// admitted.
    ///
    /// A charge that would cross the capacity is refused and leaves the used
    /// count unchanged; the caller decides whether to evict and retry.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// let budget = Budget::new(100);
    /// assert!(budget.try_take(60));
    /// assert!(!budget.try_take(60));
    /// assert_eq!(budget.used(), 60);
    /// ```
    pub fn try_take(&self, bytes: u64) -> bool {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                return false;
            };
            if next > self.capacity {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(current) => used = current,
            }
        }
    }

    /// Return `bytes` to the budget.
    ///
    /// Returning more than was charged saturates at zero rather than
    /// panicking, so a double release cannot corrupt the counter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Budget;
    ///
    /// let budget = Budget::new(100);
    /// budget.try_take(80);
    /// budget.give_back(80);
    /// assert_eq!(budget.available(), 100);
    /// budget.give_back(1);
    /// assert_eq!(budget.used(), 0);
    /// ```
    pub fn give_back(&self, bytes: u64) {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.saturating_sub(bytes);
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(current) => used = current,
            }
        }
    }
}

/// The in-memory cache of whole objects under a shared byte budget.
///
/// Entries are evicted least-recently-used first. An object larger than
/// `max_object_bytes`, or one that cannot be admitted even after every entry is
/// evicted, is refused (T9). Every byte the cache holds — the object data plus
/// a fixed metadata estimate per entry — is charged to the [`Budget`] the write
/// buffer also uses, so the cache never hides a private counter from the
/// buffer.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use tinio_core::{bucket, object};
/// use tinio_tier::{Budget, Cache};
///
/// let budget = Arc::new(Budget::new(1024));
/// let cache = Cache::new(Arc::clone(&budget), 512);
/// let data = bucket::name("data").expect("a valid bucket name");
/// let key = object::key("a.txt").expect("a valid object key");
/// assert!(cache.insert(&data, &key, Bytes::from_static(b"hello")));
/// assert_eq!(cache.len_bytes(), 5);
/// assert_eq!(budget.used(), 5);
/// ```
#[derive(Debug)]
pub struct Cache {
    budget: Arc<Budget>,
    max_object_bytes: u64,
    inner: Mutex<Inner>,
}

/// The cache's entries and their least-recently-used order.
#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<(bucket::Name, object::Key), Cached>,
    order: BTreeMap<u64, (bucket::Name, object::Key)>,
    tick: u64,
    bytes: u64,
}

/// One cached object and its position in the LRU order.
#[derive(Debug)]
struct Cached {
    /// The metadata the bytes were read with, when the filler had it. A
    /// bytes-only [`Cache::insert`] leaves it `None`. Held behind an [`Arc`] so
    /// a hit hands out the metadata without a deep copy.
    info: Option<Arc<object::Info>>,
    data: Bytes,
    tick: u64,
}

impl Cached {
    /// The budget this entry charges: its data bytes plus the metadata
    /// estimate it keeps alive.
    fn charge(&self) -> u64 {
        self.data.len() as u64 + self.info.as_ref().map_or(0, |_| INFO_FOOTPRINT)
    }
}

/// One cached object as a read answers it: the bytes and the metadata they
/// were filled from, when the filler had it.
#[derive(Debug)]
pub(crate) struct CachedObject {
    /// The metadata the tier layer read with the bytes, or `None` for an
    /// entry admitted through the bytes-only [`Cache::insert`] seam. The
    /// entry's own [`Arc`] is handed out, not a copy of the metadata.
    pub(crate) info: Option<Arc<object::Info>>,
    /// The cached bytes.
    pub(crate) data: Bytes,
}

impl Inner {
    fn new() -> Self {
        Self::default()
    }

    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    fn touch(&mut self, key: &(bucket::Name, object::Key)) {
        let Some(old) = self.entries.get(key).map(|cached| cached.tick) else {
            return;
        };
        self.order.remove(&old);
        let tick = self.next_tick();
        if let Some(cached) = self.entries.get_mut(key) {
            cached.tick = tick;
        }
        self.order.insert(tick, key.clone());
    }

    fn detach(&mut self, key: &(bucket::Name, object::Key)) -> Option<Cached> {
        let cached = self.entries.remove(key)?;
        self.order.remove(&cached.tick);
        self.bytes -= cached.charge();
        Some(cached)
    }

    fn pop_lru(&mut self) -> Option<u64> {
        loop {
            let (_, key) = self.order.pop_first()?;
            if let Some(cached) = self.entries.remove(&key) {
                self.bytes -= cached.charge();
                return Some(cached.charge());
            }
        }
    }
}

impl Cache {
    /// A cache that charges `budget` and admits objects up to
    /// `max_object_bytes`.
    ///
    /// The budget is shared with the write buffer; both charge the same
    /// counter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let budget = Arc::new(Budget::new(1024));
    /// let cache = Cache::new(Arc::clone(&budget), 256);
    /// assert_eq!(cache.len_bytes(), 0);
    /// ```
    pub fn new(budget: Arc<Budget>, max_object_bytes: u64) -> Self {
        Self {
            budget,
            max_object_bytes,
            inner: Mutex::new(Inner::new()),
        }
    }

    /// The cached bytes of `key`, if present, refreshing the entry's position.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let cache = Cache::new(Arc::new(Budget::new(1024)), 256);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// cache.insert(&data, &key, Bytes::from_static(b"hello"));
    /// assert_eq!(
    ///     cache.get(&data, &key).expect("the object is cached"),
    ///     Bytes::from_static(b"hello")
    /// );
    /// ```
    pub fn get(&self, bucket: &bucket::Name, key: &object::Key) -> Option<Bytes> {
        let pair = (bucket.clone(), key.clone());
        let mut inner = self.lock();
        let data = inner.entries.get(&pair)?.data.clone();
        inner.touch(&pair);
        Some(data)
    }

    /// Admit `data` for `key`, answering whether it was cached.
    ///
    /// The object is refused when it exceeds `max_object_bytes` or when it
    /// cannot be charged even after evicting every entry. Least-recently-used
    /// entries are evicted until the budget admits the object.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let cache = Cache::new(Arc::new(Budget::new(1024)), 8);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// assert!(cache.insert(&data, &key, Bytes::from_static(b"small")));
    /// assert!(!cache.insert(&data, &key, Bytes::from_static(b"too-large")));
    /// ```
    pub fn insert(&self, bucket: &bucket::Name, key: &object::Key, data: Bytes) -> bool {
        self.admit(bucket, key, None, data)
    }

    /// Admit `info`'s bytes as the entry for `key`, so a later hit can answer
    /// the metadata the bytes were read with without another metadata call.
    ///
    /// The info is stored behind an [`Arc`], so a hit hands out a pointer
    /// rather than a deep copy, and its approximate footprint is charged to
    /// the budget alongside the bytes. The admission rules are
    /// [`Cache::insert`]'s: the data is refused when it exceeds
    /// `max_object_bytes` or cannot be charged even after evicting every
    /// entry.
    pub(crate) fn insert_with_info(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: object::Info,
        data: Bytes,
    ) -> bool {
        self.admit(bucket, key, Some(info), data)
    }

    fn admit(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: Option<object::Info>,
        data: Bytes,
    ) -> bool {
        let len = data.len() as u64;
        if len > self.max_object_bytes {
            return false;
        }
        let charge = len + info.as_ref().map_or(0, |_| INFO_FOOTPRINT);
        let pair = (bucket.clone(), key.clone());
        let mut inner = self.lock();
        if let Some(old) = inner.detach(&pair) {
            self.budget.give_back(old.charge());
        }
        while !self.budget.try_take(charge) {
            let Some(evicted) = inner.pop_lru() else {
                return false;
            };
            // One eviction per entry the LRU pressure pushed out. Recording is
            // fire-and-forget: it cannot refuse an admission the budget allowed.
            #[cfg(test)]
            let _gate = test_lock::writer();
            record_tier_cache_eviction(1);
            self.budget.give_back(evicted);
        }
        let tick = inner.next_tick();
        inner.bytes += charge;
        inner.entries.insert(
            pair.clone(),
            Cached {
                info: info.map(Arc::new),
                data,
                tick,
            },
        );
        inner.order.insert(tick, pair);
        true
    }

    /// The cached entry for `key`, refreshing its position, with the metadata
    /// the bytes were filled from when the filler supplied it. The metadata is
    /// handed out as the entry's own [`Arc`], not a copy.
    pub(crate) fn entry(&self, bucket: &bucket::Name, key: &object::Key) -> Option<CachedObject> {
        let pair = (bucket.clone(), key.clone());
        let mut inner = self.lock();
        let entry = {
            let cached = inner.entries.get(&pair)?;
            CachedObject {
                info: cached.info.clone(),
                data: cached.data.clone(),
            }
        };
        inner.touch(&pair);
        Some(entry)
    }

    /// Drop `key` from the cache, answering whether it was present.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let cache = Cache::new(Arc::new(Budget::new(1024)), 256);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// cache.insert(&data, &key, Bytes::from_static(b"hello"));
    /// assert!(cache.remove(&data, &key));
    /// assert!(!cache.remove(&data, &key));
    /// ```
    pub fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        self.take(bucket, key).is_some()
    }

    /// Drop every entry `bucket` holds, answering how many were removed.
    ///
    /// A bucket-wide change — a versioning transition flips the version facts
    /// every cached object's metadata carries — makes the entries' recorded
    /// metadata stale, so the whole bucket's bytes are dropped rather than
    /// served against it.
    pub(crate) fn remove_bucket(&self, bucket: &bucket::Name) -> usize {
        let mut inner = self.lock();
        let pairs: Vec<(bucket::Name, object::Key)> = inner
            .entries
            .keys()
            .filter(|(name, _)| name == bucket)
            .cloned()
            .collect();
        let removed = pairs.len();
        let mut freed = 0;
        for pair in pairs {
            if let Some(cached) = inner.detach(&pair) {
                freed += cached.charge();
            }
        }
        self.budget.give_back(freed);
        removed
    }

    /// Remove `key` from the cache and answer its bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let cache = Cache::new(Arc::new(Budget::new(1024)), 256);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// cache.insert(&data, &key, Bytes::from_static(b"hello"));
    /// let taken = cache.take(&data, &key).expect("the object is cached");
    /// assert_eq!(taken.len(), 5);
    /// ```
    pub fn take(&self, bucket: &bucket::Name, key: &object::Key) -> Option<Bytes> {
        let pair = (bucket.clone(), key.clone());
        let cached = self.lock().detach(&pair)?;
        self.budget.give_back(cached.charge());
        Some(cached.data)
    }

    /// The bytes the cache currently charges to the [`Budget`]: the data it
    /// holds plus the per-entry metadata estimate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Cache};
    ///
    /// let cache = Cache::new(Arc::new(Budget::new(1024)), 256);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// cache.insert(&data, &key, Bytes::from_static(b"hello"));
    /// assert_eq!(cache.len_bytes(), 5);
    /// ```
    pub fn len_bytes(&self) -> u64 {
        self.lock().bytes
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;

    use super::*;
    use crate::{
        _core::{bucket, object},
        heat::Heat,
    };

    fn bucket() -> bucket::Name {
        bucket::name("data").expect("a valid bucket name")
    }

    fn key(name: &str) -> object::Key {
        object::key(name).expect("a valid object key")
    }

    fn payload(len: usize) -> Bytes {
        Bytes::from(vec![b'x'; len])
    }

    #[test]
    fn cache_evicts_lru_under_the_budget() {
        let budget = Arc::new(Budget::new(10));
        let cache = Cache::new(Arc::clone(&budget), 10);
        let data = bucket();

        assert!(cache.insert(&data, &key("a"), payload(4)));
        assert!(cache.insert(&data, &key("b"), payload(4)));
        assert_eq!(cache.len_bytes(), 8);
        assert_eq!(budget.used(), 8);

        assert!(
            cache.get(&data, &key("a")).is_some(),
            "the hit refreshes `a`"
        );
        assert!(
            cache.insert(&data, &key("c"), payload(4)),
            "the budget is reclaimed by evicting the least recent entry"
        );

        assert!(cache.get(&data, &key("b")).is_none(), "`b` was evicted");
        assert!(cache.get(&data, &key("a")).is_some(), "`a` survived");
        assert!(cache.get(&data, &key("c")).is_some(), "`c` was admitted");
        assert_eq!(cache.len_bytes(), 8);
        assert_eq!(budget.used(), 8);

        let taken = cache.take(&data, &key("a")).expect("`a` is cached");
        assert_eq!(taken.len(), 4);
        assert_eq!(budget.used(), 4, "a take returns its bytes");
        assert!(cache.remove(&data, &key("c")));
        assert_eq!(cache.len_bytes(), 0);
        assert_eq!(budget.used(), 0, "a remove returns its bytes");
        assert!(
            !cache.remove(&data, &key("c")),
            "a second remove is a no-op"
        );
    }

    #[test]
    fn an_over_cap_object_is_not_inserted() {
        let budget = Arc::new(Budget::new(64));
        let cache = Cache::new(Arc::clone(&budget), 8);
        let data = bucket();

        assert!(
            !cache.insert(&data, &key("over"), payload(9)),
            "an object above `max_object_bytes` is refused"
        );
        assert!(cache.get(&data, &key("over")).is_none());
        assert_eq!(cache.len_bytes(), 0);
        assert_eq!(budget.used(), 0);

        assert!(
            cache.insert(&data, &key("at-cap"), payload(8)),
            "an object exactly at the cap is admitted"
        );
        assert_eq!(cache.len_bytes(), 8);

        let tight = Arc::new(Budget::new(8));
        let cache = Cache::new(Arc::clone(&tight), 64);
        assert!(
            !cache.insert(&data, &key("over-budget"), payload(9)),
            "an object larger than the whole budget is refused"
        );
        assert_eq!(tight.used(), 0);
    }

    #[test]
    fn a_mem_hit_records_heat() {
        let budget = Arc::new(Budget::new(64));
        let cache = Cache::new(Arc::clone(&budget), 64);
        let heat = Heat::new(Duration::from_secs(60), 3);
        let data = bucket();
        let hot = key("hot");

        assert!(cache.insert(&data, &hot, payload(8)));
        for _ in 0..2 {
            assert!(cache.get(&data, &hot).is_some(), "the object is cached");
            heat.record_read(&data, &hot);
        }

        assert_eq!(heat.score(&data, &hot), 2, "mem hits are reads");
        assert!(
            heat.promote_queue().is_empty(),
            "two reads are below the threshold of three"
        );

        assert!(cache.get(&data, &key("cold")).is_none());
        assert_eq!(heat.score(&data, &key("cold")), 0, "a miss records nothing");
    }

    #[test]
    fn remove_bucket_drops_only_that_buckets_entries() {
        let budget = Arc::new(Budget::new(64));
        let cache = Cache::new(Arc::clone(&budget), 64);
        let data = bucket();
        let other = bucket::name("other").expect("a valid bucket name");

        assert!(cache.insert(&data, &key("a"), payload(4)));
        assert!(cache.insert(&other, &key("b"), payload(4)));
        assert_eq!(cache.remove_bucket(&data), 1);
        assert!(cache.get(&data, &key("a")).is_none());
        assert!(cache.get(&other, &key("b")).is_some());
        assert_eq!(budget.used(), 4, "the dropped bytes leave the budget");
    }

    /// One LRU eviction is one sample of the tier cache-eviction family.
    #[test]
    fn an_eviction_records_one_cache_eviction() {
        use tinio_util::testing::metrics::{counter, test_lock};

        let _window = test_lock::window();
        let budget = Arc::new(Budget::new(8));
        let cache = Cache::new(Arc::clone(&budget), 8);
        let data = bucket();

        assert!(cache.insert(&data, &key("a"), payload(4)));
        assert!(cache.insert(&data, &key("b"), payload(4)));
        let before = counter("tinio_tier_cache_evictions_total", &[]);
        assert!(
            cache.insert(&data, &key("c"), payload(4)),
            "the third object is admitted by evicting the least recent entry"
        );
        let after = counter("tinio_tier_cache_evictions_total", &[]);
        assert_eq!(
            after - before,
            1.0,
            "the one eviction records exactly one sample"
        );
    }

    #[test]
    fn cache_and_buffer_share_one_budget() {
        let budget = Arc::new(Budget::new(16));
        let cache = Cache::new(Arc::clone(&budget), 16);
        let data = bucket();
        let cached = key("cached");

        assert!(cache.insert(&data, &cached, payload(10)));
        assert_eq!(budget.used(), 10);
        assert_eq!(budget.available(), 6);

        assert!(
            budget.try_take(6),
            "the write buffer takes from the same pool"
        );
        assert_eq!(budget.used(), 16);
        assert_eq!(budget.available(), 0);

        assert!(
            !cache.insert(&data, &key("rejected"), payload(11)),
            "evicting the cache cannot make room while the buffer holds its share"
        );
        assert!(
            cache.get(&data, &cached).is_none(),
            "the eviction is real: the cache gave its bytes back first"
        );
        assert_eq!(cache.len_bytes(), 0);
        assert_eq!(budget.used(), 6, "the buffer's reservation survives");

        budget.give_back(6);
        assert_eq!(budget.available(), 16);
        assert!(cache.insert(&data, &key("fresh"), payload(16)));
        assert_eq!(budget.used(), 16);
    }
}
