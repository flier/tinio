//! The batch write buffer: acknowledged writes that wait for a flush (T7, T8).
//!
//! A plain write in [`FlushMode::Batch`](crate::FlushMode) is acknowledged
//! before the filesystem sees its bytes: the commit inserts the body and the
//! metadata it fixed into this buffer, and a later flush publishes them
//! through the warm plane. The buffer charges the same [`Budget`] as the
//! object cache (T9), never holds more than its `batch_bytes` cap, and its
//! flush is single-flight — one flush at a time drains the buffer, so two
//! listings can never publish the same entry twice.
//!
//! The byte work of a flush is the caller's write path
//! ([`FlushPlane`]); this module owns the entries, the budget, the cap and the
//! flight lock. A crash loses the unflushed acknowledged writes; that is the
//! mode's documented cost (T8). An entry whose flush fails every retry is
//! quarantined under the layer's state directory and leaves the buffer, so a
//! GET or HEAD of its key answers `NoSuchKey` afterwards until an operator
//! restores or discards the pair.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use tinio_util::metrics::record_tier_flush_duration;
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;
use tokio::{
    sync::{Mutex as AsyncMutex, MutexGuard as AsyncFlushGuard},
    time::sleep,
};

use crate::{
    _core::{bucket, object},
    Budget, Error,
};

/// What a buffer admission decided.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use tinio_core::{ETag, acl::Acl, bucket, object};
/// use tinio_tier::{Admission, Budget, Buffer, Buffered};
///
/// let budget = Arc::new(Budget::new(1024));
/// let buffer = Buffer::new(Arc::clone(&budget), 64);
/// let data = bucket::name("data").expect("a valid bucket name");
/// let key = object::key("a.txt").expect("a valid object key");
/// let entry = Buffered {
///     bytes: Bytes::from_static(b"hi"),
///     info: object::Info {
///         key: key.clone(),
///         size: 2,
///         last_modified: std::time::SystemTime::now(),
///         etag: ETag::from_content(b"hi"),
///         tags: object::Tags::empty(),
///         checksum: None,
///         owner: None,
///         acl: Acl::default_private(None),
///         version_id: None,
///         bucket_records_versions: false,
///     },
/// };
/// assert!(matches!(buffer.commit(&data, entry), Admission::Buffered));
/// ```
#[derive(Debug)]
pub enum Admission {
    /// The write waits in the buffer; the caller acknowledges it now.
    Buffered,
    /// The buffer has no room for the write next to what it already holds:
    /// flush it, then offer the same entry again.
    Full(Buffered),
    /// The write can never wait in the buffer — it is larger than the cap, or
    /// the shared budget cannot charge it — so the caller writes it through
    /// the synchronous path.
    Refused(Buffered),
}

/// One write waiting in the buffer: its bytes and the metadata the commit
/// already acknowledged.
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use tinio_core::{ETag, acl::Acl, object};
/// use tinio_tier::Buffered;
///
/// let key = object::key("a.txt").expect("a valid object key");
/// let entry = Buffered {
///     bytes: Bytes::from_static(b"hi"),
///     info: object::Info {
///         key,
///         size: 2,
///         last_modified: std::time::SystemTime::now(),
///         etag: ETag::from_content(b"hi"),
///         tags: object::Tags::empty(),
///         checksum: None,
///         owner: None,
///         acl: Acl::default_private(None),
///         version_id: None,
///         bucket_records_versions: false,
///     },
/// };
/// assert_eq!(entry.bytes.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct Buffered {
    /// The buffered body.
    pub bytes: Bytes,
    /// The full metadata the flush restores over the written object.
    pub info: object::Info,
}

/// The in-memory batch buffer of one tier layer.
///
/// The buffer partitions the layer's [`Budget`] with the object cache and
/// holds at most `cap_bytes`; every entry it holds is charged to the budget
/// until it is flushed or removed. A flush is single-flight: all flush paths
/// take one asynchronous lock for their whole duration, and each entry is
/// published under its key's write lock by the layer's write path.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use tinio_tier::{Budget, Buffer};
///
/// let budget = Arc::new(Budget::new(1024));
/// let buffer = Buffer::new(Arc::clone(&budget), 64);
/// assert!(buffer.is_empty());
/// assert_eq!(buffer.len_bytes(), 0);
/// ```
#[derive(Debug)]
pub struct Buffer {
    /// The pool the buffered bytes are charged to.
    budget: Arc<Budget>,
    /// The hard cap: the buffer never holds more than this many bytes.
    cap_bytes: u64,
    /// The single-flight lock every flush holds for its whole duration.
    flight: AsyncMutex<()>,
    /// The entries and their order stamp.
    inner: Mutex<Inner>,
}

/// The buffer's entries, their charged bytes and the admission sequence.
#[derive(Debug, Default)]
struct Inner {
    /// The buffered writes by `(bucket, key)`.
    entries: HashMap<(bucket::Name, object::Key), Entry>,
    /// The bytes every entry holds, which is the buffer's share of the budget.
    bytes: u64,
    /// The admission stamp: an entry's identity for a flush that snapshotted
    /// it (a replacement invalidates the snapshot).
    seq: u64,
}

impl Inner {
    /// Remove `key`'s entry, answering it and keeping [`Inner::bytes`] in
    /// step.
    fn take(&mut self, bucket: &bucket::Name, key: &object::Key) -> Option<Entry> {
        let entry = self.entries.remove(&(bucket.clone(), key.clone()))?;
        self.bytes -= entry.bytes.len() as u64;
        Some(entry)
    }
}

/// One buffered write and the admission stamp that identifies it.
#[derive(Debug)]
struct Entry {
    /// The admission stamp.
    seq: u64,
    /// The buffered body.
    bytes: Bytes,
    /// The metadata the flush restores.
    info: object::Info,
}

impl Entry {
    /// The entry as the caller sees it.
    fn buffered(&self) -> Buffered {
        Buffered {
            bytes: self.bytes.clone(),
            info: self.info.clone(),
        }
    }
}

/// Books one flush's duration on drop, so a drain that published an entry and
/// then failed part-way still records the sample the failed attempt took.
///
/// Only a drain that actually published at least one entry books a sample: an
/// empty drain is not a flush, and a drain whose every entry was quarantined
/// published nothing either.
#[must_use = "the sample is recorded on drop"]
struct FlushTimer {
    /// When the flush took the flight lock.
    started: Instant,
    /// Whether at least one entry was published by the drain.
    published: bool,
}

impl FlushTimer {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            published: false,
        }
    }

    /// Mark that the drain published an entry, so its sample is booked.
    fn mark_published(&mut self) {
        self.published = true;
    }
}

impl Drop for FlushTimer {
    fn drop(&mut self) {
        if !self.published {
            return;
        }
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_flush_duration(self.started.elapsed());
    }
}

/// How many times one entry's publish is attempted before it is quarantined:
/// the first attempt and three retries.
const FLUSH_ATTEMPTS: usize = 4;

/// The backoff before each retry, so a transient filesystem or row failure
/// gets three increasingly spaced chances.
const FLUSH_BACKOFF: [Duration; FLUSH_ATTEMPTS - 1] = [
    Duration::from_millis(50),
    Duration::from_millis(200),
    Duration::from_millis(800),
];

/// The byte work a flush drives, one buffered entry at a time.
///
/// The implementation writes the entry through the warm plane and then calls
/// [`Buffer::finish`]; an error leaves the entry buffered for the next attempt.
/// Once every attempt fails, the drain quarantines the entry through
/// [`FlushPlane::quarantine_entry`] rather than wedging every later flush.
#[async_trait]
pub(crate) trait FlushPlane: Send + Sync {
    /// Publish the entry admitted under `seq`; the caller holds the flight
    /// lock and this method takes the key's write lock itself.
    async fn flush_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error>;

    /// Persist the entry admitted under `seq` outside the buffer, so its bytes
    /// and metadata survive for an operator; the caller holds the flight lock.
    ///
    /// A failure here is the plane's bookkeeping failure (a full disk, an
    /// unreadable state directory): the drain drops the entry either way and
    /// never aborts.
    async fn quarantine_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error>;
}

impl Buffer {
    /// A buffer charging `budget` and holding at most `cap_bytes` bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_tier::{Budget, Buffer};
    ///
    /// let buffer = Buffer::new(Arc::new(Budget::new(1024)), 64);
    /// assert_eq!(buffer.len_bytes(), 0);
    /// ```
    pub fn new(budget: Arc<Budget>, cap_bytes: u64) -> Self {
        Self {
            budget,
            cap_bytes,
            flight: AsyncMutex::new(()),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Offer `entry` for `bucket` to the buffer.
    ///
    /// An entry larger than the cap, or one the shared budget cannot charge,
    /// is refused outright. An entry the buffer lacks room for is answered
    /// back uncharged so the caller can flush and retry. A replacement of the
    /// same key supersedes the waiting entry: the old bytes return to the
    /// budget, as last-write-wins requires.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{ETag, acl::Acl, bucket, object};
    /// use tinio_tier::{Admission, Budget, Buffer, Buffered};
    ///
    /// let budget = Arc::new(Budget::new(1024));
    /// let buffer = Buffer::new(Arc::clone(&budget), 64);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// let entry = Buffered {
    ///     bytes: Bytes::from_static(b"hi"),
    ///     info: object::Info {
    ///         key: key.clone(),
    ///         size: 2,
    ///         last_modified: std::time::SystemTime::now(),
    ///         etag: ETag::from_content(b"hi"),
    ///         tags: object::Tags::empty(),
    ///         checksum: None,
    ///         owner: None,
    ///         acl: Acl::default_private(None),
    ///         version_id: None,
    ///         bucket_records_versions: false,
    ///     },
    /// };
    /// assert!(matches!(buffer.commit(&data, entry), Admission::Buffered));
    /// assert_eq!(buffer.len_bytes(), 2);
    /// assert_eq!(budget.used(), 2);
    /// ```
    pub fn commit(&self, bucket: &bucket::Name, entry: Buffered) -> Admission {
        let len = entry.bytes.len() as u64;
        if len > self.cap_bytes {
            return Admission::Refused(entry);
        }
        let pair = (bucket.clone(), entry.info.key.clone());
        let mut inner = self.lock();
        let existing = inner
            .entries
            .get(&pair)
            .map_or(0, |old| old.bytes.len() as u64);
        if inner.bytes - existing + len > self.cap_bytes {
            return Admission::Full(entry);
        }
        let old = inner.entries.remove(&pair);
        let old_len = old.as_ref().map_or(0, |old| old.bytes.len() as u64);
        inner.bytes -= old_len;
        self.budget.give_back(old_len);
        if !self.budget.try_take(len) {
            // The shared budget refused the charge (the cache holds its
            // share). Restore the entry this write would have replaced.
            if let Some(old) = old
                && self.budget.try_take(old.bytes.len() as u64)
            {
                inner.bytes += old.bytes.len() as u64;
                inner.entries.insert(pair, old);
            }
            return Admission::Refused(entry);
        }
        inner.seq = inner.seq.wrapping_add(1);
        let seq = inner.seq;
        inner.bytes += len;
        inner.entries.insert(
            pair,
            Entry {
                seq,
                bytes: entry.bytes,
                info: entry.info,
            },
        );
        Admission::Buffered
    }

    /// Drop `key`'s buffered write, answering it and returning its bytes to
    /// the budget.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use tinio_core::{ETag, acl::Acl, bucket, object};
    /// use tinio_tier::{Budget, Buffer, Buffered};
    ///
    /// let budget = Arc::new(Budget::new(1024));
    /// let buffer = Buffer::new(Arc::clone(&budget), 64);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// let entry = Buffered {
    ///     bytes: Bytes::from_static(b"hi"),
    ///     info: object::Info {
    ///         key: key.clone(),
    ///         size: 2,
    ///         last_modified: std::time::SystemTime::now(),
    ///         etag: ETag::from_content(b"hi"),
    ///         tags: object::Tags::empty(),
    ///         checksum: None,
    ///         owner: None,
    ///         acl: Acl::default_private(None),
    ///         version_id: None,
    ///         bucket_records_versions: false,
    ///     },
    /// };
    /// buffer.commit(&data, entry);
    /// assert!(buffer.remove(&data, &key).is_some());
    /// assert!(buffer.is_empty());
    /// assert_eq!(budget.used(), 0);
    /// ```
    pub fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Option<Buffered> {
        let entry = self.lock().take(bucket, key)?;
        let len = entry.bytes.len() as u64;
        self.budget.give_back(len);
        Some(entry.buffered())
    }

    /// The bytes the buffer currently holds, which is its share of the
    /// [`Budget`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_tier::{Budget, Buffer};
    ///
    /// let buffer = Buffer::new(Arc::new(Budget::new(1024)), 64);
    /// assert_eq!(buffer.len_bytes(), 0);
    /// ```
    pub fn len_bytes(&self) -> u64 {
        self.lock().bytes
    }

    /// Whether the buffer holds no write.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_tier::{Budget, Buffer};
    ///
    /// assert!(Buffer::new(Arc::new(Budget::new(1024)), 64).is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    /// Whether `key` waits in the buffer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Budget, Buffer};
    ///
    /// let buffer = Buffer::new(Arc::new(Budget::new(1024)), 64);
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// assert!(!buffer.contains(&data, &key));
    /// ```
    pub fn contains(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        self.lock()
            .entries
            .contains_key(&(bucket.clone(), key.clone()))
    }

    /// Flush every buffered write, one entry at a time, under the flight
    /// lock.
    ///
    /// The `tier.flush` span is raised only when the drain selects at least
    /// one entry, so an empty call is not an info-level flush; the duration
    /// sample follows the same rule and is booked only when an entry published.
    pub(crate) async fn flush(&self, plane: &dyn FlushPlane) -> Result<(), Error> {
        self.flush_matching(plane, |_, _| true).await
    }

    /// Flush every write buffered for `bucket` (T8's bucket-wide gate).
    pub(crate) async fn flush_bucket(
        &self,
        bucket: &bucket::Name,
        plane: &dyn FlushPlane,
    ) -> Result<(), Error> {
        self.flush_matching(plane, |candidate, _| candidate == bucket)
            .await
    }

    /// Flush `key`'s buffered write, if it has one.
    pub(crate) async fn flush_key(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        plane: &dyn FlushPlane,
    ) -> Result<(), Error> {
        if !self.contains(bucket, key) {
            return Ok(());
        }
        self.flush_matching(plane, |candidate, waiting| {
            candidate == bucket && waiting == key
        })
        .await
    }

    /// The entry admitted under `seq`, if it still waits unchanged.
    pub(crate) fn peek(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Option<Buffered> {
        let inner = self.lock();
        let entry = inner.entries.get(&(bucket.clone(), key.clone()))?;
        (entry.seq == seq).then(|| entry.buffered())
    }

    /// Drop the entry admitted under `seq`, returning its bytes to the budget
    /// (the flush published it).
    pub(crate) fn finish(&self, bucket: &bucket::Name, key: &object::Key, seq: u64) {
        let mut inner = self.lock();
        let pair = (bucket.clone(), key.clone());
        if !inner
            .entries
            .get(&pair)
            .is_some_and(|entry| entry.seq == seq)
        {
            return;
        }
        let Some(entry) = inner.entries.remove(&pair) else {
            return;
        };
        inner.bytes -= entry.bytes.len() as u64;
        let len = entry.bytes.len() as u64;
        drop(inner);
        self.budget.give_back(len);
    }

    /// The single-flight drain: snapshot the selected entries under the
    /// flight lock and hand each to `plane`, which takes its key's write lock
    /// and re-checks the stamp.
    async fn flush_matching<F>(&self, plane: &dyn FlushPlane, select: F) -> Result<(), Error>
    where
        F: Fn(&bucket::Name, &object::Key) -> bool,
    {
        let _flight: AsyncFlushGuard<'_, ()> = self.flight.lock().await;
        let mut candidates = {
            let inner = self.lock();
            inner
                .entries
                .iter()
                .filter(|((bucket, key), _)| select(bucket, key))
                .map(|((bucket, key), entry)| (bucket.clone(), key.clone(), entry.seq))
                .collect::<Vec<_>>()
        };
        candidates.sort_by(|left, right| {
            (left.0.as_ref(), left.1.as_ref()).cmp(&(right.0.as_ref(), right.1.as_ref()))
        });
        // A drain that selected nothing is not a flush: it raises neither the
        // span nor the duration sample.
        if candidates.is_empty() {
            return Ok(());
        }
        // The span and the timer both start once an entry is selected; the
        // timer books only when an entry actually published.
        let mut timer = FlushTimer::new();
        self.drain_all(plane, candidates, &mut timer).await;
        Ok(())
    }

    /// Drain the selected entries, marking the timer for each one published.
    ///
    /// The `tier.flush` span is raised here, so the caller's empty selection
    /// opens none.
    #[tracing::instrument(target = "tinio::tier", name = "tier.flush", level = "info", skip_all)]
    async fn drain_all(
        &self,
        plane: &dyn FlushPlane,
        candidates: Vec<(bucket::Name, object::Key, u64)>,
        timer: &mut FlushTimer,
    ) {
        for (bucket, key, seq) in candidates {
            if self.drain_entry(plane, &bucket, &key, seq).await {
                timer.mark_published();
            }
        }
    }

    /// Publish one entry, retrying a failure and quarantining it once every
    /// attempt has failed.
    ///
    /// Answers whether the entry was published; an entry that was quarantined
    /// (or whose quarantine could not be written) answers `false`.
    ///
    /// The entry is only removed after it is published or its quarantine is
    /// recorded, so a cancelled drain (the future dropped at any await) leaves
    /// it buffered for the next flush. A quarantine that cannot be written is
    /// logged and the entry dropped, never returned: one poison entry must not
    /// wedge the layer's listings and shutdown.
    async fn drain_entry(
        &self,
        plane: &dyn FlushPlane,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> bool {
        for attempt in 1..=FLUSH_ATTEMPTS {
            match plane.flush_entry(bucket, key, seq).await {
                Ok(()) => return true,
                Err(err) if attempt < FLUSH_ATTEMPTS => {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        attempt,
                        error = %err,
                        "a buffered write failed; retrying"
                    );
                    sleep(FLUSH_BACKOFF[attempt - 1]).await;
                }
                Err(err) => {
                    tracing::error!(
                        bucket = %bucket,
                        key = %key,
                        attempts = attempt,
                        error = %err,
                        "a buffered write failed every attempt; quarantining it"
                    );
                    if let Err(record) = plane.quarantine_entry(bucket, key, seq).await {
                        tracing::error!(
                            bucket = %bucket,
                            key = %key,
                            error = %record,
                            "the quarantine record could not be written; dropping the buffered write"
                        );
                    }
                    self.finish(bucket, key, seq);
                    return false;
                }
            }
        }
        false
    }

    /// The shared check the buffer's own methods take.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
