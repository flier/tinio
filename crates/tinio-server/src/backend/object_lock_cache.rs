//! The process-local Object Lock configuration cache (OL18).
//!
//! Scoped to the GET/HEAD echo (`S3Backend::has_lock_config`): the six lock
//! operations and the write path read the store through
//! `S3Backend::object_lock_config` instead, so a stale entry here can never
//! turn a configured bucket into a refusal.
//!
//! **Writers insert the fact, not a generation.** Every path that knows the
//! resulting fact writes it: a configuration put inserts `true`, a create
//! inserts its `lock_enabled`, a delete inserts `false`. The miss path fills
//! a missing entry with `entry().or_insert(read)`, so a writer's fact always
//! wins over a store read that was already in flight. That is what keeps
//! OL18's rule — **a stale `false` never hides a new config** — true without a
//! process-global counter: a reader that read `false` before the write cannot
//! overwrite the `true` the writer installed, and a reader that read `true`
//! before a delete cannot resurrect the deleted bucket's fact.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[cfg(feature = "object-lock")]
use crate::_core::bucket;

/// A cloneable handle to the process-local Object Lock configuration cache (OL18).
///
/// The `S3Backend` owns one, and the **management plane's Object Lock admin shares it**: a
/// configuration written through `PUT /buckets/{bucket}/object-lock` is not seen by the backend's
/// own `op_put_bucket_object_lock` path, so the admin's [`insert`](Self::insert) of the resulting
/// fact is what makes a subsequent S3 `GET`/`HEAD` echo the new lock in the same process. The CLI
/// creates one handle, hands it to the admin and passes it to `DataPlane` through
/// [`PlaneConfig`](crate::PlaneConfig) — one cache, two writers.
///
/// The map is read-mostly (every GET/HEAD) and written rarely (config writes and invalidation),
/// so the entries sit behind an [`RwLock`]: concurrent readers share the map, and a write takes
/// it exclusively. Entries are `bucket name` → `stores an Object Lock configuration`.
#[derive(Debug, Clone, Default)]
pub struct ObjectLockCache {
    /// Only the feature-gated readers and writers touch the map; with the
    /// `object-lock` feature off the handle is still carried by `PlaneConfig`.
    #[cfg_attr(
        not(feature = "object-lock"),
        expect(
            dead_code,
            reason = "argued: every accessor is object-lock-only, so a stripped build carries the \
                      empty handle without touching the map"
        )
    )]
    entries: Arc<RwLock<HashMap<String, bool>>>,
}

impl ObjectLockCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached answer for `bucket`, when there is one.
    #[cfg(feature = "object-lock")]
    pub(crate) fn cached(&self, bucket: &bucket::Name) -> Option<bool> {
        self.entries
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .get(bucket.as_ref().as_str())
            .copied()
    }

    /// Whether the cache holds no entry.
    #[cfg(all(test, feature = "object-lock"))]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .is_empty()
    }

    /// Whether the two handles are the same cache — the wiring test's assertion that a
    /// `PlaneConfig`'s handle reached the backend rather than being replaced by a fresh one.
    #[cfg(all(test, feature = "object-lock"))]
    pub(crate) fn shares_entries_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entries, &other.entries)
    }

    /// Record one bucket's fact, replacing whatever entry was there.
    ///
    /// Every writer knows the fact its write produced, so it writes the fact rather than
    /// evicting and waiting for a later read: a stale `false` never outlives an enablement,
    /// and a deleted bucket's entry is `false` rather than a gap a racing reader could refill.
    #[cfg(feature = "object-lock")]
    pub fn insert(&self, bucket: &bucket::Name, has: bool) {
        self.entries
            .write()
            .unwrap_or_else(|err| err.into_inner())
            .insert(bucket.as_ref().as_str().to_owned(), has);
    }

    /// Fill a missing entry with a store read and answer the entry's value.
    ///
    /// `or_insert` is the whole race protocol: an entry a writer installed between the caller's
    /// miss and this call is kept and answered, so a read that started before a write cannot undo
    /// it (the module doc owns the OL18 rule). With no writer in the window, the read is cached
    /// and answered.
    #[cfg(feature = "object-lock")]
    pub(crate) fn merge(&self, bucket: &bucket::Name, read: bool) -> bool {
        *self
            .entries
            .write()
            .unwrap_or_else(|err| err.into_inner())
            .entry(bucket.as_ref().as_str().to_owned())
            .or_insert(read)
    }
}
