//! The per-key and per-bucket locks the tier decorator holds (T14).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::_core::{bucket, object};

/// The per-key read/write lock table (T14).
///
/// Reads take the shared side and hold it through the body stream; a move or
/// a delete takes the exclusive side. A slot is evicted when its last guard
/// drops, so the table stays bounded by the keys locked at one moment.
#[derive(Clone, Default)]
pub(crate) struct KeyLocks {
    table: Arc<Mutex<HashMap<LockKey, Arc<RwLock<()>>>>>,
}

/// One key's lock identity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct LockKey {
    bucket: String,
    key: String,
}

impl LockKey {
    fn of(bucket: &bucket::Name, key: &object::Key) -> Self {
        Self {
            bucket: bucket.as_ref().to_owned(),
            key: key.as_ref().to_owned(),
        }
    }
}

/// The lock table's one insertion policy: a live slot is reused, a missing one
/// is inserted.
pub(super) fn slot(
    table: &Mutex<HashMap<LockKey, Arc<RwLock<()>>>>,
    key: &LockKey,
) -> Arc<RwLock<()>> {
    let mut table = table.lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(
        table
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(()))),
    )
}

impl KeyLocks {
    pub(super) async fn read(&self, bucket: &bucket::Name, key: &object::Key) -> ReadGuard {
        let key = LockKey::of(bucket, key);
        let slot = slot(&self.table, &key);
        let guard = slot.read_owned().await;
        ReadGuard {
            key,
            table: Arc::clone(&self.table),
            guard: Some(guard),
        }
    }

    pub(crate) async fn write(&self, bucket: &bucket::Name, key: &object::Key) -> WriteGuard {
        let key = LockKey::of(bucket, key);
        let slot = slot(&self.table, &key);
        let guard = slot.write_owned().await;
        WriteGuard {
            key,
            table: Arc::clone(&self.table),
            guard: Some(guard),
        }
    }

    /// Take the write side only when it is free; a mover skips a busy key
    /// rather than waiting for a reader or another move (T14).
    pub(crate) fn try_write(&self, bucket: &bucket::Name, key: &object::Key) -> Option<WriteGuard> {
        let key = LockKey::of(bucket, key);
        let slot = slot(&self.table, &key);
        let guard = slot.try_write_owned().ok()?;
        Some(WriteGuard {
            key,
            table: Arc::clone(&self.table),
            guard: Some(guard),
        })
    }
}

/// A held read lock; dropping it evicts the slot when it is the last handle.
pub(super) struct ReadGuard {
    key: LockKey,
    table: Arc<Mutex<HashMap<LockKey, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockReadGuard<()>>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        let slot = OwnedRwLockReadGuard::rwlock(&guard);
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if table
            .get(&self.key)
            .is_some_and(|live| Arc::ptr_eq(live, slot))
            && Arc::strong_count(slot) == 2
        {
            table.remove(&self.key);
        }
    }
}

/// A held write lock; dropping it evicts the slot when it is the last handle.
pub(crate) struct WriteGuard {
    key: LockKey,
    table: Arc<Mutex<HashMap<LockKey, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        let slot = OwnedRwLockWriteGuard::rwlock(&guard);
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if table
            .get(&self.key)
            .is_some_and(|live| Arc::ptr_eq(live, slot))
            && Arc::strong_count(slot) == 2
        {
            table.remove(&self.key);
        }
    }
}

/// The per-bucket listing gate (T14).
///
/// A client listing holds the read side for its whole duration, and so does a
/// move's finalize — the per-key write lock is what excludes a competing move
/// — so a finalize cannot swap a plane under a page that is being assembled.
/// Like [`KeyLocks`], a slot is evicted when its last guard drops, so the table
/// stays bounded by the buckets gated at one moment.
#[derive(Clone, Default)]
pub(crate) struct BucketGate {
    table: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
}

impl BucketGate {
    /// Take the read side for `bucket`, held until the guard drops.
    pub(crate) async fn read(&self, bucket: &bucket::Name) -> BucketReadGuard {
        let name = bucket.as_ref().to_owned();
        let slot = {
            let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(
                table
                    .entry(name.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(()))),
            )
        };
        let guard = slot.read_owned().await;
        BucketReadGuard {
            bucket: name,
            table: Arc::clone(&self.table),
            guard: Some(guard),
        }
    }

    /// Take the write side for `bucket`, held until the guard drops: a
    /// bucket-wide transition no listing or move may interleave.
    pub(crate) async fn write(&self, bucket: &bucket::Name) -> BucketWriteGuard {
        let name = bucket.as_ref().to_owned();
        let slot = {
            let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(
                table
                    .entry(name.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(()))),
            )
        };
        let guard = slot.write_owned().await;
        BucketWriteGuard {
            bucket: name,
            table: Arc::clone(&self.table),
            guard: Some(guard),
        }
    }
}

/// A held bucket read gate; dropping it evicts the slot when it is the last
/// handle.
pub(crate) struct BucketReadGuard {
    bucket: String,
    table: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockReadGuard<()>>,
}

impl Drop for BucketReadGuard {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        let slot = OwnedRwLockReadGuard::rwlock(&guard);
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if table
            .get(&self.bucket)
            .is_some_and(|live| Arc::ptr_eq(live, slot))
            && Arc::strong_count(slot) == 2
        {
            table.remove(&self.bucket);
        }
    }
}

/// A held bucket write gate; dropping it evicts the slot when it is the last
/// handle.
pub(crate) struct BucketWriteGuard {
    bucket: String,
    table: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}

impl Drop for BucketWriteGuard {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        let slot = OwnedRwLockWriteGuard::rwlock(&guard);
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if table
            .get(&self.bucket)
            .is_some_and(|live| Arc::ptr_eq(live, slot))
            && Arc::strong_count(slot) == 2
        {
            table.remove(&self.bucket);
        }
    }
}
