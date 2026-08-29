//! An evicting per-key lock map.
//!
//! A [`Map`] hands out one `tokio` mutex per key — the lock actually
//! awaited across `.await` points — and evicts a slot when its last
//! handle drops, so the table stays bounded by the number of
//! concurrently locked keys.

use std::{collections::HashMap, hash::Hash, sync::Arc};

use tokio::sync::{self, OwnedMutexGuard};

/// The slot table: key → per-key mutex. `Arc` so the table outlives any
/// [`Guard`] that still references it for eviction.
type Table<K> = Arc<std::sync::Mutex<HashMap<K, Arc<sync::Mutex<()>>>>>;

/// The per-key lock table. Slots are evicted when the last [`Guard`] for
/// a key drops: the table holds one `Arc` to the slot and the guard the
/// other (`strong_count == 2`); a waiter holds a third and its own `Drop`
/// performs the eviction later.
///
/// The table is a `std::sync::Mutex`: the critical sections are the slot
/// clone and the removal only (never held across an await), and `Drop`
/// can therefore block on it — the removal is never skipped under
/// contention (a `try_lock` + early return would leak the slot forever,
/// since only `Drop` removes entries).
#[derive(Debug, Clone)]
pub struct Map<K> {
    map: Table<K>,
}

impl<K> Default for Map<K> {
    fn default() -> Self {
        Self {
            map: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl<K: Clone + Eq + Hash> Map<K> {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of held slots (live locks plus waiters).
    pub fn len(&self) -> usize {
        self.map
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Whether no slot is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lock `key`, awaiting the per-key mutex. The returned [`Guard`]
    /// evicts the slot on drop when it is the last handle.
    pub async fn lock(&self, key: K) -> Guard<K> {
        let map = Arc::clone(&self.map);
        let slot = {
            // The clone happens under the table lock, and the count check
            // + removal in `Drop` run under the same lock, so a slot is
            // never removed while a waiter still references it (a later
            // task would otherwise lock a fresh slot concurrently).
            let mut table = map.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            table
                .entry(key.clone())
                .or_insert_with(|| Arc::new(sync::Mutex::new(())))
                .clone()
        };
        let guard = Arc::clone(&slot).lock_owned().await;
        Guard {
            key,
            slot: Some(slot),
            map,
            guard: Some(guard),
        }
    }
}

/// The held per-key lock. Evicts the map slot on drop when this is the
/// last handle.
pub struct Guard<K: Clone + Eq + Hash> {
    key: K,
    slot: Option<Arc<sync::Mutex<()>>>,
    map: Table<K>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl<K: Clone + Eq + Hash> Drop for Guard<K> {
    fn drop(&mut self) {
        // Release the mutex first so a waiter can proceed, then evict the
        // slot only if we were the last handle.
        drop(self.guard.take());
        let Some(slot) = self.slot.take() else {
            return;
        };
        let mut table = match self.map.lock() {
            Ok(table) => table,
            // Poison recovery: a panicked predecessor must not leak the
            // slot forever.
            Err(poisoned) => poisoned.into_inner(),
        };
        if Arc::strong_count(&slot) == 2 {
            table.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn new_map_is_empty() {
        assert!(Map::<String>::new().is_empty());
    }

    #[tokio::test]
    async fn lock_serializes_same_key() {
        let map = Map::new();
        let _held = map.lock("k".to_string()).await;
        let waiter = tokio::spawn({
            let map = map.clone();
            async move { map.lock("k".to_string()).await }
        });
        // The second lock on the same key must not resolve while the
        // first is held.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), waiter)
                .await
                .is_err(),
            "the same-key lock must wait for the held guard"
        );
    }

    #[tokio::test]
    async fn distinct_keys_lock_independently() {
        let map = Map::new();
        let a = map.lock("a".to_string()).await;
        let b = tokio::spawn({
            let map = map.clone();
            async move { map.lock("b".to_string()).await }
        });
        // A different key must resolve immediately.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), b)
                .await
                .is_ok(),
            "distinct keys must not contend"
        );
        drop(a);
    }

    #[tokio::test]
    async fn slot_survives_waiter_and_evicts_at_last_drop() {
        let map = Map::new();
        let _held = map.lock("k".to_string()).await;
        let waiter = tokio::spawn({
            let map = map.clone();
            async move {
                let _guard = map.lock("k".to_string()).await;
            }
        });
        tokio::task::yield_now().await;
        drop(_held);
        // The waiter (whose slot clone pins the entry) acquires the key
        // next; whichever interleaving wins, the last guard's drop must
        // leave the table empty — the slot is never evicted out from
        // under a waiter, and never leaks after the final release.
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(map.is_empty(), "the last drop must evict the slot");
    }

    #[tokio::test]
    async fn slot_evicts_after_single_lock() {
        let map = Map::new();
        let _held = map.lock("k".to_string()).await;
        assert_eq!(map.len(), 1);
        drop(_held);
        assert!(map.is_empty());
    }

    #[test]
    fn poisoned_table_recovers_and_evicts() {
        let map = Map::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = map.map.lock().unwrap();
            panic!("poison the table");
        }));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _held = map.lock("k".to_string()).await;
            drop(_held);
        });
        assert!(map.is_empty(), "poison recovery must not leak the slot");
    }
}
