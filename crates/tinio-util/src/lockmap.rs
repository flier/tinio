//! An evicting per-key lock map.
//!
//! A [`Map`] hands out one `tokio` mutex per key — the lock actually
//! awaited across `.await` points — and evicts a slot when its last
//! handle drops, so the table stays bounded by the number of
//! concurrently locked keys.

use std::{hash::Hash, sync::Arc};

use papaya::HashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// One per-key mutex. The table holds one `Arc`; a holder's [`Guard`] —
/// through its `OwnedMutexGuard` — another; a waiter holds a third.
type Slot = Arc<Mutex<()>>;

/// The slot table: key → per-key mutex. `Arc` so [`Map`] clones share
/// the table (`papaya::HashMap` itself clones by snapshot).
type Table<K> = Arc<HashMap<K, Slot>>;

/// The per-key lock table. Slots are evicted when the last [`Guard`] for
/// a key drops: the table holds one `Arc` to the slot and the guard the
/// other (`strong_count == 2`); a waiter holds a third and its own `Drop`
/// performs the eviction later.
///
/// The table is a lock-free [`papaya::HashMap`]. `lock` probes the table
/// first (one lookup on the hot path) and inserts only on a miss. Between
/// `get`/`get_or_insert_with` handing out a slot reference and the caller
/// cloning it, a concurrent [`Guard::drop`] can `remove_if` the slot (its
/// eviction predicate sees `strong_count == 2` — the clone has not landed
/// yet); the clone then holds the only reference (`strong_count == 1`),
/// which `lock` detects and retries — it never locks an orphaned mutex
/// while a fresh slot occupies the key. `Drop` uses `remove_if` so it
/// never deletes a slot another waiter has already cloned (a later `lock`
/// would otherwise see a fresh mutex and split the key).
#[derive(Debug, Clone)]
pub struct Map<K: Hash + Eq> {
    map: Table<K>,
}

impl<K: Hash + Eq> Default for Map<K> {
    fn default() -> Self {
        Self {
            map: Arc::new(HashMap::new()),
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
        self.map.pin().len()
    }

    /// Whether no slot is held.
    pub fn is_empty(&self) -> bool {
        self.map.pin().is_empty()
    }

    /// Lock `key`, awaiting the per-key mutex. The returned [`Guard`]
    /// evicts the slot on drop when it is the last handle.
    pub async fn lock(&self, key: K) -> Guard<K> {
        let map = Arc::clone(&self.map);
        let slot = loop {
            let pinned = map.pin();
            // Hot path: the slot usually exists — clone it and check it is
            // still the live entry. A concurrent `Drop` may have
            // `remove_if`'d it between the `get` reference and the clone
            // (its eviction predicate saw `strong_count == 2` before the
            // clone landed); our clone then holds the only reference
            // (`strong_count == 1`) and we retry instead of locking an
            // orphaned mutex while a fresh slot occupies the key.
            //
            // F02: the clone is re-verified by IDENTITY against the live
            // table entry, not by the count — the count reads `>= 2` for
            // an orphaned slot too, when a concurrent `Guard::drop` is
            // mid-flight (its own ref not yet dropped) after the
            // eviction: the waiter would break and lock the orphaned
            // mutex while a fresh slot already owns the key. The identity
            // check is airtight: once this clone exists, the eviction
            // predicate (`strong_count == 2`) can never fire for this
            // slot, so a slot that passes the check cannot be evicted
            // before `lock_owned`; a slot that fails it is retried (the
            // cold path below inserts or finds the fresh owner).
            let slot = if let Some(live) = pinned.get(&key) {
                let slot = Arc::clone(live);
                if Arc::strong_count(&slot) >= 2
                    && pinned
                        .get(&key)
                        .is_some_and(|live| Arc::ptr_eq(live, &slot))
                {
                    break slot;
                }
                continue;
            } else {
                pinned
                    .get_or_insert_with(key.clone(), || Arc::new(Mutex::new(())))
                    .clone()
            };
            // Cold path: the slot was inserted a moment ago — the same
            // eviction race applies, verified by identity against the
            // current live entry.
            if pinned
                .get(&key)
                .is_some_and(|live| Arc::ptr_eq(live, &slot))
            {
                break slot;
            }
        };
        // `lock_owned` runs after the pin is dropped (no pin across
        // `.await`).
        Guard {
            key,
            map,
            guard: Some(slot.lock_owned().await),
        }
    }
}

/// The held per-key lock. Evicts the map slot on drop when this is the
/// last handle.
pub struct Guard<K: Clone + Eq + Hash> {
    key: K,
    map: Table<K>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl<K: Clone + Eq + Hash> Drop for Guard<K> {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        // Evict the slot only if we were the last handle: the same `Arc`
        // still in the table and no waiter clones (`strong_count == 2`).
        // The mutex is still held here — harmless: the eviction predicate
        // already proves no other handle exists, so nobody can queue on
        // it; `guard` drops right after, releasing the mutex.
        let slot = OwnedMutexGuard::<()>::mutex(&guard);
        let _ = self.map.pin().remove_if(&self.key, |_, live| {
            Arc::ptr_eq(live, slot) && Arc::strong_count(live) == 2
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tokio::{task::yield_now, time::timeout};

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
            timeout(Duration::from_millis(50), waiter).await.is_err(),
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
            timeout(Duration::from_millis(50), b).await.is_ok(),
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
        yield_now().await;
        drop(_held);
        // The waiter (whose slot clone pins the entry) acquires the key
        // next; whichever interleaving wins, the last guard's drop must
        // leave the table empty — the slot is never evicted out from
        // under a waiter, and never leaks after the final release.
        timeout(Duration::from_secs(1), waiter)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lock_split_is_impossible_under_contention() {
        // F02 regression net: the hot path must never hand out a slot
        // that was evicted between the table lookup and the clone. The
        // bad interleaving: a waiter pins and borrows the slot, the
        // holder's `Guard::drop` evicts it (the predicate saw
        // `strong_count == 2` before the clone landed), then the waiter
        // clones and — with the old `strong_count >= 2` check alone —
        // locks the ORPHANED mutex while a fresh slot already owns the
        // key. Two tasks then hold DIFFERENT mutexes for one key (the
        // delete-vs-PUT race the map exists to prevent). The window is
        // ~10ns of OS preemption, so no practical iteration count makes
        // this test *reliably* fail on the old code; it is a probabilistic
        // invariant net (and a deterministic failure if a future change
        // widens the window). The fix's own airtightness is the identity
        // re-check: once a waiter holds a clone, the eviction predicate
        // (`strong_count == 2`) can never fire for that slot, so a slot
        // that passes the identity check cannot be evicted before
        // `lock_owned`. Deliberate runtime shape: a single-thread runtime
        // cannot preempt mid-pin.
        let map = Map::new();
        let holders = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let map = map.clone();
            let holders = Arc::clone(&holders);
            tasks.push(tokio::spawn(async move {
                for _ in 0..20_000 {
                    let _guard = map.lock("k".to_string()).await;
                    let prev = holders.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(prev, 0, "two tasks hold one key: the slot split (F02)");
                    tokio::task::yield_now().await;
                    holders.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(map.is_empty(), "the last drop must evict the slot");
    }
}
