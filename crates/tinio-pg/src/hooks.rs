//! The engine's `Hooks` seam: the bucket-delete action and the part-write lock.
//!
//! The seam exists for the two steps the engine cannot take itself (E4 holds it at two
//! methods). Against PostgreSQL the bucket delete needs no arrangement — the store's rows and
//! its content relation are the whole of a bucket — while the part write needs a real
//! serialization step the database's own statement ordering does not supply. The two bodies
//! below state each reason where a reader will look for it.

use std::sync::Arc;

use async_trait::async_trait;
use tinio_core::{bucket, storage};
use tinio_ops::{Guard, Hooks, NoGuard};

use crate::_store_pg::{PgStore, teardown::PartLock};

/// `PgStorage`'s [`Hooks`]: the empty bucket-delete seam and the real part lock.
///
/// The value carries the store because the part lock is the store's own action — a
/// session-level advisory lock on a dedicated connection
/// ([`PgStore::lock_part`](crate::_store_pg::PgStore::lock_part)) — and a hook that reached
/// the database itself would be a second store over one pool.
#[derive(Debug, Clone)]
pub(crate) struct PgHooks {
    /// The store the part lock is taken on.
    store: Arc<PgStore>,
}

impl PgHooks {
    /// The seam over `store`.
    pub(crate) fn new(store: Arc<PgStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Hooks for PgHooks {
    async fn before_bucket_delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
        // **Nothing to arrange.** fs's hook owns a teardown because a bucket there is a
        // directory with derived state beside it — a tombstone move, a staging area, block
        // files — none of which a row can express. pg's bucket *is* its rows: the bucket row,
        // the object rows, the version rows and the content chunks all name the bucket as a
        // column value, so removing them is a row-plane action and not an arrangement this
        // seam could make. `PgStorage::delete_bucket` runs it in one transaction —
        // content, version rows, the key slots, the versioning state and the bucket row
        // together, and nothing if an object row still names the bucket — right after the
        // engine's probes and this no-op hook. The commit point is that transaction, and a
        // hook body that reached the store would be the same call one frame deeper.
        Ok(())
    }

    /// The per-upload advisory lock, held across the engine's commit phase.
    ///
    /// **What it guarantees.** The engine takes this guard after the body has streamed into
    /// the staged value and drops it once the part row is written. The commit phase is two
    /// statements — the publish, which flips the key's head, and the part row — and each is
    /// last-writer-wins on its own. Without a lock the two orders interleave: A-publish →
    /// B-publish → B-row → A-row leaves the head serving **B's** bytes with **A's** ETag and
    /// length in the row, and a completion then composes an ETag that describes neither
    /// writer's content. The lock makes the pair one writer's, which is what a reader and a
    /// completion need.
    ///
    /// **What it is not.** The lock is per `(bucket, upload_id)`, not per part number: the
    /// hook receives no part number, so concurrent writes of **different** parts of one
    /// upload serialize against each other too. That is coarser than fs's per-part-number
    /// mutex, and it is the honest shape of what this seam can name.
    ///
    /// A lock that cannot be taken (the connection fails) is logged and the write proceeds
    /// unserialized: the body is already staged, the seam's signature carries no error, and
    /// failing the part write would be a worse answer than the window the lock closes.
    async fn lock_part(&self, bucket: &bucket::Name, upload_id: &str) -> Box<dyn Guard> {
        match self.store.lock_part(bucket, upload_id).await {
            Ok(lock) => Box::new(PgPartGuard { _lock: lock }),
            Err(error) => {
                tracing::warn!(
                    bucket = %bucket,
                    upload_id = %upload_id,
                    error = %error,
                    "the per-upload part lock could not be taken; this part write proceeds unserialized"
                );
                Box::new(NoGuard)
            }
        }
    }
}

/// pg's part-write guard: the store's held advisory lock, alive as long as this value is.
pub(crate) struct PgPartGuard {
    /// The held lock (released when this guard drops). The field is never read: holding it
    /// IS the guard.
    _lock: PartLock,
}

impl Guard for PgPartGuard {}
