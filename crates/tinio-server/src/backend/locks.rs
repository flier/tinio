//! The per-object write lock (RFC 7232 exclusivity): every destination
//! write — conditional put, delete, copy destination, and the whole
//! conditional complete — runs its check-then-commit under one per-key
//! lock so a concurrent writer can never invalidate the state a
//! precondition was evaluated against. Lock-free: reads, and the
//! multipart abort (its state is (bucket, upload_id)-scoped and the
//! storage drains it in one transaction).

use super::S3Backend;
use crate::{
    _core::{bucket, object, storage::Storage},
    _util::lockmap,
};

/// The held per-object lock — a [`lockmap::Guard`] over the
/// [`S3Backend::conditional_put_locks`] table (see [`lockmap::Map`] for
/// the eviction semantics).
pub(crate) type ObjectLock = lockmap::Guard<String>;

impl<S: Storage> S3Backend<S> {
    /// The per-key write lock of the destination writes.
    pub(crate) async fn lock_object(&self, bucket: &bucket::Name, key: &object::Key) -> ObjectLock {
        self.conditional_put_locks
            .lock(format!("{bucket}/{key}"))
            .await
    }
}
