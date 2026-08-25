//! The per-object write lock for conditional PUT (RFC 7232 exclusivity).

use tinio_core::{bucket, object, storage::Storage};
use tinio_util::lockmap;

use super::S3Backend;

/// The held per-object lock — a [`lockmap::Guard`] over the
/// [`S3Backend::conditional_put_locks`] table (see [`lockmap::Map`] for
/// the eviction semantics).
pub(crate) type ObjectLock = lockmap::Guard<String>;

impl<S: Storage> S3Backend<S> {
    /// Per-object lock for conditional PUT (RFC 7232 exclusivity).
    pub(crate) async fn lock_object(&self, bucket: &bucket::Name, key: &object::Key) -> ObjectLock {
        self.conditional_put_locks
            .lock(format!("{bucket}/{key}"))
            .await
    }
}
