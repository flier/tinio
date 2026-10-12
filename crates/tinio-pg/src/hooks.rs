//! The engine's `Hooks` seam, answered empty — and why it can be.
//!
//! The seam exists for the two steps the engine cannot take itself (E4 holds it at two
//! methods): the bucket-delete action and the part-write lock. Against PostgreSQL neither is
//! an action: the store's rows and its content relation are the whole of a bucket, and a part
//! write is one generation flip the database serializes by itself. The two bodies below state
//! each reason where a reader will look for it.

use async_trait::async_trait;
use tinio_core::{bucket, storage};
use tinio_ops::{Guard, Hooks, NoGuard};

/// `PgStorage`'s [`Hooks`]: both steps answer nothing, because against PostgreSQL there is
/// nothing left for them to do.
///
/// The value carries no handles: a hook that needed the store would be the seam reaching back
/// into the backend, and the reasons below are why neither does.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PgHooks;

#[async_trait]
impl Hooks for PgHooks {
    async fn before_bucket_delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
        // **Nothing to arrange.** fs's hook owns a teardown because a bucket there is a
        // directory with derived state beside it — a tombstone move, a staging area, block
        // files — none of which a row can express. pg's bucket *is* its rows: the bucket row,
        // the object rows, the version rows and the content chunks all name the bucket as a
        // column value, so removing them is a row-plane action and not an arrangement this
        // seam could make. `PgStorage::delete_bucket` runs it (the content namespace, the
        // version rows and the bucket row, in that order) right after the engine's probes and
        // this no-op hook, which is why the seam stays empty here: the commit point is the
        // row's own guarded removal, and a hook body that reached the store would be the same
        // call one frame deeper.
        Ok(())
    }

    async fn lock_part(&self, _bucket: &bucket::Name, _upload_id: &str) -> Box<dyn Guard> {
        // **No lock is needed across the commit phase.** The engine takes this guard after the
        // body has streamed into the staged value and drops it when the part row is written —
        // the window a backend without a generation model has to protect (fs's two writers of
        // one part number would otherwise interleave chunks on one file). pg's part bytes are
        // a **generation**: each writer stages its own bytes and the publish flips the head in
        // one statement, so two concurrent writers of the same part number produce two whole
        // generations and the newest head wins — never a spliced part. The part row's own
        // write is one statement after it, last-writer-wins, so the row and the bytes a reader
        // then serves are one writer's. A lock here would serialize writes the database
        // already serializes.
        Box::new(NoGuard)
    }
}
