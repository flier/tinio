//! The bucket teardown and the per-upload part lock: the two store-side actions
//! `tinio-pg`'s backend takes beside the four persistence contracts.
//!
//! # Why the two share a module
//!
//! Neither is one of the four contracts: they are the two steps the **engine's
//! seam** leaves to a backend — the bucket delete (`Hooks::before_bucket_delete`
//! is empty for pg, because a bucket here *is* rows, so `PgStorage::delete_bucket`
//! runs this teardown where a directory store would tear things down in the hook)
//! and the part-write lock (`Hooks::lock_part`). Both need the store's own
//! connection rather than a contract call: the teardown is one transaction over
//! the bucket's whole namespace, and the part lock is a session-level advisory
//! lock held on a dedicated connection for the guard's lifetime. So both are
//! `impl PgStore` blocks here, beside the store's other connection-level facts,
//! rather than code in `tinio-pg` — which holds no pool and no dsn, and could not
//! open either connection.
//!
//! # The teardown is one transaction (R3)
//!
//! `delete_bucket`'s engine body runs three probes before this call — the
//! content plane's emptiness, the upload rows and the object rows
//! (`ObjectMeta::has_objects`, which a **delete marker** is: an object row with
//! no live bytes) — and then the seam. A marker-only bucket is therefore
//! refused before this teardown is reached. The store's own gate below is **not
//! redundant** for it: the engine's probes run outside any transaction here, so
//! a writer that commits after them — or one that was waiting on the bucket-row
//! lock taken below — is unseen by them and is exactly what the statement's own
//! `object_meta` gate catches, atomically with the drains. Deleting that gate
//! would reopen the probe→teardown window. So the store decides, inside one
//! transaction:
//!
//! 1. the bucket row is locked (`FOR UPDATE`, the lock every guarded writer
//!    takes through `assert_bucket_row`), so a stage or a guarded write
//!    serializes with the teardown instead of interleaving it;
//! 2. emptiness is probed **under that lock, in a later statement** — no
//!    `object_meta` row, no `uploads` row, no live content generation — so a
//!    writer that waited on the lock is seen;
//! 3. the version rows, their index entries, the key slots, the versioning
//!    state, the content chunks, the lifecycle row and the bucket row leave
//!    together.
//!
//! A refusal rolls all of it back: the client gets `NotEmpty` and the bucket
//! keeps every one of its rows, which is what makes a marker-only bucket
//! refuse-able without destroying it (the before-R3 shape committed the drains
//! and then answered `NotEmpty`, stranding an undeletable bucket).
//!
//! # The part lock is per upload, on a dedicated out-of-pool connection
//!
//! `tinio_ops::Hooks::lock_part` receives no part number, so the lock is per
//! `(bucket, upload_id)`: coarser than `tinio-fs`'s per-part-number mutex, and
//! deliberately so. What it fixes is the window a single-statement publish
//! does not close by itself — **two independent last-writer-wins writes**. A
//! publish and the part row it feeds are two statements, so A-publish →
//! B-publish → B-row → A-row would serve B's bytes under A's ETag and length,
//! and a completion would then compose an ETag that describes neither. Holding
//! this lock across the guard's lifetime makes the pair of writes one writer's,
//! which is the same guarantee fs's part lock gives.
//!
//! The lock is session-scoped, and that is why the connection is **dedicated and
//! out of the pool**: PostgreSQL frees a session-level advisory lock when the
//! session ends, so dropping the guard drops the connection and the lock with it.
//! A pooled connection cannot serve — it returns to the pool still open, lock
//! held — and a pooled connection would also need an `async` unlock no `Drop`
//! can perform. The store's pool is therefore not used, and the cost is one
//! connection per held guard (see [`PgStore::lock_part`], which states the
//! namespace the key is taken in and why it cannot collide with the ownership
//! lock).

use sqlx::{Connection, PgConnection, query_file_as, query_file_scalar};

use crate::{_core::bucket, Error, begin, meta::assert_bucket_row, store::PgStore};

/// One row of `teardown_bucket.sql`: whether the bucket was clear to remove and
/// whether its row left.
#[derive(Debug)]
struct Teardown {
    clear: bool,
    removed: bool,
}

impl PgStore {
    /// Delete a bucket that nothing names, version rows included — the whole
    /// teardown in one transaction (R3).
    ///
    /// The bucket row is locked first, and the emptiness check runs in the
    /// next statement so it sees everything committed while the lock waited.
    /// A bucket that still holds an object row (a delete marker included), an
    /// upload or a live generation answers `NotEmpty` and commits nothing.
    /// Blocks stay for the sweep, because liveness is derived from the rows.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the bucket row is already gone, `NotEmpty` when an
    /// object row, an upload or a live generation still names the bucket.
    pub async fn teardown_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        let answer = query_file_as!(Teardown, "sql/store/teardown_bucket.sql", bucket)
            .fetch_one(&mut *tx)
            .await?;
        if !answer.clear || !answer.removed {
            tx.rollback().await?;
            return Err(Error::NotEmpty(bucket.clone()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Take the per-upload advisory lock and hold it for the guard's lifetime.
    ///
    /// The lock is session-level and lives on a **dedicated** connection, so
    /// the guard's drop is the release: the connection closes and PostgreSQL
    /// frees the lock with the session. It is taken in the two-key
    /// (`classid`/`objid` plus `objsubid = 2`) advisory namespace, so it can
    /// never collide with the ownership lock, which uses the single-key form.
    ///
    /// The cost is one out-of-pool connection per held guard, which is what a
    /// session-level lock needs; the store's pool is deliberately not used,
    /// because a pooled connection returns to the pool with its lock still
    /// held.
    ///
    /// # Errors
    ///
    /// The driver's answer when the connection or the lock statement fails.
    pub async fn lock_part(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<PartLock, Error> {
        let mut connection = PgConnection::connect_with(&self.pool().dedicated_options()).await?;
        query_file_scalar!("sql/store/part_lock.sql", bucket, upload_id)
            .fetch_one(&mut connection)
            .await?;
        Ok(PartLock {
            _connection: connection,
        })
    }
}

/// A held per-upload advisory lock, released when the guard drops.
///
/// The lock is session-scoped and the connection is dedicated, so the drop is
/// the release: the driver closes the connection and PostgreSQL frees the lock
/// with it. No `Drop` impl is written, for the reason the ownership guard
/// states.
pub struct PartLock {
    /// The dedicated connection holding the lock. The field is never read:
    /// holding it IS the guard.
    _connection: PgConnection,
}
