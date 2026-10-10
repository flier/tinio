//! The `Hooks` seam: the two steps the engine cannot take itself.
//!
//! A backend implements [`Hooks`] once and hands the value to the group
//! receivers it builds. The seam is deliberately two methods wide — the
//! bucket-delete action and the part-write lock guard. A third needs a named
//! consumer, a spec amendment and the operator's ruling, never a local
//! addition: everything else the engine needs is a persistence contract.

use async_trait::async_trait;
use tinio_core::{bucket, storage};

/// A guard the engine holds across one part's commit phase.
///
/// The backend's [`Hooks::lock_part`] answers it; the engine drops it when the
/// part row is written. Releasing is the guard's `Drop`, so an implementation
/// that holds a lock holds it in what this value owns.
///
/// The bound is `Send`, not `Sync`: a backend's guard is held across the
/// commit phase's awaits inside the engine's future, which must be `Send`, but
/// it is never shared between threads — `tinio-fs`'s part guard wraps
/// `tokio::sync::OwnedMutexGuard`, which is `Send` and deliberately not
/// `Sync`.
///
/// # Examples
///
/// ```rust
/// use tinio_ops::Guard;
///
/// struct Held;
/// impl Guard for Held {}
///
/// let guard: Box<dyn Guard> = Box::new(Held);
/// drop(guard);
/// ```
pub trait Guard: Send + 'static {}

/// The no-op guard a backend with no part lock answers ([`Hooks::lock_part`]
/// for a store whose part writes are one transaction): it holds nothing.
///
/// # Examples
///
/// ```rust
/// use tinio_ops::{Guard, NoGuard};
///
/// let guard: Box<dyn Guard> = Box::new(NoGuard);
/// drop(guard);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoGuard;

impl Guard for NoGuard {}

/// The two steps the engine cannot take itself.
///
/// A backend's implementation is the one place its own storage action is
/// reachable from a body the engine owns; everything else crosses the
/// persistence contracts.
///
/// # Examples
///
/// A backend implements both methods — memory answers the delete with nothing
/// and the part lock with a no-op guard:
///
/// ```ignore
/// use async_trait::async_trait;
/// use tinio_core::{bucket, storage};
/// use tinio_ops::{Guard, Hooks, NoGuard};
///
/// struct MemHooks;
///
/// #[async_trait]
/// impl Hooks for MemHooks {
///     async fn before_bucket_delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
///         Ok(())
///     }
///
///     async fn lock_part(&self, _bucket: &bucket::Name, _upload_id: &str) -> Box<dyn Guard> {
///         Box::new(NoGuard)
///     }
/// }
/// ```
#[async_trait]
pub trait Hooks: Send + Sync + 'static {
    /// The backend's action before a bucket's row and metadata are removed
    /// (fs: the tombstone move; memory and pg: nothing).
    ///
    /// **The commit point.** The engine has established that the bucket holds
    /// no content and no in-progress upload when this is called, and does
    /// nothing to the bucket after it returns: once it returns `Ok`, the
    /// operation has happened, and the backend owns the whole teardown — the
    /// content namespace, the rows and anything derived. An `Err` is the
    /// operation's answer and nothing has happened.
    async fn before_bucket_delete(&self, bucket: &bucket::Name) -> Result<(), storage::Error>;

    /// A guard held across one part's commit phase (fs: the per-upload part
    /// lock; memory and pg: a no-op guard).
    ///
    /// The engine takes it after the body has been streamed into the staged
    /// value and drops it when the part row is written, so a slow body stream
    /// never blocks another part write of the same upload.
    async fn lock_part(&self, bucket: &bucket::Name, upload_id: &str) -> Box<dyn Guard>;
}

/// The do-nothing [`Hooks`]: a bucket delete takes no action and the part lock
/// is [`NoGuard`]. It exists for tests and benches — `fs` locks through its own
/// table and `mem` keeps a private value, so a **production** path adopting
/// this one means the seam went unused: the delete leaves the backend's
/// teardown to nobody, and two part writes of one upload can interleave.
pub struct NoHooks;

#[async_trait::async_trait]
impl Hooks for NoHooks {
    async fn before_bucket_delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
        Ok(())
    }

    async fn lock_part(&self, _bucket: &bucket::Name, _upload_id: &str) -> Box<dyn Guard> {
        Box::new(NoGuard)
    }
}
