//! The byte-accounting decorator: [`CountingContent`] wraps an
//! [`ObjectContent`] plane and owns every count of the bytes mem holds.
//!
//! mem's resource limits ([`MemoryOptions`](crate::MemoryOptions)) are
//! enforced here and nowhere else (round 29): [`CountingContent::stage`]
//! counts the chunks it accepts against `max_object_bytes`, and
//! [`CountingContent::publish`] moves the shared total by the **difference**
//! a write makes against `max_total_bytes`. The whole accounting step —
//! the key's stored length, the reservation and the inner write — runs in
//! one critical section (the decorator's accounting lock), so two racing
//! publishes to one key bill the difference instead of each billing the
//! whole length, and two racing writes cannot both pass a cap they jointly
//! exceed. A refused write publishes nothing, and an inner failure gives the
//! reservation back.
//!
//! The lock has a second audience: mem's own single-transaction paths (the
//! versioned write/delete, the batch removal) hold it themselves through
//! [`CountingContent::lock_accounting`] and meter their raw writes with the
//! `*_unlocked` entry points. [`CountingContent::adjust_total_with_credit_unlocked`]
//! lets a write land under a cap that only fits the final state: the credit
//! forgives bytes the caller is about to release, while the reservation
//! stays the exact delta.
//!
//! The wrapper is mem-typed: its `ObjectContent::Error` is mem's
//! [`Error`](crate::Error), so a typed refusal (`EntityTooLarge`) reaches
//! the caller as itself rather than collapsing through the inner plane's
//! error type (D38): an inner failure rides back through the inner plane's
//! `Into<Error>` lift, and no reverse projection is needed.

use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

use crate::{
    _core::{
        bucket,
        content::{ContentEntry, ContentKey, ObjectContent, PublishId, StagedObject},
        multipart, object,
        storage::{BodyStream, ByteRange},
    },
    Error,
    error::entity_too_large,
    storage::MemoryOptions,
};

/// Report a book-keeping anomaly as a `warn` event on this crate's target.
/// Every caller reports a broken invariant — a total that went below zero, a
/// compensation whose own inner call failed, a refund that took bytes out of
/// band — never a client-visible failure, so the event carries the message
/// verbatim and the operator's subscriber decides what to keep.
pub(crate) fn report(message: fmt::Arguments<'_>) {
    tracing::warn!("{message}");
}

/// The identity of a multipart part whose landed length is remembered: the
/// bucket it belongs to, its upload and its number.
///
/// The key carries the bucket dimension `ContentKey::Part` does not (its
/// `(upload_id, number)` pair alone would let two buckets share one ledger
/// row), so the ledger names the part by this triple, not by a
/// `ContentKey`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartKey {
    bucket: bucket::Name,
    upload_id: String,
    number: multipart::PartNumber,
}

impl PartKey {
    /// The ledger key of `number` in `upload_id` under `bucket`.
    fn new(bucket: &bucket::Name, upload_id: &str, number: multipart::PartNumber) -> Self {
        Self {
            bucket: bucket.clone(),
            upload_id: upload_id.to_owned(),
            number,
        }
    }
}

/// The staged handle of [`CountingContent`]: the inner plane's staged write
/// plus the bytes accepted so far and the object cap they are counted
/// against.
///
/// `write` refuses the chunk that would cross `max_object_bytes` before
/// the inner plane sees it, so a refused stage stores nothing and bills
/// nothing.
pub struct CountingStaged<C: ObjectContent> {
    inner: C::Staged,
    len: u64,
    cap: Option<u64>,
}

#[async_trait]
impl<C: ObjectContent> StagedObject for CountingStaged<C>
where
    C::Error: Into<Error>,
{
    /// mem's own failure — the cap refusal is mem's `EntityTooLarge`, and
    /// an inner failure lifts into the same type.
    type Error = Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), Self::Error> {
        let len = self.len + chunk.len() as u64;
        if let Some(cap) = self.cap
            && len > cap
        {
            return Err(entity_too_large(len, cap));
        }
        self.inner.write(chunk).await.map_err(Into::into)?;
        self.len = len;
        Ok(())
    }
}

/// mem's byte-accounting decorator over an [`ObjectContent`] plane.
///
/// Every object and part byte mem holds passes through this type, and the
/// shared total moves by the difference a write makes — never by the whole
/// length, so an overwrite of one key bills only what it changes (round 29;
/// the audit's add-only rule billed ten 1 MiB overwrites as 10 MiB). Every
/// accounting mutation runs under one lock, so the length a publish reads is
/// the length the previous publish landed.
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use tinio_core::{
///     bucket,
///     content::{ContentKey, ObjectContent, StagedObject},
///     object,
/// };
/// use tinio_mem::{CountingContent, MemoryOptions};
/// use tinio_store_redb::RedbContent;
///
/// let runtime = tokio::runtime::Runtime::new().unwrap();
/// runtime.block_on(async {
///     let content = CountingContent::new(
///         RedbContent::in_memory().unwrap(),
///         MemoryOptions {
///             max_object_bytes: Some(1024),
///             max_total_bytes: Some(5),
///         },
///     );
///     let bucket = bucket::name("data").unwrap();
///     let key = object::key("a.txt").unwrap();
///     let content_key = ContentKey::Object(key);
///     let mut staged = content.stage(&bucket, &content_key).await.unwrap();
///     staged.write(Bytes::from_static(b"hello")).await.unwrap();
///     content
///         .publish(&bucket, staged, &content_key)
///         .await
///         .unwrap();
///     // The counter bills the object once, by its difference: an
///     // overwrite at the same length adds nothing, so it still fits the
///     // five-byte cap.
///     let mut staged = content.stage(&bucket, &content_key).await.unwrap();
///     staged.write(Bytes::from_static(b"world")).await.unwrap();
///     content
///         .publish(&bucket, staged, &content_key)
///         .await
///         .unwrap();
/// });
/// ```
pub struct CountingContent<C: ObjectContent> {
    inner: C,
    options: MemoryOptions,
    total: AtomicU64,
    /// The stored length of the keys the inner plane cannot probe: the
    /// contract's `stat` addresses object keys only, so the length a part
    /// publish landed — the value an overwrite of that part is metered
    /// against — is remembered here, under the part's own key.
    parts: Mutex<HashMap<PartKey, u64>>,
    /// The last *tagged* publish at each key: what lets a stale compensation
    /// tell itself from the live publish even when the bytes are identical
    /// (an empty body matches an empty body). Process-local by design — a
    /// compensation never outlives the process that issued its publish — and
    /// ordered by the accounting lock; this std lock only lends the map
    /// interior mutability.
    publish_tags: Mutex<HashMap<(bucket::Name, ContentKey), PublishId>>,
    /// Mints the identity of each tagged publish: monotonic, never reused, so
    /// a stale compensation can never accidentally match a later publish.
    next_publish: AtomicU64,
    /// Keys whose bytes a failed removal released anyway (design §4) while
    /// their rows survived: the teardown that later sweeps those rows must not
    /// release them a second time. Process-local, like the tag map; a key
    /// leaves the set when the teardown consumes or purges it.
    undead_releases: Mutex<HashSet<(bucket::Name, ContentKey)>>,
    /// Serializes the accounting that must read and write together: a
    /// publish reads the key's stored length, reserves the difference and
    /// writes the bytes as one step, so a racing publish to the same key
    /// reads what landed instead of the length it is about to replace. The
    /// same lock covers remove/rename/copy, whose deltas are read the same
    /// way; it is never taken again inside a critical section.
    accounting: AsyncMutex<()>,
}

impl<C: ObjectContent> CountingContent<C>
where
    C::Error: Into<Error>,
{
    /// Wrap `inner` and enforce `options`: the decorator is the one
    /// accounting home (round 29), counter included, and the tests read the
    /// counter through `total_bytes` (a `#[cfg(test)]` accessor).
    pub fn new(inner: C, options: MemoryOptions) -> Self {
        Self {
            inner,
            options,
            total: AtomicU64::new(0),
            parts: Mutex::new(HashMap::new()),
            publish_tags: Mutex::new(HashMap::new()),
            next_publish: AtomicU64::new(0),
            undead_releases: Mutex::new(HashSet::new()),
            accounting: AsyncMutex::new(()),
        }
    }

    /// Take the accounting lock: the critical section every counted
    /// mutation reads and writes in. A caller outside the decorator (the
    /// versioned single-transaction paths) holds this guard across its own
    /// work and uses the `*_unlocked` entry points. The lock is **not**
    /// reentrant: a caller holding it must never call a `*_locked` method,
    /// [`Self::publish_replacing`] or [`Self::remove_many`].
    pub(crate) async fn lock_accounting(&self) -> AsyncMutexGuard<'_, ()> {
        self.accounting.lock().await
    }

    /// Mint the identity of one publish. Monotonic and never reused (the
    /// counter starts the sequence at 1), so no later publish can carry a
    /// stale compensation's id.
    pub(crate) fn mint_publish_id(&self) -> PublishId {
        PublishId::new(self.next_publish.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Record `id` as the key's latest publish. Call with
    /// [`Self::lock_accounting`] held — the versioned plane's raw
    /// transactions hold it when they publish their own bytes.
    pub(crate) fn record_publish_tag(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        id: PublishId,
    ) {
        self.publish_tags()
            .insert((bucket.clone(), key.clone()), id);
    }

    /// Take the key's tag back while it is still `id`, answering whether it
    /// was — the caller's publish is the key's latest only then. Call with
    /// [`Self::lock_accounting`] held.
    fn take_publish_tag(&self, bucket: &bucket::Name, key: &ContentKey, id: PublishId) -> bool {
        let mut tags = self.publish_tags();
        let key = (bucket.clone(), key.clone());
        if tags.get(&key).is_some_and(|tag| *tag == id) {
            tags.remove(&key);
            true
        } else {
            false
        }
    }

    /// Forget the key's tag whatever it is: its bytes left this caller, so no
    /// compensation may claim them again. Call with [`Self::lock_accounting`]
    /// held.
    pub(crate) fn forget_publish_tag(&self, bucket: &bucket::Name, key: &ContentKey) {
        self.publish_tags().remove(&(bucket.clone(), key.clone()));
    }

    /// Retire this call's publish identity once the row describing it landed:
    /// no compensation can follow a landed row write, so the tag leaves the
    /// map instead of waiting for the key's next publish to replace it. A
    /// writer that published after this one keeps its own tag (the take
    /// compares). Call after the row write succeeded; a refused one
    /// compensates instead.
    pub(crate) async fn retire_publish_tag(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        id: PublishId,
    ) {
        let _section = self.lock_accounting().await;
        let _ = self.take_publish_tag(bucket, key, id);
    }

    /// Remember that `key`'s bytes were released while its rows survived: the
    /// teardown that sweeps those rows must not count them twice. Call with
    /// [`Self::lock_accounting`] held.
    fn note_released_undead(&self, bucket: &bucket::Name, key: &ContentKey) {
        self.undead_releases().insert((bucket.clone(), key.clone()));
    }

    /// Whether `key`'s bytes were already released while its rows survived —
    /// the teardown's sum must skip those. Call with the accounting lock held.
    pub(crate) fn released_undead(&self, bucket: &bucket::Name, key: &ContentKey) -> bool {
        self.undead_releases()
            .contains(&(bucket.clone(), key.clone()))
    }

    /// Forget every mark `bucket` still holds: its teardown consumed the ones
    /// that matched rows, and the rest name bytes that are gone. Call with the
    /// accounting lock held.
    pub(crate) fn purge_released_undead(&self, bucket: &bucket::Name) {
        self.undead_releases().retain(|(owner, _)| owner != bucket);
    }

    /// The released-but-present mark set; poison-proof like the part ledger.
    fn undead_releases(&self) -> MutexGuard<'_, HashSet<(bucket::Name, ContentKey)>> {
        self.undead_releases
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    /// The tag map; poison-proof like the part ledger.
    fn publish_tags(&self) -> MutexGuard<'_, HashMap<(bucket::Name, ContentKey), PublishId>> {
        self.publish_tags
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    /// Apply a signed byte delta to the shared total, refusing the delta
    /// that would push it past `max_total_bytes`. The atomic update is the
    /// reservation step of the accounting critical section: two racing
    /// publishes can never both pass a cap they jointly exceed. A refusal
    /// leaves the total untouched, and the error reports the size the write
    /// would have produced. A delta that does not grow the total is never
    /// refused. A no-op when no total limit is configured.
    ///
    /// For a caller already holding [`Self::lock_accounting`]; everyone
    /// else uses [`Self::adjust_total_locked`].
    pub(crate) fn adjust_total_unlocked(&self, delta: i64) -> Result<(), Error> {
        self.adjust_total_with_credit_unlocked(delta, 0)
    }

    /// [`Self::adjust_total_unlocked`] with a `credit` the caller is about
    /// to release: the cap check forgives those bytes
    /// (`total + delta - credit <= limit`), while the reservation stays the
    /// exact `delta` — **never** `delta - credit`.
    ///
    /// A credit therefore widens the check only. The total is exact the
    /// moment this returns, and the release that follows subtracts its own
    /// bytes exactly once, so a credit can never drift the total. This is
    /// what lets a write land under a cap that only fits the final state: a
    /// multipart complete assembling over the parts it is about to drop, a
    /// versioned overwrite displacing the version it supersedes.
    ///
    /// Call with [`Self::lock_accounting`] held.
    pub(crate) fn adjust_total_with_credit_unlocked(
        &self,
        delta: i64,
        credit: u64,
    ) -> Result<(), Error> {
        let Some(limit) = self.options.max_total_bytes else {
            return Ok(());
        };
        // A delta that does not grow the total is never a cap breach: the
        // paths that only move or drop bytes must not fail for capacity.
        if delta <= 0 {
            self.apply_total_unlocked(delta);
            return Ok(());
        }
        // The projected size is carried out of the closure: `try_update`'s
        // `Err` carries only the current total, which is racy under
        // concurrent deltas.
        let projected = Cell::new(0u64);
        self.total
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total: u64| {
                let next = (total as i128 + delta as i128).max(0);
                if next - credit as i128 > limit as i128 {
                    projected.set(next as u64);
                    return None;
                }
                Some(next as u64)
            })
            .map_err(|_| entity_too_large(projected.get(), limit))?;
        Ok(())
    }

    /// Apply a signed byte delta **without** the limit check. The paths that
    /// only move or drop bytes the backend already holds — a compensation
    /// after a refused or failed publish, a version blob's release, a
    /// teardown — must never fail for capacity: the limit governs what a
    /// client write may add. The delta clamps at zero, so a compensation
    /// that would otherwise fail still lands. A no-op when no total limit is
    /// configured.
    ///
    /// For a caller already holding [`Self::lock_accounting`]; a caller that
    /// does not hold it takes the lock around its whole read-modify-write
    /// rather than releasing through a locked form of this one.
    pub(crate) fn release_total_unlocked(&self, delta: i64) {
        self.apply_total_unlocked(delta);
    }

    /// [`Self::adjust_total_unlocked`] behind the accounting lock.
    pub(crate) async fn adjust_total_locked(&self, delta: i64) -> Result<(), Error> {
        let _section = self.accounting.lock().await;
        self.adjust_total_unlocked(delta)
    }

    /// Move the total by `delta`, clamped at zero and with no cap check:
    /// the shared arithmetic of a release and of a delta that does not grow
    /// the total.
    ///
    /// A delta that would take the total below zero is a broken invariant.
    /// The total is the bytes the backend still holds, so an underflow means
    /// some other path moved the counter outside the accounting lock. The
    /// clamp keeps the soft limit defensible, but it silently swallows live
    /// bytes another bucket still holds and never gives them back — so an
    /// underflow is asserted in debug builds and reported in all of them,
    /// never silent.
    fn apply_total_unlocked(&self, delta: i64) {
        if self.options.max_total_bytes.is_none() {
            return;
        }
        let underflow: Cell<Option<u64>> = Cell::new(None);
        self.total
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total: u64| {
                let next = total as i128 + delta as i128;
                if next < 0 {
                    underflow.set(Some(total));
                    return Some(0);
                }
                Some(next as u64)
            })
            .ok();
        if let Some(total) = underflow.get() {
            debug_assert!(false, "the tracked total underflowed: {total} + {delta}");
            report(format_args!(
                "the tracked total underflowed ({total} + {delta}); the clamp to zero \
                 swallows bytes another path still holds"
            ));
        }
    }

    /// The currently tracked total bytes. Test hook.
    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Refuse a body of `size` bytes against `max_object_bytes` before a
    /// byte is staged — the same typed refusal [`CountingStaged::write`]
    /// raises, and without touching the total. The versioned write path uses
    /// it to pick between the raw and the staged branch ahead of the copy.
    pub(crate) fn check_object_bytes(&self, size: u64) -> Result<(), Error> {
        self.check_object_bytes_unlocked(size)
    }

    /// [`Self::check_object_bytes`] for a caller already holding the
    /// accounting lock: the single-object cap alone, never the total. The
    /// versioned single-transaction writes check the assembled body they
    /// write raw through this.
    pub(crate) fn check_object_bytes_unlocked(&self, len: u64) -> Result<(), Error> {
        match self.options.max_object_bytes {
            Some(limit) if len > limit => Err(entity_too_large(len, limit)),
            _ => Ok(()),
        }
    }

    /// Whether any total accounting runs at all. With no `max_total_bytes`
    /// the counter is never read or written (the type's own rule), so the
    /// key's current length is not probed either.
    fn counts_total(&self) -> bool {
        self.options.max_total_bytes.is_some()
    }

    /// The bytes the inner plane stores at `key`: the plane's own `stat`
    /// for an object, the remembered length for a part.
    async fn stored_len(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<u64, Error> {
        match key {
            ContentKey::Object(key) => Ok(self.object_len(bucket, key).await?.unwrap_or(0)),
            ContentKey::Part { upload_id, number } => {
                Ok(self.part_length(bucket, upload_id, *number))
            }
        }
    }

    /// The object's stored length, `None` when it holds nothing.
    async fn object_len(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<u64>, Error> {
        Ok(self
            .inner
            .stat(bucket, key)
            .await
            .map_err(Into::into)?
            .map(|entry| entry.len))
    }

    /// The remembered length of a part, `0` when none is remembered.
    fn part_length(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: multipart::PartNumber,
    ) -> u64 {
        let lengths = self.part_lengths();
        lengths
            .get(&PartKey::new(bucket, upload_id, number))
            .copied()
            .unwrap_or(0)
    }

    /// Record the length a part publish landed, so the next publish of the
    /// same part is metered by its difference.
    fn record_part_length(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: multipart::PartNumber,
        len: u64,
    ) {
        let mut lengths = self.part_lengths();
        lengths.insert(PartKey::new(bucket, upload_id, number), len);
    }

    /// Drop a part key's remembered length — its bytes have left the plane.
    fn drop_part_length(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: multipart::PartNumber,
    ) {
        let mut lengths = self.part_lengths();
        lengths.remove(&PartKey::new(bucket, upload_id, number));
    }

    /// The remembered part lengths, keyed by the part's own triple
    /// ([\`PartKey\`]: bucket, upload, number) — not by a `ContentKey`, which
    /// lacks the bucket dimension. Poison-proof: an earlier panic must not
    /// make every later write fail.
    fn part_lengths(&self) -> MutexGuard<'_, HashMap<PartKey, u64>> {
        self.parts.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// The publish critical section: read the key's stored length, reserve
    /// the difference, then let the inner plane write. A refusal returns
    /// before the inner plane sees a byte; an inner failure hands the
    /// reservation back. Callers hold the accounting lock.
    async fn publish_counted(
        &self,
        bucket: &bucket::Name,
        staged: C::Staged,
        landed: u64,
        key: &ContentKey,
    ) -> Result<(), Error> {
        let stored = self.stored_len(bucket, key).await?;
        let delta = landed as i64 - stored as i64;
        self.adjust_total_unlocked(delta)?;
        if let Err(err) = self.inner.publish(bucket, staged, key).await {
            self.release_total_unlocked(delta);
            return Err(err.into());
        }
        if let ContentKey::Part { upload_id, number } = key {
            self.record_part_length(bucket, upload_id, *number, landed);
        }
        Ok(())
    }

    /// The publish body inside the accounting lock: counted when a total is
    /// tracked, a plain inner publish when it is not.
    async fn publish_locked(
        &self,
        bucket: &bucket::Name,
        staged: C::Staged,
        landed: u64,
        key: &ContentKey,
    ) -> Result<(), Error> {
        if self.counts_total() {
            self.publish_counted(bucket, staged, landed, key).await
        } else {
            self.inner
                .publish(bucket, staged, key)
                .await
                .map_err(Into::into)
        }
    }

    /// The publish critical section with a pre-release credit: the same read,
    /// reserve and write as [`Self::publish_counted`], except that the
    /// bytes `releasing` names are counted as the cap check's credit (see
    /// [`Self::adjust_total_with_credit_unlocked`]). The reservation is the
    /// exact difference this publish makes, so the total is right the moment
    /// it lands; the caller's own releases, when they come, subtract their
    /// bytes exactly once.
    ///
    /// The versioned single-transaction path does not call this (it holds
    /// the accounting lock already, and the lock is not reentrant): it
    /// writes raw and meters with
    /// [`Self::adjust_total_with_credit_unlocked`].
    pub(crate) async fn publish_replacing(
        &self,
        bucket: &bucket::Name,
        staged: CountingStaged<C>,
        key: &ContentKey,
        releasing: &[ContentKey],
    ) -> Result<(), Error> {
        let landed = staged.len;
        if !self.counts_total() {
            return self
                .inner
                .publish(bucket, staged.inner, key)
                .await
                .map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        let mut credit = 0u64;
        for key in releasing {
            credit += self.stored_len(bucket, key).await?;
        }
        let stored = self.stored_len(bucket, key).await?;
        let delta = landed as i64 - stored as i64;
        self.adjust_total_with_credit_unlocked(delta, credit)?;
        if let Err(err) = self.inner.publish(bucket, staged.inner, key).await {
            self.release_total_unlocked(delta);
            return Err(err.into());
        }
        if let ContentKey::Part { upload_id, number } = key {
            self.record_part_length(bucket, upload_id, *number, landed);
        }
        Ok(())
    }

    /// The remove critical section: what is stored now is what the removal
    /// releases, and both the read and the release stay inside the lock.
    ///
    /// The release runs **even when the inner removal fails** (design §4).
    /// The caller's rows already say the bytes are gone — a version's
    /// materialize, a drained bucket, a completed multipart — so the books
    /// follow the rows: a compensation that kept phantom bytes would refuse
    /// later writes for space the backend no longer holds, and would keep
    /// refusing until a restart. The failure is reported instead of
    /// swallowed, and the original error still reaches the caller.
    async fn remove_counted(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        let stored = self.stored_len(bucket, key).await?;
        let outcome = self.inner.remove(bucket, key).await;
        if let Err(err) = &outcome {
            report(format_args!(
                "the removal of {key:?} failed ({err}); its {stored} bytes leave the total anyway"
            ));
            // The rows survived the release: a later teardown sweeping them
            // must not release these bytes a second time.
            self.note_released_undead(bucket, key);
        }
        self.release_counted(bucket, key, stored);
        outcome.map_err(Into::into)
    }

    /// Release what `key` held once its bytes have come back to this
    /// caller: the total drops by `stored` and a part's remembered length
    /// goes with it, so the ledger never keeps a row for bytes that left.
    fn release_counted(&self, bucket: &bucket::Name, key: &ContentKey, stored: u64) {
        self.release_total_unlocked(-(stored as i64));
        if let ContentKey::Part { upload_id, number } = key {
            self.drop_part_length(bucket, upload_id, *number);
        }
        // The bytes left this caller, so the key holds no publish of theirs
        // any more: a stale tag must not let a second compensation match.
        self.forget_publish_tag(bucket, key);
    }

    /// The guarded removal's critical section, shared by the byte-only and
    /// the tagged override: read the stored length, compare and remove in the
    /// inner plane's one commit, then book the outcome on `remove_counted`'s
    /// rule — `true` releases the bytes, `false` moves nothing, an inner
    /// failure reports and releases anyway (design §4). Call with
    /// [`Self::lock_accounting`] held; `guard` names the guard in the report.
    async fn remove_guarded_counted(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        guard: &str,
    ) -> Result<bool, Error> {
        let stored = self.stored_len(bucket, key).await?;
        match self.inner.remove_if_matches(bucket, key, expected).await {
            Ok(true) => {
                self.release_counted(bucket, key, stored);
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(err) => {
                report(format_args!(
                    "the {guard} removal of {key:?} failed ({err}); its {stored} bytes leave the total anyway"
                ));
                self.note_released_undead(bucket, key);
                self.release_counted(bucket, key, stored);
                Err(err.into())
            }
        }
    }

    /// The rename critical section: the source's bytes move and the target's
    /// stored ones leave, both read inside the lock.
    async fn rename_counted(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if self.object_len(bucket, from).await?.is_none() {
            // No source bytes: the inner plane refuses the move, and the
            // total must not move either.
            return self
                .inner
                .rename(bucket, from, to)
                .await
                .map_err(Into::into);
        }
        let target = self.object_len(bucket, to).await?.unwrap_or(0);
        self.inner
            .rename(bucket, from, to)
            .await
            .map_err(Into::into)?;
        self.release_total_unlocked(-(target as i64));
        // The bytes moved: the source holds nothing and the target holds a
        // publish that is not the one either key recorded, so no compensation
        // naming an old publish of theirs may match any more.
        self.forget_publish_tag(bucket, &ContentKey::Object(from.clone()));
        self.record_publish_tag(
            bucket,
            &ContentKey::Object(to.clone()),
            self.mint_publish_id(),
        );
        Ok(())
    }

    /// The copy critical section: the source's bytes arrive and the target's
    /// stored ones leave, both read inside the lock.
    async fn copy_counted(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        let Some(source) = self.object_len(bucket, from).await? else {
            // No source bytes: the inner plane refuses the copy, and the
            // total must not move either.
            return self.inner.copy(bucket, from, to).await.map_err(Into::into);
        };
        let target = self.object_len(bucket, to).await?.unwrap_or(0);
        let delta = source as i64 - target as i64;
        self.adjust_total_unlocked(delta)?;
        if let Err(err) = self.inner.copy(bucket, from, to).await {
            self.release_total_unlocked(delta);
            return Err(err.into());
        }
        // The target holds a fresh publish of the source's bytes: the identity
        // it recorded is not the one it holds any more.
        self.record_publish_tag(
            bucket,
            &ContentKey::Object(to.clone()),
            self.mint_publish_id(),
        );
        Ok(())
    }
}

#[async_trait]
impl<C> ObjectContent for CountingContent<C>
where
    C: ObjectContent,
    C::Error: Into<Error>,
{
    /// mem's own failure: a typed refusal (a size cap) must surface as
    /// itself, so the decorator does not adopt the inner plane's error.
    type Error = Error;
    /// The inner staged write, counted and capped.
    type Staged = CountingStaged<C>;

    async fn stage(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<Self::Staged, Error> {
        let inner = self.inner.stage(bucket, key).await.map_err(Into::into)?;
        Ok(CountingStaged {
            inner,
            len: 0,
            cap: self.options.max_object_bytes,
        })
    }

    /// Publish inside the accounting lock: the length this write replaces,
    /// the reservation it makes and the write itself are one step, so a
    /// racing publish to the same key reads what landed instead of the
    /// length it replaces. A refusal publishes nothing, and an inner failure
    /// hands the reservation back. Without a total limit there is nothing to
    /// *account*, but the publish still takes the lock: its identity is
    /// recorded beside the bytes, and the compensation guard's premise is the
    /// key's latest publish — a publish that recorded no tag would leave the
    /// previous one looking current.
    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Error> {
        let landed = staged.len;
        let _section = self.lock_accounting().await;
        self.publish_locked(bucket, staged.inner, landed, key)
            .await?;
        self.record_publish_tag(bucket, key, self.mint_publish_id());
        Ok(())
    }

    /// [`publish`](ObjectContent::publish) with the publish's identity
    /// recorded: the tag moves under the same accounting lock as the bytes, so
    /// no compensation can see one publish's bytes under another publish's
    /// tag. The tag is recorded even without a total limit — the refusal the
    /// guard exists for (a drained bucket, a refused row) has nothing to do
    /// with capacity.
    async fn publish_tagged(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
        id: PublishId,
    ) -> Result<(), Error> {
        let landed = staged.len;
        let _section = self.lock_accounting().await;
        self.publish_locked(bucket, staged.inner, landed, key)
            .await?;
        // Only a landed publish becomes the key's tag: a refusal or an inner
        // failure leaves the previous publish's tag in place.
        self.record_publish_tag(bucket, key, id);
        Ok(())
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        self.inner
            .open(bucket, key, range)
            .await
            .map_err(Into::into)
    }

    /// Release what the removal drops — the plane's `stat` for an object,
    /// the remembered length for a part — with the read and the release in
    /// one critical section, after the bytes are gone.
    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        if !self.counts_total() {
            return self.inner.remove(bucket, key).await.map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        self.remove_counted(bucket, key).await
    }

    /// Remove a whole batch's bytes in **one** inner call: every key's stored
    /// length is read inside the accounting lock, the batch is handed to the
    /// inner plane once (redb commits it as a single transaction), and only
    /// then is the batch's bytes released. A key repeated in the batch is
    /// released once, exactly as the per-key default would.
    ///
    /// An inner failure releases the batch anyway, exactly as
    /// `remove_counted` does and for the same reason (design §4):
    /// the caller's rows already dropped the bytes, so the books must
    /// follow, the failure is reported, and the error still reaches the
    /// caller.
    async fn remove_many(&self, bucket: &bucket::Name, keys: &[ContentKey]) -> Result<(), Error> {
        if !self.counts_total() {
            return self
                .inner
                .remove_many(bucket, keys)
                .await
                .map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        let mut held = Vec::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            // The inner plane removes each key once, so a key the batch
            // names again holds nothing by then.
            let len = if keys[..index].contains(key) {
                0
            } else {
                self.stored_len(bucket, key).await?
            };
            held.push(len);
        }
        let outcome = self.inner.remove_many(bucket, keys).await;
        if let Err(err) = &outcome {
            let released: u64 = held.iter().sum();
            report(format_args!(
                "the batch removal of {} keys failed ({err}); its {released} bytes leave the total anyway",
                keys.len()
            ));
        }
        for (key, len) in keys.iter().zip(held) {
            if outcome.is_err() {
                // The rows survived the release: the teardown that later
                // sweeps them must not release these bytes again.
                self.note_released_undead(bucket, key);
            }
            self.release_counted(bucket, key, len);
        }
        outcome.map_err(Into::into)
    }

    /// Remove `key`'s bytes only while they are still `expected`.
    ///
    /// The contract's default reads the bytes back, compares, and then
    /// removes them — two transactions, and the writer this guard exists for
    /// publishes between them, so the default can delete the bytes that just
    /// replaced them. This override delegates to the inner plane's own
    /// single-transaction compare-and-delete and books the result on
    /// `remove_counted`'s rule: the bytes come back to this caller
    /// only when the guard answers `true`. A `false` guard means the key
    /// holds someone else's bytes (or holds none), so neither the key nor the
    /// total moves. An inner failure reports and releases anyway, exactly as
    /// `remove_counted` does (design §4).
    ///
    /// Holding the accounting lock across the call is what keeps the
    /// remembered length and the guarded delete in one critical section.
    ///
    /// This stays a separate override from [`Self::remove_many`]: the two
    /// ask different questions. A batch removal commits "these keys are gone"
    /// with no comparison and no answer, while this one asks "are these bytes
    /// still mine?" and answers `bool`. Folding them behind an
    /// `expected: Option<&[u8]>` would either turn the batch into N
    /// single-key compare transactions — the very cost the batch exists to
    /// avoid — or force a `Vec<bool>` no batch caller wants; and it would
    /// simplify nobody, since each inner override is one `driver.write`.
    async fn remove_if_matches(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
    ) -> Result<bool, Error> {
        if !self.counts_total() {
            return self
                .inner
                .remove_if_matches(bucket, key, expected)
                .await
                .map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        self.remove_guarded_counted(bucket, key, expected, "guarded")
            .await
    }

    /// [`remove_if_matches`](ObjectContent::remove_if_matches) guarded by the
    /// publish identity as well as by the bytes: the removal happens only
    /// while the key still holds the tagged publish. A later writer's
    /// byte-identical content — two empty bodies — is then never taken, which
    /// the byte compare alone cannot promise. Taking the tag, the compare,
    /// the removal and the release share the accounting lock; a tag taken on
    /// a match also retires the entry, so the key's publish is claimed once.
    async fn remove_if_tagged(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        id: PublishId,
    ) -> Result<bool, Error> {
        let _section = self.lock_accounting().await;
        if !self.take_publish_tag(bucket, key, id) {
            // A later publish owns the key; equal bytes are not this call's
            // to take.
            return Ok(false);
        }
        self.remove_guarded_counted(bucket, key, expected, "tagged")
            .await
    }

    /// Move the source's bytes and drop the target's stored ones: the total
    /// changes by the target's old length alone (a self-rename moves
    /// nothing).
    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return self
                .inner
                .rename(bucket, from, to)
                .await
                .map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        self.rename_counted(bucket, from, to).await
    }

    /// Add the target's difference: the source's bytes arrive, the target's
    /// stored ones leave (a self-copy duplicates nothing).
    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return self.inner.copy(bucket, from, to).await.map_err(Into::into);
        }
        let _section = self.lock_accounting().await;
        self.copy_counted(bucket, from, to).await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Error> {
        self.inner
            .scan(bucket, prefix, after, limit)
            .await
            .map_err(Into::into)
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Error> {
        self.inner.stat(bucket, key).await.map_err(Into::into)
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.inner.create_bucket(bucket).await.map_err(Into::into)
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.inner.remove_bucket(bucket).await.map_err(Into::into)
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        self.inner.is_empty(bucket).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io, slice,
        sync::{Arc, atomic::AtomicBool},
    };

    use super::*;
    use crate::{
        _core::{multipart, storage},
        _store_redb::RedbContent,
    };

    /// The decorator over a fresh in-memory plane, with `options` enforced.
    fn counted(options: MemoryOptions) -> CountingContent<RedbContent> {
        CountingContent::new(RedbContent::in_memory().unwrap(), options)
    }

    /// Stage, write and publish `bytes` at `key` through the decorator.
    async fn put<C: ObjectContent>(
        content: &CountingContent<C>,
        bucket: &bucket::Name,
        key: &ContentKey,
        bytes: &[u8],
    ) -> Result<(), Error>
    where
        C::Error: Into<Error>,
    {
        let mut staged = content.stage(bucket, key).await?;
        staged.write(Bytes::copy_from_slice(bytes)).await?;
        content.publish(bucket, staged, key).await
    }

    /// Stage, write and publish `bytes` at `key`, crediting `releasing`
    /// against the total cap (the pre-release a multipart complete makes).
    async fn put_replacing(
        content: &CountingContent<RedbContent>,
        bucket: &bucket::Name,
        key: &ContentKey,
        bytes: &[u8],
        releasing: &[ContentKey],
    ) -> Result<(), Error> {
        let mut staged = content.stage(bucket, key).await?;
        staged.write(Bytes::copy_from_slice(bytes)).await?;
        content
            .publish_replacing(bucket, staged, key, releasing)
            .await
    }

    fn object_key(key: &str) -> ContentKey {
        ContentKey::Object(object::key(key).unwrap())
    }

    /// The object cap is refused while the chunks land: the crossing chunk
    /// fails, nothing is published, and the shared total stays untouched. At
    /// the cap the write lands.
    #[tokio::test]
    async fn the_object_cap_is_refused_at_stage_with_the_total_untouched() {
        let content = counted(MemoryOptions {
            max_object_bytes: Some(4),
            max_total_bytes: Some(100),
        });
        let bucket = bucket::name("data").unwrap();
        let key = object_key("a.bin");

        let mut staged = content.stage(&bucket, &key).await.unwrap();
        staged.write(Bytes::from_static(b"1234")).await.unwrap();
        let err = staged.write(Bytes::from_static(b"5")).await.unwrap_err();
        let Error::Storage(storage::Error::EntityTooLarge { size, limit }) = err else {
            panic!("expected EntityTooLarge, got {err:?}");
        };
        assert_eq!((size, limit), (5, 4));
        assert_eq!(content.total_bytes(), 0, "a refused stage bills nothing");
        assert!(
            content
                .stat(&bucket, key.object().unwrap())
                .await
                .unwrap()
                .is_none(),
            "a refused stage publishes nothing"
        );

        // The pre-flight probe answers the same typed refusal without
        // touching the total (the versioned path's early decision).
        let err = content.check_object_bytes(5).unwrap_err();
        assert!(matches!(
            err,
            Error::Storage(storage::Error::EntityTooLarge { size: 5, limit: 4 })
        ));
        content.check_object_bytes(4).unwrap();
        assert_eq!(content.total_bytes(), 0);

        // The cap is inclusive: exactly the limit lands.
        put(&content, &bucket, &key, b"1234").await.unwrap();
        assert_eq!(
            content
                .stat(&bucket, key.object().unwrap())
                .await
                .unwrap()
                .unwrap()
                .len,
            4
        );
        assert_eq!(content.total_bytes(), 4);
    }

    /// The total cap is enforced by the atomic reservation: eight racing
    /// 8-byte publishes cannot jointly pass a 16-byte cap, so exactly two
    /// land and the total is exactly the cap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_total_cap_is_enforced_atomically_under_racing_publishes() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(16),
        });
        let content = Arc::new(content);
        let bucket = bucket::name("cap").unwrap();

        let mut handles = Vec::new();
        for i in 0..8 {
            let content = Arc::clone(&content);
            let bucket = bucket.clone();
            handles.push(tokio::spawn(async move {
                put(&content, &bucket, &object_key(&format!("k{i}")), &[b'x'; 8]).await
            }));
        }
        let mut landed = 0;
        let mut refused = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(()) => landed += 1,
                Err(Error::Storage(storage::Error::EntityTooLarge { .. })) => refused += 1,
                Err(err) => panic!("unexpected failure: {err:?}"),
            }
        }
        assert_eq!(landed, 2, "the cap admits exactly two 8-byte objects");
        assert_eq!(refused, 6);
        assert_eq!(content.total_bytes(), 16);
        assert_eq!(content.total_bytes(), 16);
    }

    /// Two publishes to the **same** key race: the stored length is read and
    /// the difference reserved in one critical section, so the second bills
    /// only the difference and is not refused by the cap. Before the lock
    /// both read 0 and reserved 6, and the second crossed the 6-byte cap.
    #[tokio::test]
    async fn racing_publishes_to_one_key_bill_only_the_difference() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(6),
        });
        let content = Arc::new(content);
        let bucket = bucket::name("race").unwrap();
        let key = object_key("k.bin");

        let first = {
            let content = Arc::clone(&content);
            let bucket = bucket.clone();
            let key = key.clone();
            tokio::spawn(async move { put(&content, &bucket, &key, &[b'x'; 6]).await })
        };
        let second = {
            let content = Arc::clone(&content);
            let bucket = bucket.clone();
            let key = key.clone();
            tokio::spawn(async move { put(&content, &bucket, &key, &[b'x'; 6]).await })
        };
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        assert_eq!(
            content
                .stat(&bucket, key.object().unwrap())
                .await
                .unwrap()
                .unwrap()
                .len,
            6
        );
        assert_eq!(
            content.total_bytes(),
            6,
            "the total tracks what is stored, not the whole length twice"
        );
    }

    /// The delta rule: an overwrite of one key bills only the difference,
    /// never the whole new length (the audit's add-only rule billed ten
    /// 1 MiB overwrites as 10 MiB).
    #[tokio::test]
    async fn an_overwrite_bills_only_the_difference() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(10),
        });
        let bucket = bucket::name("data").unwrap();
        let key = object_key("a.bin");

        put(&content, &bucket, &key, b"123456").await.unwrap();
        assert_eq!(content.total_bytes(), 6);
        // The same length again: an add-only rule would bill 12 and refuse.
        put(&content, &bucket, &key, b"abcdef").await.unwrap();
        assert_eq!(content.total_bytes(), 6);
        // A shrinking overwrite hands the difference back.
        put(&content, &bucket, &key, b"12").await.unwrap();
        assert_eq!(content.total_bytes(), 2);
        // The freed capacity is usable: 2 + 8 fits the cap.
        put(&content, &bucket, &object_key("b.bin"), &[b'x'; 8])
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 10);
    }

    /// Remove releases what is stored, rename moves the source and drops the
    /// target's old bytes, copy adds the target's difference, and both
    /// self-moves are no-ops.
    #[tokio::test]
    async fn remove_rename_and_copy_move_the_total_by_the_stored_bytes() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(64),
        });
        let bucket = bucket::name("data").unwrap();
        let (a, b, c, d, e) = (
            object::key("a.bin").unwrap(),
            object::key("b.bin").unwrap(),
            object::key("c.bin").unwrap(),
            object::key("d.bin").unwrap(),
            object::key("e.bin").unwrap(),
        );

        put(&content, &bucket, &ContentKey::Object(a.clone()), b"12345")
            .await
            .unwrap();
        put(&content, &bucket, &ContentKey::Object(b.clone()), b"123")
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 8);

        content
            .remove(&bucket, &ContentKey::Object(a.clone()))
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 3, "remove releases the stored bytes");
        // Removing again is a no-op: nothing is stored any more.
        content
            .remove(&bucket, &ContentKey::Object(a))
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 3);

        // A self-rename moves nothing.
        content.rename(&bucket, &b, &b).await.unwrap();
        assert_eq!(content.total_bytes(), 3);

        content.rename(&bucket, &b, &c).await.unwrap();
        assert_eq!(
            content.total_bytes(),
            3,
            "a rename moves the bytes, it does not add them"
        );
        assert!(content.stat(&bucket, &b).await.unwrap().is_none());
        assert_eq!(content.stat(&bucket, &c).await.unwrap().unwrap().len, 3);

        // Renaming onto an occupied target drops the target's old bytes.
        put(&content, &bucket, &ContentKey::Object(d.clone()), b"1234")
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 7);
        content.rename(&bucket, &c, &d).await.unwrap();
        assert_eq!(content.total_bytes(), 3, "the target's old bytes leave");
        assert_eq!(content.stat(&bucket, &d).await.unwrap().unwrap().len, 3);

        // A self-copy duplicates nothing.
        content.copy(&bucket, &d, &d).await.unwrap();
        assert_eq!(content.total_bytes(), 3);

        // A copy adds the target's difference.
        content.copy(&bucket, &d, &e).await.unwrap();
        assert_eq!(content.total_bytes(), 6);
        content.copy(&bucket, &d, &e).await.unwrap();
        assert_eq!(
            content.total_bytes(),
            6,
            "copying onto the same key adds the same length once"
        );
    }

    /// A part key has no `stat` on the contract, so the decorator remembers
    /// the length its publish landed: re-uploading a part is metered by its
    /// difference exactly as an object overwrite is, and its removal
    /// releases the remembered length.
    #[tokio::test]
    async fn a_part_overwrite_is_metered_by_its_remembered_length() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(10),
        });
        let bucket = bucket::name("data").unwrap();
        let part = ContentKey::Part {
            upload_id: "u1".into(),
            number: multipart::part_number(1).unwrap(),
        };

        put(&content, &bucket, &part, b"12345678").await.unwrap();
        assert_eq!(content.total_bytes(), 8);
        // An overwrite at a smaller length hands the difference back.
        put(&content, &bucket, &part, b"12").await.unwrap();
        assert_eq!(content.total_bytes(), 2);
        content.remove(&bucket, &part).await.unwrap();
        assert_eq!(content.total_bytes(), 0);
        // The freed capacity is reusable.
        put(&content, &bucket, &part, b"1234567890").await.unwrap();
        assert_eq!(content.total_bytes(), 10);
    }

    /// A publish may be metered with the bytes it is about to displace: the
    /// credit widens the cap check only, so the reservation — and the total —
    /// stays exact and the release that follows lands the true number.
    #[tokio::test]
    async fn a_credit_widens_the_cap_check_without_shrinking_the_reservation() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(10),
        });
        let bucket = bucket::name("credit").unwrap();
        let part = |number: u32| ContentKey::Part {
            upload_id: "u1".into(),
            number: multipart::part_number(number).unwrap(),
        };
        let (first, second) = (part(1), part(2));

        put(&content, &bucket, &first, b"123456").await.unwrap();
        assert_eq!(content.total_bytes(), 6);

        // Without a credit, the second six bytes cross the ten-byte cap.
        let err = put(&content, &bucket, &second, b"123456")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Storage(storage::Error::EntityTooLarge {
                size: 12,
                limit: 10
            })
        ));
        assert_eq!(content.total_bytes(), 6, "a refusal bills nothing");

        // With the credit — the part this complete is about to release — it
        // lands, and the reservation is the exact delta: 6 + 6, not 6 + 0.
        put_replacing(
            &content,
            &bucket,
            &second,
            b"123456",
            slice::from_ref(&first),
        )
        .await
        .unwrap();
        assert_eq!(
            content.total_bytes(),
            12,
            "a credit widens the check, never the reservation"
        );

        // The release that follows lands the total on the bytes actually
        // held.
        content
            .remove_many(&bucket, slice::from_ref(&first))
            .await
            .unwrap();
        assert_eq!(content.total_bytes(), 6);
        assert_eq!(content.stored_len(&bucket, &second).await.unwrap(), 6);
    }

    /// A batch removal commits once and releases exactly the batch's stored
    /// bytes — including a part's remembered length — and a repeated call has
    /// nothing left to release. A key the batch names twice is released once,
    /// as the per-key default would.
    #[tokio::test]
    async fn remove_many_releases_the_whole_batch_once() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(64),
        });
        let bucket = bucket::name("batch").unwrap();
        let a = object_key("a.bin");
        let b = object_key("b.bin");
        let part = ContentKey::Part {
            upload_id: "u1".into(),
            number: multipart::part_number(1).unwrap(),
        };

        put(&content, &bucket, &a, b"12345").await.unwrap();
        put(&content, &bucket, &b, b"123").await.unwrap();
        put(&content, &bucket, &part, b"1234567").await.unwrap();
        assert_eq!(content.total_bytes(), 15);

        let batch = [a.clone(), b.clone(), part.clone()];
        content.remove_many(&bucket, &batch).await.unwrap();
        assert_eq!(
            content.total_bytes(),
            0,
            "the batch's bytes are released once"
        );
        assert!(
            content
                .stat(&bucket, a.object().unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            content
                .stat(&bucket, b.object().unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(content.stored_len(&bucket, &part).await.unwrap(), 0);

        // Idempotent: nothing is stored, so nothing is released again.
        content.remove_many(&bucket, &batch).await.unwrap();
        assert_eq!(content.total_bytes(), 0);

        // A repeated key leaves once, not twice.
        put(&content, &bucket, &a, b"12345").await.unwrap();
        put(&content, &bucket, &b, b"123").await.unwrap();
        assert_eq!(content.total_bytes(), 8);
        content
            .remove_many(&bucket, &[a.clone(), a.clone()])
            .await
            .unwrap();
        assert_eq!(
            content.total_bytes(),
            3,
            "the repeated key's bytes leave once"
        );
    }

    /// An inner plane whose removals fail on demand, and whose read-back can
    /// be poisoned. The compensation's own failure (design §4) has no other
    /// way to happen in a unit test — the real plane answers a missing key by
    /// doing nothing — and the poisoned `open` proves the guarded removal
    /// never falls back to the contract's read-compare-remove default.
    struct FlakyRemove {
        inner: RedbContent,
        failing: AtomicBool,
        no_readback: AtomicBool,
    }

    /// The inner plane's error type, named so the double can raise one.
    type PlaneError = <RedbContent as ObjectContent>::Error;

    #[async_trait]
    impl ObjectContent for FlakyRemove {
        type Error = PlaneError;
        type Staged = <RedbContent as ObjectContent>::Staged;

        async fn stage(
            &self,
            bucket: &bucket::Name,
            key: &ContentKey,
        ) -> Result<Self::Staged, PlaneError> {
            self.inner.stage(bucket, key).await
        }

        async fn publish(
            &self,
            bucket: &bucket::Name,
            staged: Self::Staged,
            key: &ContentKey,
        ) -> Result<(), PlaneError> {
            self.inner.publish(bucket, staged, key).await
        }

        async fn open(
            &self,
            bucket: &bucket::Name,
            key: &ContentKey,
            range: Option<ByteRange>,
        ) -> Result<BodyStream, PlaneError> {
            if self.no_readback.load(Ordering::Relaxed) {
                return Err(PlaneError::Io(io::Error::other("read-back poisoned")));
            }
            self.inner.open(bucket, key, range).await
        }

        async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), PlaneError> {
            if self.failing.load(Ordering::Relaxed) {
                return Err(PlaneError::Io(io::Error::other("injected removal failure")));
            }
            self.inner.remove(bucket, key).await
        }

        async fn remove_many(
            &self,
            bucket: &bucket::Name,
            keys: &[ContentKey],
        ) -> Result<(), PlaneError> {
            if self.failing.load(Ordering::Relaxed) {
                return Err(PlaneError::Io(io::Error::other(
                    "injected batch removal failure",
                )));
            }
            self.inner.remove_many(bucket, keys).await
        }

        async fn remove_if_matches(
            &self,
            bucket: &bucket::Name,
            key: &ContentKey,
            expected: &[u8],
        ) -> Result<bool, PlaneError> {
            if self.failing.load(Ordering::Relaxed) {
                return Err(PlaneError::Io(io::Error::other(
                    "injected guarded removal failure",
                )));
            }
            self.inner.remove_if_matches(bucket, key, expected).await
        }

        async fn rename(
            &self,
            bucket: &bucket::Name,
            from: &object::Key,
            to: &object::Key,
        ) -> Result<(), PlaneError> {
            self.inner.rename(bucket, from, to).await
        }

        async fn copy(
            &self,
            bucket: &bucket::Name,
            from: &object::Key,
            to: &object::Key,
        ) -> Result<(), PlaneError> {
            self.inner.copy(bucket, from, to).await
        }

        async fn scan(
            &self,
            bucket: &bucket::Name,
            prefix: &str,
            after: Option<&object::Key>,
            limit: usize,
        ) -> Result<Vec<ContentEntry>, PlaneError> {
            self.inner.scan(bucket, prefix, after, limit).await
        }

        async fn stat(
            &self,
            bucket: &bucket::Name,
            key: &object::Key,
        ) -> Result<Option<ContentEntry>, PlaneError> {
            self.inner.stat(bucket, key).await
        }

        async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), PlaneError> {
            self.inner.create_bucket(bucket).await
        }

        async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), PlaneError> {
            self.inner.remove_bucket(bucket).await
        }

        async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, PlaneError> {
            self.inner.is_empty(bucket).await
        }
    }

    /// Design §4: the caller's rows already dropped the bytes, so a
    /// compensation that itself fails still moves the books — loudly — and
    /// still hands the failure back.
    #[tokio::test]
    async fn a_failed_inner_removal_still_releases_the_bytes() {
        let content = CountingContent::new(
            FlakyRemove {
                inner: RedbContent::in_memory().unwrap(),
                failing: AtomicBool::new(false),
                no_readback: AtomicBool::new(false),
            },
            MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(64),
            },
        );
        let bucket_name = bucket::name("compensating").unwrap();
        let first = object_key("a.bin");
        let second = object_key("b.bin");
        put(&content, &bucket_name, &first, b"12345").await.unwrap();
        put(&content, &bucket_name, &second, b"123").await.unwrap();
        assert_eq!(content.total_bytes(), 8);

        content.inner.failing.store(true, Ordering::Relaxed);
        let err = content.remove(&bucket_name, &first).await.unwrap_err();
        assert!(
            err.to_string().contains("injected removal failure"),
            "{err:?}"
        );
        assert_eq!(
            content.total_bytes(),
            3,
            "a failed removal still releases the bytes its caller dropped"
        );
        assert!(
            content.released_undead(&bucket_name, &first),
            "the failed removal marks the key, so a teardown sweeping its surviving rows cannot release it twice"
        );
        content.purge_released_undead(&bucket_name);
        assert!(
            !content.released_undead(&bucket_name, &first),
            "the teardown's purge clears the bucket's marks"
        );

        let err = content
            .remove_many(&bucket_name, &[second])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected batch removal failure"),
            "{err:?}"
        );
        assert_eq!(
            content.total_bytes(),
            0,
            "a failed batch still releases every key it was given"
        );
    }

    /// The guarded removal's failure arm books its own release, exactly once —
    /// the rule `upload_part`'s caller leans on: its drop half must not
    /// subtract the part's remembered length a second time, or the total is
    /// billed below the bytes the plane still holds (a part's stored length IS
    /// its ledger entry, and the entry leaves with this one release).
    #[tokio::test]
    async fn a_failed_guarded_removal_releases_a_parts_bytes_exactly_once() {
        let content = CountingContent::new(
            FlakyRemove {
                inner: RedbContent::in_memory().unwrap(),
                failing: AtomicBool::new(false),
                no_readback: AtomicBool::new(false),
            },
            MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(64),
            },
        );
        let bucket_name = bucket::name("guarded-failure").unwrap();
        let keeper = object_key("keeper.bin");
        put(&content, &bucket_name, &keeper, b"12345")
            .await
            .unwrap();
        let part = ContentKey::Part {
            upload_id: "u".to_owned(),
            number: multipart::part_number(1).unwrap(),
        };
        put(&content, &bucket_name, &part, b"abc").await.unwrap();
        assert_eq!(content.total_bytes(), 8, "the keeper plus the part");

        content.inner.failing.store(true, Ordering::Relaxed);
        let err = content
            .remove_if_matches(&bucket_name, &part, b"abc")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected guarded removal failure"),
            "{err:?}"
        );
        assert_eq!(
            content.total_bytes(),
            5,
            "the part's 3 bytes leave once — 5 is the keeper, not 2"
        );
    }

    /// The guarded removal books only what it took back: `true` releases the
    /// key's bytes (a part's remembered length included), `false` — someone
    /// else's bytes, or none — moves nothing.
    #[tokio::test]
    async fn a_guarded_removal_releases_only_the_bytes_it_took_back() {
        let content = counted(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(64),
        });
        let bucket = bucket::name("guarded").unwrap();
        let key = object_key("a.bin");
        put(&content, &bucket, &key, b"12345").await.unwrap();
        assert_eq!(content.total_bytes(), 5);

        // Someone else published here: the guard refuses, the bytes and the
        // books stay.
        assert!(
            !content
                .remove_if_matches(&bucket, &key, b"other")
                .await
                .unwrap()
        );
        assert_eq!(content.total_bytes(), 5, "a refused guard moves nothing");
        assert_eq!(content.stored_len(&bucket, &key).await.unwrap(), 5);

        // Our bytes: the guard takes them back and the total follows.
        assert!(
            content
                .remove_if_matches(&bucket, &key, b"12345")
                .await
                .unwrap()
        );
        assert_eq!(content.total_bytes(), 0, "the taken-back bytes leave");

        // Absence is not a match, and not an error.
        assert!(
            !content
                .remove_if_matches(&bucket, &key, b"12345")
                .await
                .unwrap()
        );
        assert_eq!(content.total_bytes(), 0);

        // A part's remembered length leaves with its bytes.
        let part = ContentKey::Part {
            upload_id: "u1".into(),
            number: multipart::part_number(1).unwrap(),
        };
        put(&content, &bucket, &part, b"1234567").await.unwrap();
        assert_eq!(content.total_bytes(), 7);
        assert!(
            !content
                .remove_if_matches(&bucket, &part, b"other")
                .await
                .unwrap()
        );
        assert_eq!(content.total_bytes(), 7);
        assert!(
            content
                .remove_if_matches(&bucket, &part, b"1234567")
                .await
                .unwrap()
        );
        assert_eq!(content.total_bytes(), 0);
        assert_eq!(
            content.stored_len(&bucket, &part).await.unwrap(),
            0,
            "the ledger row left with the bytes"
        );
    }

    /// The override must not fall back to the contract's read-compare-remove
    /// default: this plane's `open` is poisoned, and the guarded removal
    /// still answers `true` because the comparison and the delete share the
    /// inner plane's own transaction.
    #[tokio::test]
    async fn a_guarded_removal_never_reads_the_bytes_back() {
        let content = CountingContent::new(
            FlakyRemove {
                inner: RedbContent::in_memory().unwrap(),
                failing: AtomicBool::new(false),
                no_readback: AtomicBool::new(true),
            },
            MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(64),
            },
        );
        let bucket_name = bucket::name("no-readback").unwrap();
        let key = object_key("a.bin");
        put(&content, &bucket_name, &key, b"12345").await.unwrap();
        assert_eq!(content.total_bytes(), 5);
        assert!(
            content
                .remove_if_matches(&bucket_name, &key, b"12345")
                .await
                .unwrap(),
            "the guard answers from the inner transaction, not from a read-back"
        );
        assert_eq!(content.total_bytes(), 0);
    }
}
