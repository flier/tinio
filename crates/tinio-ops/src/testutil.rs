//! The in-memory reference base for the engine's own tests.
//!
//! One in-memory database serves both planes — the row plane
//! ([`RedbMetadata::in_memory`]) and the content plane
//! ([`RedbContent::from_driver`] over that same database, so the two see one
//! another's buckets) — one in-memory [`BlockStore`] double stands in for a
//! backend's block plane, and one recording [`Hooks`] double stands in for a
//! backend's seam. This is the base the spec's E7 pins; it carries no backend
//! crate and no filesystem.
//!
//! Three instruments sit on top of the base, each one the pin for a claim a
//! test must not take on faith: [`TestMeta`] delegates every contract method to
//! the reference rows and **counts** the calls a cost rule names (one
//! `ObjectMeta::load` per listing page, the guarded or plain row write, the
//! preserving heal), [`BlockMem`] is the in-memory block plane the version
//! engine runs over, and [`RecordingContent`] logs the content-plane calls a
//! body makes (a copy that is a `copy`, a rename that is a `rename`) and
//! snapshots the key's history rows **at publish time** — the one observation
//! that pins the supersede-before-publish order.

use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
pub(crate) use tinio_util::testing::blocks::MemBlocks as BlockMem;

use crate::{
    _blob::Store,
    _core::{
        ByteRange, ETag, MultipartUpload, acl,
        blob::BlobRef,
        bucket, checksum,
        content::{ContentEntry, ContentKey, ObjectContent, PublishId, StagedObject},
        cors, lifecycle, meta, multipart, object,
        storage::{self, BodyStream},
        version::{Id, State},
    },
    _store_redb::{RedbContent, RedbMetadata},
    _version::{Current, CurrentRows, MarkerWrite, Versions},
    BucketOps, BucketRow, Guard, Hooks, MetadataFailure, MultipartOps, ObjectOps, VersionPlane,
    object::row_matches,
};

/// One body stream of borrowed chunks — the crate's tests' one builder, so
/// every body a test hands the engine is spelled the same way.
pub(crate) fn body(chunks: &[&'static [u8]]) -> BodyStream {
    let chunks: Vec<io::Result<Bytes>> = chunks
        .iter()
        .map(|chunk| Ok(Bytes::from_static(chunk)))
        .collect();
    Box::pin(stream::iter(chunks))
}

/// One call the recording hooks double saw.
///
/// The hook's own calls are logged here, and so are the two events that
/// bracket them: a part row the counting row plane wrote, and a content-plane
/// call the recording content double served (when a test wires one to the same
/// timeline). One ordered vector is what makes a *window* assertable — the
/// part lock's span is a claim about the order of three different
/// collaborators, not about one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    /// [`Hooks::before_bucket_delete`] for the named bucket.
    BeforeBucketDelete(String),
    /// [`Hooks::lock_part`] took the named upload's part guard.
    LockPart {
        /// The upload whose part guard was taken.
        upload_id: String,
    },
    /// The guard that [`Hooks::lock_part`] answered was dropped (the engine's
    /// release point).
    LockReleased {
        /// The upload whose part guard was released.
        upload_id: String,
    },
    /// The row plane wrote one part row of the named upload.
    PartRow {
        /// The upload the part row belongs to.
        upload_id: String,
    },
    /// One chunk of a **test's own body stream** was yielded. A test logs it
    /// when the claim it pins is about *when* the engine reads the body (the
    /// part lock is taken after the stream, not before it), which no hook or
    /// plane can observe: the stream is the only collaborator that knows.
    Body,
    /// One content-plane call the recording content double served.
    Content(ContentCall),
}

/// The recording [`Hooks`] double: it logs every call — and, for the part
/// lock, the guard's whole life — and a refusing one answers the bucket delete
/// with `NotEmpty` (the commit point's error arm).
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordingHooks {
    calls: Arc<Mutex<Vec<Call>>>,
    refuse_delete: bool,
}

impl RecordingHooks {
    /// A hook that records and never refuses.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A hook that records and refuses the bucket delete.
    pub(crate) fn refusing() -> Self {
        Self {
            calls: Arc::default(),
            refuse_delete: true,
        }
    }

    /// Every call this double saw, in order.
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("the call log").clone()
    }

    /// The ordered log itself — the timeline a test wires its other doubles to,
    /// so a window that spans them is one vector.
    pub(crate) fn timeline(&self) -> Arc<Mutex<Vec<Call>>> {
        Arc::clone(&self.calls)
    }

    /// Log one event on the timeline.
    pub(crate) fn log(&self, call: Call) {
        self.calls.lock().expect("the call log").push(call);
    }
}

/// The guard [`RecordingHooks::lock_part`] answers: it logs the release when
/// the engine drops it, so the lock's span is observable end to end.
pub(crate) struct RecordedGuard {
    calls: Arc<Mutex<Vec<Call>>>,
    upload_id: String,
}

impl Drop for RecordedGuard {
    fn drop(&mut self) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(Call::LockReleased {
                upload_id: self.upload_id.clone(),
            });
        }
    }
}

impl Guard for RecordedGuard {}

#[async_trait]
impl Hooks for RecordingHooks {
    async fn before_bucket_delete(&self, bucket: &bucket::Name) -> Result<(), storage::Error> {
        self.log(Call::BeforeBucketDelete(bucket.to_string()));
        if self.refuse_delete {
            return Err(storage::not_empty(bucket));
        }
        Ok(())
    }

    async fn lock_part(&self, _bucket: &bucket::Name, upload_id: &str) -> Box<dyn Guard> {
        self.log(Call::LockPart {
            upload_id: upload_id.to_string(),
        });
        Box::new(RecordedGuard {
            calls: Arc::clone(&self.calls),
            upload_id: upload_id.to_string(),
        })
    }
}

/// The row error the reference store answers, spelled once.
type RowError = <RedbMetadata as meta::Metadata>::Error;

/// The contract calls a test counts, per body.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    /// `ObjectMeta::load` — the listing page's one enrichment batch.
    pub(crate) loads: usize,
    /// `ObjectMeta::put` — a plain (unguarded) object-row write.
    pub(crate) puts: usize,
    /// `ObjectMeta::put_guarded` — a guarded object-row write.
    pub(crate) put_guarded: usize,
    /// `ObjectMeta::put_preserving` — the read-heal's refresh.
    pub(crate) put_preserving: usize,
    /// `ObjectMeta::put_with_parts` — the row and its retained parts.
    pub(crate) put_with_parts: usize,
    /// `ObjectMeta::move_` — the rename's metadata move.
    pub(crate) moves: usize,
    /// `ObjectMeta::get` — the point row read.
    pub(crate) gets: usize,
    /// `ObjectMeta::parts` — the retained part list.
    pub(crate) parts: usize,
}

/// The counting row plane: every contract call delegates to the in-memory
/// reference implementation, and the handful a cost rule names are tallied.
///
/// It is also the base's [`CurrentRows`] implementation — the gated current
/// slot a version plane needs. The gate is the read-path rule (the recorded
/// identity is served only while the content still matches it) and the slot's
/// sequence is the one the last [`meta::VersionMeta::advance_slot`] recorded,
/// because the contract's object row carries no version columns (D37) and the
/// slot is the only home of that sequence.
#[derive(Debug, Clone)]
pub(crate) struct TestMeta {
    inner: Arc<RedbMetadata>,
    content: Arc<RedbContent>,
    counts: Arc<Mutex<Counts>>,
    slots: Arc<Mutex<HashMap<(String, String), u64>>>,
    /// The keys whose row writes this double refuses (a hard I/O failure, not
    /// a refusal the engine may degrade): the fault a backend's row plane
    /// answers when the store is broken, which a read path must propagate.
    refused_writes: Arc<Mutex<HashSet<(String, String)>>>,
    /// Refuse the next `put_with_parts` once. Later restorative writes are
    /// not this refusal: they are what puts the outgoing version back.
    refuse_next_put_with_parts: Arc<AtomicBool>,
    /// Refuse the next slot advance once. The row write has already landed;
    /// the undo puts the outgoing version back without advancing again.
    refuse_next_advance_slot: Arc<AtomicBool>,
    /// Refuse the next marker-port write once, before it changes a row. A
    /// versioned delete has already superseded and removed the key; the
    /// engine puts that version back.
    refuse_next_marker: Arc<AtomicBool>,
    /// The next `version_seq_of` answer, once: a stale index entry the store
    /// itself will not produce, so a delete can be shown refusing it.
    next_version_seq: Arc<Mutex<Option<u64>>>,
    /// The next `latest` answers `None`, once. The contract's corrupt-newest
    /// row does that while older rows stay readable; the store will not plant
    /// a corrupt row through its public writes.
    hide_latest: Arc<AtomicBool>,
    /// The base's shared timeline: the events a *window* spans land here
    /// beside the hook's own calls (see [`Call`]).
    timeline: Arc<Mutex<Vec<Call>>>,
    /// The upload the next `UploadMeta::get` drains **behind** its own
    /// answer: the window in which a body failure races a concurrent abort,
    /// which no stream-side side effect can build (`BodyStream` is `Sync`,
    /// and every contract call's future is not).
    vanish_after_get: Arc<Mutex<Option<(String, String)>>>,
}

impl TestMeta {
    /// Wrap the reference rows and the content plane they describe.
    pub(crate) fn new(
        inner: Arc<RedbMetadata>,
        content: Arc<RedbContent>,
        timeline: Arc<Mutex<Vec<Call>>>,
    ) -> Self {
        Self {
            inner,
            content,
            counts: Arc::default(),
            slots: Arc::default(),
            refused_writes: Arc::default(),
            refuse_next_put_with_parts: Arc::default(),
            refuse_next_advance_slot: Arc::default(),
            refuse_next_marker: Arc::default(),
            next_version_seq: Arc::default(),
            hide_latest: Arc::default(),
            timeline,
            vanish_after_get: Arc::default(),
        }
    }

    /// Drain `upload_id` behind the next `UploadMeta::get` that answers it:
    /// the upload is served once more, then it is gone — the race a body
    /// stream can lose.
    pub(crate) fn vanish_upload_after_get(&self, bucket: &bucket::Name, upload_id: &str) {
        *self.vanish_after_get.lock().expect("the vanish slot") =
            Some((bucket.to_string(), upload_id.to_string()));
    }

    /// Log one event on the base's timeline.
    fn log(&self, call: Call) {
        self.timeline.lock().expect("the call log").push(call);
    }

    /// The calls counted so far.
    pub(crate) fn counts(&self) -> Counts {
        *self.counts.lock().expect("the count log")
    }

    /// Refuse every row write for `key` from now on, with a hard I/O failure.
    pub(crate) fn refuse_writes_for(&self, bucket: &bucket::Name, key: &object::Key) {
        self.refused_writes
            .lock()
            .expect("the refusal set")
            .insert((bucket.to_string(), key.to_string()));
    }

    /// Refuse the next object-row `put_with_parts`, once.
    pub(crate) fn refuse_next_put_with_parts(&self) {
        self.refuse_next_put_with_parts
            .store(true, AtomicOrdering::Relaxed);
    }

    /// Refuse the next version-slot advance, once.
    pub(crate) fn refuse_next_advance_slot(&self) {
        self.refuse_next_advance_slot
            .store(true, AtomicOrdering::Relaxed);
    }

    /// Refuse the next marker-port write, once, before it changes a row.
    pub(crate) fn refuse_next_marker(&self) {
        self.refuse_next_marker.store(true, AtomicOrdering::Relaxed);
    }

    /// Answer the next `version_seq_of` with `seq`, once. The row at that
    /// sequence keeps its own identity, so the index and the row disagree.
    pub(crate) fn answer_next_version_seq(&self, seq: u64) {
        *self.next_version_seq.lock().expect("the seq alias") = Some(seq);
    }

    /// Answer the next `latest` with `None`, once.
    pub(crate) fn hide_next_latest(&self) {
        self.hide_latest.store(true, AtomicOrdering::Relaxed);
    }

    /// The refusal this double owes a row write for `(bucket, key)`.
    fn refusal(&self, bucket: &bucket::Name, key: &object::Key) -> Option<RowError> {
        self.refused_writes
            .lock()
            .expect("the refusal set")
            .contains(&(bucket.to_string(), key.to_string()))
            .then(|| io::Error::other("the row plane refused this key's write").into())
    }

    /// Tally one counted call.
    fn counted(&self, what: impl Fn(&mut Counts)) {
        what(&mut self.counts.lock().expect("the count log"));
    }
}

/// The reference contracts, delegated whole: one method per contract method,
/// so a test's counter observes what the engine actually called — and the one
/// method a *window* claim names (`put_part`, the part row the part lock
/// spans) also lands on the timeline.
macro_rules! forward {
    ($trait:path { $($name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)* }) => {
        #[async_trait]
        impl $trait for TestMeta {
            $(
                async fn $name(&self, $($arg: $ty),*) -> $ret {
                    <RedbMetadata as $trait>::$name(&self.inner, $($arg),*).await
                }
            )*
        }
    };
}

forward!(meta::BucketMeta {
    created_at(bucket: &bucket::Name) -> Result<Option<SystemTime>, RowError>;
    load_many(buckets: &[bucket::Name]) -> Result<Vec<Option<SystemTime>>, RowError>;
    load_all() -> Result<Vec<(String, SystemTime)>, RowError>;
    row(bucket: &bucket::Name) -> Result<Option<meta::bucket::Row>, RowError>;
    owners(buckets: &[bucket::Name]) -> Result<Vec<Option<acl::OwnerId>>, RowError>;
    record(bucket: &bucket::Name, created: SystemTime) -> Result<(), RowError>;
    create(bucket: &bucket::Name, row: &meta::bucket::Row) -> Result<(), RowError>;
    record_if_absent(bucket: &bucket::Name, now: SystemTime) -> Result<SystemTime, RowError>;
    record_full(bucket: &bucket::Name, row: &meta::bucket::Row) -> Result<(), RowError>;
    set_tags(bucket: &bucket::Name, tags: &object::Tags) -> Result<bool, RowError>;
    set_tags_or_create(bucket: &bucket::Name, tags: &object::Tags, now: SystemTime) -> Result<(), RowError>;
    clear_tags(bucket: &bucket::Name) -> Result<bool, RowError>;
    set_acl(bucket: &bucket::Name, owner: Option<&acl::OwnerId>, grants: &acl::AclGrants) -> Result<bool, RowError>;
    set_grants_or_create(bucket: &bucket::Name, grants: &acl::AclGrants, now: SystemTime) -> Result<(), RowError>;
    set_cors(bucket: &bucket::Name, config: &cors::Config) -> Result<bool, RowError>;
    clear_cors(bucket: &bucket::Name) -> Result<bool, RowError>;
    lifecycle(bucket: &bucket::Name) -> Result<Option<lifecycle::Config>, RowError>;
    lifecycles() -> Result<Vec<(bucket::Name, lifecycle::Config)>, RowError>;
    set_lifecycle(bucket: &bucket::Name, config: &lifecycle::Config) -> Result<bool, RowError>;
    set_lifecycle_or_create(bucket: &bucket::Name, config: &lifecycle::Config, now: SystemTime) -> Result<(), RowError>;
    merge_lifecycle(bucket: &bucket::Name, rules: &[lifecycle::Rule]) -> Result<lifecycle::Merged, RowError>;
    merge_lifecycle_or_create(bucket: &bucket::Name, rules: &[lifecycle::Rule], now: SystemTime) -> Result<lifecycle::Merged, RowError>;
    clear_lifecycle(bucket: &bucket::Name) -> Result<bool, RowError>;
    remove(bucket: &bucket::Name) -> Result<(), RowError>;
    remove_if_empty(bucket: &bucket::Name) -> Result<(), RowError>;
});

/// The object rows, with the counted calls tallied around the delegation.
#[async_trait]
impl meta::ObjectMeta for TestMeta {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<meta::object::Row>, RowError> {
        self.counted(|counts| counts.gets += 1);
        <RedbMetadata as meta::ObjectMeta>::get(&self.inner, bucket, key).await
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &meta::object::Row,
    ) -> Result<(), RowError> {
        self.counted(|counts| counts.puts += 1);
        if let Some(refusal) = self.refusal(bucket, key) {
            return Err(refusal);
        }
        <RedbMetadata as meta::ObjectMeta>::put(&self.inner, bucket, key, row).await
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &meta::object::Row,
    ) -> Result<(), RowError> {
        self.counted(|counts| counts.put_guarded += 1);
        if let Some(refusal) = self.refusal(bucket, key) {
            return Err(refusal);
        }
        <RedbMetadata as meta::ObjectMeta>::put_guarded(&self.inner, bucket, key, row).await
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &meta::object::Row,
        parts: &[meta::object::PartRow],
    ) -> Result<(), RowError> {
        self.counted(|counts| counts.put_with_parts += 1);
        if self
            .refuse_next_put_with_parts
            .swap(false, AtomicOrdering::Relaxed)
        {
            return Err(io::Error::other("the row plane refused this key's write").into());
        }
        if let Some(refusal) = self.refusal(bucket, key) {
            return Err(refusal);
        }
        <RedbMetadata as meta::ObjectMeta>::put_with_parts(&self.inner, bucket, key, row, parts)
            .await
    }

    async fn has_objects(&self, bucket: &bucket::Name) -> Result<bool, RowError> {
        <RedbMetadata as meta::ObjectMeta>::has_objects(&self.inner, bucket).await
    }

    async fn parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<meta::object::PartRow>, RowError> {
        self.counted(|counts| counts.parts += 1);
        <RedbMetadata as meta::ObjectMeta>::parts(&self.inner, bucket, key).await
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &meta::object::Identity,
    ) -> Result<meta::object::Preserved, RowError> {
        self.counted(|counts| counts.put_preserving += 1);
        if let Some(refusal) = self.refusal(bucket, key) {
            return Err(refusal);
        }
        <RedbMetadata as meta::ObjectMeta>::put_preserving(&self.inner, bucket, key, identity).await
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), RowError> {
        self.counted(|counts| counts.moves += 1);
        <RedbMetadata as meta::ObjectMeta>::move_(&self.inner, bucket, from, to).await
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, RowError> {
        <RedbMetadata as meta::ObjectMeta>::set_tags(&self.inner, bucket, key, tags).await
    }

    async fn clear_tags(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, RowError> {
        <RedbMetadata as meta::ObjectMeta>::clear_tags(&self.inner, bucket, key).await
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::ObjectMeta>::ensure_tags(&self.inner, bucket, key, tags).await
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, RowError> {
        <RedbMetadata as meta::ObjectMeta>::set_acl(&self.inner, bucket, key, owner, grants).await
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, meta::object::Row)],
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::ObjectMeta>::put_batch(&self.inner, bucket, rows).await
    }

    async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, RowError> {
        <RedbMetadata as meta::ObjectMeta>::remove(&self.inner, bucket, key).await
    }

    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::ObjectMeta>::remove_many(&self.inner, bucket, keys).await
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<meta::object::Row>>, RowError> {
        self.counted(|counts| counts.loads += 1);
        <RedbMetadata as meta::ObjectMeta>::load(&self.inner, bucket, keys).await
    }

    async fn walk(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, meta::object::Row)>, RowError> {
        <RedbMetadata as meta::ObjectMeta>::walk(&self.inner, bucket).await
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<meta::object::Row>)>, RowError> {
        <RedbMetadata as meta::ObjectMeta>::walk_healing(&self.inner, bucket).await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), RowError> {
        <RedbMetadata as meta::ObjectMeta>::drain_bucket(&self.inner, bucket).await
    }
}

/// The upload rows, delegated, with the part row — the one write the part
/// lock's span names — also landing on the timeline (and honouring this
/// double's refusal set, so a refused part row is testable like any other row
/// write).
#[async_trait]
impl meta::UploadMeta for TestMeta {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &meta::upload::Row,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::UploadMeta>::create(&self.inner, bucket, upload_id, row).await
    }

    async fn create_guarded(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &meta::upload::Row,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::UploadMeta>::create_guarded(&self.inner, bucket, upload_id, row)
            .await
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<meta::upload::Row>, RowError> {
        let row =
            <RedbMetadata as meta::UploadMeta>::get(&self.inner, bucket, key, upload_id).await?;
        let vanishing = {
            let mut slot = self.vanish_after_get.lock().expect("the vanish slot");
            let matches = slot
                .as_ref()
                .is_some_and(|(b, id)| b == bucket.as_ref().as_str() && id == upload_id);
            matches.then(|| slot.take()).flatten()
        };
        if row.is_some() && vanishing.is_some() {
            <RedbMetadata as meta::UploadMeta>::drain_upload_checked(
                &self.inner,
                bucket,
                key,
                upload_id,
            )
            .await?;
        }
        Ok(row)
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: multipart::PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), RowError> {
        if let Some(refusal) = self.refusal(bucket, key) {
            return Err(refusal);
        }
        self.log(Call::PartRow {
            upload_id: upload_id.to_string(),
        });
        <RedbMetadata as meta::UploadMeta>::put_part(
            &self.inner,
            bucket,
            key,
            upload_id,
            number,
            etag,
            checksum,
        )
        .await
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: multipart::PartNumber,
    ) -> Result<Vec<meta::upload::PartRow>, RowError> {
        <RedbMetadata as meta::UploadMeta>::list_parts(&self.inner, bucket, upload_id, from).await
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<meta::upload::PartsPage, RowError> {
        <RedbMetadata as meta::UploadMeta>::list_parts_page(
            &self.inner,
            bucket,
            upload_id,
            from,
            max,
        )
        .await
    }

    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[multipart::PartNumber],
    ) -> Result<Option<Vec<Option<meta::upload::PartRecord>>>, RowError> {
        <RedbMetadata as meta::UploadMeta>::part_records(
            &self.inner,
            bucket,
            key,
            upload_id,
            numbers,
        )
        .await
    }

    async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, RowError> {
        <RedbMetadata as meta::UploadMeta>::list_uploads(&self.inner, bucket).await
    }

    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), RowError> {
        <RedbMetadata as meta::UploadMeta>::list_uploads_page(
            &self.inner,
            bucket,
            prefix,
            delimiter,
            marker,
            max_uploads,
        )
        .await
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, RowError> {
        <RedbMetadata as meta::UploadMeta>::has_uploads(&self.inner, bucket).await
    }

    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, RowError> {
        <RedbMetadata as meta::UploadMeta>::walk_uploads(&self.inner).await
    }

    async fn drain_upload(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), RowError> {
        <RedbMetadata as meta::UploadMeta>::drain_upload(&self.inner, bucket, upload_id).await
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::UploadMeta>::drain_upload_checked(
            &self.inner,
            bucket,
            key,
            upload_id,
        )
        .await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), RowError> {
        <RedbMetadata as meta::UploadMeta>::drain_bucket(&self.inner, bucket).await
    }
}

/// The version rows, delegated, with the slot advance also **recording** its
/// sequence: the contract's object row carries no version columns (D37), so
/// the base's gated current slot keeps that fact itself.
#[async_trait]
impl meta::VersionMeta for TestMeta {
    type Error = RowError;

    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, RowError> {
        <RedbMetadata as meta::VersionMeta>::bucket_state(&self.inner, bucket).await
    }

    async fn set_bucket_state(&self, bucket: &bucket::Name, state: State) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::set_bucket_state(&self.inner, bucket, state).await
    }

    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::set_bucket_state_guarded(&self.inner, bucket, state)
            .await
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<meta::version::Current>, RowError> {
        <RedbMetadata as meta::VersionMeta>::current_slot(&self.inner, bucket, key).await
    }

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), RowError> {
        if self
            .refuse_next_advance_slot
            .swap(false, AtomicOrdering::Relaxed)
        {
            return Err(io::Error::other("the slot plane refused this key's advance").into());
        }
        let advanced =
            <RedbMetadata as meta::VersionMeta>::advance_slot(&self.inner, bucket, key, id, seq)
                .await;
        if advanced.is_ok() {
            self.slots
                .lock()
                .expect("the slot log")
                .insert((bucket.to_string(), key.to_string()), seq);
        }
        advanced
    }

    /// Delegated like every other `VersionMeta` call, with no extra
    /// instrumentation: the restore's commit is the reference store's own
    /// transaction.
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &meta::object::Row,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::restore_slot(&self.inner, bucket, key, row, id, seq)
            .await
    }

    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &meta::version::Stored,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::put_version(&self.inner, bucket, key, row).await
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<meta::version::Stored>, RowError> {
        <RedbMetadata as meta::VersionMeta>::get_version(&self.inner, bucket, key, seq).await
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, meta::version::Stored)>, RowError> {
        if self.hide_latest.swap(false, AtomicOrdering::Relaxed) {
            return Ok(None);
        }
        <RedbMetadata as meta::VersionMeta>::latest(&self.inner, bucket, key).await
    }

    async fn bucket_rows(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<meta::version::Stored>, RowError> {
        <RedbMetadata as meta::VersionMeta>::bucket_rows(&self.inner, bucket).await
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<meta::version::Stored>, RowError> {
        <RedbMetadata as meta::VersionMeta>::list_versions(&self.inner, bucket, key).await
    }

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::expire(&self.inner, bucket, key, seq).await
    }

    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::expire_many(&self.inner, bucket, key, seqs).await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), RowError> {
        <RedbMetadata as meta::VersionMeta>::drain_bucket(&self.inner, bucket).await
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&Id>,
    ) -> Result<Option<u64>, RowError> {
        if let Some(seq) = self.next_version_seq.lock().expect("the seq alias").take() {
            return Ok(Some(seq));
        }
        <RedbMetadata as meta::VersionMeta>::version_seq_of(&self.inner, bucket, key, id).await
    }
}

/// The marker port over the reference rows.
///
/// A current marker is a slot with no version row. The reference `advance_slot`
/// derives the marker bit from a version row, so this plants one at `seq`,
/// advances, then expires that scaffold. `moved_off` is stamped by rewriting
/// the history row: the object row is already gone when the delete calls this,
/// and `advance_slot` only stamps the sequence the object row still records.
#[async_trait]
impl MarkerWrite for TestMeta {
    async fn mark_current_marker(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
        moved_off: Option<u64>,
    ) -> Result<(), storage::Error> {
        if self.refuse_next_marker.swap(false, AtomicOrdering::Relaxed) {
            return Err(storage::io(io::Error::other(
                "the marker port refused this key",
            )));
        }
        if let Some(moved_off) = moved_off.filter(|moved_off| *moved_off != seq) {
            self.stamp_moved_off(bucket, key, moved_off).await?;
        }
        let marker = meta::version::Stored {
            id: id.clone(),
            size: 0,
            blob: BlobRef::empty(),
            etag: ETag::EMPTY,
            created: SystemTime::now(),
            became_noncurrent: 0,
            kind: meta::version::Kind::Marker,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
            seq,
        };
        meta::VersionMeta::put_version(self, bucket, key, &marker)
            .await
            .map_err(|err| -> storage::Error { err.into() })?;
        meta::VersionMeta::advance_slot(self, bucket, key, id, seq)
            .await
            .map_err(|err| -> storage::Error { err.into() })?;
        meta::VersionMeta::expire(self, bucket, key, seq)
            .await
            .map_err(|err| -> storage::Error { err.into() })
    }
}

impl TestMeta {
    /// Stamp the history row at `seq`. The reference store keeps an existing
    /// row's stamp, including `0`, so the row is dropped and written back with
    /// the stamp this move owes it.
    async fn stamp_moved_off(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), storage::Error> {
        let Some(mut stored) = meta::VersionMeta::get_version(self, bucket, key, seq)
            .await
            .map_err(|err| -> storage::Error { err.into() })?
        else {
            return Ok(());
        };
        if stored.became_noncurrent != 0 {
            return Ok(());
        }
        meta::VersionMeta::expire(self, bucket, key, seq)
            .await
            .map_err(|err| -> storage::Error { err.into() })?;
        stored.became_noncurrent = storage::to_nanos(SystemTime::now());
        meta::VersionMeta::put_version(self, bucket, key, &stored)
            .await
            .map_err(|err| -> storage::Error { err.into() })
    }
}

impl meta::Metadata for TestMeta {
    type Error = RowError;
}

/// The gated current slot a version plane needs.
#[async_trait]
impl CurrentRows for TestMeta {
    async fn current(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Current, storage::Error> {
        let Some(slot) =
            <RedbMetadata as meta::VersionMeta>::current_slot(&self.inner, bucket, key)
                .await
                .map_err(|err| -> storage::Error { err.into() })?
        else {
            return Ok(Current::default());
        };
        // The read-path gate: a recorded identity the content no longer
        // matches is not served, and the slot degrades to the null current.
        let row = <RedbMetadata as meta::ObjectMeta>::get(&self.inner, bucket, key)
            .await
            .map_err(|err| -> storage::Error { err.into() })?;
        let entry = self
            .content
            .stat(bucket, key)
            .await
            .map_err(|err| -> storage::Error { err.into() })?;
        let degraded = match (&row, &entry) {
            (Some(row), Some(entry)) => !row_matches(row, entry),
            (None, Some(_)) => true,
            _ => false,
        };
        // The recorded sequence is read out of the lock before the fallback
        // await: the guard must not live across it.
        let recorded = self
            .slots
            .lock()
            .expect("the slot log")
            .get(&(bucket.to_string(), key.to_string()))
            .copied();
        let seq = match recorded {
            Some(seq) => seq,
            None => <RedbMetadata as meta::VersionMeta>::latest(&self.inner, bucket, key)
                .await
                .map_err(|err| -> storage::Error { err.into() })?
                .map(|(seq, _)| seq)
                .unwrap_or(0),
        };
        Ok(Current {
            exists: true,
            version: if degraded { None } else { slot.version },
            is_delete_marker: slot.is_delete_marker,
            seq,
            degraded,
        })
    }
}

/// What a publish saw of the key's state **at the moment it ran** — the
/// ordered pipeline's instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishSight {
    /// The key's version rows already committed (the supersede leg).
    pub(crate) history: usize,
    /// The key's object row at that moment (the "row last" leg: a write that
    /// recorded its row before publishing would show the incoming ETag here,
    /// so seeing the *outgoing* ETag is the order's pin).
    pub(crate) row: Option<ETag>,
}

/// One content-plane call a recording double saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContentCall {
    /// [`ObjectContent::stage`] for the key.
    Stage(ContentKey),
    /// [`ObjectContent::publish`] (or its tagged form, which a plane that
    /// records identity serves) with the publish-time sight.
    Publish(ContentKey, PublishSight),
    /// [`ObjectContent::open`] for the key.
    Open(ContentKey),
    /// [`ObjectContent::copy`] from one object key to another.
    Copy(object::Key, object::Key),
    /// [`ObjectContent::rename`] from one object key to another.
    Rename(object::Key, object::Key),
    /// [`ObjectContent::scan`] for the bucket, with its prefix bound.
    Scan(String),
    /// [`ObjectContent::stat`] for the key.
    Stat(object::Key),
    /// [`ObjectContent::remove`] for the key.
    Remove(ContentKey),
    /// [`ObjectContent::remove_if_tagged`] for the key, with the publish
    /// identity it was given and whether the bytes were taken.
    RemoveIfTagged(ContentKey, u64, bool),
}

/// The recording content double: every call reaches the reference content
/// plane, and the calls a claim rests on are logged.
///
/// A copy that is a read-then-write shows as `Open` + `Stage` + `Publish` with
/// no `Copy`; a rename that moved the bytes itself shows as `Rename` beside the
/// row plane's `move_`. This double is also the stand-in for a plane that
/// **records publish identity** (the contract's tagged pair, which the in-memory
/// reference content plane serves through its documented byte-compare fallback):
/// the tag map is this double's own, exactly as `tinio-mem`'s accounting
/// decorator keeps its.
#[derive(Debug, Clone)]
pub(crate) struct RecordingContent {
    inner: Arc<RedbContent>,
    meta: Arc<TestMeta>,
    calls: Arc<Mutex<Vec<ContentCall>>>,
    /// The key's latest publish identity, for the tagged pair.
    tags: Arc<Mutex<HashMap<(String, String), u64>>>,
    /// The identity sequence (monotonic from 1, never reused).
    next_id: Arc<AtomicU64>,
    /// A **newer writer** this double plays: when set, a tagged publish
    /// immediately publishes these bytes over the same key under a fresh
    /// identity — the race a compensation must survive.
    follower: Option<Vec<u8>>,
    /// Whether this double deletes what it just scanned — a concurrent
    /// deletion landing between the walk and the page.
    vanish_after_scan: bool,
    /// Whether `open` fails hard (an I/O error, never a not-found): the
    /// content plane that is unreadable rather than empty.
    hard_open_failure: bool,
    /// The base's shared timeline: when a test wires this double to it, a
    /// content call lands beside the hooks' calls, so a window that spans
    /// both planest is one ordered vector.
    timeline: Option<Arc<Mutex<Vec<Call>>>>,
}

impl RecordingContent {
    /// Record the calls this double serves for `inner`, reading the version
    /// rows through `meta` for the publish-time snapshot.
    pub(crate) fn new(inner: Arc<RedbContent>, meta: Arc<TestMeta>) -> Self {
        Self {
            inner,
            meta,
            calls: Arc::default(),
            tags: Arc::default(),
            next_id: Arc::default(),
            follower: None,
            vanish_after_scan: false,
            hard_open_failure: false,
            timeline: None,
        }
    }

    /// The same double wired to the base's timeline: every logged call also
    /// lands there, in order with the hooks' calls.
    pub(crate) fn with_timeline(mut self, timeline: Arc<Mutex<Vec<Call>>>) -> Self {
        self.timeline = Some(timeline);
        self
    }

    /// The same double with a **concurrent deletion** behind every scan: the
    /// entries it answers are gone by the time the engine comes back for them,
    /// which is the page whose keys all vanished between the walk and the page.
    pub(crate) fn with_vanish_after_scan(mut self) -> Self {
        self.vanish_after_scan = true;
        self
    }

    /// The same double with an **unreadable** content plane: `open` fails with
    /// an I/O error (never a not-found), so a read that needs the bytes cannot
    /// degrade into "the key is not there".
    pub(crate) fn with_hard_open_failure(mut self) -> Self {
        self.hard_open_failure = true;
        self
    }

    /// The same double with a **newer writer** behind every tagged publish:
    /// once the engine's own publish lands, these bytes land over it under a
    /// fresh identity, so a compensation can only be told apart by the
    /// identity it recorded.
    pub(crate) fn with_follower(mut self, bytes: &[u8]) -> Self {
        self.follower = Some(bytes.to_vec());
        self
    }

    /// Every call this double saw, in order.
    pub(crate) fn calls(&self) -> Vec<ContentCall> {
        self.calls.lock().expect("the content log").clone()
    }

    /// Whether any call matches the predicate.
    pub(crate) fn saw(&self, what: impl Fn(&ContentCall) -> bool) -> bool {
        self.calls().iter().any(what)
    }

    /// Log one call — on this double's own log, and on the base's timeline
    /// when a test wired one (the ordered window).
    fn logged(&self, call: ContentCall) {
        self.calls
            .lock()
            .expect("the content log")
            .push(call.clone());
        if let Some(timeline) = &self.timeline {
            timeline
                .lock()
                .expect("the call log")
                .push(Call::Content(call));
        }
    }

    /// The key's history rows at this moment.
    async fn history_len(&self, bucket: &bucket::Name, key: &object::Key) -> usize {
        <TestMeta as meta::VersionMeta>::list_versions(&*self.meta, bucket, key)
            .await
            .map(|rows| rows.len())
            .unwrap_or_default()
    }

    /// What a publish of `key` sees right now: the history rows committed and
    /// the object row's ETag.
    async fn sight(&self, bucket: &bucket::Name, key: &ContentKey) -> PublishSight {
        let Some(object) = key.object().cloned() else {
            return PublishSight {
                history: 0,
                row: None,
            };
        };
        PublishSight {
            history: self.history_len(bucket, &object).await,
            row: <TestMeta as meta::ObjectMeta>::get(&*self.meta, bucket, &object)
                .await
                .ok()
                .flatten()
                .map(|row| row.etag),
        }
    }

    /// Mint this double's next publish identity.
    fn mint(&self) -> u64 {
        self.next_id.fetch_add(1, AtomicOrdering::Relaxed) + 1
    }

    /// Record `id` as the key's latest publish.
    fn remember(&self, bucket: &bucket::Name, key: &ContentKey, id: u64) {
        if let Some(slot) = content_slot(bucket, key) {
            self.tags.lock().expect("the tag map").insert(slot, id);
        }
    }

    /// The follower's own publish: the newer writer landing `follower`'s bytes
    /// over the key under a fresh identity, logged as the publish it is.
    async fn follow(&self, bucket: &bucket::Name, key: &ContentKey) {
        let Some(bytes) = self.follower.clone() else {
            return;
        };
        let Ok(mut staged) = ObjectContent::stage(&*self.inner, bucket, key).await else {
            return;
        };
        if StagedObject::write(&mut staged, Bytes::from(bytes))
            .await
            .is_err()
        {
            return;
        }
        let _ = ObjectContent::publish(&*self.inner, bucket, staged, key).await;
        let id = self.mint();
        self.remember(bucket, key, id);
        let sight = self.sight(bucket, key).await;
        self.logged(ContentCall::Publish(key.clone(), sight));
    }
}

/// The `(bucket, object key)` slot a publish identity is filed under.
fn content_slot(bucket: &bucket::Name, key: &ContentKey) -> Option<(String, String)> {
    let object = key.object()?;
    Some((bucket.to_string(), object.to_string()))
}

#[async_trait]
impl ObjectContent for RecordingContent {
    type Error = <RedbContent as ObjectContent>::Error;
    type Staged = <RedbContent as ObjectContent>::Staged;

    async fn stage(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error> {
        self.logged(ContentCall::Stage(key.clone()));
        ObjectContent::stage(&*self.inner, bucket, key).await
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        let sight = self.sight(bucket, key).await;
        self.logged(ContentCall::Publish(key.clone(), sight));
        ObjectContent::publish(&*self.inner, bucket, staged, key).await
    }

    async fn publish_tagged(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
        id: PublishId,
    ) -> Result<(), Self::Error> {
        let sight = self.sight(bucket, key).await;
        self.logged(ContentCall::Publish(key.clone(), sight));
        // Only a landed publish becomes the key's tag.
        ObjectContent::publish(&*self.inner, bucket, staged, key).await?;
        self.remember(bucket, key, u64::from(id));
        self.follow(bucket, key).await;
        Ok(())
    }

    async fn remove_if_tagged(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        id: PublishId,
    ) -> Result<bool, Self::Error> {
        // The identity first, the way `tinio-mem`'s plane does it: a later
        // publish owns the key, and equal bytes are not this call's to take.
        let mine = content_slot(bucket, key)
            .and_then(|slot| self.tags.lock().expect("the tag map").get(&slot).copied())
            == Some(u64::from(id));
        if !mine {
            self.logged(ContentCall::RemoveIfTagged(
                key.clone(),
                u64::from(id),
                false,
            ));
            return Ok(false);
        }
        let removed = ObjectContent::remove_if_matches(&*self.inner, bucket, key, expected).await?;
        self.logged(ContentCall::RemoveIfTagged(
            key.clone(),
            u64::from(id),
            removed,
        ));
        Ok(removed)
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        self.logged(ContentCall::Open(key.clone()));
        if self.hard_open_failure {
            return Err(io::Error::other("the content plane is unreadable").into());
        }
        ObjectContent::open(&*self.inner, bucket, key, range).await
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        self.logged(ContentCall::Remove(key.clone()));
        ObjectContent::remove(&*self.inner, bucket, key).await
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.logged(ContentCall::Rename(from.clone(), to.clone()));
        ObjectContent::rename(&*self.inner, bucket, from, to).await
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.logged(ContentCall::Copy(from.clone(), to.clone()));
        ObjectContent::copy(&*self.inner, bucket, from, to).await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error> {
        self.logged(ContentCall::Scan(prefix.to_owned()));
        let page = ObjectContent::scan(&*self.inner, bucket, prefix, after, limit).await?;
        if self.vanish_after_scan {
            // The concurrent deletion this mode plays: the entries the walk
            // answered are gone before the engine reads any of them.
            for entry in &page {
                ObjectContent::remove(&*self.inner, bucket, &ContentKey::Object(entry.key.clone()))
                    .await?;
            }
        }
        Ok(page)
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error> {
        self.logged(ContentCall::Stat(key.clone()));
        ObjectContent::stat(&*self.inner, bucket, key).await
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        ObjectContent::create_bucket(&*self.inner, bucket).await
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        ObjectContent::remove_bucket(&*self.inner, bucket).await
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        ObjectContent::is_empty(&*self.inner, bucket).await
    }
}

/// The in-memory reference base: the two planes, the block plane and the
/// recording hooks.
pub(crate) struct Base {
    /// The row plane (bucket, object, upload and version rows).
    pub(crate) meta: Arc<RedbMetadata>,
    /// The content plane, over the same in-memory database as the rows.
    pub(crate) content: Arc<RedbContent>,
    /// The counting row plane the object group's receivers carry.
    counted: Arc<TestMeta>,
    /// The in-memory block plane the version engine runs over.
    blocks: BlockMem,
    hooks: RecordingHooks,
}

impl Base {
    /// A fresh base whose hook records and never refuses.
    pub(crate) fn new() -> Self {
        Self::with_hooks(RecordingHooks::new())
    }

    /// A fresh base whose hook records and refuses the bucket delete.
    pub(crate) fn refusing() -> Self {
        Self::with_hooks(RecordingHooks::refusing())
    }

    fn with_hooks(hooks: RecordingHooks) -> Self {
        let meta = Arc::new(RedbMetadata::in_memory().expect("an in-memory row plane"));
        let content = Arc::new(RedbContent::from_driver(meta.driver()));
        Self::over(Arc::clone(&meta), content, hooks)
    }

    /// A base over handles a test built itself — a file-backed store, or a
    /// read-only one (the read-path rule's own fixture).
    pub(crate) fn over(
        meta: Arc<RedbMetadata>,
        content: Arc<RedbContent>,
        hooks: RecordingHooks,
    ) -> Self {
        let counted = Arc::new(TestMeta::new(
            Arc::clone(&meta),
            Arc::clone(&content),
            hooks.timeline(),
        ));
        Self {
            meta,
            content,
            counted,
            blocks: BlockMem::default(),
            hooks,
        }
    }

    /// The multipart-group receiver over these handles, under the given row
    /// policy — the hooks are the base's own recording double, so the part
    /// lock's whole life lands on the timeline the counted row plane shares.
    pub(crate) fn multipart(
        &self,
        bucket_row: BucketRow,
    ) -> MultipartOps<TestMeta, RedbContent, RecordingHooks> {
        MultipartOps {
            meta: Arc::clone(&self.counted),
            content: Arc::clone(&self.content),
            hooks: self.hooks.clone(),
            bucket_row,
        }
    }

    /// The multipart-group receiver over the **recording** content plane: the
    /// part pipeline's content calls join the same timeline, so one vector
    /// shows the stage, the lock's span and the row together.
    pub(crate) fn recording_multipart(
        &self,
        bucket_row: BucketRow,
    ) -> (
        MultipartOps<TestMeta, RecordingContent, RecordingHooks>,
        RecordingContent,
    ) {
        let content = RecordingContent::new(Arc::clone(&self.content), Arc::clone(&self.counted))
            .with_timeline(self.hooks.timeline());
        (
            MultipartOps {
                meta: Arc::clone(&self.counted),
                content: Arc::new(content.clone()),
                hooks: self.hooks.clone(),
                bucket_row,
            },
            content,
        )
    }

    /// The bucket-group receiver over these handles, under the given row
    /// policy. The policies are the row-backed store's: a failed row write is
    /// the operation's answer (`Fail`), and the row is the bucket
    /// (`Guaranteed`) unless a test asks for the lazy store.
    pub(crate) fn ops(
        &self,
        bucket_row: BucketRow,
    ) -> BucketOps<RedbMetadata, RedbContent, RecordingHooks> {
        BucketOps {
            meta: Arc::clone(&self.meta),
            content: Arc::clone(&self.content),
            hooks: self.hooks.clone(),
            metadata_failure: MetadataFailure::Fail,
            bucket_row,
        }
    }

    /// The object-group receiver over these handles, under the given policies.
    /// The counted row plane is the receiver's: a test's counters observe the
    /// calls the engine actually made.
    pub(crate) fn objects(
        &self,
        bucket_row: BucketRow,
        metadata_failure: MetadataFailure,
    ) -> ObjectOps<TestMeta, RedbContent> {
        ObjectOps {
            meta: Arc::clone(&self.counted),
            content: Arc::clone(&self.content),
            metadata_failure,
            bucket_row,
        }
    }

    /// The object-group receiver over the **recording** content plane, so the
    /// calls a body makes are observable.
    pub(crate) fn recording_objects(
        &self,
        bucket_row: BucketRow,
    ) -> (ObjectOps<TestMeta, RecordingContent>, RecordingContent) {
        self.recording_objects_with(
            bucket_row,
            RecordingContent::new(Arc::clone(&self.content), Arc::clone(&self.counted)),
        )
    }

    /// The same receiver over a recording plane a test configured itself — the
    /// **newer writer** behind a tagged publish, which is the race a
    /// compensation has to survive.
    pub(crate) fn recording_objects_with(
        &self,
        bucket_row: BucketRow,
        content: RecordingContent,
    ) -> (ObjectOps<TestMeta, RecordingContent>, RecordingContent) {
        (
            ObjectOps {
                meta: Arc::clone(&self.counted),
                content: Arc::new(content.clone()),
                metadata_failure: MetadataFailure::Fail,
                bucket_row,
            },
            content,
        )
    }

    /// The version plane: the version state engine over these handles, the
    /// version rows beside it and the in-memory block plane.
    pub(crate) fn version_plane(
        &self,
    ) -> VersionPlane<Versions<TestMeta, RedbContent, BlockMem>, TestMeta, BlockMem> {
        self.plane_with((*self.content).clone())
    }

    /// The version plane over a content plane a test built itself — the
    /// recording double, whose log makes a versioned write's publish
    /// observable at the moment it happens.
    pub(crate) fn plane_with<C>(
        &self,
        content: C,
    ) -> VersionPlane<Versions<TestMeta, C, BlockMem>, TestMeta, BlockMem>
    where
        C: ObjectContent,
    {
        VersionPlane {
            versions: Arc::new(Versions::rows_as_gate(
                (*self.counted).clone(),
                content,
                Store::new(self.blocks.clone()),
            )),
            version_rows: Arc::clone(&self.counted),
            blobs: Store::new(self.blocks.clone()),
        }
    }

    /// The counted row plane itself — a test that configures its own content
    /// double hands it the same row plane the base's receivers carry.
    pub(crate) fn counted(&self) -> &TestMeta {
        &self.counted
    }

    /// The calls the engine made on the counted row plane.
    pub(crate) fn counts(&self) -> Counts {
        self.counted.counts()
    }

    /// The recording hooks this base's receivers carry.
    pub(crate) fn hooks(&self) -> &RecordingHooks {
        &self.hooks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::meta::BucketMeta as _;

    /// The base is one database: a bucket the bucket group created is a bucket
    /// the content plane sees, and the version plane's state engine reads the
    /// same rows.
    #[tokio::test]
    async fn the_base_shares_one_row_store() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        base.ops(BucketRow::Guaranteed)
            .create_bucket(&name, None, &acl::Acl::default_private(None))
            .await
            .unwrap();
        assert!(base.meta.row(&name).await.unwrap().is_some());
        assert!(base.content.is_empty(&name).await.unwrap());

        let plane = base.version_plane();
        assert_eq!(
            plane.versions.state(&name).await.unwrap(),
            State::Unversioned
        );
    }
}
