//! The filesystem implementation of the version engine's three ports.
//!
//! `tinio-version` owns the state machine and knows nothing about paths,
//! tables or files; this module is the fs side of the seam. It maps a content
//! **key** to the key path (`resolve_key`, so the symlink policy and the
//! containment proof apply exactly as they do to every other object
//! operation), reads and publishes that content through the file stream and
//! the atomic writer, and keeps the version rows, their point-lookup index and
//! the bucket's state in the state database's redb tables.
//!
//! Two rules are this side's rather than the engine's:
//!
//! - **The current-slot gate.** [`CurrentRows::current`] applies the same
//!   size/mtime/file-identity staleness rule the read paths use (`F01`/`F04`):
//!   a recorded identity the content no longer matches — an out-of-band edit,
//!   or a file removed out of band while the row still described it — is
//!   reported as the **null** version, exactly as the design's
//!   out-of-band-edit rule has it, so the engine only ever sees gated truth.
//! - **The two-table transaction.** A version row and its index entry are
//!   written — and dropped — in one redb write transaction, in the shape
//!   `complete_object_state` already uses. The crash window between that
//!   commit and the key path's overwrite is the engine's protocol step 3; the
//!   atomicity that makes it recoverable is this module's.
//! - **The one-null-version rule.** A key holds at most one `null`-id row in
//!   the version table (AWS's model): the index is keyed by the identity and
//!   can name only one of two rows sharing the `null` wire, so when a
//!   supersede commits the null identity and an older null-id row already
//!   sits in history, that row is replaced — removed with its index entry in
//!   the same transaction — rather than a second one added, and no older null
//!   row is ever stranded unaddressable.
//! - **The row-overwrite guard.** The row at the outgoing version's sequence
//!   is rewritten only when the row already sitting there names the *same*
//!   version — same identity, same kind, same content address (the blob hash
//!   and the byte count) — which is exactly the adoption path the crash
//!   window needs. A row naming anything else at that sequence is refused
//!   loudly rather than overwritten: the row is what keeps that version's
//!   blob in the live set, and replacing it would drop a committed version's
//!   content for the sweep to reclaim. The content address is part of the
//!   check because the identity wire cannot separate two null versions, which
//!   share it by construction.

use std::{io::ErrorKind, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use tokio::fs;

use crate::{
    _core::{
        ETag, MultipartUpload, acl,
        bucket::{self, Name},
        checksum, cors, lifecycle,
        meta::{
            BucketMeta, Metadata, ObjectMeta, UploadMeta, VersionMeta, bucket as bucket_row,
            object as object_row, upload as upload_row,
            version::{Current as RowCurrent, Stored},
        },
        multipart, object, storage,
        version::{self, State},
    },
    _store_redb::{RedbMetadata, bucket_versioning, object_meta},
    _version::{Current, CurrentRows},
    Error, FsStorage,
    error::contract,
    fsutil,
    meta::{Content, Verdict, gate},
};

/// The key's current slot as the gate left it: the port's [`Current`] and the
/// `object_meta` row the gate judged.
///
/// One read serves both the engine (which takes `current`) and fs's own
/// paths that need the judged row (the current-version probe), so a request
/// never reads the row twice.
#[derive(Debug, Clone)]
pub(crate) struct Judged {
    /// The gated slot — the port's answer.
    pub(crate) current: Current,
    /// The judged `object_meta` row (`None` when none records the key).
    pub(crate) stored: Option<object_meta::Stored>,
}

/// The fs implementation of the contracts the engine runs on — [`Metadata`],
/// [`VersionMeta`] and the surviving [`CurrentRows`] — plus the fs's own
/// gated current slot: the state database's version tables reached through the
/// contract implementation, and the key's gated slot.
#[derive(Debug, Clone)]
pub(crate) struct Rows {
    /// The backend this adapter serves (its state-database handle, its meta
    /// store and its path mapping) — held as the shared `Arc` every
    /// version-plane handle is built from, so cloning the adapter is a
    /// pointer bump rather than a copy of the backend's paths and owner map.
    storage: Arc<FsStorage>,
    /// The contract implementation over the same state database: every
    /// forwarded call runs through it, so the two-table transactions and the
    /// version-index rules have exactly one home.
    engine: Arc<RedbMetadata>,
}

impl Rows {
    /// Serve a backend's rows.
    pub(crate) fn new(storage: Arc<FsStorage>) -> Self {
        // The contract implementation the storage's meta facade already holds
        // over this same driver, cloned rather than rebuilt: a per-call
        // `Rows` shares one engine handle instead of allocating a second.
        let engine = storage.meta_store().engine();
        Self { storage, engine }
    }

    /// The key's gated current slot — the port's [`CurrentRows::current`]
    /// answer — with the `object_meta` row the gate was applied to, from one
    /// read: a caller that needs the row (fs's own `slot`, and the current
    /// version's judgement) never reads it twice.
    ///
    /// A row whose etag is domain-invalid is reported as absent — the port's
    /// own self-heal, so the two answers can never disagree.
    pub(crate) async fn current_with_row(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Judged, storage::Error> {
        // No recorded row: the key has no current version at all.
        let Some(stored) = self
            .storage
            .meta_store()
            .stored_entry(bucket, key)
            .await
            .map_err(contract)?
        else {
            return Ok(Judged {
                current: Current::default(),
                stored: None,
            });
        };
        let dir = self.storage.ensure_bucket(bucket).await.map_err(contract)?;
        let target = self
            .storage
            .resolve_key(&dir, key)
            .await
            .map_err(contract)?;
        let content = match fs::metadata(&target).await {
            Ok(metadata) => Content::Present {
                size: metadata.len(),
                mtime: metadata.modified().map_err(contract)?,
                identity: fsutil::file_identity(&target, &metadata),
            },
            // The file is gone. What that means is the gate's own decision
            // below: the row's **recorded** marker flag says which state this
            // is — the simple-DELETE path writes it before removing the file,
            // and every object write clears it — while a row that still
            // describes content is an out-of-band removal, where the mirror
            // wins. The flag is read, never inferred from the row's shape: a
            // genuinely zero-byte object's row is shape-identical to a
            // marker's on a filesystem without a file identity.
            Err(err) if err.kind() == ErrorKind::NotFound => Content::Missing,
            Err(err) => return Err(contract(err)),
        };
        // The gate's verdict — the ONE home of the decision
        // (`meta::gate`), shared with the plain read's version facts
        // (`resolve_object_slot`), so the two can never disagree about what
        // is current — and the slot it names.
        let slot = match gate(&stored, content) {
            // The recorded identity names what the key holds, or the key
            // holds nothing and the row is the recorded delete marker, which
            // is the current version itself. A row whose file is still there
            // is content whatever it was written for, so the marker flag is
            // the one the missing file state reads.
            Verdict::Live => Current {
                exists: true,
                version: stored.version_id.clone(),
                is_delete_marker: stored.is_delete_marker && matches!(content, Content::Missing),
                seq: stored.version_seq,
                degraded: false,
            },
            // The recorded identity is stale: the content drifted from the
            // row ([`entry_matches`]), or the file is gone while the row
            // still described content. The object's version identity
            // degrades to the null version — a stale identifier is never
            // served as if it still named content. The row still records an
            // identity while the slot records none, which the `degraded` flag
            // carries: the two are not the same state as a recorded null
            // version.
            Verdict::Degraded => Current {
                exists: true,
                version: None,
                is_delete_marker: false,
                seq: 0,
                degraded: stored.version_id.is_some(),
            },
        };
        Ok(Judged {
            current: slot,
            stored: Some(stored),
        })
    }
}

#[async_trait]
impl CurrentRows for Rows {
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error> {
        Ok(self.current_with_row(bucket, key).await?.current)
    }
}

#[async_trait]
impl Metadata for Rows {
    type Error = storage::Error;
}

#[async_trait]
impl BucketMeta for Rows {
    async fn created_at(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<SystemTime>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::created_at(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn load_many(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<SystemTime>>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::load_many(&self.engine, buckets)
            .await
            .map_err(Into::into)
    }

    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::load_all(&self.engine)
            .await
            .map_err(Into::into)
    }

    async fn row(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<bucket_row::Row>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::row(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn owners(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<acl::OwnerId>>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::owners(&self.engine, buckets)
            .await
            .map_err(Into::into)
    }

    async fn record(
        &self,
        bucket: &bucket::Name,
        created: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::record(&self.engine, bucket, created)
            .await
            .map_err(Into::into)
    }

    async fn create(
        &self,
        bucket: &bucket::Name,
        row: &bucket_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::create(&self.engine, bucket, row)
            .await
            .map_err(Into::into)
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::record_if_absent(&self.engine, bucket, now)
            .await
            .map_err(Into::into)
    }

    async fn record_full(
        &self,
        bucket: &bucket::Name,
        row: &bucket_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::record_full(&self.engine, bucket, row)
            .await
            .map_err(Into::into)
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_tags(&self.engine, bucket, tags)
            .await
            .map_err(Into::into)
    }

    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_tags_or_create(&self.engine, bucket, tags, now)
            .await
            .map_err(Into::into)
    }

    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::clear_tags(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_acl(&self.engine, bucket, owner, grants)
            .await
            .map_err(Into::into)
    }

    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &acl::AclGrants,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_grants_or_create(&self.engine, bucket, grants, now)
            .await
            .map_err(Into::into)
    }

    async fn set_cors(
        &self,
        bucket: &bucket::Name,
        config: &cors::Config,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_cors(&self.engine, bucket, config)
            .await
            .map_err(Into::into)
    }

    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::clear_cors(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn lifecycle(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::lifecycle(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn lifecycles(
        &self,
    ) -> Result<Vec<(bucket::Name, lifecycle::Config)>, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::lifecycles(&self.engine)
            .await
            .map_err(Into::into)
    }

    async fn set_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_lifecycle_or_create(&self.engine, bucket, config, now)
            .await
            .map_err(Into::into)
    }

    async fn set_lifecycle(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::set_lifecycle(&self.engine, bucket, config)
            .await
            .map_err(Into::into)
    }

    async fn merge_lifecycle(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::merge_lifecycle(&self.engine, bucket, rules)
            .await
            .map_err(Into::into)
    }

    async fn merge_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
        now: SystemTime,
    ) -> Result<lifecycle::Merged, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::merge_lifecycle_or_create(&self.engine, bucket, rules, now)
            .await
            .map_err(Into::into)
    }

    async fn clear_lifecycle(
        &self,
        bucket: &bucket::Name,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::clear_lifecycle(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::remove(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn remove_if_empty(
        &self,
        bucket: &bucket::Name,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as BucketMeta>::remove_if_empty(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl ObjectMeta for Rows {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<object_row::Row>, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::get(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::put(&self.engine, bucket, key, row)
            .await
            .map_err(Into::into)
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::put_guarded(&self.engine, bucket, key, row)
            .await
            .map_err(Into::into)
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
        parts: &[object_row::PartRow],
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::put_with_parts(&self.engine, bucket, key, row, parts)
            .await
            .map_err(Into::into)
    }

    async fn parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<object_row::PartRow>, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::parts(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn has_objects(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        // The row plane is the shared redb one (fs's rows are the state
        // database's), so the teardown probe is its own — one home for the
        // rule, and no fs-local emptiness authority beside it.
        <RedbMetadata as ObjectMeta>::has_objects(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &object_row::Identity,
    ) -> Result<object_row::Preserved, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::put_preserving(&self.engine, bucket, key, identity)
            .await
            .map_err(Into::into)
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::move_(&self.engine, bucket, from, to)
            .await
            .map_err(Into::into)
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::set_tags(&self.engine, bucket, key, tags)
            .await
            .map_err(Into::into)
    }

    async fn clear_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::clear_tags(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::ensure_tags(&self.engine, bucket, key, tags)
            .await
            .map_err(Into::into)
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::set_acl(&self.engine, bucket, key, owner, grants)
            .await
            .map_err(Into::into)
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, object_row::Row)],
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::put_batch(&self.engine, bucket, rows)
            .await
            .map_err(Into::into)
    }

    async fn remove(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::remove(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::remove_many(&self.engine, bucket, keys)
            .await
            .map_err(Into::into)
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<object_row::Row>>, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::load(&self.engine, bucket, keys)
            .await
            .map_err(Into::into)
    }

    async fn walk(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, object_row::Row)>, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::walk(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<object_row::Row>)>, <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::walk_healing(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as ObjectMeta>::drain_bucket(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl UploadMeta for Rows {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::create(&self.engine, bucket, upload_id, row)
            .await
            .map_err(Into::into)
    }

    /// The guarded create, delegated like every other upload write: the
    /// engine's own transaction reads the bucket row before it writes.
    async fn create_guarded(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::create_guarded(&self.engine, bucket, upload_id, row)
            .await
            .map_err(Into::into)
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<upload_row::Row>, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::get(&self.engine, bucket, key, upload_id)
            .await
            .map_err(Into::into)
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: multipart::PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::put_part(
            &self.engine,
            bucket,
            key,
            upload_id,
            number,
            etag,
            checksum,
        )
        .await
        .map_err(Into::into)
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: multipart::PartNumber,
    ) -> Result<Vec<upload_row::PartRow>, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::list_parts(&self.engine, bucket, upload_id, from)
            .await
            .map_err(Into::into)
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<upload_row::PartsPage, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::list_parts_page(&self.engine, bucket, upload_id, from, max)
            .await
            .map_err(Into::into)
    }

    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[multipart::PartNumber],
    ) -> Result<Option<Vec<Option<upload_row::PartRecord>>>, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::part_records(&self.engine, bucket, key, upload_id, numbers)
            .await
            .map_err(Into::into)
    }

    async fn list_uploads(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::list_uploads(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), <Self as Metadata>::Error>
    {
        <RedbMetadata as UploadMeta>::list_uploads_page(
            &self.engine,
            bucket,
            prefix,
            delimiter,
            marker,
            max_uploads,
        )
        .await
        .map_err(Into::into)
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::has_uploads(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::walk_uploads(&self.engine)
            .await
            .map_err(Into::into)
    }

    async fn drain_upload(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::drain_upload(&self.engine, bucket, upload_id)
            .await
            .map_err(Into::into)
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::drain_upload_checked(&self.engine, bucket, key, upload_id)
            .await
            .map_err(Into::into)
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        <RedbMetadata as UploadMeta>::drain_bucket(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl VersionMeta for Rows {
    type Error = storage::Error;

    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Self::Error> {
        <RedbMetadata as VersionMeta>::bucket_state(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn set_bucket_state(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::set_bucket_state(&self.engine, bucket, state)
            .await
            .map_err(Into::into)
    }

    /// The guarded state write, delegated like every other version write: the
    /// engine's own transaction reads the bucket row before it writes.
    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::set_bucket_state_guarded(&self.engine, bucket, state)
            .await
            .map_err(Into::into)
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<RowCurrent>, Self::Error> {
        <RedbMetadata as VersionMeta>::current_slot(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::advance_slot(&self.engine, bucket, key, id, seq)
            .await
            .map_err(Into::into)
    }

    /// The restore's commit, delegated like every other version write: the
    /// engine's own transaction reads the row, writes the object row and its
    /// slot columns, and consumes the row and its index entry.
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::restore_slot(&self.engine, bucket, key, row, id, seq)
            .await
            .map_err(Into::into)
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, Self::Error> {
        <RedbMetadata as VersionMeta>::get_version(&self.engine, bucket, key, seq)
            .await
            .map_err(Into::into)
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Self::Error> {
        <RedbMetadata as VersionMeta>::latest(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, Self::Error> {
        <RedbMetadata as VersionMeta>::bucket_rows(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, Self::Error> {
        <RedbMetadata as VersionMeta>::list_versions(&self.engine, bucket, key)
            .await
            .map_err(Into::into)
    }

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::expire(&self.engine, bucket, key, seq)
            .await
            .map_err(Into::into)
    }

    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::expire_many(&self.engine, bucket, key, seqs)
            .await
            .map_err(Into::into)
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        <RedbMetadata as VersionMeta>::drain_bucket(&self.engine, bucket)
            .await
            .map_err(Into::into)
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Result<Option<u64>, Self::Error> {
        <RedbMetadata as VersionMeta>::version_seq_of(&self.engine, bucket, key, id)
            .await
            .map_err(Into::into)
    }

    /// The one call the fs adds to the engine's own: the commit bumps the
    /// version-reference epoch the sweeper validates its live set against.
    async fn put_version(
        &self,
        bucket: &Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), <Self as VersionMeta>::Error> {
        <RedbMetadata as VersionMeta>::put_version(&self.engine, bucket, key, row)
            .await
            .map_err(Into::<storage::Error>::into)?;
        self.storage.bump_block_epoch();
        Ok(())
    }
}

impl FsStorage {
    /// How many buckets of this root hold version data — the number the
    /// toggle-off startup warning names (spec 2026-10-03: the feature
    /// compiled in, `[s3] versioning` off, a root that still holds version
    /// data). One range scan of the bucket-versioning table, never a
    /// per-bucket lookup, so a large root stays cheap; the scan runs on the
    /// blocking pool like every full-table read.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{
    ///     acl::Acl,
    ///     bucket,
    ///     storage::{BucketOps, VersionOps},
    ///     version::State,
    /// };
    /// use tinio_fs::{FsStorage, testing};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().unwrap();
    /// let storage = FsStorage::new(root.path(), testing::fs_options()).unwrap();
    /// let b = bucket::name("data").unwrap();
    /// Runtime::new().unwrap().block_on(async {
    ///     storage
    ///         .create_bucket(&b, None, &Acl::default_private(None))
    ///         .await
    ///         .unwrap();
    ///     storage
    ///         .set_bucket_versioning(&b, State::Enabled)
    ///         .await
    ///         .unwrap();
    ///     assert_eq!(storage.versioned_bucket_count().await.unwrap(), 1);
    /// });
    /// ```
    pub async fn versioned_bucket_count(&self) -> Result<usize, Error> {
        self.handle()
            .read_blocking(|txn| {
                Ok(bucket_versioning::Table::open_readonly(txn)?.versioned_count()?)
            })
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs as std_fs, path::PathBuf};

    use super::*;
    use crate::{
        _core::{
            BucketOps, ETag, acl,
            blob::{BlobRef, BlockHash, BlockStore},
            bucket,
            meta::version::{Kind, Stored},
            object::Tags,
            storage::VersionOps,
            to_nanos, version,
        },
        _store_redb::{version as rows, version_index},
        testutil,
    };

    /// A storage with a `demo` bucket directory, its name and a key.
    async fn fixture() -> (tempfile::TempDir, FsStorage, Name, object::Key) {
        let (root, storage) = testutil::storage();
        let bucket = bucket::name("demo").unwrap();
        // The bucket through the production path: the directory is only half of
        // a bucket, and the metadata row it records is what every guarded
        // writer — the version rows these tests exercise included — probes.
        storage
            .create_bucket(&bucket, None, &acl::Acl::default_private(None))
            .await
            .unwrap();
        (root, storage, bucket, object::key("notes.txt").unwrap())
    }

    /// The key's target path.
    async fn target(storage: &FsStorage, bucket: &Name, key: &object::Key) -> PathBuf {
        let dir = storage.ensure_bucket(bucket).await.unwrap();
        storage.resolve_key(&dir, key).await.unwrap()
    }

    /// Plant a versioned object: the file at the key path plus the
    /// `object_meta` row recording its identity and sequence — the shape
    /// Task 4's write path commits.
    async fn plant(
        storage: &FsStorage,
        bucket: &Name,
        key: &object::Key,
        bytes: &[u8],
        id: Option<&str>,
        seq: u64,
    ) {
        let path = target(storage, bucket, key).await;
        fs::write(&path, bytes).await.unwrap();
        let metadata = std_fs::metadata(&path).unwrap();
        let stored = object_meta::Stored {
            version_id: id.map(|id| version::Id::new(id).unwrap()),
            version_seq: seq,
            is_delete_marker: false,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&path, &metadata),
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let bucket = bucket.clone();
        let key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                object_meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Plant a delete marker: the marker-shaped row the delete path writes
    /// (no content, the empty-content ETag, no file identity) with no file at
    /// the key path — the row `current_with_row` reads as a marker.
    async fn plant_marker(
        storage: &FsStorage,
        bucket: &Name,
        key: &object::Key,
        id: Option<&str>,
        seq: u64,
    ) {
        let stored = object_meta::Stored {
            version_id: id.map(|id| version::Id::new(id).unwrap()),
            version_seq: seq,
            is_delete_marker: true,
            etag: ETag::EMPTY,
            size: 0,
            mtime: to_nanos(SystemTime::now()),
            file_identity: 0,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let bucket = bucket.clone();
        let key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                object_meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// The contract row a version commit carries: the identity, sequence,
    /// content address and kind a test plants. The metadata elements are the
    /// writer's, so a test that asserts on them builds its own row.
    fn stored(id: Option<&str>, seq: u64, blob: BlobRef, kind: Kind) -> Stored {
        Stored {
            id: id.map(|id| version::Id::new(id).unwrap()),
            blob: blob.clone(),
            size: blob.len(),
            etag: ETag::EMPTY,
            created: SystemTime::now(),
            became_noncurrent: 0,
            kind,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
            seq,
        }
    }

    #[tokio::test]
    async fn a_supersede_moves_the_file_into_the_block_store_and_leaves_the_row_not_yet_history() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        versions.set_state(&bucket, State::Enabled).await.unwrap();
        assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions
            .latest(&bucket, &key)
            .await
            .unwrap()
            .expect("a version row");
        assert_eq!(
            (seq, row.id.as_ref().expect("a named version").as_str()),
            (1, "v1"),
            "the row carries the identity and sequence the meta row recorded"
        );
        assert_eq!(row.size, 5);
        assert_eq!(row.kind, Kind::Object);
        assert_eq!(
            row.etag,
            ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            "the row carries the ETag the object_meta row recorded (here deliberately \
             not the content's MD5), never a hash of the bytes"
        );
        // The engine's supersede alone is the crash window's first half: the
        // row is committed while the version is still current, so it is not
        // history yet — `became_noncurrent` stays 0 and `is_history` says no
        // for the slot's own sequence. The write's state transaction stamps it
        // (see `the_publish_stamps_the_superseded_row_as_history` in the
        // version tests).
        assert_eq!(
            row.became_noncurrent, 0,
            "the row is not stamped until the current slot moves off it"
        );
        // (The slot-aware `is_history` predicate is the raw table row's; the
        // contract row's half of it is the stamp asserted just above.)
        let block = versions.blobs().blocks().block_path(row.blob.hash());
        assert!(
            block.starts_with(storage.blobs_dir()),
            "the block lives under the bucket's own state-dir subtree: {}",
            block.display()
        );
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(row.blob.hash())
                .await
                .unwrap(),
            "the content is in the block store"
        );
    }

    #[tokio::test]
    async fn a_re_supersede_adopts_its_own_row() {
        // The crash window's recovery: the same version superseded twice
        // rewrites the row it already committed, never a second identity.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        versions.supersede(&bucket, &key).await.unwrap();
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            (
                seq,
                row.id.as_ref().expect("a named version").as_str(),
                row.kind
            ),
            (1, "v1", Kind::Object)
        );
        assert_eq!(row.size, 5);
    }

    #[tokio::test]
    async fn an_out_of_band_removal_degrades_the_slot_and_supersedes_nothing() {
        // The file is gone while `object_meta` still records the committed
        // version: the design's out-of-band rule — the identity degrades to
        // the null version and the next write treats the missing file as the
        // null version it now is, preserving nothing. The committed row is
        // the newer, row-shaped truth and survives untouched.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let (_, before) = versions.latest(&bucket, &key).await.unwrap().unwrap();

        let path = target(&storage, &bucket, &key).await;
        fs::remove_file(&path).await.unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.is_null(),
            "no file plus a row still describing content is the null version, not a marker"
        );
        assert_eq!(current.seq, 0, "the null version's sequence");

        versions.supersede(&bucket, &key).await.unwrap();
        let (_, after) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            after, before,
            "nothing was current to preserve: the committed version's row is byte-identical"
        );
        assert_eq!(after.kind, Kind::Object);
        assert_eq!(
            after.blob,
            BlobRef::single(BlockHash::of(b"first"), 5),
            "and its content still reads"
        );
    }

    #[tokio::test]
    async fn a_supersede_that_would_overwrite_different_content_at_the_same_identity_is_refused() {
        // The null-vs-null shape: two distinct versions share the identity
        // wire (`null`, the null version's) and the kind, so the identity
        // check alone cannot tell them apart — only the content address can.
        // A guard that compared identity and kind only would overwrite the
        // committed row and drop its blob out of the live set.
        let (_root, storage, bucket, key) = fixture().await;
        let rows = Rows::new(Arc::new(storage.clone()));
        let committed = BlobRef::single(BlockHash::of(b"first"), 5);
        rows.put_version(
            &bucket,
            &key,
            &stored(None, 0, committed.clone(), Kind::Object),
        )
        .await
        .unwrap();

        // The same identity, kind and byte count — a different content
        // address (`first` and `secnd` are both five bytes).
        let err = rows
            .put_version(
                &bucket,
                &key,
                &stored(
                    None,
                    0,
                    BlobRef::single(BlockHash::of(b"secnd"), 5),
                    Kind::Object,
                ),
            )
            .await
            .unwrap_err();
        let storage::Error::Io(io) = err else {
            panic!("expected the refusal as a contract I/O error, got {err:?}");
        };
        assert_eq!(io.kind(), ErrorKind::Other, "the refusal is loud: {io}");
        // The wire message stays generic (2026-10-06 review): the bucket,
        // key, sequence and identity go to the log, never to the client.
        assert_eq!(
            io.to_string(),
            "a version row conflicts with the version being superseded"
        );

        // The committed row is untouched, content address included.
        let (_, row) = storage
            .versions(&bucket)
            .latest(&bucket, &key)
            .await
            .unwrap()
            .expect("the committed row survives");
        assert_eq!(
            (row.seq, row.id.as_ref().map(version::Id::as_str), row.kind),
            (0, None, Kind::Object),
            "the null version's identity reads as absent, never as its wire"
        );
        assert_eq!(
            row.blob, committed,
            "the committed version's content still reads"
        );
    }

    #[tokio::test]
    async fn materializing_a_version_writes_its_bytes_back_onto_the_key_path() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let (_, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();

        // The overwrite that follows a supersede; the materialize restores.
        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"second").await.unwrap();
        let published = versions
            .materialize(&bucket, &key, &row)
            .await
            .unwrap()
            .expect("an object row publishes content");
        assert_eq!(published.size, 5, "the publish answers the byte count");
        assert_eq!(
            published.etag, row.etag,
            "the version's recorded ETag — the row's authoritative value, so a \
             multipart-composed form survives the restore — never a re-hash of \
             the bytes the publish streamed"
        );

        assert_eq!(fs::read(&path).await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn current_applies_the_staleness_gate_and_tells_a_marker_row_from_a_missing_file() {
        let (_root, storage, bucket, key) = fixture().await;
        let versions = storage.versions(&bucket);

        // Nothing recorded: no current version.
        assert!(!versions.current(&bucket, &key).await.unwrap().exists);

        // A recorded version whose file matches: the real identity.
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && !current.is_delete_marker);
        assert_eq!(current.version.as_ref().unwrap().as_str(), "v1");
        assert_eq!(current.seq, 1);
        assert!(!current.is_null());

        // The file is replaced out of band: the gate degrades the identity to
        // the null version, never a stale identifier.
        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"replaced out of band, and longer")
            .await
            .unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && current.is_null());
        assert_eq!(current.seq, 0, "the null version's sequence");

        // The file is gone while its row still describes the content it held:
        // an out-of-band removal, which the mirror reports as the null version
        // — not a delete marker the API never created.
        fs::remove_file(&path).await.unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.exists && current.is_null() && !current.is_delete_marker,
            "the removal degrades the identity to null, never to a marker"
        );
        assert!(
            current.version.is_none(),
            "and the recorded id is not served"
        );

        // Only a row whose **recorded** marker flag is set — what the delete
        // path writes before it removes the file — reads as a delete marker.
        plant_marker(&storage, &bucket, &key, Some("m1"), 3).await;
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && current.is_delete_marker);
        assert_eq!(current.version.as_ref().unwrap().as_str(), "m1");
        assert_eq!(current.seq, 3);
        assert!(!current.is_null(), "a marker is not the null version");
    }

    #[tokio::test]
    async fn a_zero_byte_object_row_never_reads_as_a_marker() {
        // The residual the recorded flag closes: a genuinely zero-byte
        // object's row on a filesystem whose file identity is unavailable is
        // shape-identical to a marker's row (size 0, the empty-content ETag,
        // identity 0), so the shape cannot discriminate. The object write
        // clears the flag, and the absence of the file after an out-of-band
        // removal must therefore degrade to the null version — never the
        // marker the API never created.
        let (_root, storage, bucket, key) = fixture().await;
        let versions = storage.versions(&bucket);
        let stored = object_meta::Stored {
            version_id: Some(version::Id::new("z1").unwrap()),
            version_seq: 1,
            is_delete_marker: false,
            etag: ETag::EMPTY,
            size: 0,
            mtime: to_nanos(SystemTime::now()),
            file_identity: 0,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let write_bucket = bucket.clone();
        let write_key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                object_meta::Table::open(txn)?.put(&write_bucket, &write_key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();

        // The row's shape matches a marker's exactly; only the flag differs.
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.exists && current.is_null() && !current.is_delete_marker,
            "a zero-byte object's row must never read as a marker: {current:?}"
        );
        assert_eq!(current.seq, 0, "the removal degrades to the null version");
    }

    #[tokio::test]
    async fn an_out_of_band_edit_supersedes_as_the_null_version() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"edited out of band").await.unwrap();
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(seq, 0, "the gated slot is the null version");
        assert!(row.id.is_none(), "and its identity is the null version's");
        assert_eq!(row.size, 18);
    }

    #[tokio::test]
    async fn a_marker_current_supersedes_without_content() {
        let (_root, storage, bucket, key) = fixture().await;
        plant_marker(&storage, &bucket, &key, Some("m1"), 3).await;
        let versions = storage.versions(&bucket);

        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            (seq, row.id.as_ref().expect("a named version").as_str()),
            (3, "m1")
        );
        assert_eq!(row.kind, Kind::Marker);
        assert!(row.blob.is_empty(), "a marker carries the empty blob");
        assert!(
            versions
                .live_blocks(&bucket)
                .await
                .unwrap()
                .contains(&BlockHash::of(b"")),
            "the marker's live reference is the empty blob's block"
        );
        assert!(
            !versions
                .blobs()
                .blocks()
                .contains(&BlockHash::of(b"first"))
                .await
                .unwrap(),
            "a marker's supersede reads no content"
        );
    }

    #[tokio::test]
    async fn the_committed_version_row_and_its_index_entry_drop_together() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        assert!(
            versions
                .live_blocks(&bucket)
                .await
                .unwrap()
                .contains(&BlockHash::of(b"first"))
        );

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "v1")?)
                })
                .unwrap()
        };
        assert_eq!(indexed(&storage), Some(1));

        versions.expire(&bucket, &key, 1).await.unwrap();
        versions.expire(&bucket, &key, 1).await.unwrap(); // idempotent

        assert!(versions.latest(&bucket, &key).await.unwrap().is_none());
        assert_eq!(
            indexed(&storage),
            None,
            "the index entry drops with the row"
        );
    }

    #[tokio::test]
    async fn removing_a_version_row_that_holds_nothing_is_a_no_op() {
        // The port's missing-row branch: a sequence with no row removes
        // nothing and errors nothing, leaving a neighbouring version's row
        // and index entry untouched — the idempotence the sweep's expiry and
        // a repeated version delete both stand on.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let rows = Rows::new(Arc::new(storage.clone()));

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "v1")?)
                })
                .unwrap()
        };

        // A sequence the key never used: nothing drops.
        rows.expire(&bucket, &key, 7).await.unwrap();
        assert!(
            rows.latest(&bucket, &key).await.unwrap().is_some(),
            "the committed row is untouched"
        );
        assert_eq!(indexed(&storage), Some(1), "its index entry rides");

        // The row that IS there still drops, with its index entry...
        rows.expire(&bucket, &key, 1).await.unwrap();
        assert!(rows.latest(&bucket, &key).await.unwrap().is_none());
        assert_eq!(indexed(&storage), None);

        // ...and repeating it takes the missing-row branch again.
        rows.expire(&bucket, &key, 1).await.unwrap();
    }

    #[tokio::test]
    async fn superseding_the_null_version_converges_on_one_null_id_row() {
        // The null identity is one wire for what would be two rows (a
        // superseded null object version and a null-identity marker); the
        // write side converges them, so the index's single `null` entry
        // always names the one null-id row and no older row is stranded
        // unaddressable once the pointed-at row expires.
        let (_root, storage, bucket, key) = fixture().await;
        let rows = Rows::new(Arc::new(storage.clone()));
        rows.put_version(
            &bucket,
            &key,
            &stored(
                None,
                0,
                BlobRef::single(BlockHash::of(b"first"), 5),
                Kind::Object,
            ),
        )
        .await
        .unwrap();
        rows.put_version(
            &bucket,
            &key,
            &stored(
                None,
                3,
                BlobRef::single(BlockHash::of(b""), 0),
                Kind::Marker,
            ),
        )
        .await
        .unwrap();

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "null")?)
                })
                .unwrap()
        };
        assert_eq!(
            indexed(&storage),
            Some(3),
            "the index names the converged row"
        );
        let surviving = {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    let mut seen = Vec::new();
                    rows::Table::open_readonly(txn)?.for_key(&bucket, &key, |row| {
                        seen.push(row);
                        Ok(())
                    })?;
                    Ok(seen)
                })
                .unwrap()
        };
        assert_eq!(
            surviving.len(),
            1,
            "the supersede replaced the older null row instead of adding a second: {surviving:?}"
        );
        assert_eq!(surviving[0].seq, 3);
        assert_eq!(surviving[0].kind, rows::Kind::Marker);
        assert_eq!(
            rows.latest(&bucket, &key).await.unwrap().unwrap().0,
            3,
            "the converged row is the key's newest"
        );
    }

    /// The toggle-off warning's number: one scan counts the buckets whose
    /// state row is not `Unversioned` — a plain directory is not version
    /// data, and a bucket that only ever suspended still counts.
    #[tokio::test]
    async fn the_versioned_bucket_count_counts_only_buckets_that_hold_a_state() {
        let (root, storage) = testutil::storage();
        fs::create_dir(root.path().join("plain")).await.unwrap();
        fs::create_dir(root.path().join("versioned")).await.unwrap();
        assert_eq!(
            storage.versioned_bucket_count().await.unwrap(),
            0,
            "bucket directories alone hold no version data"
        );
        storage
            .set_bucket_versioning(&bucket::name("versioned").unwrap(), version::State::Enabled)
            .await
            .unwrap();
        assert_eq!(storage.versioned_bucket_count().await.unwrap(), 1);
        storage
            .set_bucket_versioning(&bucket::name("plain").unwrap(), version::State::Suspended)
            .await
            .unwrap();
        assert_eq!(
            storage.versioned_bucket_count().await.unwrap(),
            2,
            "a suspended bucket holds version data too"
        );
    }
}
