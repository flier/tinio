//! The `ObjectOps` implementation for [`MemoryStorage`].
//!
//! Object put/get/head/delete/listing, the tag ops, rename, and the
//! retained-part listing. The metadata access layer is the shared
//! implementation's: every object row read and write goes through the
//! ObjectMeta contract, and the object bytes go through the accounting
//! decorator, content first and the row that describes it after (design
//! 2026-10-03 §1 property 6).
//!
//! A bucket that **records versions** is the one exception: its write paths
//! keep the version-domain single transaction, because the supersede must read
//! the outgoing bytes before they are replaced, the slot columns the contract's
//! row cannot carry (D37) must land with the row, and the object and history
//! bytes share one all-or-nothing admission decision (see `write_object`).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(feature = "versioning")]
use crate::_store_redb::{object_meta, object_part, objects};
use crate::counting::report;
#[cfg(feature = "versioning")]
use crate::version::check_bucket_write;
#[cfg(feature = "versioning")]
use crate::version::{insert_marker, prepare_publish, record_current_as_version};
use crate::{
    _core::{
        BodyStream, ByteRange, ETag, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps,
        TagWriteResult, acl,
        acl::{Acl, AclGrants},
        bucket::Name,
        checksum, collect_body,
        content::{ContentKey, ObjectContent, StagedObject},
        from_nanos, group_and_paginate,
        meta::{ObjectMeta, VersionMeta, object::Row},
        multipart::ObjectPart,
        now_nanos, object,
        storage::VersionOps,
        version,
    },
    Error,
    error::{access_denied, invalid_key, no_such_key},
    storage::MemoryStorage,
    version::object_result,
};

/// A staged object body: the buffered payload (the commit inserts it)
/// plus the stage's recorded checksum (spec 2026-08-31) — the explicit
/// `carried` value when the caller passed one (a version-addressed
/// copy's source version, kind included), else the validated checksum
/// computed while the body streamed under the server's tee slot and
/// recorded as FULL_OBJECT (the write path's kind) — committed with no
/// re-hashing. `None` when the stage fixed neither.
pub struct StagedBody {
    data: Vec<u8>,
    checksum: Option<checksum::Recorded>,
}

impl MemoryStorage {
    /// Whether the bucket records versions (enabled or suspended) — the write
    /// path's discriminator and the carrier bit every served Info reports. The
    /// bucket's versioning state is the version contract's bucket half (D26),
    /// so the contract's own read answers it: a bucket with no state row is
    /// `State::Unversioned`.
    async fn bucket_records_versions(&self, bucket: &Name) -> Result<bool, Error> {
        Ok(VersionMeta::bucket_state(self.metadata().as_ref(), bucket)
            .await
            .map_err(Error::from)?
            .records_versions())
    }

    /// The key's current version identity — the version domain's fact (D37).
    /// The contract's object row carries no version column, so the identity
    /// comes from the current slot.
    async fn current_version_id(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<version::Id>, Error> {
        let slot = VersionMeta::current_slot(self.metadata().as_ref(), bucket, key)
            .await
            .map_err(Error::from)?;
        Ok(slot.and_then(|current| current.version))
    }

    /// The tag-write transaction of the object trio — the shared body of
    /// `put_object_tags` and `delete_object_tags` (the bucket and key
    /// gates stay in the ops): the contract replaces the row's tags element,
    /// and the key's current slot and the bucket's records-versions bit answer
    /// the pair ([TagWriteResult]) in the same round. `None` when the row is
    /// missing (nothing changed — nothing to name).
    async fn rewrite_tags_element(
        &self,
        bucket: &Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<Option<TagWriteResult>, Error> {
        // The row write is the contract's (a missing row answers false). The
        // version identity is the version domain's (D37) and the state bit is
        // the version contract's bucket half (D26).
        if !self
            .metadata()
            .set_tags(bucket, key, tags)
            .await
            .map_err(Error::from)?
        {
            return Ok(None);
        }
        Ok(Some(TagWriteResult {
            version_id: self.current_version_id(bucket, key).await?,
            bucket_records_versions: self.bucket_records_versions(bucket).await?,
        }))
    }

    /// The shared commit tail of the object write paths
    /// ([ObjectOps::commit_object] and the copy primitive). Folder markers
    /// never become objects (s3-surface.md): the body is dropped, the record
    /// stores the empty-content ETag, and the tags/checksum are
    /// accept-and-dropped — the fs backend's marker commit behaves the same.
    ///
    /// A bucket that records versions keeps the version-domain single
    /// transaction ([Self::write_object_recording_versions]); every other
    /// bucket takes the contract path ([Self::write_object_plain]) — bytes
    /// first through the accounting decorator, then the guarded row.
    #[allow(clippy::too_many_arguments)]
    async fn write_object(
        &self,
        bucket: &Name,
        key: &object::Key,
        mut data: Vec<u8>,
        mut tags: object::Tags,
        mut checksum: Option<checksum::Recorded>,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        // Defensive: the staged-body path already rejects reserved keys —
        // a direct stage/commit must not create an invisible, undeletable
        // object (the fs backend re-checks in its commit path too).
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // Folder markers are never objects (s3-surface.md): the staged body is
        // dropped, and a direct stage/commit with a non-empty body must not
        // strand invisible bytes counted as bucket content.
        if key.is_folder_marker() {
            data = Vec::new();
            tags = object::Tags::empty();
            checksum = None;
        }
        // The per-object cap is the accounting decorator's alone (round 29):
        // one gate for both write paths, after the marker normalization above
        // and before anything is staged or published.
        self.content().check_object_bytes(data.len() as u64)?;
        #[cfg(feature = "versioning")]
        if self.bucket_records_versions(bucket).await? {
            return self
                .write_object_recording_versions(bucket, key, data, tags, checksum, owner, acl)
                .await;
        }
        self.write_object_plain(bucket, key, data, tags, checksum, owner, acl)
            .await
    }

    /// The contract write path of a bucket that records no versions: the bytes
    /// are published through the accounting decorator first — it refuses
    /// max_object_bytes while the chunk is accepted and moves the shared total
    /// by the difference the write makes — then the row through
    /// [ObjectMeta::put_guarded] (D40's guaranteed bucket-row policy, so a
    /// concurrent bucket delete cannot be raced). A refused row write removes
    /// the published bytes again, so an object that was never recorded is
    /// billed for nothing.
    #[allow(clippy::too_many_arguments)]
    async fn write_object_plain(
        &self,
        bucket: &Name,
        key: &object::Key,
        data: Vec<u8>,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        let size = data.len() as u64;
        let etag = if key.is_folder_marker() {
            ETag::EMPTY
        } else {
            ETag::from_content(&data)
        };
        let now = now_nanos();
        let content = ContentKey::Object(key.clone());
        // The published bytes stay reachable as a handle: a refused row write's
        // refund must compare them against the key's content before it removes
        // anything (a newer writer's bytes are not this call's to take).
        let published = Bytes::from(data);
        // The publish's identity, recorded with the bytes: a later writer can
        // publish the same bytes (an empty body matches an empty body), and
        // the refusal's refund must still tell its own publish from theirs.
        let publish_id = self.content().mint_publish_id();
        let mut staged = self.content().stage(bucket, &content).await?;
        if !published.is_empty() {
            staged.write(published.clone()).await?;
        }
        self.content()
            .publish_tagged(bucket, staged, &content, publish_id)
            .await?;
        let row = Row {
            etag: etag.clone(),
            size,
            mtime: from_nanos(now),
            identity: 0,
            tags: tags.clone(),
            checksum: checksum.clone(),
            owner: owner.cloned(),
            acl: acl.clone(),
        };
        if let Err(err) = self
            .metadata()
            .put_guarded(bucket, key, &row)
            .await
            .map_err(Error::from)
        {
            // The refused row write must not strand the bytes it staged — and
            // must not take a newer writer's with them: the refund compares
            // before it deletes (the contract's `remove_if_matches`, §4). A
            // refund that itself fails is reported — it books its own release
            // either way — and is never promoted over the refusal it
            // compensates: the caller's error is the row write's.
            if let Err(failure) = self
                .roll_back_publish(bucket, &content, &published, publish_id, "OBJECT")
                .await
            {
                report(format_args!(
                    "object compensation after a refused row write failed: {failure}"
                ));
            }
            return Err(err);
        }
        // The row landed, so no compensation can follow this publish: its
        // identity leaves the map instead of waiting for the key's next
        // publish to replace it.
        self.content()
            .retire_publish_tag(bucket, &content, publish_id)
            .await;
        // The bucket can begin recording versions between this write's dispatch
        // read and its row landing — only a concurrent `PutBucketVersioning`
        // can do that — and the row just written is then an unversioned one in a
        // bucket that records versions: give it the identity a versioned write
        // would have given it, in one transaction
        // (`version::record_current_as_version`).
        #[cfg(feature = "versioning")]
        let (version_id, bucket_records_versions) = if self.bucket_records_versions(bucket).await? {
            // A repair that fails, or finds the row already identified, is
            // reported at most — never escalated: the write itself landed, and
            // the client's `put_object` must not fail for the repair. `None`
            // claims no identity: a row an earlier repair or a racing
            // versioned writer identified is not this write's to report.
            match record_current_as_version(self, bucket, key).await {
                Ok(Some(repaired)) => {
                    tracing::info!(
                        "{} was written while its bucket began recording versions; the row took \
                         the identity {repaired:?} after the fact (the content it replaced in \
                         that window is not recoverable)",
                        key.as_ref()
                    );
                    (Some(repaired), true)
                }
                Ok(None) => (None, true),
                Err(failure) => {
                    tracing::warn!(
                        "the version-window repair of {} failed: {failure}",
                        key.as_ref()
                    );
                    (None, true)
                }
            }
        } else {
            (None, false)
        };
        #[cfg(not(feature = "versioning"))]
        let (version_id, bucket_records_versions) = (None, false);
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(now),
            etag,
            tags,
            checksum,
            owner: owner.cloned(),
            acl: acl.clone(),
            version_id,
            bucket_records_versions,
        })
    }

    /// The write path of a bucket that records versions: one raw transaction.
    /// The contract's order (publish the bytes, then the row) cannot express
    /// it — the supersede must read the outgoing bytes before they are
    /// replaced, the slot columns the contract's row cannot carry (D37) land
    /// with the row, and the object and history bytes are one admission
    /// decision (version.rs's the_version_history_counts_against_the_total_limit).
    #[cfg(feature = "versioning")]
    #[allow(clippy::too_many_arguments)]
    async fn write_object_recording_versions(
        &self,
        bucket: &Name,
        key: &object::Key,
        data: Vec<u8>,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        let marker = key.is_folder_marker();
        let size = data.len() as u64;
        let etag = if marker {
            ETag::EMPTY
        } else {
            ETag::from_content(&data)
        };
        // The accounting lock is taken BEFORE the transaction and held across
        // it: the adjustment below runs inside the write, and the decorator's
        // lock is not reentrant — the closure may only use the `*_unlocked`
        // entry points.
        let guard = self.lock_accounting().await;
        let (now, version_id, records_versions) =
            self.db
                .write(|txn| -> Result<(u64, Option<version::Id>, bool), Error> {
                    check_bucket_write(txn, bucket)?;
                    let b = bucket.as_ref().as_str();
                    let k = key.as_ref().as_str();
                    let old_len = {
                        let objects = objects::Table::open(txn)?;
                        objects
                            .get(b, k)?
                            .map(|guard| guard.value().len() as u64)
                            .unwrap_or(0)
                    };
                    let delta = data.len() as i64 - old_len as i64;
                    let now = now_nanos();
                    // The current version's identity is the version state
                    // machine's call, and it runs BEFORE the publish below: what
                    // the machine moves into history is the outgoing content, so
                    // the object row must still hold it. Its answer also carries
                    // the bytes the supersede newly stored in the history's blob
                    // table (a dedup hit stores nothing: those bytes are already
                    // metered).
                    let (version_id, version_seq, history_bytes, records_versions) =
                        prepare_publish(txn, bucket, key, now)?;
                    {
                        let mut objects = objects::Table::open(txn)?;
                        objects.put(b, k, &data)?;
                    }
                    let stored = object_meta::Stored {
                        version_id,
                        version_seq,
                        is_delete_marker: false,
                        etag: etag.clone(),
                        size,
                        mtime: now,
                        file_identity: 0,
                        tags: tags.clone(),
                        checksum: checksum.clone(),
                        owner: owner.cloned(),
                        acl: acl.clone(),
                    };
                    {
                        let mut table = object_meta::Table::open(txn)?;
                        table.put(b, k, &stored)?;
                    }
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.remove_key(b, k)?;
                    }
                    // The write's bytes, in ONE adjustment after every fallible
                    // row operation and inside the transaction: the object row's
                    // own delta plus whatever the publish newly stored in the
                    // history's blob table. A limit breach aborts the whole
                    // transaction here, leaving the rows and the tracked total
                    // untouched. The accounting lock is already held (the caller
                    // took it before opening the transaction), so this is the
                    // unlocked variant.
                    self.adjust_total_unlocked(delta + history_bytes)?;
                    Ok((now, stored.version_id.clone(), records_versions))
                })?;
        // The versioned write published bytes through its own transaction:
        // give them an identity too, so a stale compensation of the plain
        // path can never mistake this content for its own (the guard is held,
        // so the unlocked entry point is the one to use).
        self.content().record_publish_tag(
            bucket,
            &ContentKey::Object(key.clone()),
            self.content().mint_publish_id(),
        );
        drop(guard);
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(now),
            etag,
            tags,
            checksum,
            owner: owner.cloned(),
            acl: acl.clone(),
            version_id,
            bucket_records_versions: records_versions,
        })
    }

    /// The delete of a bucket that records versions: one raw transaction, for
    /// the write path's reasons — the version state machine moves the outgoing
    /// version into history (or replaces the current marker) in the transaction
    /// that removes the rows, and the history's bytes and the object's bytes
    /// are one admission decision.
    #[cfg(feature = "versioning")]
    async fn delete_object_recording_versions(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        let (old_len, marker, history_bytes) =
            self.db
                .write(|txn| -> Result<(u64, version::Marker, i64), Error> {
                    check_bucket_write(txn, bucket)?;
                    let b = bucket.as_ref().as_str();
                    let k = key.as_ref().as_str();
                    let old_len = {
                        let objects = objects::Table::open(txn)?;
                        objects
                            .get(b, k)?
                            .map(|guard| guard.value().len() as u64)
                            .unwrap_or(0)
                    };
                    // Before the rows below are removed: a versioned bucket
                    // moves the outgoing version into history and makes a
                    // delete marker current in this same transaction. A folder
                    // marker never reaches it — folder markers stay outside the
                    // version model, so their delete creates no marker.
                    let (marker, history_bytes) = if key.is_folder_marker() {
                        (version::Marker::none(), 0)
                    } else {
                        insert_marker(txn, bucket, key, now_nanos())?
                    };
                    {
                        let mut objects = objects::Table::open(txn)?;
                        objects.remove(b, k)?;
                    }
                    {
                        let mut object_meta = object_meta::Table::open(txn)?;
                        object_meta.remove(b, k)?;
                    }
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.remove_key(b, k)?;
                    }
                    Ok((old_len, marker, history_bytes))
                })?;
        // The bytes that left the backend: the object row the delete removed,
        // minus the bytes its supersede newly stored in the history's blob
        // table (a versioned delete moves the content there instead of dropping
        // it — the same bytes stay metered; a dedup hit stores nothing fresh).
        // The net is never positive, so it can never exceed a limit and needs
        // no check. The adjustment lands after the transaction, so it takes the
        // accounting lock itself rather than riding a guard.
        if let Err(error) = self
            .adjust_total_locked(history_bytes - old_len as i64)
            .await
        {
            // The delta only ever removes bytes, so this is the "cannot happen"
            // arm of the accounting API — logged rather than dropped, because a
            // silently lost adjustment is exactly the drift the tracker exists
            // to prevent (§4).
            tracing::warn!(
                "the versioned delete of {} could not settle the tracked total: {error}",
                key.as_ref()
            );
        }
        // The key's bytes left (or a marker now stands where they were):
        // forget its publish identity, so no stale compensation claims them.
        {
            let _section = self.lock_accounting().await;
            self.content()
                .forget_publish_tag(bucket, &ContentKey::Object(key.clone()));
        }
        Ok(marker)
    }
}

#[async_trait]
impl ObjectOps for MemoryStorage {
    /// A staged body: the buffered payload (the commit inserts it) plus
    /// the stage's tee digest (see `StagedBody`).
    type StagedBody = StagedBody;

    async fn stage_body(
        &self,
        bucket: &Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<StagedBody, Error> {
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // Fast-fail on a missing bucket before buffering the body (the
        // commit's guarded row write re-checks, closing the race).
        self.ensure_bucket(bucket).await?;
        // Folder markers are never objects (s3-surface.md): no body is
        // buffered — the commit answers the marker's empty-content ETag
        // (the fs backend creates a directory instead). A marker's stage
        // carries no tee digest — no bytes streamed.
        if key.is_folder_marker() {
            return Ok(StagedBody {
                data: Vec::new(),
                checksum: None,
            });
        }
        // Stream the body before opening the transaction (the body future
        // cannot borrow the transaction guard). `tee` is the server's
        // checksum slot (spec 2026-08-31 — the `upload_part` pattern): the
        // interface wraps the body when the client sent a single
        // `x-amz-checksum-*` header, the digest is computed while the body
        // streams, a mismatch fails the staging as the tee's stream error
        // (propagated here like any body failure — the mem adds no error
        // of its own), and the validated digest rides into the commit as
        // FULL_OBJECT (the write path's kind). Absent, no digest is
        // computed. `carried` — a version-addressed copy's source version
        // checksum, kind included — wins over the streamed digest.
        let data = collect_body(body).await?;
        let computed = tee.as_ref().and_then(|c| c.digest.get()).cloned();
        Ok(StagedBody {
            data,
            checksum: carried.or(computed.map(|part| checksum::Recorded {
                part,
                kind: checksum::Type::FullObject,
            })),
        })
    }

    async fn commit_object(
        &self,
        bucket: &Name,
        key: &object::Key,
        staged: StagedBody,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        self.write_object(bucket, key, staged.data, tags, staged.checksum, owner, acl)
            .await
    }

    async fn copy_object(
        &self,
        src_bucket: &Name,
        src_key: &object::Key,
        dst_bucket: &Name,
        dst_key: &object::Key,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        // The contract's stream default (get → stage → commit) cannot
        // carry `checksum` — the mem override commits the served bytes
        // directly, so the copy's recorded checksum (kind included) rides
        // into the destination row like the fs fast path. The copy is a
        // fresh object: its tags are the caller's — never the source's —
        // and the shared commit tail clears any retained parts the
        // destination key held.
        let get = self.get_object(src_bucket, src_key, None).await?;
        let data = collect_body(get.body).await?;
        self.write_object(dst_bucket, dst_key, data, tags, checksum, owner, acl)
            .await
    }

    async fn get_object(
        &self,
        bucket: &Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        // The row is the existence gate (a domain-invalid etag row reads as
        // missing — the shared self-heal). The records-versions bit is the
        // version contract's bucket half (D26) and the version identity is the
        // key's current slot (D37) — the object row carries no version column.
        let row = self
            .metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| no_such_key(key))?;
        let records_versions = self.bucket_records_versions(bucket).await?;
        let version_id = self.current_version_id(bucket, key).await?;
        let size = row.size;
        let served_range = match range {
            None => None,
            Some(r) => Some(r.resolve(size)?),
        };
        // The content plane windows the range itself, so a partial read never
        // copies the whole object.
        let content = ContentKey::Object(key.clone());
        let data = collect_body(self.content().open(bucket, &content, range).await?).await?;
        let mut result = object_result(
            key,
            size,
            row.mtime,
            row.etag,
            row.tags,
            row.checksum,
            row.owner,
            row.acl,
            version_id,
            records_versions,
            &data,
            None,
        )?;
        result.served_range = served_range;
        Ok(result)
    }

    async fn head_object(&self, bucket: &Name, key: &object::Key) -> Result<object::Info, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_folder_marker() || key.is_reserved() {
            return Err(no_such_key(key));
        }
        let row = self
            .metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| no_such_key(key))?;
        Ok(object::Info {
            key: key.clone(),
            size: row.size,
            last_modified: row.mtime,
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
            owner: row.owner,
            acl: row.acl,
            version_id: self.current_version_id(bucket, key).await?,
            bucket_records_versions: self.bucket_records_versions(bucket).await?,
        })
    }

    async fn delete_object(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        self.ensure_bucket(bucket).await?;
        // The reserved segment is never a delete target (FR-020 — the fs
        // backend's `resolve_key` refusal).
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // A bucket that records versions keeps the version-domain transaction
        // (see `delete_object_recording_versions`).
        #[cfg(feature = "versioning")]
        if self.bucket_records_versions(bucket).await? {
            return self.delete_object_recording_versions(bucket, key).await;
        }
        // Unversioned: the bytes leave through the decorator's own removal
        // (which releases exactly what was counted), the row and its retained
        // part list after — the delete's mirror of the write order.
        self.content()
            .remove(bucket, &ContentKey::Object(key.clone()))
            .await?;
        self.metadata()
            .remove(bucket, key)
            .await
            .map_err(Error::from)?;
        Ok(version::Marker::none())
    }

    /// Rename moves the object's rows — except on a bucket that records
    /// versions, where the contract's guard refuses it (`AccessDenied`): no
    /// spec defines rename's versioning semantics, and the move would destroy
    /// the destination's current version and the source's history. An
    /// unversioned bucket renames exactly as before. The decorator accounts
    /// the move by difference, so an overwritten destination's stored bytes
    /// are subtracted — observable when `max_total_bytes` is set and the
    /// destination existed, and the contract rename's correct accounting.
    async fn rename_object(
        &self,
        bucket: &Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, Error> {
        if src == dst {
            // Degenerate — the interface answers 412 before calling; a
            // backend-level guard keeps the move idempotent.
            return self.head_object(bucket, src).await;
        }
        // A reserved destination is never a legal rename target (FR-020
        // — a write through the reserved segment, refused like every
        // other write path); a marker destination cannot hold an object
        // (mirror `complete_multipart_upload`'s refusal). A reserved or
        // marker source is never an object — `NoSuchKey` like head.
        if dst.is_reserved() {
            return Err(access_denied(dst));
        }
        if dst.is_folder_marker() {
            return Err(invalid_key(dst.to_string()));
        }
        if src.is_reserved() || src.is_folder_marker() {
            return Err(no_such_key(src));
        }
        self.ensure_bucket(bucket).await?;
        // The contract's rename guard: the whole policy, so a recording
        // bucket is refused before anything moves (the interface's rule).
        self.ensure_rename_allowed(bucket, dst).await?;
        // The source row is read first: the move answers the metadata the old
        // single transaction built from the moved row, and the contract's
        // move_ does not answer it. A rename is not a fresh object — nothing
        // is recomputed.
        let row = self
            .metadata()
            .get(bucket, src)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| no_such_key(src))?;
        // The bytes move first (the move is a content-plane rename), the row
        // and its retained part list after, in the contract's one move
        // transaction.
        self.content().rename(bucket, src, dst).await?;
        self.metadata()
            .move_(bucket, src, dst)
            .await
            .map_err(Error::from)?;
        Ok(object::Info {
            key: dst.clone(),
            size: row.size,
            last_modified: row.mtime,
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
            owner: row.owner,
            acl: row.acl,
            // The guard above passed, so the bucket records no versions.
            version_id: None,
            bucket_records_versions: false,
        })
    }

    async fn get_object_tags(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        // Object existence is the object row — head_object's gate (rows and
        // bytes are written together, so a row-less key is a missing object,
        // and folder-marker / reserved keys are never objects — NoSuchKey,
        // mirroring head).
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        self.metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
            .map(|row| row.tags)
            .ok_or_else(|| no_such_key(key))
    }

    async fn put_object_tags(
        &self,
        bucket: &Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, Error> {
        // Existence is the object row (NoSuchKey when missing — mirroring
        // head_object; rows are written with their object, so the fs backend's
        // row-heal for hand-dropped files has no mem equivalent). The bucket
        // and key gates answer first, like every write path (a reserved key on
        // a missing bucket is NoSuchBucket).
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        self.rewrite_tags_element(bucket, key, tags)
            .await?
            .ok_or_else(|| no_such_key(key))
    }

    async fn delete_object_tags(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, Error> {
        // Idempotent like `delete_object`: only the bucket must exist
        // (NoSuchBucket when missing); a missing object — key, marker, or row —
        // is a no-op that names no identity.
        self.ensure_bucket(bucket).await?;
        Ok(self
            .rewrite_tags_element(bucket, key, &object::Tags::empty())
            .await?
            .unwrap_or(TagWriteResult {
                version_id: None,
                bucket_records_versions: false,
            }))
    }

    async fn get_object_acl(&self, bucket: &Name, key: &object::Key) -> Result<Acl, Error> {
        // Existence is the object row (NoSuchKey when missing); the ACL
        // recombines the stored owner element with the stored grant set (the
        // row form keeps them apart — the shared decode-home rule).
        self.metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
            .map(|row| Acl {
                owner: row.owner,
                grants: row.acl.grants,
            })
            .ok_or_else(|| no_such_key(key))
    }

    async fn put_object_acl(
        &self,
        bucket: &Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        // Replace-all: the contract replaces the row's ACL element; the owner
        // element is preserved — a put never changes it (contract ruling).
        // NoSuchKey when the object is missing.
        let Some(row) = self
            .metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
        else {
            return Err(no_such_key(key));
        };
        self.metadata()
            .set_acl(bucket, key, row.owner.as_ref(), grants)
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn list_object_parts(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // Existence is the object row (T2-B ruling: a missing object answers
        // NoSuchKey, mirroring `get_object_tags`); the retained rows of a
        // multipart-completed object are served in part-number order — empty
        // for an object that was never multipart-completed (a plain put or
        // copy has no parts).
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        if self
            .metadata()
            .get(bucket, key)
            .await
            .map_err(Error::from)?
            .is_none()
        {
            return Err(no_such_key(key));
        }
        let parts = self
            .metadata()
            .parts(bucket, key)
            .await
            .map_err(Error::from)?;
        Ok(parts
            .into_iter()
            .map(|row| ObjectPart {
                part_number: row.number.into(),
                size: row.size,
                checksum: row.checksum,
            })
            .collect())
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The page's objects all carry the bucket's state — one read for the
        // page, the carrier's value on every row.
        let records_versions = self.bucket_records_versions(&params.bucket).await?;
        // The healing walk is the old per-row validation: a domain-invalid row
        // is treated as missing, where the strict `walk` would fail the whole
        // listing instead.
        let walked = self
            .metadata()
            .walk_healing(&params.bucket)
            .await
            .map_err(Error::from)?;
        let mut rows: Vec<object::Info> = Vec::new();
        for (key, row) in walked {
            let Some(row) = row else {
                continue; // a garbage-etag row is treated as missing
            };
            if key.is_folder_marker() || key.is_reserved() {
                continue;
            }
            if !key.as_ref().starts_with(&params.prefix) {
                continue;
            }
            // The contract's row carries no version column (D37), so a bucket
            // that records versions pays one point read per listed key; a
            // bucket that does not has nothing to look up.
            let version_id = if records_versions {
                self.current_version_id(&params.bucket, &key).await?
            } else {
                None
            };
            rows.push(object::Info {
                key,
                size: row.size,
                last_modified: row.mtime,
                etag: row.etag,
                tags: row.tags,
                checksum: row.checksum,
                owner: row.owner,
                acl: row.acl,
                version_id,
                bucket_records_versions: records_versions,
            });
        }
        let (keys, common_prefixes, truncated, next_start_after) = group_and_paginate(
            rows,
            &params.prefix,
            params.delimiter.as_deref(),
            params.start_after.as_deref(),
            params.max_keys,
            |o| o.key.as_ref(),
        );
        Ok(ObjectListing {
            objects: keys,
            common_prefixes,
            truncated,
            next_start_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream::iter;
    use redb::ReadableDatabase;

    use super::*;
    use crate::{
        _core::{
            BodyStream, BucketOps, ListObjectsParams, ObjectListing, ObjectOps, acl::Acl, bucket,
            checksum, object, storage::Error::*,
        },
        _util::testing::{body, complete_single_part, read_body},
        MemoryOptions,
        testutil::{checksum_tee, publish_tagged_bytes},
    };

    async fn with_bucket() -> (MemoryStorage, Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        (storage, name)
    }

    /// The refused row write's refund is conditional on the publish's identity
    /// (ruling 2026-10-08): it takes back only this call's own publish, so a
    /// newer writer at the same key keeps its content — even when that writer
    /// published **byte-identical** content, which the byte compare alone
    /// cannot tell from ours.
    #[tokio::test]
    async fn the_staged_bytes_refund_leaves_a_newer_writers_content_alone() {
        let (storage, b) = with_bucket().await;
        let key = object::key("raced.bin").unwrap();
        let content = ContentKey::Object(key.clone());

        // Ours first, then a later writer's over the same key — the same
        // bytes, so only the recorded publish identity separates them.
        let ours = storage.content().mint_publish_id();
        publish_tagged_bytes(&storage, &b, &content, b"same body", ours).await;
        let theirs = storage.content().mint_publish_id();
        publish_tagged_bytes(&storage, &b, &content, b"same body", theirs).await;

        // The stale compensation names its own publish and finds a newer
        // one: nothing is deleted.
        storage
            .roll_back_publish(&b, &content, b"same body", ours, "OBJECT")
            .await
            .unwrap();
        assert_eq!(
            read_body(storage.content().open(&b, &content, None).await.unwrap())
                .await
                .unwrap(),
            b"same body".to_vec(),
            "the newer writer's byte-identical content stays"
        );

        // The live publish's refund does take its bytes back.
        storage
            .roll_back_publish(&b, &content, b"same body", theirs, "OBJECT")
            .await
            .unwrap();
        assert!(
            storage.content().open(&b, &content, None).await.is_err(),
            "the live publish is taken back"
        );
    }

    /// Complete a fresh one-part multipart upload onto `key` — a single
    /// tiny part is the final part (no 5 MiB minimum). The fs suite's
    /// helper of the same shape.

    #[tokio::test]
    async fn object_size_limit_rejects_oversized_objects() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: Some(4),
            max_total_bytes: None,
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("big.bin").unwrap();

        let err = storage
            .put_object(&name, &key, body(b"12345"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { size: 5, limit: 4 })),
            "{err}"
        );
        // At-or-below the limit succeeds.
        storage
            .put_object(&name, &key, body(b"1234"))
            .await
            .unwrap();
        // An overwrite that pushes past the limit is refused too.
        let err = storage
            .put_object(&name, &key, body(b"12345"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { .. })),
            "{err}"
        );
    }

    #[tokio::test]
    async fn total_size_limit_rejects_and_releases_on_delete() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(10),
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k1 = object::key("a.bin").unwrap();
        let k2 = object::key("b.bin").unwrap();

        storage
            .put_object(&name, &k1, body(b"12345"))
            .await
            .unwrap();
        // 5 + 6 = 11 > 10 → refused.
        let err = storage
            .put_object(&name, &k2, body(b"123456"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { .. })),
            "{err}"
        );
        // 5 + 5 = 10 fits.
        storage
            .put_object(&name, &k2, body(b"12345"))
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 10);

        // Deleting frees the capacity.
        storage.delete_object(&name, &k1).await.unwrap();
        let k3 = object::key("c.bin").unwrap();
        let err = storage
            .put_object(&name, &k3, body(b"123456"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { .. })),
            "{err}"
        );
        storage
            .put_object(&name, &k3, body(b"12345"))
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 10);
    }

    fn chunked(parts: &[&[u8]]) -> BodyStream {
        let owned: Vec<_> = parts
            .iter()
            .map(|p| Ok(Bytes::from((*p).to_vec())))
            .collect();
        Box::pin(iter(owned))
    }

    fn params(
        bucket: &Name,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> ListObjectsParams {
        ListObjectsParams {
            bucket: bucket.clone(),
            prefix: prefix.into(),
            delimiter: delimiter.map(str::to_string),
            start_after: start_after.map(str::to_string),
            max_keys,
        }
    }

    async fn put_keys(storage: &MemoryStorage, bucket: &Name, keys: &[&str]) {
        for key in keys {
            storage
                .put_object(
                    bucket,
                    &object::key(*key).unwrap(),
                    body(key.as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
    }

    fn object_keys(page: &ObjectListing) -> Vec<&str> {
        page.objects.iter().map(|o| &*o.key).collect()
    }

    #[tokio::test]
    async fn object_ops_on_missing_bucket_are_no_such_bucket() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("gone").unwrap();
        let key = object::key("a.txt").unwrap();
        assert!(matches!(
            storage
                .put_object(&bucket, &key, body(b"x".to_vec()))
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.get_object(&bucket, &key, None).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.head_object(&bucket, &key).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.delete_object(&bucket, &key).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage
                .list_objects(ListObjectsParams {
                    bucket: bucket.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    start_after: None,
                    max_keys: 1000,
                })
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn put_concatenates_body_chunks() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("chunked").unwrap();
        storage
            .put_object(&bucket, &key, chunked(&[b"hel", b"lo", b"", b"!"]))
            .await
            .unwrap();
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"hello!");
    }

    #[tokio::test]
    async fn list_objects_empty_bucket_is_not_truncated() {
        let (storage, bucket) = with_bucket().await;
        let page = storage
            .list_objects(params(&bucket, "", None, None, 1000))
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert!(!page.truncated);
        assert_eq!(page.next_start_after, None);
    }

    #[tokio::test]
    async fn list_objects_exact_page_is_not_truncated() {
        let (storage, bucket) = with_bucket().await;
        put_keys(&storage, &bucket, &["a.txt", "b.txt"]).await;
        let page = storage
            .list_objects(params(&bucket, "", None, None, 2))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["a.txt", "b.txt"]);
        assert!(!page.truncated);
        assert_eq!(page.next_start_after, None);
    }

    #[tokio::test]
    async fn list_objects_start_after_inside_prefix_excludes_the_marker() {
        let (storage, bucket) = with_bucket().await;
        put_keys(&storage, &bucket, &["dir/a.txt", "dir/b.txt", "dir/c.txt"]).await;
        let page = storage
            .list_objects(params(&bucket, "dir/", None, Some("dir/a.txt"), 1000))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["dir/b.txt", "dir/c.txt"]);
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn list_objects_start_after_before_prefix_still_lists_the_prefix() {
        let (storage, bucket) = with_bucket().await;
        put_keys(
            &storage,
            &bucket,
            &["a.txt", "dir/a.txt", "dir/b.txt", "z.txt"],
        )
        .await;
        let page = storage
            .list_objects(params(&bucket, "dir/", None, Some("a.txt"), 1000))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["dir/a.txt", "dir/b.txt"]);
        assert!(page.common_prefixes.is_empty());
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn list_objects_object_marker_inside_rollup_skips_the_prefix() {
        let (storage, bucket) = with_bucket().await;
        put_keys(
            &storage,
            &bucket,
            &["dir/a.txt", "dir/c.txt", "dir/e.txt", "z.txt"],
        )
        .await;
        let page = storage
            .list_objects(params(&bucket, "", Some("/"), Some("dir/c.txt"), 1000))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["z.txt"]);
        assert!(page.common_prefixes.is_empty());
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn list_objects_nested_delimiter_under_prefix() {
        let (storage, bucket) = with_bucket().await;
        put_keys(
            &storage,
            &bucket,
            &["dir/a.txt", "dir/sub/b.txt", "dir/sub/c.txt"],
        )
        .await;
        let page = storage
            .list_objects(params(&bucket, "dir/", Some("/"), None, 1000))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["dir/a.txt"]);
        assert_eq!(page.common_prefixes, ["dir/sub/"]);
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn list_objects_max_zero_returns_an_empty_untruncated_page() {
        let (storage, bucket) = with_bucket().await;
        put_keys(&storage, &bucket, &["a.txt"]).await;
        let page = storage
            .list_objects(params(&bucket, "", None, None, 0))
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        // No resume marker: an exclusive-after marker would skip the
        // first object of the next page forever.
        assert!(!page.truncated);
        assert_eq!(page.next_start_after, None);
    }

    #[tokio::test]
    async fn list_objects_does_not_cross_buckets() {
        let (storage, bucket) = with_bucket().await;
        let other = bucket::name("other").unwrap();
        storage
            .create_bucket(&other, None, &Acl::default_private(None))
            .await
            .unwrap();
        put_keys(&storage, &bucket, &["a.txt"]).await;
        put_keys(&storage, &other, &["b.txt"]).await;
        let page = storage
            .list_objects(params(&bucket, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(object_keys(&page), ["a.txt"]);
    }

    // --- tags + recorded checksums + retained parts (spec 2026-08-31) ---

    #[tokio::test]
    async fn mem_object_tags_round_trip_and_replace() {
        let (storage, b) = with_bucket().await;
        let k = object::key("t.txt").unwrap();
        storage
            .put_object(&b, &k, body(b"x".to_vec()))
            .await
            .unwrap();
        assert!(
            storage.get_object_tags(&b, &k).await.unwrap().is_empty(),
            "an untagged object answers the empty set"
        );

        // Put → Get round-trip (replace-all, no merge).
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        storage.put_object_tags(&b, &k, &tags).await.unwrap();
        assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), tags);
        let replaced = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
        storage.put_object_tags(&b, &k, &replaced).await.unwrap();
        assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), replaced);
        // head carries the same tags (Info.tags — one source of truth).
        assert_eq!(storage.head_object(&b, &k).await.unwrap().tags, replaced);

        // Delete clears; the row's other elements survive the tag write.
        storage.delete_object_tags(&b, &k).await.unwrap();
        assert!(storage.get_object_tags(&b, &k).await.unwrap().is_empty());
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 1, "the etag/size row must survive tag ops");
        assert!(head.tags.is_empty());

        // Missing object: get/put → NoSuchKey, delete succeeds.
        let missing = object::key("missing.txt").unwrap();
        let err: Error = storage.get_object_tags(&b, &missing).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchKey(_))));
        let err: Error = storage
            .put_object_tags(&b, &missing, &tags)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchKey(_))));
        storage.delete_object_tags(&b, &missing).await.unwrap();
        // A missing bucket answers NoSuchBucket (get and delete alike).
        let ghost = bucket::name("ghost").unwrap();
        let err: Error = storage.get_object_tags(&ghost, &k).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
        let err: Error = storage.delete_object_tags(&ghost, &k).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
    }

    #[tokio::test]
    async fn mem_commit_and_copy_carry_tags() {
        let (storage, b) = with_bucket().await;
        let a = object::key("a.txt").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        // The commit records the tags with the object — no post-commit
        // tag window.
        let staged = storage
            .stage_body(&b, &a, body(b"hi".to_vec()), None, None)
            .await
            .unwrap();
        let info = storage
            .commit_object(
                &b,
                &a,
                staged,
                tags.clone(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        assert_eq!(info.etag, ETag::from_content(b"hi"));
        assert_eq!(info.tags, tags);
        assert_eq!(storage.head_object(&b, &a).await.unwrap().tags, tags);

        // A copy is a fresh object whose tags are the caller's — never
        // the source's.
        let dst = object::key("b.txt").unwrap();
        let copy_tags = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
        storage
            .copy_object(
                &b,
                &a,
                &b,
                &dst,
                copy_tags.clone(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(storage.get_object_tags(&b, &dst).await.unwrap(), copy_tags);
        assert_eq!(storage.get_object_tags(&b, &a).await.unwrap(), tags);
    }

    #[tokio::test]
    async fn mem_commit_records_the_stage_tee_checksum() {
        // spec 2026-08-31: a plain PUT under the checksum toggle records
        // the tee's validated digest as the object's FULL_OBJECT checksum
        // — the backend never re-hashes.
        let (storage, b) = with_bucket().await;
        let k = object::key("c.txt").unwrap();
        let staged = storage
            .stage_body(
                &b,
                &k,
                body(b"hello".to_vec()),
                Some(checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==")),
                None,
            )
            .await
            .unwrap();
        storage
            .commit_object(
                &b,
                &k,
                staged,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let head = storage.head_object(&b, &k).await.unwrap();
        let recorded = head.checksum.expect("the tee digest must be recorded");
        assert_eq!(recorded.part.algorithm, checksum::Algorithm::Crc32);
        assert_eq!(recorded.part.value.0, "NhCmhg==");
        assert_eq!(recorded.kind, checksum::Type::FullObject);
        // A put without a tee records none.
        storage
            .put_object(&b, &object::key("plain.txt").unwrap(), body(b"x".to_vec()))
            .await
            .unwrap();
        let head = storage
            .head_object(&b, &object::key("plain.txt").unwrap())
            .await
            .unwrap();
        assert!(head.checksum.is_none());
    }

    #[tokio::test]
    async fn mem_object_parts_lifecycle() {
        // The OBJECT_PARTS lifecycle (spec 2026-08-31): an overwrite via
        // commit removes the rows, delete removes them, rename migrates
        // them with the record, and copy never inherits them.
        let (storage, b) = with_bucket().await;
        let k = object::key("mp.bin").unwrap();
        complete_single_part(&storage, &b, &k).await;
        assert_eq!(storage.list_object_parts(&b, &k).await.unwrap().len(), 1);

        // (a) An overwriting commit is a fresh single-part object: its
        // parts rows are gone.
        let staged = storage
            .stage_body(&b, &k, body(b"plain".to_vec()), None, None)
            .await
            .unwrap();
        storage
            .commit_object(
                &b,
                &k,
                staged,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        assert!(
            storage.list_object_parts(&b, &k).await.unwrap().is_empty(),
            "an overwrite must not leave the completed object's parts"
        );

        // (c) rename migrates the parts rows with the record.
        complete_single_part(&storage, &b, &k).await;
        let moved = object::key("moved.bin").unwrap();
        storage.rename_object(&b, &k, &moved).await.unwrap();
        assert_eq!(
            storage.list_object_parts(&b, &moved).await.unwrap().len(),
            1
        );
        let err: Error = storage.list_object_parts(&b, &k).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchKey(_))));
        // A rename over an existing destination replaces it (the dst's
        // own stale rows die).
        let dst = object::key("dst.bin").unwrap();
        complete_single_part(&storage, &b, &dst).await;
        storage.rename_object(&b, &moved, &dst).await.unwrap();
        assert_eq!(storage.list_object_parts(&b, &dst).await.unwrap().len(), 1);

        // (d) copy_object never inherits the source's parts.
        let copy = object::key("copy.bin").unwrap();
        storage
            .copy_object(
                &b,
                &dst,
                &b,
                &copy,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap();
        assert!(
            storage
                .list_object_parts(&b, &copy)
                .await
                .unwrap()
                .is_empty(),
            "a copy is single-part: the source's rows must not follow"
        );

        // (b) delete removes the rows — proved straight in the database
        // (the object is gone, so the parts list answers NoSuchKey).
        storage.delete_object(&b, &dst).await.unwrap();
        let err: Error = storage.list_object_parts(&b, &dst).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchKey(_))));
        let txn = storage.db.db().unwrap().begin_read().unwrap();
        let parts = object_part::Table::open_readonly(&txn).unwrap();
        let bucket_str = b.as_ref().as_str();
        let mut rows: Vec<String> = Vec::new();
        for entry in parts.range((bucket_str, "", 0)..).unwrap() {
            let (k, _) = entry.unwrap();
            let (rb, rk, _) = k.value();
            if rb != bucket_str {
                break;
            }
            rows.push(rk.to_string());
        }
        assert!(
            rows.is_empty(),
            "no OBJECT_PARTS rows may outlive their object: {rows:?}"
        );
    }

    #[tokio::test]
    async fn rename_moves_rows_on_an_unversioned_bucket() {
        let (storage, b) = with_bucket().await;
        let src = object::key("mv-src.txt").unwrap();
        let dst = object::key("mv-dst.txt").unwrap();
        storage
            .put_object(&b, &src, body(b"src".to_vec()))
            .await
            .unwrap();
        // The destination exists too: the move overwrites it, today's
        // semantics.
        storage
            .put_object(&b, &dst, body(b"dst".to_vec()))
            .await
            .unwrap();

        let moved = storage.rename_object(&b, &src, &dst).await.unwrap();
        assert_eq!(moved.key, dst);
        assert_eq!(
            read_body(storage.get_object(&b, &dst, None).await.unwrap().body)
                .await
                .unwrap(),
            b"src"
        );
        assert!(storage.head_object(&b, &src).await.is_err());
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn rename_is_refused_on_a_versioned_bucket_without_touching_either_key() {
        use crate::_core::{storage::VersionOps, version::State};

        let (storage, b) = with_bucket().await;
        let src = object::key("v-src.txt").unwrap();
        let dst = object::key("v-dst.txt").unwrap();
        storage
            .put_object(&b, &src, body(b"src".to_vec()))
            .await
            .unwrap();
        storage
            .put_object(&b, &dst, body(b"dst".to_vec()))
            .await
            .unwrap();
        let src_head = storage.head_object(&b, &src).await.unwrap();
        let dst_head = storage.head_object(&b, &dst).await.unwrap();
        storage
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();

        let err = storage.rename_object(&b, &src, &dst).await.unwrap_err();
        assert!(matches!(err, Error::Storage(AccessDenied(_))), "{err}");
        // Neither key moved: both contents and their version identities are
        // exactly what they were before the refused call.
        let kept_src = storage.head_object(&b, &src).await.unwrap();
        assert_eq!(kept_src.version_id, src_head.version_id);
        assert_eq!(kept_src.etag, src_head.etag);
        let kept_dst = storage.head_object(&b, &dst).await.unwrap();
        assert_eq!(kept_dst.version_id, dst_head.version_id);
        assert_eq!(kept_dst.etag, dst_head.etag);
        assert_eq!(
            read_body(storage.get_object(&b, &dst, None).await.unwrap().body)
                .await
                .unwrap(),
            b"dst"
        );

        // Suspended refuses too — the same row move would corrupt the null
        // version's history.
        storage
            .set_bucket_versioning(&b, State::Suspended)
            .await
            .unwrap();
        let err = storage.rename_object(&b, &src, &dst).await.unwrap_err();
        assert!(matches!(err, Error::Storage(AccessDenied(_))), "{err}");
    }
    /// The shared metadata conformance suite, run from mem's tests (the second
    /// consumer's leg): the store the ops above serve must satisfy every
    /// bucket, object and upload rule the contract pins.
    #[tokio::test]
    async fn metadata_conformance() {
        use crate::_util::testing::metadata::assert_metadata_conformance;

        let storage = MemoryStorage::new().unwrap();
        assert_metadata_conformance(storage.metadata().as_ref()).await;
    }

    /// Review focus 1: a put whose guarded row write is refused (its bucket is
    /// gone) must leave none of the staged bytes counted — the decorator's
    /// publish happened, and the compensation removal takes it back out.
    #[tokio::test]
    async fn a_refused_guarded_row_write_leaves_the_total_unchanged() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(64),
        })
        .unwrap();
        let name = bucket::name("gone").unwrap();
        let key = object::key("a.txt").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        // Stage while the bucket still exists, then take the bucket away: the
        // commit's row write is the refusal under test.
        let staged = storage
            .stage_body(&name, &key, body(b"staged".to_vec()), None, None)
            .await
            .unwrap();
        storage.delete_bucket(&name).await.unwrap();
        let err = storage
            .commit_object(
                &name,
                &key,
                staged,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err}");
        assert_eq!(storage.total_bytes(), 0, "a refused write meters nothing");
    }

    /// The window the repair exists for (ruling 2026-10-08), driven through the
    /// plain write itself: a write whose dispatch read the bucket as unversioned
    /// while the bucket now records versions must not land an identity-less row.
    #[cfg(feature = "versioning")]
    mod versioned {
        use super::*;
        use crate::{
            _core::{
                storage::VersionOps,
                version::{Id, State},
            },
            _store_redb::{object_meta, version as stored},
        };

        #[tokio::test]
        async fn a_plain_write_into_a_bucket_that_records_versions_is_repaired() {
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(4096),
            })
            .unwrap();
            let b = bucket::name("data").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            let key = object::key("window.txt").unwrap();
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            // The dispatch read the state before the flip: the plain path runs
            // against a bucket that now records versions.
            let info = storage
                .write_object_plain(
                    &b,
                    &key,
                    b"window!!".to_vec(),
                    object::Tags::empty(),
                    None,
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
            let id = info
                .version_id
                .clone()
                .expect("the recheck minted the identity a versioned write would have");
            assert!(
                info.bucket_records_versions,
                "the answer reports the bucket's state"
            );
            assert_eq!(storage.total_bytes(), 8, "the bytes are metered once");
            let bucket_name = b.as_ref().as_str().to_owned();
            let key_name = key.as_ref().as_str().to_owned();
            let (slot, rows) = storage
                .db
                .read(|txn| -> Result<(Option<Id>, u64), Error> {
                    let slot = object_meta::Table::open_readonly(txn)?
                        .get(&bucket_name, &key_name)?
                        .and_then(|row| row.version_id.clone());
                    let mut rows = 0u64;
                    stored::Table::open_readonly(txn)?.for_key(&bucket_name, &key_name, |_| {
                        rows += 1;
                        Ok(())
                    })?;
                    Ok((slot, rows))
                })
                .unwrap();
            assert_eq!(slot, Some(id), "the row's slot carries the identity");
            assert_eq!(
                rows, 0,
                "the current version is the object row: none was written"
            );
        }
    }
}
