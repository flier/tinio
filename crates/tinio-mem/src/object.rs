//! The `ObjectOps` implementation for [`MemoryStorage`].
//!
//! Object put/get/head/delete/listing, the tag ops, rename, and the
//! retained-part listing over the `objects` + `object_meta` +
//! `object_parts` tables (spec 2026-08-31). Reads use read transactions
//! with zero-copy `&str` / `&[u8]` access; bodies are copied out before
//! the transaction ends (streams are `'static` and cannot borrow the
//! transaction guard).

use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;

#[cfg(feature = "versioning")]
use crate::version::{insert_marker, prepare_publish, rename_refusal};
use crate::{
    _core::{
        BodyStream, ByteRange, ETag, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps,
        TagWriteResult, acl,
        acl::{Acl, AclGrants},
        bucket::Name,
        checksum, collect_body, from_nanos, group_and_paginate,
        multipart::ObjectPart,
        now_nanos, object, version,
    },
    _store::{bucket, bucket_versioning, meta, object_part, objects, scan::for_each_pair},
    Error,
    error::{access_denied, invalid_key, no_such_bucket, no_such_key},
    storage::{MemoryStorage, check_bucket},
    version::{object_result, records_versions},
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
    /// The tag-write transaction of the object trio — the shared body of
    /// `put_object_tags` and `delete_object_tags` (the bucket and key
    /// gates stay in the ops): the table accessor replaces the row's
    /// tags element, keeping the row's other elements (etag, size,
    /// mtime, recorded checksum) verbatim, and the same transaction
    /// reads the rewritten row's current-version identity and the
    /// bucket's records-versions bit — the answer's pair ([`TagWriteResult`]),
    /// one transaction, no second read. `None` when the row is missing
    /// (nothing changed — nothing to name).
    async fn rewrite_tags_element(
        &self,
        bucket: &Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<Option<TagWriteResult>, Error> {
        self.db
            .write(|txn| -> Result<Option<TagWriteResult>, Error> {
                let b = bucket.as_ref().as_str();
                let k = key.as_ref().as_str();
                let row = {
                    let mut table = meta::Table::open(txn)?;
                    if table.put_tags(b, k, tags)?.is_none() {
                        return Ok(None);
                    }
                    table.get(b, k)?
                };
                let Some(row) = row else {
                    return Ok(None);
                };
                let records_versions = bucket_versioning::Table::open(txn)?
                    .get(b)?
                    .unwrap_or_default()
                    .records_versions();
                Ok(Some(TagWriteResult {
                    version_id: row.version_id,
                    bucket_records_versions: records_versions,
                }))
            })
    }

    /// The shared commit tail of the object write paths
    /// ([`ObjectOps::commit_object`] and the copy primitive): atomically
    /// publish `data` onto `key` in one write transaction and record the
    /// interface-validated `tags` and the recorded `checksum` (the
    /// FULL_OBJECT tee digest of a plain commit, or an explicit carried
    /// value — kind included) in the same `OBJECT_META` row — no
    /// post-commit tag window — while removing any stale `OBJECT_PARTS`
    /// rows of the key (a fresh object is single-part: overwriting a
    /// previously multipart-completed object must not leave its parts
    /// behind). Folder markers never become objects (s3-surface.md): the
    /// body is dropped, the record stores the empty-content ETag, and the
    /// tags/checksum are accept-and-dropped (the fs backend's marker
    /// commit behaves the same way). Returns the committed metadata.
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
        // Folder markers are never objects (s3-surface.md): the staged
        // body is dropped and the record stores the empty-content ETag —
        // still counted as bucket content (delete-bucket's non-empty
        // check), matching the fs backend's directory. A marker's bytes
        // are never stored: a direct stage/commit with a non-empty body
        // must not strand invisible bytes counted as content (the fs
        // backend's commit creates a directory and drops the temp).
        let marker = key.is_folder_marker();
        if marker {
            // A marker records no body, tags, or checksum (accepted and
            // dropped) — the fs backend's marker commit behaves the same.
            data = Vec::new();
            tags = object::Tags::empty();
            checksum = None;
        }
        let size = data.len() as u64;
        // Enforce the per-object size limit before opening the write
        // transaction (fast fail; folder markers are empty and pass).
        self.check_object_size(size)?;
        let etag = if marker {
            ETag::EMPTY
        } else {
            ETag::from_content(&data)
        };
        let (now, version_id, records_versions) = self
            .db
            .write(|txn| -> Result<(u64, Option<version::Id>, bool), Error> {
            {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
            }
            let b = bucket.as_ref().as_str();
            let k = key.as_ref().as_str();
            // The shared handles borrow the write transaction exclusively,
            // so each table is a separate pass (the fs pattern).
            let old_len = {
                let objects = objects::Table::open(txn)?;
                objects
                    .get(b, k)?
                    .map(|guard| guard.value().len() as u64)
                    .unwrap_or(0)
            };
            let delta = data.len() as i64 - old_len as i64;
            let now = now_nanos();
            // The current version's identity is the version state machine's
            // call (spec 2026-10-03), and it runs BEFORE the publish below:
            // what the machine moves into history is the outgoing content, so
            // the object row must still hold it. An unversioned bucket
            // records no identity, an enabled one records the freshly minted
            // one, a suspended one records the null version. Its answer also
            // carries the bytes the supersede newly stored in the history's
            // blob table (a dedup hit stores nothing: those bytes are already
            // metered).
            #[cfg(feature = "versioning")]
            let (version_id, version_seq, history_bytes, records_versions) =
                prepare_publish(txn, bucket, key, now)?;
            #[cfg(not(feature = "versioning"))]
            let (history_bytes, records_versions) = (0i64, false);
            {
                let mut objects = objects::Table::open(txn)?;
                objects.put(b, k, &data)?;
            }
            // The 6-tuple row (etag, size, mtime, file identity 0, tags
            // wire, checksum wire) via the shared `meta::Stored` — the
            // write-path encode home. A folder marker records no tags /
            // checksum (accepted and dropped), like the fs backend's
            // marker commit. (The `mut` is the versioning branch's.)
            #[cfg_attr(not(feature = "versioning"), allow(unused_mut))]
            let mut stored = meta::Stored {
                version_id: None,
                version_seq: 0,
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
            #[cfg(feature = "versioning")]
            {
                stored.version_id = version_id;
                stored.version_seq = version_seq;
            }
            {
                let mut table = meta::Table::open(txn)?;
                table.put(b, k, &stored)?;
            }
            {
                let mut parts = object_part::Table::open(txn)?;
                parts.remove_key(b, k)?;
            }
            // The write's bytes, in ONE adjustment after every fallible row
            // operation and inside the transaction: the object row's own
            // delta (the replaced content leaves, the new content enters) plus
            // whatever the publish newly stored in the history's blob table.
            // A limit breach aborts the whole transaction here, leaving both
            // the rows and the tracked total untouched.
            self.adjust_total(delta + history_bytes)?;
            Ok((now, stored.version_id.clone(), records_versions))
        })
        // The in-memory backend's commit cannot fail (no disk path — an
        // abort-only transaction is the only failure mode), so no
        // rollback_total compensation is needed here.
        ?;
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
        // commit transaction re-checks, closing the race).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
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
        self.db.read(|txn| {
            {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
            }
            if key.is_reserved() || key.is_folder_marker() {
                return Err(no_such_key(key));
            }
            // The bucket's versioning state rides the same transaction as
            // the row it describes — the carrier for the plain read's
            // version-header rule (the state the server used to read again
            // per request).
            let records_versions = records_versions(txn, bucket)?;
            let objects = objects::Table::open_readonly(txn)?;
            let meta = meta::Table::open_readonly(txn)?;
            // The meta row is the existence gate; the shared validation decodes
            // the 6-tuple (etag, size, mtime, file identity 0, tags wire,
            // checksum wire) and self-heals a garbage tags/checksum element to
            // empty/`None` (a garbage etag row reads as missing — the caller
            // recomputes, the mem self-heal of a domain-invalid row).
            let stored = meta
                .get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .ok_or_else(|| no_such_key(key))?;
            let size = stored.size;
            let served_range = match range {
                None => None,
                Some(r) => Some(r.resolve(size)?),
            };
            // The served slice is copied straight out of the zero-copy redb
            // guard (the shared construction does the slicing) — a range
            // request never copies the full object.
            let data_guard = objects
                .get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .ok_or_else(|| no_such_key(key))?;
            Ok(object_result(
                key,
                size,
                from_nanos(stored.mtime),
                stored.etag,
                stored.tags,
                stored.checksum,
                stored.owner.clone(),
                stored.acl.clone(),
                stored.version_id.clone(),
                records_versions,
                data_guard.value(),
                served_range,
            ))
        })
    }

    async fn head_object(&self, bucket: &Name, key: &object::Key) -> Result<object::Info, Error> {
        self.db.read(|txn| {
            {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
            }
            if key.is_folder_marker() || key.is_reserved() {
                return Err(no_such_key(key));
            }
            let records_versions = records_versions(txn, bucket)?;
            let meta = meta::Table::open_readonly(txn)?;
            let stored = meta
                .get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .ok_or_else(|| no_such_key(key))?;
            Ok(object::Info {
                key: key.clone(),
                size: stored.size,
                last_modified: from_nanos(stored.mtime),
                etag: stored.etag,
                tags: stored.tags,
                checksum: stored.checksum,
                owner: stored.owner.clone(),
                acl: stored.acl.clone(),
                version_id: stored.version_id.clone(),
                bucket_records_versions: records_versions,
            })
        })
    }

    async fn delete_object(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        let (old_len, marker, history_bytes) =
            self.db
                .write(|txn| -> Result<(u64, version::Marker, i64), Error> {
                    {
                        check_bucket(&bucket::Table::open(txn)?, bucket)?;
                    }
                    // The reserved segment is never a delete target (FR-020 —
                    // the fs backend's `resolve_key` refusal).
                    if key.is_reserved() {
                        return Err(access_denied(key));
                    }
                    let b = bucket.as_ref().as_str();
                    let k = key.as_ref().as_str();
                    let old_len = {
                        let objects = objects::Table::open(txn)?;
                        objects
                            .get(b, k)?
                            .map(|guard| guard.value().len() as u64)
                            .unwrap_or(0)
                    };
                    // The version state machine's call (spec 2026-10-03), before the
                    // rows below are removed: an unversioned bucket deletes as today
                    // (no marker), a versioned one moves the outgoing version into
                    // history and makes a delete marker current — in this same
                    // transaction. A folder marker never reaches it: folder markers
                    // stay outside the version model (the design's ruling), so their
                    // delete removes the rows and creates no marker — the fs
                    // backend's directory delete.
                    #[cfg(feature = "versioning")]
                    let (marker, history_bytes) = if key.is_folder_marker() {
                        (version::Marker::none(), 0)
                    } else {
                        insert_marker(txn, bucket, key, now_nanos())?
                    };
                    #[cfg(not(feature = "versioning"))]
                    let (marker, history_bytes) = (version::Marker::none(), 0i64);
                    {
                        let mut objects = objects::Table::open(txn)?;
                        objects.remove(b, k)?;
                    }
                    {
                        let mut meta = meta::Table::open(txn)?;
                        meta.remove(b, k)?;
                    }
                    // The retained part list dies with the object (one
                    // transaction — a delete must not orphan the rows).
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.remove_key(b, k)?;
                    }
                    Ok((old_len, marker, history_bytes))
                })?;
        // The bytes that left the backend: the object row the delete removed,
        // minus the bytes its supersede newly stored in the history's blob
        // table (a versioned delete moves the content there instead of
        // dropping it — the same bytes stay metered; a dedup hit stores
        // nothing fresh). The net is never positive, so it can never exceed a
        // limit and needs no check.
        let _ = self.adjust_total(history_bytes - old_len as i64);
        Ok(marker)
    }

    /// Rename moves the object's rows in one transaction — except on a bucket
    /// that records versions, where it is refused (`AccessDenied`): no spec
    /// defines rename's versioning semantics, and the move would destroy the
    /// destination's current version and the source's history. An unversioned
    /// bucket renames exactly as before.
    async fn rename_object(
        &self,
        bucket: &Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, Error> {
        // A rename moves the object's rows — bytes, metadata (mtime,
        // tags, recorded checksum), retained parts — in ONE all-or-nothing
        // transaction (the mem backend has no file to move first; the fs
        // backend's file-first/crash-window shape does not apply). A
        // rename is not a fresh object: nothing is recomputed.
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
        self.db.write(|txn| -> Result<object::Info, Error> {
            {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
            }
            // A rename has no versioning semantics, so a bucket that records
            // versions is refused here rather than corrupted by the row move
            // below (the destination's current version would be overwritten
            // without superseding it, and the source key's history would
            // vanish without a marker). The rule itself is the contract's
            // (`rename_allowed`); the answer is checked inside this
            // transaction so no state change can slip between the read and
            // the move. An unversioned bucket — and a build without the
            // feature — moves the rows exactly as before.
            #[cfg(feature = "versioning")]
            rename_refusal(txn, bucket, dst)?;
            let b = bucket.as_ref().as_str();
            let src_key = src.as_ref().as_str();
            let dst_key = dst.as_ref().as_str();
            // The source's rows are copied to owned values first (the redb
            // guards borrow the transaction), then re-keyed under `dst`. The
            // shared handles borrow the write transaction exclusively, so
            // each table is a separate pass.
            let data = {
                let objects = objects::Table::open(txn)?;
                match objects.get(b, src_key)? {
                    Some(guard) => guard.value().to_vec(),
                    None => return Err(no_such_key(src)),
                }
            };
            // The meta row moves via the shared `meta::Stored` — a rename is
            // not a fresh object: nothing is recomputed, the six elements
            // (file identity included) ride verbatim (re-encoded canonically
            // from the validated row; rows are API-written).
            let stored = {
                let meta = meta::Table::open(txn)?;
                meta.get(b, src_key)?.ok_or_else(|| no_such_key(src))?
            };
            {
                let mut objects = objects::Table::open(txn)?;
                objects.put(b, dst_key, &data)?;
                objects.remove(b, src_key)?;
            }
            {
                let mut meta = meta::Table::open(txn)?;
                meta.put(b, dst_key, &stored)?;
                meta.remove(b, src_key)?;
            }
            // The retained part rows migrate with the record: list the src's
            // rows, clear the dst's stale rows (an overwritten destination's
            // part list dies), re-key under dst, drop the src's.
            let rows = {
                let parts = object_part::Table::open(txn)?;
                parts.list(b, src_key)?
            };
            {
                let mut parts = object_part::Table::open(txn)?;
                // Both range deletes precede the inserts — a redb 4.2.0
                // debug build asserts when an insert precedes another key's
                // range delete in one transaction.
                parts.remove_key(b, dst_key)?;
                parts.remove_key(b, src_key)?;
                for row in rows {
                    parts.put(b, dst_key, &row)?;
                }
            }
            Ok(object::Info {
                key: dst.clone(),
                size: stored.size,
                last_modified: from_nanos(stored.mtime),
                etag: stored.etag,
                tags: stored.tags,
                checksum: stored.checksum,
                owner: stored.owner.clone(),
                acl: stored.acl.clone(),
                version_id: stored.version_id.clone(),
                // The rename guard above passed, so the bucket is
                // unversioned — it records no versions by construction.
                bucket_records_versions: false,
            })
        })
    }

    async fn get_object_tags(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        // Object existence is the `OBJECT_META` row — head_object's gate
        // (rows and bytes are written in one transaction, so a row-less
        // key is a missing object, and folder-marker / reserved keys are
        // never objects — `NoSuchKey`, mirroring head). The tags come
        // from the row's tags element, empty when the wire is
        // domain-invalid (self-healing).
        self.db.read(|txn| {
            {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
            }
            if key.is_reserved() || key.is_folder_marker() {
                return Err(no_such_key(key));
            }
            let meta = meta::Table::open_readonly(txn)?;
            let Some(stored) = meta.get(bucket.as_ref().as_str(), key.as_ref().as_str())? else {
                return Err(no_such_key(key));
            };
            Ok(stored.tags)
        })
    }

    async fn put_object_tags(
        &self,
        bucket: &Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, Error> {
        // Existence is the `OBJECT_META` row (`NoSuchKey` when missing —
        // mirroring `head_object`; rows are written with their object, so
        // the fs backend's row-heal for hand-dropped files has no mem
        // equivalent). The bucket and key gates answer first, like every
        // write path (a reserved key on a missing bucket is NoSuchBucket).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
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
        // (`NoSuchBucket` when missing); a missing object — key, marker,
        // or row — is a no-op that names no identity.
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        Ok(self
            .rewrite_tags_element(bucket, key, &object::Tags::empty())
            .await?
            .unwrap_or(TagWriteResult {
                version_id: None,
                bucket_records_versions: false,
            }))
    }

    async fn get_object_acl(&self, bucket: &Name, key: &object::Key) -> Result<Acl, Error> {
        // Existence is the `OBJECT_META` row (`NoSuchKey` when missing);
        // the ACL recombines the stored owner element with the stored
        // grant set (the row form keeps them apart — the shared
        // decode-home rule).
        self.db.read(|txn| {
            let meta = meta::Table::open_readonly(txn)?;
            meta.get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .map(|stored| Acl {
                    owner: stored.owner.clone(),
                    grants: stored.acl.grants.clone(),
                })
                .ok_or_else(|| no_such_key(key))
        })
    }

    async fn put_object_acl(
        &self,
        bucket: &Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        // Replace-all: the grant set replaces the row's ACL element; the
        // owner element is preserved — a put never changes it (contract
        // ruling). `NoSuchKey` when the object is missing.
        let fresh = Acl {
            owner: None,
            grants: grants.clone(),
        };
        self.db
            .write(|txn| -> Result<bool, Error> {
                let mut meta = meta::Table::open(txn)?;
                let Some(row) = meta.get(bucket.as_ref().as_str(), key.as_ref().as_str())? else {
                    return Ok(false);
                };
                meta.put(
                    bucket.as_ref().as_str(),
                    key.as_ref().as_str(),
                    &meta::Stored { acl: fresh, ..row },
                )?;
                Ok(true)
            })
            .map(|found| if found { Ok(()) } else { Err(no_such_key(key)) })?
    }

    async fn list_object_parts(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // Existence is the `OBJECT_META` row (T2-B ruling: a missing
        // object answers `NoSuchKey`, mirroring `get_object_tags`); the
        // retained rows of a multipart-completed object are served in
        // part-number order — empty for an object that was never
        // multipart-completed (a plain put or copy has no parts).
        self.db.read(|txn| {
            {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
            }
            if key.is_reserved() || key.is_folder_marker() {
                return Err(no_such_key(key));
            }
            let meta = meta::Table::open_readonly(txn)?;
            if meta
                .get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .is_none()
            {
                return Err(no_such_key(key));
            }
            let parts = object_part::Table::open_readonly(txn)?;
            let out = parts
                .list(bucket.as_ref().as_str(), key.as_ref().as_str())?
                .into_iter()
                .map(|row| ObjectPart {
                    part_number: row.part_number.into(),
                    size: row.size,
                    checksum: row.checksum,
                })
                .collect();
            Ok(out)
        })
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        self.db.read(|txn| {
            {
                check_bucket(&bucket::Table::open_readonly(txn)?, &params.bucket)?;
            }
            // The page's objects all carry the bucket's state — one read
            // for the page, the carrier's value on every row.
            let records_versions = records_versions(txn, &params.bucket)?;
            let meta = meta::Table::open_readonly(txn)?;
            let bucket_str = params.bucket.as_ref().as_str();
            // The shared per-bucket walk starts at the prefix lower bound
            // (rows below it cannot affect the rollups or the marker — the
            // prefix-filter equivalence), with the keep boundary on the
            // first element. Rows are validated after the marker/prefix
            // skips (a domain-invalid key is skipped, a garbage etag reports
            // `None` — a mem listing treats it as missing); the row key is
            // the raw key (no bucket-prefix slicing). The sync scan runs
            // inline on the async executor by design (mem is the reference
            // backend, rows are owned copies, and the redb read txn is MVCC
            // — no lock is held). The page's rows are materialized eagerly
            // and handed to the pagination engine (the engine protocol).
            let mut rows: Vec<object::Info> = Vec::new();
            for_each_pair(
                meta.deref(),
                (bucket_str, params.prefix.as_str()),
                |b, _| b == bucket_str,
                |_, raw_key, value| -> Result<(), Error> {
                    let Ok(key) = object::key(raw_key) else {
                        return Ok(()); // invalid key domain → skip the row
                    };
                    if key.is_folder_marker() || key.is_reserved() {
                        return Ok(());
                    }
                    if !key.as_ref().starts_with(&params.prefix) {
                        return Ok(());
                    }
                    let Some(stored) = meta::validate(value) else {
                        return Ok(()); // a garbage-etag row is treated as missing
                    };
                    rows.push(object::Info {
                        key,
                        size: stored.size,
                        last_modified: from_nanos(stored.mtime),
                        etag: stored.etag,
                        tags: stored.tags,
                        checksum: stored.checksum,
                        owner: stored.owner.clone(),
                        acl: stored.acl.clone(),
                        version_id: stored.version_id.clone(),
                        bucket_records_versions: records_versions,
                    });
                    Ok(())
                },
            )?;
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
        testutil::checksum_tee,
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
}
