//! The `ObjectOps` implementation for [`MemoryStorage`].
//!
//! Object put/get/head/delete/listing, the tag ops, rename, and the
//! retained-part listing over the `objects` + `object_meta` +
//! `object_parts` tables (spec 2026-08-31). Reads use read transactions
//! with zero-copy `&str` / `&[u8]` access; bodies are copied out before
//! the transaction ends (streams are `'static` and cannot borrow the
//! transaction guard).

use std::{iter::from_fn, ops::Bound, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::iter;
use redb::{ReadableDatabase, ReadableTable};

use crate::{
    _core::{
        BodyStream, ByteRange, ETag, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps,
        bucket, checksum, collect_body, from_nanos, group_and_paginate,
        multipart::ObjectPart,
        now_nanos, object,
        storage::{RollupMirror, common_prefix},
    },
    Error,
    error::{access_denied, database_storage, invalid_key, no_such_bucket, no_such_key},
    storage::{
        BUCKETS, MemoryStorage, OBJECT_META, OBJECT_PARTS, OBJECTS, band_start, collect_part_rows,
        object_key, remove_object_parts,
    },
};

/// A staged object body: the buffered payload (the commit inserts it)
/// plus the stage's tee digest (spec 2026-08-31) — the validated checksum
/// of the staged content, computed while the body streamed under the
/// server's `checksum` slot; committed as the object's recorded checksum
/// (`FULL_OBJECT` kind) with no re-hashing. `None` when the stage carried
/// no tee slot.
pub struct StagedBody {
    data: Vec<u8>,
    checksum: Option<checksum::Part>,
}

impl MemoryStorage {
    /// The tag-write transaction of the object trio — the shared body of
    /// `put_object_tags` and `delete_object_tags` (the bucket and key
    /// gates stay in the ops): one read-modify-write transaction
    /// replaces the row's tags element with `tags_wire`, keeping the
    /// row's other elements (etag, size, mtime, recorded checksum)
    /// verbatim. Returns whether the row existed (a missing row commits
    /// nothing — nothing changed).
    async fn rewrite_tags_element(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags_wire: &str,
    ) -> Result<bool, Error> {
        let txn = self.db.begin_write()?;
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let existed = {
            let mut meta = txn.open_table(OBJECT_META)?;
            // Owned copies — the redb guards cannot outlive the txn.
            let Some((etag, size, mtime, checksum_wire)) = meta.get(ok.as_str())?.map(|guard| {
                let (etag, size, mtime, _tags, checksum_wire) = guard.value();
                (etag.to_string(), size, mtime, checksum_wire.to_string())
            }) else {
                return Ok(false);
            };
            meta.insert(
                ok.as_str(),
                (
                    etag.as_str(),
                    size,
                    mtime,
                    tags_wire,
                    checksum_wire.as_str(),
                ),
            )?;
            true
        };
        txn.commit()?;
        Ok(existed)
    }

    /// The shared commit tail of the object write paths
    /// ([`ObjectOps::commit_object`] and the copy primitive): atomically
    /// publish `data` onto `key` in one write transaction and record the
    /// interface-validated `tags` and the recorded `checksum` (the
    /// FULL_OBJECT tee digest of a plain commit, or the copy's carried
    /// value — kind included) in the same `OBJECT_META` row — no
    /// post-commit tag window — while removing any stale `OBJECT_PARTS`
    /// rows of the key (a fresh object is single-part: overwriting a
    /// previously multipart-completed object must not leave its parts
    /// behind). Folder markers never become objects (s3-surface.md): the
    /// body is dropped, the record stores the empty-content ETag, and the
    /// tags/checksum are accept-and-dropped (the fs backend's marker
    /// commit behaves the same way). Returns the committed metadata.
    async fn write_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mut data: Vec<u8>,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
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
            data = Vec::new();
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
        let txn = self.db.begin_write()?;
        let (delta, now) = {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let etag_str = etag.as_str();
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            let mut objects = txn.open_table(OBJECTS)?;
            let mut meta = txn.open_table(OBJECT_META)?;
            let mut parts = txn.open_table(OBJECT_PARTS)?;
            let old_len = objects
                .get(ok.as_str())?
                .map(|v| v.value().len() as u64)
                .unwrap_or(0);
            let delta = data.len() as i64 - old_len as i64;
            self.adjust_total(delta)?;
            objects.insert(ok.as_str(), data.as_slice())?;
            let now = now_nanos();
            let tags_wire = if marker {
                String::new()
            } else {
                tags.to_wire()
            };
            let checksum_wire = if marker {
                String::new()
            } else {
                checksum.as_ref().map(|c| c.to_wire()).unwrap_or_default()
            };
            meta.insert(
                ok.as_str(),
                (
                    etag_str.as_str(),
                    size,
                    now,
                    tags_wire.as_str(),
                    checksum_wire.as_str(),
                ),
            )?;
            remove_object_parts(&mut parts, &ok)?;
            (delta, now)
        };
        if let Err(err) = txn.commit() {
            self.rollback_total(delta);
            return Err(err.into());
        }
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(now),
            etag,
            tags: if marker { object::Tags::empty() } else { tags },
            checksum: if marker { None } else { checksum },
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
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
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
        // cannot borrow the transaction guard). `checksum` is the
        // server's tee slot (spec 2026-08-31 — the `upload_part` pattern):
        // the interface wraps the body when the client sent a single
        // `x-amz-checksum-*` header, the digest is computed while the body
        // streams, a mismatch fails the staging as the tee's stream error
        // (propagated here like any body failure — the mem adds no error
        // of its own), and the validated digest rides into the commit.
        // Absent, no digest is computed.
        let data = collect_body(body).await?;
        let computed = checksum.as_ref().and_then(|c| c.digest.get()).cloned();
        Ok(StagedBody {
            data,
            checksum: computed,
        })
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody,
        tags: object::Tags,
    ) -> Result<object::Info, Error> {
        // The stage's tee digest records as the object's FULL_OBJECT
        // checksum (the kind is fixed by the write path — a plain PUT's
        // digest is over the whole content).
        let checksum = staged.checksum.map(|part| checksum::Recorded {
            part,
            kind: checksum::Type::FullObject,
        });
        self.write_object(bucket, key, staged.data, tags, checksum)
            .await
    }

    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
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
        self.write_object(dst_bucket, dst_key, data, tags, checksum)
            .await
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let objects = txn.open_table(OBJECTS)?;
        let meta = txn.open_table(OBJECT_META)?;
        let meta_guard = meta.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let (etag_str, size, mtime, tags_wire, checksum_wire) = meta_guard.value();
        let etag: ETag = etag_str.parse()?;
        // The tags/checksum elements self-heal on a domain-invalid wire
        // (empty / `None`) — rows are API-written, so garbage is treated
        // as missing, exactly like the invalid-etag rows.
        let tags = object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
            .unwrap_or_default();
        let checksum = checksum::Recorded::from_wire_opt(checksum_wire);
        let served_range = match range {
            None => None,
            Some(r) => Some(r.resolve(size)?),
        };
        // The served slice is copied straight out of the zero-copy redb
        // guard — a range request never copies the full object.
        let data_guard = objects.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let served = match served_range {
            Some((start, end)) => data_guard.value()[start as usize..=end as usize].to_vec(),
            None => data_guard.value().to_vec(),
        };
        let body: BodyStream = Box::pin(iter(vec![Ok(Bytes::from(served))]));
        Ok(GetObjectResult {
            info: object::Info {
                key: key.clone(),
                size,
                last_modified: from_nanos(mtime),
                etag,
                tags,
                checksum,
            },
            body,
            served_range,
        })
    }

    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_folder_marker() || key.is_reserved() {
            return Err(no_such_key(key));
        }
        let meta = txn.open_table(OBJECT_META)?;
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let meta_guard = meta.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let (etag_str, size, mtime, tags_wire, checksum_wire) = meta_guard.value();
        let etag: ETag = etag_str.parse()?;
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(mtime),
            etag,
            // The tags/checksum elements self-heal on a domain-invalid
            // wire (empty / `None`).
            tags: object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
                .unwrap_or_default(),
            checksum: checksum::Recorded::from_wire_opt(checksum_wire),
        })
    }

    async fn delete_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        let old_len = {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            let mut objects = txn.open_table(OBJECTS)?;
            let mut meta = txn.open_table(OBJECT_META)?;
            let mut parts = txn.open_table(OBJECT_PARTS)?;
            let old_len = objects
                .get(ok.as_str())?
                .map(|v| v.value().len() as u64)
                .unwrap_or(0);
            objects.remove(ok.as_str())?;
            meta.remove(ok.as_str())?;
            // The retained part list dies with the object (one
            // transaction — a delete must not orphan the rows).
            remove_object_parts(&mut parts, &ok)?;
            old_len
        };
        if let Err(err) = txn.commit() {
            return Err(err.into());
        }
        // A delete only shrinks the total; it cannot exceed a limit.
        let _ = self.adjust_total(-(old_len as i64));
        Ok(())
    }

    async fn rename_object(
        &self,
        bucket: &bucket::Name,
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
        let txn = self.db.begin_write()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        let ok_src = object_key(bucket.as_ref().as_str(), src.as_ref().as_str());
        let ok_dst = object_key(bucket.as_ref().as_str(), dst.as_ref().as_str());
        // The source's rows are copied to owned values first (the redb
        // guards borrow the transaction), then re-keyed under `dst`.
        let data = {
            let objects = txn.open_table(OBJECTS)?;
            match objects.get(ok_src.as_str())? {
                Some(guard) => guard.value().to_vec(),
                None => return Err(no_such_key(src)),
            }
        };
        let row = {
            let meta = txn.open_table(OBJECT_META)?;
            match meta.get(ok_src.as_str())? {
                Some(guard) => {
                    let (etag, size, mtime, tags_wire, checksum_wire) = guard.value();
                    (
                        etag.to_string(),
                        size,
                        mtime,
                        tags_wire.to_string(),
                        checksum_wire.to_string(),
                    )
                }
                None => return Err(no_such_key(src)),
            }
        };
        let (etag_wire, size, mtime, tags_wire, checksum_wire) = row;
        // A garbage-etag row errors like every mem read of one (rows are
        // API-written; tampering is the only source).
        let etag: ETag = etag_wire.parse()?;
        {
            let mut objects = txn.open_table(OBJECTS)?;
            objects.insert(ok_dst.as_str(), data.as_slice())?;
            objects.remove(ok_src.as_str())?;
        }
        {
            let mut meta = txn.open_table(OBJECT_META)?;
            meta.insert(
                ok_dst.as_str(),
                (
                    etag_wire.as_str(),
                    size,
                    mtime,
                    tags_wire.as_str(),
                    checksum_wire.as_str(),
                ),
            )?;
            meta.remove(ok_src.as_str())?;
        }
        // The retained part rows migrate with the record: list the src's
        // rows, clear the dst's stale rows (an overwritten destination's
        // part list dies), re-key under dst, drop the src's (one table
        // handle serves the scan and the writes).
        let rows = {
            let parts = txn.open_table(OBJECT_PARTS)?;
            collect_part_rows(&parts, &ok_src)?
        };
        {
            let mut parts = txn.open_table(OBJECT_PARTS)?;
            // Both range deletes precede the inserts — a redb 4.2.0
            // debug build asserts when an insert precedes another key's
            // range delete in one transaction.
            remove_object_parts(&mut parts, &ok_dst)?;
            remove_object_parts(&mut parts, &ok_src)?;
            for (n, part_size, algorithm, value) in rows {
                let pk = crate::storage::object_part_key(&ok_dst, n);
                parts.insert(pk.as_str(), (part_size, algorithm.as_str(), value.as_str()))?;
            }
        }
        txn.commit()?;
        Ok(object::Info {
            key: dst.clone(),
            size,
            last_modified: from_nanos(mtime),
            etag,
            // The moved elements parse on the way out (garbage wires
            // self-heal to empty/`None`, like every read path).
            tags: object::Tags::parse_wire_limited(&tags_wire, object::OBJECT_TAGS_MAX)
                .unwrap_or_default(),
            checksum: checksum::Recorded::from_wire_opt(&checksum_wire),
        })
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        // Object existence is the `OBJECT_META` row — head_object's gate
        // (rows and bytes are written in one transaction, so a row-less
        // key is a missing object, and folder-marker / reserved keys are
        // never objects — `NoSuchKey`, mirroring head). The tags come
        // from the row's tags element, empty when the wire is
        // domain-invalid (self-healing).
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let meta = txn.open_table(OBJECT_META)?;
        let Some(guard) = meta.get(ok.as_str())? else {
            return Err(no_such_key(key));
        };
        let (_, _, _, tags_wire, _) = guard.value();
        Ok(
            object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
                .unwrap_or_default(),
        )
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), Error> {
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
        if !self
            .rewrite_tags_element(bucket, key, &tags.to_wire())
            .await?
        {
            return Err(no_such_key(key));
        }
        Ok(())
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        // Idempotent like `delete_object`: only the bucket must exist
        // (`NoSuchBucket` when missing); a missing object — key, marker,
        // or row — is a no-op.
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        self.rewrite_tags_element(bucket, key, "").await?;
        Ok(())
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // Existence is the `OBJECT_META` row (T2-B ruling: a missing
        // object answers `NoSuchKey`, mirroring `get_object_tags`); the
        // retained rows of a multipart-completed object are served in
        // part-number order — empty for an object that was never
        // multipart-completed (a plain put or copy has no parts).
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let meta = txn.open_table(OBJECT_META)?;
        if meta.get(ok.as_str())?.is_none() {
            return Err(no_such_key(key));
        }
        let parts = txn.open_table(OBJECT_PARTS)?;
        let out = collect_part_rows(&parts, &ok)?
            .into_iter()
            .map(|(part_number, size, algorithm, value)| ObjectPart {
                part_number: part_number.into(),
                size,
                // A domain-invalid checksum row self-heals: the part is
                // served without a checksum (the `""` algorithm of a
                // checksum-less part parses to `None` the same way).
                checksum: checksum::Part::from_wire_opt(&algorithm, value),
            })
            .collect();
        Ok(out)
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(params.bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(&params.bucket));
            }
        }
        let meta = txn.open_table(OBJECT_META)?;
        let scan_prefix = object_key(params.bucket.as_ref().as_str(), &params.prefix);
        let bucket_prefix = format!("{}\0", params.bucket.as_ref().as_str());
        // Exclusive `start_after` when it sits inside the prefix; otherwise
        // the prefix itself is the lower bound. Grouping still applies the
        // marker: a continuation token may be a common prefix (`dir/`),
        // which is not the same as skipping raw keys `<= start_after`.
        let after_key = params
            .start_after
            .as_deref()
            .map(|after| object_key(params.bucket.as_ref().as_str(), after));
        let start = band_start(&scan_prefix, after_key.as_deref());
        let mut range = meta.range::<&str>((start, Bound::Unbounded))?;
        let mut scan_error = None;
        // `bucket\0key` order is already lexicographic. Folder markers and
        // reserved keys are skipped; `group_and_paginate` stops after one
        // probe entry past `max_keys`, so the range is not drained. The
        // sync scan runs inline on the async executor by design (mem is
        // the reference backend, rows are owned copies, and the redb read
        // txn is MVCC — no lock is held).
        // The rollup mirror (`last_cp`) drops rows a delimiter group
        // absorbs before they pay for the key copy, validation, or the
        // ETag parse (only emitted objects carry an etag). The skips are
        // state-free and the group updates mirror the engine's — an
        // object row resets the group, a rollup row is its group's first
        // — so the pre-filter cannot change the engine's output (it
        // re-checks every surviving row). The mirror is the shared
        // [`RollupMirror`] (A4 — one home with the engines); its
        // two-phase shape matches the engine's ordering: the dedup check
        // runs before this row's validation, the record after it, so a
        // discarded row never advances the group.
        let mut rollup = RollupMirror::new();
        let objects = from_fn(|| {
            loop {
                let (k, v) = match range.next() {
                    None => return None,
                    Some(Err(e)) => {
                        scan_error = Some(database_storage(e));
                        return None;
                    }
                    Some(Ok(entry)) => entry,
                };
                if !k.value().starts_with(&scan_prefix) {
                    return None;
                }
                let raw_key = &k.value()[bucket_prefix.len()..];
                let cp = params
                    .delimiter
                    .as_deref()
                    .and_then(|delim| common_prefix(raw_key, &params.prefix, delim));
                if let Some(cp) = cp
                    && rollup.is_rolled(cp)
                {
                    continue; // the group already rolled up — skip the row
                }
                // A tampered row (a key/etag that cannot be domain-valid)
                // is skipped, never a panic — same tolerance as the fs
                // walk's unrepresentable entries. Rows were validated at
                // insert; read-side checks are defense-in-depth.
                let Ok(key) = object::key(raw_key) else {
                    continue;
                };
                if key.is_folder_marker() || key.is_reserved() {
                    continue;
                }
                let (etag, size, mtime, tags_wire, checksum_wire) = v.value();
                let Ok(etag) = etag.parse() else {
                    continue;
                };
                // The engine's rollup state, mirrored (kept through the
                // marker skip, like the engine's `last_prefix`); the
                // marker skip lands here too, so a row the engine would
                // discard never builds an `Info`.
                match cp {
                    Some(cp) => {
                        rollup.record_rollup(cp);
                        if params
                            .start_after
                            .as_deref()
                            .is_some_and(|after| cp <= after)
                        {
                            continue;
                        }
                    }
                    None => {
                        rollup.reset();
                        if params
                            .start_after
                            .as_deref()
                            .is_some_and(|after| raw_key <= after)
                        {
                            continue;
                        }
                    }
                }
                return Some(object::Info {
                    key,
                    size,
                    last_modified: from_nanos(mtime),
                    etag,
                    // The tags/checksum elements self-heal on a
                    // domain-invalid wire (empty / `None`).
                    tags: object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
                        .unwrap_or_default(),
                    checksum: checksum::Recorded::from_wire_opt(checksum_wire),
                });
            }
        });
        let (keys, common_prefixes, truncated, next_start_after) = group_and_paginate(
            objects,
            &params.prefix,
            params.delimiter.as_deref(),
            params.start_after.as_deref(),
            params.max_keys,
            |o| o.key.as_ref(),
        );
        if let Some(e) = scan_error {
            return Err(e);
        }
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

    use super::*;
    use crate::{
        _core::{
            BodyStream, BucketOps, ListObjectsParams, ObjectListing, ObjectOps, bucket, checksum,
            object, storage::Error::*,
        },
        _util::testing::{body, complete_single_part, read_body},
        MemoryOptions,
        testutil::checksum_tee,
    };

    async fn with_bucket() -> (MemoryStorage, bucket::Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
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
        storage.create_bucket(&name).await.unwrap();
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
        storage.create_bucket(&name).await.unwrap();
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
        bucket: &bucket::Name,
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

    async fn put_keys(storage: &MemoryStorage, bucket: &bucket::Name, keys: &[&str]) {
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
                .list_objects(crate::_core::ListObjectsParams {
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
        storage.create_bucket(&other).await.unwrap();
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
            .stage_body(&b, &a, body(b"hi".to_vec()), None)
            .await
            .unwrap();
        let info = storage
            .commit_object(&b, &a, staged, tags.clone())
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
            .copy_object(&b, &a, &b, &dst, copy_tags.clone(), None)
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
            )
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, object::Tags::empty())
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
    async fn mem_garbage_meta_elements_self_heal() {
        // The read-side tolerance ruling: a stored row whose tags /
        // checksum elements are domain-invalid serves empty / `None`
        // (mirroring the fs `parse_wire_limited` discipline) — the row is
        // still served. Rows are API-written; the garbage below is a
        // direct database write (tampering).
        let (storage, b) = with_bucket().await;
        let k = object::key("g.txt").unwrap();
        {
            let txn = storage.db.begin_write().unwrap();
            let ok = crate::storage::object_key(b.as_ref().as_str(), k.as_ref().as_str());
            let etag = ETag::from_content(b"x");
            let etag_str = etag.as_str();
            txn.open_table(crate::storage::OBJECT_META)
                .unwrap()
                .insert(
                    ok.as_str(),
                    (
                        etag_str.as_str(),
                        1u64,
                        crate::_core::now_nanos(),
                        "env=%zz",
                        "CRC32:AA==:NOPE",
                    ),
                )
                .unwrap();
            txn.open_table(crate::storage::OBJECTS)
                .unwrap()
                .insert(ok.as_str(), b"x".as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 1);
        assert!(head.tags.is_empty(), "garbage tags wire serves empty");
        assert!(head.checksum.is_none(), "garbage checksum wire serves None");
        assert!(storage.get_object_tags(&b, &k).await.unwrap().is_empty());
        let get = storage.get_object(&b, &k, None).await.unwrap();
        assert!(get.info.tags.is_empty());
        assert!(get.info.checksum.is_none());
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
            .stage_body(&b, &k, body(b"plain".to_vec()), None)
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, object::Tags::empty())
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
            .copy_object(&b, &dst, &b, &copy, object::Tags::empty(), None)
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
        let txn = storage.db.begin_read().unwrap();
        let parts = txn.open_table(crate::storage::OBJECT_PARTS).unwrap();
        let bucket_prefix = format!("{}\0", b.as_ref().as_str());
        let rows: Vec<String> = parts
            .range(bucket_prefix.as_str()..)
            .unwrap()
            .take_while(|e| {
                e.as_ref()
                    .map(|(k, _)| k.value().starts_with(&bucket_prefix))
                    .unwrap_or(false)
            })
            .map(|e| e.unwrap().0.value().to_string())
            .collect();
        assert!(
            rows.is_empty(),
            "no OBJECT_PARTS rows may outlive their object: {rows:?}"
        );
    }
}
