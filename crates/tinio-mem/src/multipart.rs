//! The `MultipartOps` implementation for [`MemoryStorage`].
//!
//! Multipart uploads over the `uploads` + `parts` tables. Assembly,
//! completion, and abort each run in one write transaction; part keys are
//! zero-padded so string order equals part-number order.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use redb::ReadableTable;
use uuid::Uuid;

use crate::{
    _core::{
        CompletedPart, ETag, ListPartsParams, ListUploadsParams, MultipartOps, MultipartUpload,
        PartInfo, PartNumber, PartsListing, UploadsListing, bucket::Name, checksum, collect_body,
        from_nanos, group_and_paginate_unordered, key_marker_order, multipart::check_part_minimum,
        now_nanos, object, split_uploads_order, uploads_order,
    },
    _store::{
        bucket, meta, object_part, objects, part, part_checksum, part_data, part_meta, upload,
        upload_checksum,
    },
    Error,
    error::{
        access_denied, invalid_etag, invalid_key, invalid_part, no_parts, no_such_bucket,
        no_such_upload,
    },
    storage::{MemoryStorage, check_bucket, check_upload},
};

/// One in-progress upload of the lazy `list_multipart_uploads` scan — the
/// engine's item without the bucket. `params.bucket` is attached when the
/// page is built: one clone per emitted upload, not per scanned row.
struct UploadRow {
    key: object::Key,
    upload_id: String,
    initiated_at: u64,
    tags: object::Tags,
    checksum: Option<checksum::Upload>,
}

#[async_trait]
impl MultipartOps for MemoryStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
    ) -> Result<MultipartUpload, Error> {
        // Bucket existence first, like the fs backend and every S3 op: a
        // reserved/marker key on a missing bucket answers NoSuchBucket,
        // not AccessDenied/InvalidKey (cross-backend error-code parity).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // Folder markers are never objects — refuse the upload up front
        // (the fs backend rejects them at create too; completing one
        // would materialize an invisible, undeletable object).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()));
        }
        let initiated_at = SystemTime::now();
        let upload = MultipartUpload {
            upload_id: Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at,
            checksum: checksum.clone(),
            tags: tags.clone(),
        };
        self.db.write(|txn| -> Result<MultipartUpload, Error> {
            {
                let buckets = bucket::Table::open(txn)?;
                if buckets.get(bucket.as_ref().as_str())?.is_none() {
                    return Err(no_such_bucket(bucket));
                }
            }
            let b = upload.bucket.as_ref().as_str();
            let k = upload.key.as_ref().as_str();
            // The create-time tags wire rides in the UPLOADS row —
            // `(bucket, upload_id) → (key, initiated, tags)` (spec
            // 2026-08-31 — applied to the completed object).
            let tags_wire = tags.to_wire();
            {
                let mut uploads = upload::Table::open(txn)?;
                uploads.put(b, &upload.upload_id, k, initiated_at, tags_wire.as_str())?;
            }
            // The create-time checksum spec, persisted alongside the
            // UPLOADS row (spec 2026-08-31).
            if let Some(c) = checksum {
                let mut cs = upload_checksum::Table::open(txn)?;
                let (algo, ty) = c.to_wire();
                cs.put(b, &upload.upload_id, algo.as_str(), ty.as_str())?;
            }
            Ok(upload)
        })
    }

    async fn get_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        self.db.read(|txn| {
            // One `UPLOADS` row fetch serves the existence check, the
            // stored-key identity match, `initiated_at`, and the create-time
            // tags (`get_matching` filters on the stored key — a `(bucket,
            // upload_id)` point get plus `key` verification). The tags
            // element self-heals on a domain-invalid wire (empty — like the
            // checksum spec below).
            let uploads = upload::Table::open_readonly(txn)?;
            let Some((_stored_key, initiated, tags_wire)) =
                uploads.get_matching(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id)?
            else {
                return Err(no_such_upload(upload_id));
            };
            let checksums = upload_checksum::Table::open_readonly(txn)?;
            let checksum_row = checksums.get(bucket.as_ref().as_str(), upload_id)?;
            Ok(MultipartUpload {
                upload_id: upload_id.to_string(),
                bucket: bucket.clone(),
                key: key.clone(),
                initiated_at: from_nanos(initiated),
                // A domain-invalid checksum row self-heals: the upload is
                // served without a spec (F07 — the fs backend skips the
                // same way).
                checksum: checksum_row.and_then(|(a, t)| checksum::Upload::from_wire_opt(&a, &t)),
                tags: object::Tags::from_wire_limited(&tags_wire, object::OBJECT_TAGS_MAX),
            })
        })
    }

    async fn upload_part(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: crate::_core::BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        // Fast-fail on a missing bucket before buffering the body (the write
        // transaction re-checks, closing the race).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        let data = collect_body(body).await?;
        // Enforce the per-part size limit before opening the write
        // transaction (fast fail).
        self.check_object_size(data.len() as u64)?;
        // The tee's MD5: a part's ETag IS its content MD5 — the raw
        // digest in the etag cell replaces the second hash over the
        // buffer (filled by the stream end, which `collect_body` drained).
        let etag = match checksum.as_ref().and_then(|c| c.etag_digest()) {
            Some(digest) => ETag::Single(digest),
            None => ETag::from_content(&data),
        };
        let now = now_nanos();
        self.db.write(|txn| -> Result<i64, Error> {
            let b = bucket.as_ref().as_str();
            {
                let uploads = upload::Table::open(txn)?;
                check_upload(&uploads, upload_id, bucket, key)?;
            }
            let n = u32::from(part_number);
            // The shared handles borrow the write transaction exclusively,
            // so each table is a separate pass (the fs pattern).
            let old_len = {
                let part_data = part_data::Table::open(txn)?;
                part_data
                    .get(b, upload_id, n)?
                    .map(|guard| guard.value().len() as u64)
                    .unwrap_or(0)
            };
            let delta = data.len() as i64 - old_len as i64;
            self.adjust_total(delta)?;
            // The part's bytes, etag row, and stat row commit in the same
            // transaction: the etag lives in the shared `PARTS` table, the
            // size/mtime pair in the local `part_meta` (the fs split).
            {
                let mut part_data = part_data::Table::open(txn)?;
                part_data.put(b, upload_id, n, &data)?;
            }
            {
                let mut parts = part::Table::open(txn)?;
                parts.put(b, upload_id, n, &etag)?;
            }
            {
                let mut meta = part_meta::Table::open(txn)?;
                meta.put(b, upload_id, n, data.len() as u64, now)?;
            }
            // The checksum row commits atomically with the part row:
            // write the tee's digest, or clear a stale row from a
            // previous upload of this part number (it would corrupt the
            // Complete composition).
            {
                let mut checksums = part_checksum::Table::open(txn)?;
                match checksum.as_ref().and_then(|c| c.digest.get()) {
                    Some(part) => {
                        checksums.put(
                            b,
                            upload_id,
                            n,
                            part.algorithm.wire_name(),
                            part.value.as_str(),
                        )?;
                    }
                    None => {
                        checksums.remove(b, upload_id, n)?;
                    }
                }
            }
            Ok(delta)
        })
        // The in-memory backend's commit cannot fail (no disk path — an
        // abort-only transaction is the only failure mode), so no
        // rollback_total compensation is needed here.
        ?;
        Ok(PartInfo {
            part_number,
            size: data.len() as u64,
            etag,
            last_modified: from_nanos(now),
            // The digest committed atomically with the part row.
            checksum: checksum.as_ref().and_then(|c| c.digest.get()).cloned(),
        })
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        self.db.read(|txn| {
            {
                // Bucket existence first (the fs backend answers NoSuchBucket
                // before anything else).
                let buckets = bucket::Table::open_readonly(txn)?;
                check_bucket(&buckets, &params.bucket)?;
            }
            {
                let uploads = upload::Table::open_readonly(txn)?;
                check_upload(&uploads, &params.upload_id, &params.bucket, &params.key)?;
            }
            // `max_parts = 0` requests nothing — and no marker either, since
            // an exclusive-after marker would skip the first part of the next
            // page forever (the fs backend and the engine agree).
            if params.max_parts == 0 {
                return Ok(PartsListing {
                    parts: Vec::new(),
                    truncated: false,
                    next_part_number_marker: None,
                });
            }
            let b = params.bucket.as_ref().as_str();
            let id = params.upload_id.as_str();
            // The page's committed `PARTS` rows (raw rows from the marker,
            // capped at `max_parts` plus one lookahead — a page costs O(page)
            // reads, not O(total parts)); size/mtime join from the local
            // `part_meta` rows (the fs PARTS + stat join shape), the checksum
            // rows from the shared `PART_CHECKSUMS` rows on the identical
            // tuple key. The sync scan runs inline on the async executor by
            // design (mem is the reference backend, rows are owned copies,
            // and the redb read txn is MVCC — no lock is held).
            let parts = part::Table::open_readonly(txn)?;
            let start = params.part_number_marker.map_or(0, |m| m.saturating_add(1));
            let (recorded, truncated) = parts.list_from(b, id, start, params.max_parts)?;
            // The resume marker is the last RAW part number (a truncated page
            // whose parts all vanished to a join skip still advances the
            // client — the fs contract).
            let raw_last = recorded.last().map(|(n, _)| *n);
            let meta = part_meta::Table::open_readonly(txn)?;
            // The size/mtime join runs as one contiguous walk from the marker
            // (the `part_meta` rows share the `(bucket, upload_id,
            // part_number)` ordering) merged against the sorted page — a page
            // costs O(page) reads, not O(page) B-tree descents. A part row
            // without its stat row is skipped (the fs contract).
            let mut meta_rows = meta
                .range((b, id, start)..)
                .map_err(|e| Error::Database(e.into()))?;
            let mut meta_cursor: Option<(u32, (u64, u64))> = None;
            // The stored part checksums join per row (spec 2026-08-31).
            // Probed once: an upload with no checksum rows at all (the
            // checksum feature off ⇒ the table is guaranteed empty) skips
            // the per-part point reads — one probe read instead of one per
            // part (F03).
            let checksums = part_checksum::Table::open_readonly(txn)?;
            let checksums =
                part_checksum::Table::has_upload(&checksums, b, id)?.then_some(checksums);
            let mut parts_out: Vec<PartInfo> = Vec::new();
            for (n, hex) in recorded {
                // An invalid etag row is skipped (self-healing — the fs path
                // skips vanished part files the same way).
                let Ok(etag) = hex.parse::<ETag>() else {
                    continue;
                };
                while meta_cursor.is_none_or(|(mn, _)| mn < n) {
                    match meta_rows.next() {
                        Some(item) => {
                            let (mk, mv) = item.map_err(|e| Error::Database(e.into()))?;
                            let (mb, mid, mn) = mk.value();
                            if mb != b || mid != id {
                                break;
                            }
                            meta_cursor = Some((mn, mv.value()));
                        }
                        None => break,
                    }
                }
                let Some((mn, (size, mtime))) = meta_cursor else {
                    continue; // a part row without its stat row is skipped
                };
                if mn != n {
                    continue;
                }
                meta_cursor = None;
                let checksum_row = checksums
                    .as_ref()
                    .map(|table| table.get(b, id, n))
                    .transpose()?
                    .flatten();
                // A domain-invalid checksum row self-heals: the part is
                // listed without a checksum (F07 — the fs backend skips
                // the same way).
                let checksum =
                    checksum_row.and_then(|(a, value)| checksum::Part::from_wire_opt(&a, value));
                parts_out.push(PartInfo {
                    part_number: n.into(),
                    size,
                    etag,
                    last_modified: from_nanos(mtime),
                    checksum,
                });
            }
            let next = if truncated { raw_last } else { None };
            Ok(PartsListing {
                parts: parts_out,
                truncated,
                next_part_number_marker: next,
            })
        })
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        self.db.write(|txn| -> Result<object::Info, Error> {
            {
                // Bucket existence first (the fs backend answers
                // NoSuchBucket before anything else — NoParts only for a
                // real upload).
                let buckets = bucket::Table::open(txn)?;
                check_bucket(&buckets, bucket)?;
            }
            if parts.is_empty() {
                return Err(no_parts());
            }
            // The create-time tags applied at completion come from the
            // UPLOADS row this transaction consumes (spec 2026-08-31) — read
            // here, never re-ferried through the interface. `get_matching`
            // is the identity check on the shared `(bucket, upload_id)` key
            // (the stored key must equal the requested key) and the row fetch
            // in one lookup. A garbage wire self-heals to the empty set (the
            // read-path discipline).
            let b = bucket.as_ref().as_str();
            let k = key.as_ref().as_str();
            let tags = {
                let uploads = upload::Table::open(txn)?;
                let Some((_stored_key, _initiated, tags_wire)) =
                    uploads.get_matching(b, k, upload_id)?
                else {
                    return Err(no_such_upload(upload_id));
                };
                object::Tags::from_wire_limited(&tags_wire, object::OBJECT_TAGS_MAX)
            };
            let (data, etag, now) = {
                // The retained OBJECT_PARTS rows: each listed part's size and
                // its stored checksum row (written by upload_part in the same
                // transaction as the part row), in part order — joined in
                // this transaction, before the part records are consumed
                // (spec 2026-08-31). The three small part tables are read in
                // separate passes (the shared handles borrow the write
                // transaction exclusively); the per-part rows are materialized
                // owned (the redb guards cannot outlive the passes). The part
                // **bytes** are not materialized up front: one `PART_DATA`
                // walk from `(bucket, upload_id, 0)` feeds the assembly
                // `extend` per guard, so the peak holds the assembled `data`
                // only (not both bodies and data — a multipart complete peak
                // is ~1× the object, not 2×).
                let (data, etag, retained) = {
                    let metas: Vec<(u64, u64)> = {
                        let stored_meta = part_meta::Table::open(txn)?;
                        parts
                            .iter()
                            .map(|part| {
                                let n = u32::from(part.part_number);
                                stored_meta
                                    .get(b, upload_id, n)?
                                    .ok_or_else(|| invalid_part(n))
                            })
                            .collect::<Result<Vec<_>, Error>>()?
                    };
                    let hexes: Vec<String> = {
                        let stored_parts = part::Table::open(txn)?;
                        parts
                            .iter()
                            .map(|part| {
                                let n = u32::from(part.part_number);
                                stored_parts
                                    .get_hex(b, upload_id, n)?
                                    .ok_or_else(|| invalid_part(n))
                            })
                            .collect::<Result<Vec<_>, Error>>()?
                    };
                    let checksum_rows: Vec<Option<(String, String)>> = {
                        let stored_checksums = part_checksum::Table::open(txn)?;
                        parts
                            .iter()
                            .map(|part| {
                                stored_checksums
                                    .get(b, upload_id, u32::from(part.part_number))
                                    .map_err(|e| e.into())
                            })
                            .collect::<Result<Vec<_>, Error>>()?
                    };
                    let mut data = Vec::new();
                    let mut infos: Vec<PartInfo> = Vec::new();
                    let mut retained: Vec<(u32, u64, Option<(String, String)>)> = Vec::new();
                    let mut prev = 0u32;
                    let part_data = part_data::Table::open(txn)?;
                    let mut part_rows = part_data
                        .range((b, upload_id, 0)..)
                        .map_err(|e| Error::Database(e.into()))?;
                    for (index, part) in parts.iter().enumerate() {
                        let n = u32::from(part.part_number);
                        if n <= prev {
                            return Err(invalid_part(n));
                        }
                        prev = n;
                        let (size, mtime) = metas[index];
                        // The S3 non-final minimum (shared
                        // `check_part_minimum`), enforced authoritatively IN
                        // this transaction — the size and the bytes it
                        // describes come from the same snapshot the commit
                        // composes (the S3 layer additionally pre-checks its
                        // own listing snapshot; a concurrent upload_part
                        // cannot interleave with a write txn).
                        check_part_minimum(n, size, index + 1 == parts.len())?;
                        let stored_etag: ETag = hexes[index].parse().map_err(invalid_etag)?;
                        if stored_etag != part.etag {
                            return Err(invalid_part(n));
                        }
                        infos.push(PartInfo {
                            part_number: part.part_number,
                            size,
                            etag: stored_etag,
                            last_modified: from_nanos(mtime),
                            checksum: None,
                        });
                        // The part bytes join as a contiguous walk from
                        // `(bucket, upload_id, 0)` — no `bodies` collection,
                        // so the assembly peak holds `data` only. Bytes copy
                        // while the range guard is in scope (the cursor cannot
                        // hold a borrow across the next `next()`).
                        let mut found = false;
                        for item in &mut part_rows {
                            let (pk, pv) = item.map_err(|e| Error::Database(e.into()))?;
                            let (pb, pid, pn) = pk.value();
                            if pb != b || pid != upload_id || pn > n {
                                break;
                            }
                            if pn < n {
                                continue;
                            }
                            data.extend_from_slice(pv.value());
                            found = true;
                            break;
                        }
                        if !found {
                            return Err(invalid_part(n));
                        }
                        retained.push((n, size, checksum_rows[index].clone()));
                    }
                    let etag =
                        ETag::composed_from_parts(&infos).expect("parts checked non-empty above");
                    (data, etag, retained)
                };
                let now = now_nanos();
                // The object row: the completion's tags and the
                // interface-computed composite checksum ride in the same
                // transaction as the object bytes (the backend never hashes).
                {
                    let mut objects = objects::Table::open(txn)?;
                    objects.put(b, k, &data)?;
                }
                {
                    let mut obj_meta = meta::Table::open(txn)?;
                    obj_meta.put(
                        b,
                        k,
                        &meta::Stored {
                            etag: etag.clone(),
                            size: data.len() as u64,
                            mtime: now,
                            file_identity: 0,
                            tags: tags.clone(),
                            checksum: checksum.clone(),
                        },
                    )?;
                }
                {
                    let mut uploads = upload::Table::open(txn)?;
                    uploads.remove(b, upload_id)?;
                }
                {
                    let mut cs = upload_checksum::Table::open(txn)?;
                    cs.remove(b, upload_id)?;
                }
                // The four part tables drain under the upload's tuple prefix
                // (complete consume; the fs `drain_upload` parity) — one
                // scoped handle per table.
                {
                    let mut part_data = part_data::Table::open(txn)?;
                    part_data.drain_upload(b, upload_id)?;
                }
                {
                    let mut stored_parts = part::Table::open(txn)?;
                    stored_parts.drain_upload(b, upload_id)?;
                }
                {
                    let mut stored_meta = part_meta::Table::open(txn)?;
                    stored_meta.drain_upload(b, upload_id)?;
                }
                {
                    let mut stored_checksums = part_checksum::Table::open(txn)?;
                    stored_checksums.drain_upload(b, upload_id)?;
                }
                {
                    // The retained part list: replace any stale rows of the
                    // key, then insert this completion's parts (a re-completed
                    // key must not accumulate rows).
                    let mut parts_table = object_part::Table::open(txn)?;
                    parts_table.remove_key(b, k)?;
                    for (n, part_size, checksum_row) in retained {
                        let (algorithm, value) = match checksum_row {
                            Some((algorithm, value)) => (algorithm, value),
                            // `""` marks a part stored without a checksum.
                            None => (String::new(), String::new()),
                        };
                        parts_table.put(b, k, n, part_size, &algorithm, &value)?;
                    }
                }
                (data, etag, now)
            };
            // The assembled object replaces the parts byte-for-byte (the
            // tracked total is unchanged), but the per-object limit still
            // applies to the assembled size.
            self.check_object_size(data.len() as u64)?;
            Ok(object::Info {
                key: key.clone(),
                size: data.len() as u64,
                last_modified: from_nanos(now),
                etag,
                tags,
                checksum,
            })
        })
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        let removed = self.db.write(|txn| -> Result<u64, Error> {
            {
                // Bucket existence first (the fs backend answers
                // NoSuchBucket before anything else).
                let buckets = bucket::Table::open(txn)?;
                check_bucket(&buckets, bucket)?;
            }
            let b = bucket.as_ref().as_str();
            {
                let mut uploads = upload::Table::open(txn)?;
                check_upload(&uploads, upload_id, bucket, key)?;
                uploads.remove(b, upload_id)?;
            }
            {
                let mut cs = upload_checksum::Table::open(txn)?;
                cs.remove(b, upload_id)?;
            }
            let part_bytes = {
                let mut part_data = part_data::Table::open(txn)?;
                // The byte accounting walks the removed rows before the drain.
                let removed = part_data.total_len(b, upload_id)?;
                part_data.drain_upload(b, upload_id)?;
                removed
            };
            {
                let mut stored_meta = part_meta::Table::open(txn)?;
                stored_meta.drain_upload(b, upload_id)?;
            }
            {
                let mut stored_parts = part::Table::open(txn)?;
                stored_parts.drain_upload(b, upload_id)?;
            }
            {
                let mut stored_checksums = part_checksum::Table::open(txn)?;
                stored_checksums.drain_upload(b, upload_id)?;
            }
            Ok(part_bytes)
        })?;
        // An abort only shrinks the total; it cannot exceed a limit.
        let _ = self.adjust_total(-(removed as i64));
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        self.db.read(|txn| {
            {
                let buckets = bucket::Table::open_readonly(txn)?;
                if buckets.get(params.bucket.as_ref().as_str())?.is_none() {
                    return Err(no_such_bucket(&params.bucket));
                }
            }
            let uploads = upload::Table::open_readonly(txn)?;
            // The create-time checksum specs use the identical
            // `(bucket, upload_id)` key — join them per row (spec
            // 2026-08-31).
            let checksums = upload_checksum::Table::open_readonly(txn)?;
            let b = params.bucket.as_ref().as_str();
            // The resume marker (composite `key\0upload_id`; a bare key
            // marker sorts after every upload of that key) is computed before
            // the walk (T02).
            let marker = key_marker_order(
                params.key_marker.as_deref(),
                params.upload_id_marker.as_deref(),
            );
            // The bucket's upload rows walk via the shared `for_bucket` scan
            // (`(bucket, "")` lower bound, keep boundary); the prefix filter
            // and the delimiter grouping run in the shared unordered engine
            // (the fs `list_uploads_page` shape). A tampered key row is
            // skipped, never a panic; a corrupt checksum row self-heals to a
            // checksum-less upload (F07). The sync scan runs inline on the
            // async executor by design (mem is the reference backend, rows
            // are owned copies, and the redb read txn is MVCC — no lock is
            // held).
            let mut rows: Vec<UploadRow> = Vec::new();
            uploads.for_bucket(b, |upload_id, (key, initiated_at, tags_wire)| {
                if !key.starts_with(&params.prefix) {
                    return Ok(());
                }
                let Ok(key) = object::key(key) else {
                    return Ok(()); // tampered row — skipped like list_objects
                };
                let checksum_row = checksums.get(b, upload_id)?;
                // A domain-invalid checksum row self-heals: the upload is
                // listed without a spec (F07 — the fs backend skips the
                // same way; a hard error here would fail the whole listing
                // for one bad row). The create-time tags element self-heals
                // the same way (empty — the wire is API-written).
                let checksum =
                    checksum_row.and_then(|(a, t)| checksum::Upload::from_wire_opt(&a, &t));
                rows.push(UploadRow {
                    key,
                    upload_id: upload_id.to_string(),
                    initiated_at,
                    tags: object::Tags::from_wire_limited(tags_wire, object::OBJECT_TAGS_MAX),
                    checksum,
                });
                Ok(())
            })?;
            // The resume marker pairs the key with the upload id, so a page
            // can position inside a same-key group (S3 `upload-id-marker`); a
            // bare key marker skips the whole key group — the conversion has
            // one home in tinio-core (shared with the fs backend).
            let (page, common_prefixes, truncated, next) = group_and_paginate_unordered(
                rows,
                &params.prefix,
                params.delimiter.as_deref(),
                marker.as_deref(),
                params.max_uploads,
                |u| u.key.as_ref(),
                |u| uploads_order(&u.key, &u.upload_id),
            );
            let uploads = page
                .into_iter()
                .map(|u| MultipartUpload {
                    upload_id: u.upload_id,
                    bucket: params.bucket.clone(),
                    key: u.key,
                    initiated_at: from_nanos(u.initiated_at),
                    checksum: u.checksum,
                    tags: u.tags,
                })
                .collect();
            let (next_key, next_upload_id) = match next {
                Some(next) => {
                    let (key, upload_id) = split_uploads_order(&next);
                    (Some(key.to_string()), upload_id.map(str::to_string))
                }
                None => (None, None),
            };
            Ok(UploadsListing {
                uploads,
                common_prefixes,
                truncated,
                next_key_marker: next_key,
                next_upload_id_marker: next_upload_id,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    // Raw-transaction test calls (`db().begin_read`) take the trait; the
    // `super::*` glob shadows the import name.
    #[allow(unused_imports)]
    use redb::ReadableDatabase;

    use super::*;
    use crate::{
        _core::{
            BucketOps, CompletedPart, ListUploadsParams, MultipartOps, ObjectOps, PartInfo, bucket,
            multipart::{MIN_PART_BYTES, part_number},
            object,
            storage::Error::*,
        },
        _store::{part_checksum, upload_checksum},
        _util::testing::{body, read_body},
        MemoryOptions,
        testutil::checksum_tee,
    };

    fn completed(part: &PartInfo) -> CompletedPart {
        CompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
        }
    }

    async fn with_bucket() -> (MemoryStorage, Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        (storage, name)
    }

    #[tokio::test]
    async fn corrupt_checksum_rows_self_heal() {
        // F07: domain-invalid UPLOAD_CHECKSUMS/PART_CHECKSUMS rows must
        // not fail the read paths — the upload/part still answers with
        // the checksum dropped (the fs backend self-heals the same way).
        let (storage, name) = with_bucket().await;
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&name, &key, None, object::Tags::empty())
            .await
            .unwrap();
        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        let bucket_str = name.as_ref().as_str();
        {
            let mut txn = storage.db.db().begin_write().unwrap();
            upload_checksum::Table::open(&mut txn)
                .unwrap()
                .insert((bucket_str, upload.upload_id.as_str()), ("BLAKE3", ""))
                .unwrap();
            part_checksum::Table::open(&mut txn)
                .unwrap()
                .insert(
                    (bucket_str, upload.upload_id.as_str(), 1),
                    ("BLAKE3", "AAAA"),
                )
                .unwrap();
            txn.commit().unwrap();
        }
        let got = storage
            .get_multipart_upload(&name, &key, &upload.upload_id)
            .await
            .unwrap();
        assert!(got.checksum.is_none(), "get drops the corrupt spec");
        let page = storage
            .list_parts(ListPartsParams {
                bucket: name.clone(),
                key: key.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 1000,
                part_number_marker: None,
            })
            .await
            .unwrap();
        assert_eq!(page.parts.len(), 1);
        assert!(page.parts[0].checksum.is_none());
        let listed = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: name,
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
            })
            .await
            .unwrap();
        assert_eq!(listed.uploads.len(), 1, "the listing still answers");
        assert!(listed.uploads[0].checksum.is_none());
    }

    #[tokio::test]
    async fn part_size_limit_rejects_oversized_parts() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: Some(4),
            max_total_bytes: None,
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&name, &key, None, object::Tags::empty())
            .await
            .unwrap();

        let err = storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"12345"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { size: 5, limit: 4 })),
            "{err}"
        );
        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"1234"),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abort_releases_part_bytes() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&name, &key, None, object::Tags::empty())
            .await
            .unwrap();

        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"12345"),
                None,
            )
            .await
            .unwrap();
        let err = storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(2).unwrap(),
                body(b"1234"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { .. })),
            "{err}"
        );
        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(2).unwrap(),
                body(b"123"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 8);

        storage
            .abort_multipart_upload(&name, &key, &upload.upload_id)
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 0);

        // The freed capacity is reusable by a new upload.
        let u2 = storage
            .create_multipart_upload(&name, &key, None, object::Tags::empty())
            .await
            .unwrap();
        storage
            .upload_part(
                &name,
                &key,
                &u2.upload_id,
                part_number(1).unwrap(),
                body(b"12345678"),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn upload_ids_are_unique() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let a = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        let b = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        assert_ne!(a.upload_id, b.upload_id);
        assert_eq!(a.upload_id.len(), 36);
    }

    #[tokio::test]
    async fn complete_uses_only_listed_parts() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        // The listed non-final parts must be >= the 5 MiB minimum (the
        // authoritative in-txn check); the unlisted part may be small.
        let min = MIN_PART_BYTES as usize;
        let data1 = vec![b'a'; min];
        let data2 = vec![b'b'; min];
        let mut uploaded = Vec::new();
        for (n, data) in [
            (1u32, data1.clone()),
            (2, data2.clone()),
            (3, b"ccc".to_vec()),
        ] {
            uploaded.push(
                storage
                    .upload_part(&bucket, &key, &upload.upload_id, n.into(), body(data), None)
                    .await
                    .unwrap(),
            );
        }
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[completed(&uploaded[0]), completed(&uploaded[1])],
                None,
            )
            .await
            .unwrap();
        let expect = [data1, data2].concat();
        assert_eq!(completed.size, expect.len() as u64);
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), expect);
        assert!(completed.etag.as_str().ends_with("-2"));
    }

    #[tokio::test]
    async fn a_failed_complete_survives_in_txn() {
        // The authoritative in-txn size check fails the complete — and
        // the write transaction rolls back, leaving the upload alive for
        // a corrected retry (S3). The failure shape itself (one byte
        // under the minimum → PartTooSmall) is the shared conformance
        // leg.
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let min = MIN_PART_BYTES as usize;
        let upload = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        let under = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(vec![b'b'; min - 1]),
                None,
            )
            .await
            .unwrap();
        let small = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                2.into(),
                body(b"x".to_vec()),
                None,
            )
            .await
            .unwrap();
        let err = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[completed(&under), completed(&small)],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(PartTooSmall { .. })), "{err}");
        assert!(
            storage
                .get_multipart_upload(&bucket, &key, &upload.upload_id)
                .await
                .is_ok(),
            "the failed complete must leave the upload alive"
        );
    }

    #[tokio::test]
    async fn list_uploads_on_missing_bucket_is_no_such_bucket() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("gone").unwrap();
        assert!(matches!(
            storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket,
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1000,
                })
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn mem_complete_retains_object_parts() {
        // spec 2026-08-31: completion persists the assembled parts into
        // OBJECT_PARTS — list_object_parts serves them in part order with
        // sizes and the per-part checksums, and the object row carries
        // the completion's tags and the interface-computed composite
        // checksum (the backend never hashes).
        let (storage, b) = with_bucket().await;
        let k = object::key("big.bin").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, tags.clone())
            .await
            .unwrap();
        assert_eq!(upload.tags, tags, "the create-time tags ride on the upload");
        assert_eq!(
            storage
                .get_multipart_upload(&b, &k, &upload.upload_id)
                .await
                .unwrap()
                .tags,
            tags,
            "the stored upload row echoes the create-time tags"
        );
        // Two parts: the non-final first must meet the 5 MiB S3 minimum;
        // both upload with a preset tee so the retained rows carry
        // per-part checksums (the digest values are the tee's — the
        // backend never validates them against the bytes).
        let min = MIN_PART_BYTES as usize;
        let first = vec![b'a'; min];
        let p1 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
                body(first.clone()),
                Some(checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==")),
            )
            .await
            .unwrap();
        let p2 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                2.into(),
                body(b"world".to_vec()),
                Some(checksum_tee(
                    checksum::Algorithm::Md5,
                    "N7SrFkGmSbsK8h0dJ9nK1Q==",
                )),
            )
            .await
            .unwrap();
        let composite = checksum::Recorded {
            part: checksum::Part {
                algorithm: checksum::Algorithm::Md5,
                value: checksum::Value("AAAAAAAAAAAAAAAAAAAAAA==".into()),
            },
            kind: checksum::Type::Composite,
        };
        let completed = [
            CompletedPart {
                part_number: p1.part_number,
                etag: p1.etag,
            },
            CompletedPart {
                part_number: p2.part_number,
                etag: p2.etag,
            },
        ];
        let info = storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload.upload_id,
                &completed,
                Some(composite.clone()),
            )
            .await
            .unwrap();
        assert_eq!(info.tags, tags, "completion applies the create-time tags");
        assert_eq!(info.checksum.as_ref(), Some(&composite));
        assert_eq!(
            storage.head_object(&b, &k).await.unwrap().checksum,
            Some(composite),
            "the object row carries the composite"
        );

        // The retained part rows: order, sizes, per-part checksums.
        let parts = storage.list_object_parts(&b, &k).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(u32::from(parts[0].part_number), 1);
        assert_eq!(parts[0].size, min as u64);
        let checksum = parts[0].checksum.as_ref().expect("part 1's checksum");
        assert_eq!(checksum.algorithm, checksum::Algorithm::Crc32);
        assert_eq!(checksum.value.0, "NhCmhg==");
        assert_eq!(u32::from(parts[1].part_number), 2);
        assert_eq!(parts[1].size, 5);
        let checksum = parts[1].checksum.as_ref().expect("part 2's checksum");
        assert_eq!(checksum.algorithm, checksum::Algorithm::Md5);
        assert_eq!(checksum.value.0, "N7SrFkGmSbsK8h0dJ9nK1Q==");

        // A second completion over the same key replaces the rows (the
        // old completion's parts must not accumulate).
        let upload2 = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        let p = storage
            .upload_part(&b, &k, &upload2.upload_id, 3.into(), body(b"tail"), None)
            .await
            .unwrap();
        storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload2.upload_id,
                &[CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag,
                }],
                None,
            )
            .await
            .unwrap();
        let parts = storage.list_object_parts(&b, &k).await.unwrap();
        assert_eq!(parts.len(), 1, "re-completion replaces the retained rows");
        assert_eq!(u32::from(parts[0].part_number), 3);
        assert!(
            parts[0].checksum.is_none(),
            "a part uploaded without a tee retains no checksum"
        );
    }
}
