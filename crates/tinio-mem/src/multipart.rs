//! The `MultipartOps` implementation for [`MemoryStorage`].
//!
//! Multipart uploads over the `uploads` + `parts` tables. Assembly,
//! completion, and abort each run in one write transaction; part keys are
//! zero-padded so string order equals part-number order.

use std::{ops::Bound, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::{
    _core::{
        CompletedPart, ETag, ListPartsParams, ListUploadsParams, MultipartOps, MultipartUpload,
        PartInfo, PartNumber, PartsListing, UploadsListing, bucket, checksum, collect_body,
        from_nanos, group_and_paginate_ordered, key_marker_order, multipart::check_part_minimum,
        now_nanos, object, split_uploads_order, uploads_order,
    },
    Error,
    error::{
        access_denied, database_storage, invalid_etag, invalid_key, invalid_part, no_parts,
        no_such_bucket, no_such_upload,
    },
    storage::{
        BUCKETS, MemoryStorage, OBJECT_META, OBJECT_PARTS, OBJECTS, PART_CHECKSUMS, PART_META,
        PARTS, UPLOAD_CHECKSUMS, UPLOADS, band_start, check_bucket, check_upload,
        collect_part_keys, object_key, object_part_key, parse_part_number, part_key,
        remove_all_parts, remove_object_parts, upload_key,
    },
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
        bucket: &bucket::Name,
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
        let upload = MultipartUpload {
            upload_id: Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
            checksum: checksum.clone(),
            tags: tags.clone(),
        };
        let txn = self.db.begin_write()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let ukey = upload_key(
                upload.bucket.as_ref().as_str(),
                upload.key.as_ref().as_str(),
                &upload.upload_id,
            );
            // The create-time tags wire rides in the UPLOADS row (spec
            // 2026-08-31 — applied to the completed object).
            let tags_wire = tags.to_wire();
            let mut uploads = txn.open_table(UPLOADS)?;
            uploads.insert(ukey.as_str(), (now_nanos(), tags_wire.as_str()))?;
            // The create-time checksum spec, persisted alongside the
            // UPLOADS row (spec 2026-08-31).
            if let Some(c) = checksum {
                let mut cs = txn.open_table(UPLOAD_CHECKSUMS)?;
                let (algo, ty) = c.to_wire();
                cs.insert(ukey.as_str(), (algo.as_str(), ty.as_str()))?;
            }
        }
        txn.commit()?;
        Ok(upload)
    }

    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
        let txn = self.db.begin_read()?;
        // One `UPLOADS` row fetch serves the existence check,
        // `initiated_at`, and the create-time tags (the compound key
        // already encodes the identity). The tags element self-heals on a
        // domain-invalid wire (empty — like the checksum spec below).
        let (initiated, tags) = {
            let uploads = txn.open_table(UPLOADS)?;
            let guard = uploads
                .get(ukey.as_str())?
                .ok_or_else(|| no_such_upload(upload_id))?;
            let (initiated, tags_wire) = guard.value();
            (
                initiated,
                object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
                    .unwrap_or_default(),
            )
        };
        let checksums = txn.open_table(UPLOAD_CHECKSUMS)?;
        let checksum_row = checksums.get(ukey.as_str())?.map(|v| {
            let (a, t) = v.value();
            (a.to_string(), t.to_string())
        });
        Ok(MultipartUpload {
            upload_id: upload_id.to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: from_nanos(initiated),
            // A domain-invalid checksum row self-heals: the upload is
            // served without a spec (F07 — the fs backend skips the
            // same way).
            checksum: checksum_row.and_then(|(a, t)| checksum::Upload::from_wire_opt(&a, &t)),
            tags,
        })
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
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
        let txn = self.db.begin_write()?;
        let delta = {
            let uploads = txn.open_table(UPLOADS)?;
            check_upload(&uploads, upload_id, bucket, key)?;
            let pk = part_key(upload_id, u32::from(part_number));
            let etag_str = etag.as_str();
            let mut parts = txn.open_table(PARTS)?;
            let mut meta = txn.open_table(PART_META)?;
            let old_len = parts
                .get(pk.as_str())?
                .map(|v| v.value().len() as u64)
                .unwrap_or(0);
            let delta = data.len() as i64 - old_len as i64;
            self.adjust_total(delta)?;
            parts.insert(pk.as_str(), data.as_slice())?;
            meta.insert(pk.as_str(), (etag_str.as_str(), data.len() as u64, now))?;
            // The checksum row commits atomically with the part row:
            // write the tee's digest, or clear a stale row from a
            // previous upload of this part number (it would corrupt the
            // Complete composition).
            let mut checksums = txn.open_table(PART_CHECKSUMS)?;
            match checksum.as_ref().and_then(|c| c.digest.get()) {
                Some(part) => {
                    checksums.insert(
                        pk.as_str(),
                        (part.algorithm.wire_name(), part.value.as_str()),
                    )?;
                }
                None => {
                    checksums.remove(pk.as_str())?;
                }
            }
            delta
        };
        if let Err(err) = txn.commit() {
            self.rollback_total(delta);
            return Err(err.into());
        }
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
        let txn = self.db.begin_read()?;
        {
            // Bucket existence first (the fs backend answers NoSuchBucket
            // before anything else).
            check_bucket(&txn.open_table(BUCKETS)?, &params.bucket)?;
        }
        {
            let uploads = txn.open_table(UPLOADS)?;
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
        let meta = txn.open_table(PART_META)?;
        // The stored part checksums use the identical `upload_id\0part`
        // key — join them per row (spec 2026-08-31). Probed once: an
        // upload with no checksum rows at all (the checksum feature off
        // ⇒ the table is guaranteed empty) skips the per-part point
        // reads — one probe read instead of one per part (F03).
        let prefix = format!("{}\0", params.upload_id);
        let checksums = txn.open_table(PART_CHECKSUMS)?;
        let has_checksums = checksums
            .range(prefix.as_str()..)?
            .next()
            .transpose()?
            .is_some_and(|(k, _)| k.value().starts_with(&prefix));
        let checksums = has_checksums
            .then(|| txn.open_table(PART_CHECKSUMS))
            .transpose()?;
        // The zero-padded part keys are string-ordered by number, so the
        // scan starts just after the marker and stops one probe part past
        // the page — a page costs O(page) reads, not O(total parts). The
        // sync scan runs inline on the async executor by design (mem is
        // the reference backend, rows are owned copies, and the redb read
        // txn is MVCC — no lock is held).
        let start = match params.part_number_marker {
            Some(marker) => part_key(&params.upload_id, marker.saturating_add(1)),
            None => prefix.clone(),
        };
        let parts = meta
            .range(start.as_str()..)?
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(k, _)| k.value().starts_with(&prefix))
                    .unwrap_or(false)
            })
            .take(params.max_parts.saturating_add(1))
            .map(|entry| {
                let (k, v) = entry?;
                let part_number = parse_part_number(&k.value()[prefix.len()..])?;
                let (etag, size, mtime) = v.value();
                let checksum_row = checksums
                    .as_ref()
                    .map(|table| table.get(k.value()))
                    .transpose()?
                    .flatten()
                    .map(|v| {
                        let (a, value) = v.value();
                        (a.to_string(), value.to_string())
                    });
                // A domain-invalid checksum row self-heals: the part is
                // listed without a checksum (F07 — the fs backend skips
                // the same way).
                let checksum =
                    checksum_row.and_then(|(a, value)| checksum::Part::from_wire_opt(&a, value));
                Ok(PartInfo {
                    part_number: part_number.into(),
                    size,
                    etag: etag.parse().map_err(invalid_etag)?,
                    last_modified: from_nanos(mtime),
                    checksum,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // The probe past the page sets the truncation flag; the resume
        // marker is the page's last part.
        let truncated = parts.len() > params.max_parts;
        let parts: Vec<PartInfo> = parts.into_iter().take(params.max_parts).collect();
        let next = if truncated {
            parts.last().map(|p| u32::from(p.part_number))
        } else {
            None
        };
        Ok(PartsListing {
            parts,
            truncated,
            next_part_number_marker: next,
        })
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        let txn = self.db.begin_write()?;
        {
            // Bucket existence first (the fs backend answers NoSuchBucket
            // before anything else — NoParts only for a real upload).
            check_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        }
        if parts.is_empty() {
            return Err(no_parts());
        }
        // The create-time tags applied at completion come from the
        // UPLOADS row this transaction consumes (spec 2026-08-31) — read
        // here, never re-ferried through the interface. A garbage wire
        // self-heals to the empty set (the read-path discipline).
        let tags = {
            let uploads = txn.open_table(UPLOADS)?;
            check_upload(&uploads, upload_id, bucket, key)?;
            let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
            let guard = uploads
                .get(ukey.as_str())?
                .ok_or_else(|| no_such_upload(upload_id))?;
            let tags_wire = guard.value().1.to_string();
            object::Tags::parse_wire_limited(&tags_wire, object::OBJECT_TAGS_MAX)
                .unwrap_or_default()
        };
        let (data, etag, now) = {
            let mut prev = 0u32;
            let mut data = Vec::new();
            let mut infos: Vec<PartInfo> = Vec::new();
            // The retained OBJECT_PARTS rows: each listed part's size and
            // its stored checksum row (written by upload_part in the same
            // transaction as the part row), in part order — joined in
            // this transaction, before the part records are consumed
            // (spec 2026-08-31).
            let mut retained: Vec<(u32, u64, Option<(String, String)>)> = Vec::new();
            {
                let stored_parts = txn.open_table(PARTS)?;
                let stored_meta = txn.open_table(PART_META)?;
                let stored_checksums = txn.open_table(PART_CHECKSUMS)?;
                for (index, part) in parts.iter().enumerate() {
                    let n = u32::from(part.part_number);
                    if n <= prev {
                        return Err(invalid_part(n));
                    }
                    prev = n;
                    let pk = part_key(upload_id, n);
                    let body = stored_parts
                        .get(pk.as_str())?
                        .ok_or_else(|| invalid_part(n))?;
                    let meta_guard = stored_meta
                        .get(pk.as_str())?
                        .ok_or_else(|| invalid_part(n))?;
                    let (etag_str, size, mtime) = meta_guard.value();
                    // The S3 non-final minimum (shared
                    // `check_part_minimum`), enforced authoritatively IN
                    // this transaction — the size and the bytes it
                    // describes come from the same snapshot the commit
                    // composes (the S3 layer additionally pre-checks its
                    // own listing snapshot; a concurrent upload_part
                    // cannot interleave with a write txn).
                    check_part_minimum(n, size, index + 1 == parts.len())?;
                    let stored_etag: ETag = etag_str.parse().map_err(invalid_etag)?;
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
                    data.extend_from_slice(body.value());
                    retained.push((
                        n,
                        size,
                        stored_checksums.get(pk.as_str())?.map(|v| {
                            let (a, value) = v.value();
                            (a.to_string(), value.to_string())
                        }),
                    ));
                }
            }
            let etag = ETag::composed_from_parts(&infos).expect("parts checked non-empty above");
            let etag_str = etag.as_str();
            let now = now_nanos();
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            // The object row: the completion's tags and the
            // interface-computed composite checksum ride in the same
            // transaction as the object bytes (the backend never hashes).
            let tags_wire = tags.to_wire();
            let checksum_wire = checksum.as_ref().map(|c| c.to_wire()).unwrap_or_default();
            {
                let mut objects = txn.open_table(OBJECTS)?;
                let mut obj_meta = txn.open_table(OBJECT_META)?;
                objects.insert(ok.as_str(), data.as_slice())?;
                obj_meta.insert(
                    ok.as_str(),
                    (
                        etag_str.as_str(),
                        data.len() as u64,
                        now,
                        tags_wire.as_str(),
                        checksum_wire.as_str(),
                    ),
                )?;
            }
            {
                let mut uploads = txn.open_table(UPLOADS)?;
                let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
                uploads.remove(ukey.as_str())?;
                txn.open_table(UPLOAD_CHECKSUMS)?.remove(ukey.as_str())?;
            }
            {
                let mut stored_parts = txn.open_table(PARTS)?;
                let mut stored_meta = txn.open_table(PART_META)?;
                let mut stored_checksums = txn.open_table(PART_CHECKSUMS)?;
                let prefix = format!("{upload_id}\0");
                remove_all_parts(
                    &mut stored_parts,
                    &mut stored_meta,
                    &mut stored_checksums,
                    &prefix,
                )?;
            }
            {
                // The retained part list: replace any stale rows of the
                // key, then insert this completion's parts (a re-completed
                // key must not accumulate rows).
                let mut parts_table = txn.open_table(OBJECT_PARTS)?;
                remove_object_parts(&mut parts_table, &ok)?;
                for (n, part_size, checksum_row) in retained {
                    let pk = object_part_key(&ok, n);
                    let (algorithm, value) = match checksum_row {
                        Some((algorithm, value)) => (algorithm, value),
                        // `""` marks a part stored without a checksum.
                        None => (String::new(), String::new()),
                    };
                    parts_table
                        .insert(pk.as_str(), (part_size, algorithm.as_str(), value.as_str()))?;
                }
            }
            (data, etag, now)
        };
        // The assembled object replaces the parts byte-for-byte (the
        // tracked total is unchanged), but the per-object limit still
        // applies to the assembled size.
        self.check_object_size(data.len() as u64)?;
        txn.commit()?;
        Ok(object::Info {
            key: key.clone(),
            size: data.len() as u64,
            last_modified: from_nanos(now),
            etag,
            tags,
            checksum,
        })
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            // Bucket existence first (the fs backend answers NoSuchBucket
            // before anything else).
            check_bucket(&txn.open_table(BUCKETS)?, bucket)?;
        }
        let removed = {
            {
                let uploads = txn.open_table(UPLOADS)?;
                check_upload(&uploads, upload_id, bucket, key)?;
            }
            {
                let mut uploads = txn.open_table(UPLOADS)?;
                let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
                uploads.remove(ukey.as_str())?;
                txn.open_table(UPLOAD_CHECKSUMS)?.remove(ukey.as_str())?;
            }
            {
                let mut stored_parts = txn.open_table(PARTS)?;
                let mut stored_meta = txn.open_table(PART_META)?;
                let mut stored_checksums = txn.open_table(PART_CHECKSUMS)?;
                let prefix = format!("{upload_id}\0");
                let keys = collect_part_keys(&stored_parts, &prefix)?;
                let mut removed = 0u64;
                for k in &keys {
                    if let Some(v) = stored_parts.get(k.as_str())? {
                        removed += v.value().len() as u64;
                    }
                }
                remove_all_parts(
                    &mut stored_parts,
                    &mut stored_meta,
                    &mut stored_checksums,
                    &prefix,
                )?;
                removed
            }
        };
        if let Err(err) = txn.commit() {
            return Err(err.into());
        }
        // An abort only shrinks the total; it cannot exceed a limit.
        let _ = self.adjust_total(-(removed as i64));
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(params.bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(&params.bucket));
            }
        }
        let uploads = txn.open_table(UPLOADS)?;
        // The create-time checksum specs use the identical compound key —
        // join them per row (spec 2026-08-31).
        let checksums = txn.open_table(UPLOAD_CHECKSUMS)?;
        let bucket_prefix = format!("{}\0", params.bucket.as_ref().as_str());
        // The resume marker (composite `key\0upload_id`; a bare key
        // marker sorts after every upload of that key) is computed BEFORE
        // the range so the scan can seek past it (T02).
        let marker = key_marker_order(
            params.key_marker.as_deref(),
            params.upload_id_marker.as_deref(),
        );
        // T02: the scan starts at the later of the key-prefix band and
        // the resume marker (exclusive) — a deep resume or a sparse
        // prefix never re-reads the rows before the marker (the seek the
        // object listing already uses, mem/src/object.rs).
        let prefix_bound = format!("{bucket_prefix}{}", params.prefix);
        let marker_bound = marker.as_deref().map(|m| format!("{bucket_prefix}{m}"));
        let start = band_start(&prefix_bound, marker_bound.as_deref());
        // The lazy scan feeds the engine directly — it stops one probe
        // entry past the page, so only the rows the page touches are
        // materialized (no full-bucket Vec). The engine's item carries no
        // bucket; `params.bucket` is attached when the page is built (one
        // clone per emitted upload, not per scanned row). The sync scan
        // runs inline on the async executor by design (mem is the
        // reference backend, rows are owned copies, and the redb read txn
        // is MVCC — no lock is held). An `Err` row fails the listing via
        // the error cell (the pattern of mem/src/object.rs).
        let mut scan_error = None;
        let items = uploads
            .range::<&str>((start, Bound::Unbounded))?
            .take_while(|entry| {
                // `Err` rows pass through to the error cell — only a
                // non-matching key ends the scan: the bucket's key-prefix
                // band (the bucket bound is implied by the range start,
                // but the explicit check also guards the slice below and
                // terminates at the next bucket's rows — keys never
                // contain `\0`, so the band is contiguous).
                entry
                    .as_ref()
                    .map(|(k, _)| {
                        let rest = &k.value()[bucket_prefix.len()..];
                        k.value().starts_with(&bucket_prefix) && rest.starts_with(&params.prefix)
                    })
                    .unwrap_or(true)
            })
            .filter_map(|entry| match entry {
                Ok((k, v)) => {
                    let rest = &k.value()[bucket_prefix.len()..];
                    let Some((key, upload_id)) = rest.rsplit_once('\0') else {
                        return None; // malformed row — skipped, never a panic
                    };
                    if !key.starts_with(&params.prefix) {
                        return None;
                    }
                    let Ok(key) = object::key(key) else {
                        return None; // tampered row — skipped like list_objects
                    };
                    let checksum_row = match checksums.get(k.value()) {
                        Ok(row) => row.map(|v| {
                            let (a, t) = v.value();
                            (a.to_string(), t.to_string())
                        }),
                        Err(e) => {
                            if scan_error.is_none() {
                                scan_error = Some(database_storage(e));
                            }
                            return None;
                        }
                    };
                    // A domain-invalid checksum row self-heals: the
                    // upload is listed without a spec (F07 — the fs
                    // backend skips the same way; a hard error here
                    // would fail the whole listing for one bad row). The
                    // create-time tags element self-heals the same way
                    // (empty — the wire is API-written).
                    let checksum =
                        checksum_row.and_then(|(a, t)| checksum::Upload::from_wire_opt(&a, &t));
                    let (initiated_at, tags_wire) = v.value();
                    Some(UploadRow {
                        key,
                        upload_id: upload_id.to_string(),
                        initiated_at,
                        tags: object::Tags::parse_wire_limited(tags_wire, object::OBJECT_TAGS_MAX)
                            .unwrap_or_default(),
                        checksum,
                    })
                }
                Err(e) => {
                    if scan_error.is_none() {
                        scan_error = Some(database_storage(e));
                    }
                    None
                }
            });
        // Compound keys (`bucket\0key\0upload_id`) scan in (key, id) order,
        // so key order — and thus delimiter grouping — needs no re-sort.
        // The resume marker pairs the key with the upload id, so a page
        // can position inside a same-key group (S3 `upload-id-marker`); a
        // bare key marker skips the whole key group — the conversion has
        // one home in tinio-core (shared with the fs backend).
        let (rows, common_prefixes, truncated, next) = group_and_paginate_ordered(
            items,
            &params.prefix,
            params.delimiter.as_deref(),
            marker.as_deref(),
            params.max_uploads,
            |u| u.key.as_ref(),
            |u| uploads_order(&u.key, &u.upload_id),
        );
        if let Some(err) = scan_error {
            return Err(err);
        }
        let uploads = rows
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{
            BucketOps, CompletedPart, ListUploadsParams, MultipartOps, ObjectOps, PartInfo, bucket,
            multipart::{MIN_PART_BYTES, part_number},
            object,
            storage::Error::*,
        },
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

    async fn with_bucket() -> (MemoryStorage, bucket::Name) {
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
        let ukey = upload_key(
            name.as_ref().as_str(),
            key.as_ref().as_str(),
            &upload.upload_id,
        );
        let pk = part_key(&upload.upload_id, 1);
        {
            let txn = storage.db.begin_write().unwrap();
            txn.open_table(UPLOAD_CHECKSUMS)
                .unwrap()
                .insert(ukey.as_str(), ("BLAKE3", ""))
                .unwrap();
            txn.open_table(PART_CHECKSUMS)
                .unwrap()
                .insert(pk.as_str(), ("BLAKE3", "AAAA"))
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
