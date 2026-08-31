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
        from_nanos, group_and_paginate_ordered, key_marker_order, now_nanos, object,
        split_uploads_order, uploads_order,
    },
    Error,
    error::{
        access_denied, database_storage, invalid_etag, invalid_key, invalid_part, no_parts,
        no_such_bucket, no_such_upload,
    },
    storage::{
        BUCKETS, MemoryStorage, OBJECT_META, OBJECTS, PART_CHECKSUMS, PART_META, PARTS,
        UPLOAD_CHECKSUMS, UPLOADS, band_start, check_bucket, check_upload, collect_part_keys,
        object_key, parse_part_number, part_key, remove_all_parts, upload_key,
    },
};

/// One in-progress upload of the lazy `list_multipart_uploads` scan — the
/// engine's item without the bucket. `params.bucket` is attached when the
/// page is built: one clone per emitted upload, not per scanned row.
struct UploadRow {
    key: object::Key,
    upload_id: String,
    initiated_at: u64,
    checksum: Option<checksum::Upload>,
}

#[async_trait]
impl MultipartOps for MemoryStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
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
        };
        let txn = self.db.begin_write()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let mut uploads = txn.open_table(UPLOADS)?;
            uploads.insert(
                upload_key(
                    upload.bucket.as_ref().as_str(),
                    upload.key.as_ref().as_str(),
                    &upload.upload_id,
                )
                .as_str(),
                now_nanos(),
            )?;
            // The create-time checksum spec, persisted alongside the
            // UPLOADS row (spec 2026-08-31).
            if let Some(c) = checksum {
                let mut cs = txn.open_table(UPLOAD_CHECKSUMS)?;
                let (algo, ty) = c.to_wire();
                cs.insert(
                    upload_key(
                        upload.bucket.as_ref().as_str(),
                        upload.key.as_ref().as_str(),
                        &upload.upload_id,
                    )
                    .as_str(),
                    (algo.as_str(), ty.as_str()),
                )?;
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
        // One `UPLOADS` row fetch serves both the existence check and
        // `initiated_at` (the compound key already encodes the identity).
        let uploads = txn.open_table(UPLOADS)?;
        let initiated = uploads
            .get(ukey.as_str())?
            .ok_or_else(|| no_such_upload(upload_id))?
            .value();
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
            checksum: checksum_row
                .map(|(a, t)| checksum::Upload::from_wire(&a, &t))
                .transpose()?,
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
        // The tee's MD5 (etag_md5): a part's ETag IS its content MD5 —
        // the slot value replaces the second hash over the buffer.
        let etag = match checksum
            .as_ref()
            .filter(|c| c.etag_md5)
            .and_then(|c| c.digest.get())
        {
            Some(part) => {
                let digest = part
                    .value
                    .md5_raw()
                    .expect("the tee's md5 is valid and 16 bytes");
                ETag::Single(digest)
            }
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
        // key — join them per row (spec 2026-08-31).
        let checksums = txn.open_table(PART_CHECKSUMS)?;
        let prefix = format!("{}\0", params.upload_id);
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
                let checksum_row = checksums.get(k.value())?.map(|v| {
                    let (a, value) = v.value();
                    (a.to_string(), value.to_string())
                });
                let checksum = match checksum_row {
                    None => None,
                    Some((a, value)) => Some(checksum::Part::from_wire(&a, value)?),
                };
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
        let (data, etag, now) = {
            {
                let uploads = txn.open_table(UPLOADS)?;
                check_upload(&uploads, upload_id, bucket, key)?;
            }
            let mut prev = 0u32;
            let mut data = Vec::new();
            let mut infos: Vec<PartInfo> = Vec::new();
            {
                let stored_parts = txn.open_table(PARTS)?;
                let stored_meta = txn.open_table(PART_META)?;
                for part in parts {
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
                }
            }
            let etag = ETag::composed_from_parts(&infos).expect("parts checked non-empty above");
            let etag_str = etag.as_str();
            let now = now_nanos();
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            {
                let mut objects = txn.open_table(OBJECTS)?;
                let mut obj_meta = txn.open_table(OBJECT_META)?;
                objects.insert(ok.as_str(), data.as_slice())?;
                obj_meta.insert(ok.as_str(), (etag_str.as_str(), data.len() as u64, now))?;
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
                    let checksum = match checksum_row {
                        None => None,
                        Some((a, t)) => match checksum::Upload::from_wire(&a, &t) {
                            Ok(upload) => Some(upload),
                            Err(e) => {
                                if scan_error.is_none() {
                                    scan_error = Some(e.into());
                                }
                                return None;
                            }
                        },
                    };
                    Some(UploadRow {
                        key,
                        upload_id: upload_id.to_string(),
                        initiated_at: v.value(),
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
            BucketOps, CompletedPart, ListPartsParams, ListUploadsParams, MultipartOps, ObjectOps,
            PartInfo, bucket, multipart::part_number, object, storage::Error::*,
        },
        _util::testing::{body, read_body},
        MemoryOptions,
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
            .create_multipart_upload(&name, &key, None)
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
            .create_multipart_upload(&name, &key, None)
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
            .create_multipart_upload(&name, &key, None)
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
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let b = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        assert_ne!(a.upload_id, b.upload_id);
        assert_eq!(a.upload_id.len(), 36);
    }

    #[tokio::test]
    async fn upload_part_rejects_part_numbers_outside_1_to_10000() {
        assert!(matches!(part_number(0), Err(InvalidPartNumber(0))));
        assert!(matches!(
            part_number(10_001),
            Err(InvalidPartNumber(10_001))
        ));
    }

    #[tokio::test]
    async fn upload_part_rejects_mismatched_bucket_or_key() {
        let (storage, bucket) = with_bucket().await;
        let other = bucket::name("other").unwrap();
        storage.create_bucket(&other).await.unwrap();
        let key = object::key("a.bin").unwrap();
        let other_key = object::key("b.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        assert!(matches!(
            storage
                .upload_part(
                    &other,
                    &key,
                    &upload.upload_id,
                    1.into(),
                    body(b"x".to_vec()),
                    None
                )
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            storage
                .upload_part(
                    &bucket,
                    &other_key,
                    &upload.upload_id,
                    1.into(),
                    body(b"x".to_vec()),
                    None
                )
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            storage
                .upload_part(
                    &bucket,
                    &key,
                    "no-such",
                    1.into(),
                    body(b"x".to_vec()),
                    None
                )
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
    }

    #[tokio::test]
    async fn overwrite_part_replaces_previous() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"old".to_vec()),
                None,
            )
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"newer".to_vec()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(part.size, 5);
        assert_eq!(part.etag, ETag::from_content(b"newer"));
        let completed = storage
            .complete_multipart_upload(&bucket, &key, &upload.upload_id, &[completed(&part)])
            .await
            .unwrap();
        assert_eq!(completed.size, 5);
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"newer");
    }

    #[tokio::test]
    async fn complete_without_parts_is_invalid() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        assert!(matches!(
            storage
                .complete_multipart_upload(&bucket, &key, &upload.upload_id, &[])
                .await
                .unwrap_err(),
            Error::Storage(NoParts)
        ));
    }

    #[tokio::test]
    async fn complete_rejects_unknown_part_number() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let phantom = CompletedPart {
            part_number: 7.into(),
            etag: ETag::from_content(b"never-uploaded"),
        };
        assert!(matches!(
            storage
                .complete_multipart_upload(&bucket, &key, &upload.upload_id, &[phantom])
                .await
                .unwrap_err(),
            Error::Storage(InvalidPart(7))
        ));
    }

    #[tokio::test]
    async fn complete_and_abort_reject_mismatched_identity() {
        let (storage, bucket) = with_bucket().await;
        let other = bucket::name("other").unwrap();
        storage.create_bucket(&other).await.unwrap();
        let key = object::key("a.bin").unwrap();
        let other_key = object::key("b.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"x".to_vec()),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            storage
                .complete_multipart_upload(&other, &key, &upload.upload_id, &[completed(&part)])
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            storage
                .abort_multipart_upload(&bucket, &other_key, &upload.upload_id)
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
    }

    #[tokio::test]
    async fn complete_removes_upload_and_parts() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"x".to_vec()),
                None,
            )
            .await
            .unwrap();
        storage
            .complete_multipart_upload(&bucket, &key, &upload.upload_id, &[completed(&part)])
            .await
            .unwrap();
        assert!(matches!(
            storage
                .complete_multipart_upload(&bucket, &key, &upload.upload_id, &[completed(&part)])
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            storage
                .list_parts(ListPartsParams {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload.upload_id.clone(),
                    max_parts: 1000,
                    part_number_marker: None,
                })
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
    }

    #[tokio::test]
    async fn list_parts_paginates() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        for n in 1..=3 {
            storage
                .upload_part(
                    &bucket,
                    &key,
                    &upload.upload_id,
                    n.into(),
                    body(format!("p{n}").into_bytes()),
                    None,
                )
                .await
                .unwrap();
        }
        let page = storage
            .list_parts(ListPartsParams {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 2,
                part_number_marker: None,
            })
            .await
            .unwrap();
        assert_eq!(
            page.parts
                .iter()
                .map(|p| u32::from(p.part_number))
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(page.truncated);
        assert_eq!(page.next_part_number_marker, Some(2));
        let page2 = storage
            .list_parts(ListPartsParams {
                bucket,
                key,
                upload_id: upload.upload_id,
                max_parts: 2,
                part_number_marker: Some(2),
            })
            .await
            .unwrap();
        assert_eq!(
            page2
                .parts
                .iter()
                .map(|p| u32::from(p.part_number))
                .collect::<Vec<_>>(),
            [3]
        );
        assert!(!page2.truncated);
        assert!(page2.next_part_number_marker.is_none());
    }

    #[tokio::test]
    async fn list_uploads_filters_and_paginates() {
        let (storage, bucket) = with_bucket().await;
        for key in ["a.bin", "b.bin", "c.bin"] {
            storage
                .create_multipart_upload(&bucket, &object::key(key).unwrap(), None)
                .await
                .unwrap();
        }
        let prefixed = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: bucket.clone(),
                prefix: "b".into(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
            })
            .await
            .unwrap();
        let keys: Vec<_> = prefixed.uploads.iter().map(|u| u.key.as_ref()).collect();
        assert_eq!(keys, ["b.bin"]);
        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })
            .await
            .unwrap();
        assert_eq!(page.uploads.len(), 1);
        assert!(page.truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("a.bin"));
        let page2 = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket,
                prefix: String::new(),
                delimiter: None,
                key_marker: page.next_key_marker.clone(),
                upload_id_marker: page.next_upload_id_marker.clone(),
                max_uploads: 10,
            })
            .await
            .unwrap();
        let keys: Vec<_> = page2.uploads.iter().map(|u| u.key.as_ref()).collect();
        assert_eq!(keys, ["b.bin", "c.bin"]);
        assert!(!page2.truncated);
    }

    #[tokio::test]
    async fn bare_key_marker_skips_the_whole_key_group() {
        // A key-marker without an upload-id-marker skips the entire
        // same-key group (S3: only keys strictly greater than the marker
        // are listed).
        let (storage, bucket) = with_bucket().await;
        let key = object::key("same.bin").unwrap();
        let u1 = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket,
                prefix: String::new(),
                delimiter: None,
                key_marker: Some(u1.key.to_string()),
                upload_id_marker: None,
                max_uploads: 10,
            })
            .await
            .unwrap();
        assert!(page.uploads.is_empty(), "{:?}", page.uploads);
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn complete_uses_only_listed_parts() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None)
            .await
            .unwrap();
        let mut uploaded = Vec::new();
        for (n, data) in [(1u32, b"aaa" as &[u8]), (2, b"bbb"), (3, b"ccc")] {
            uploaded.push(
                storage
                    .upload_part(
                        &bucket,
                        &key,
                        &upload.upload_id,
                        n.into(),
                        body(data.to_vec()),
                        None,
                    )
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
            )
            .await
            .unwrap();
        assert_eq!(completed.size, 6);
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"aaabbb");
        assert!(completed.etag.as_str().ends_with("-2"));
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
}
