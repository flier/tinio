//! The `MultipartOps` implementation for [`MemoryStorage`].
//!
//! Multipart uploads over the `uploads` + `parts` tables. Assembly,
//! completion, and abort each run in one write transaction; part keys are
//! zero-padded so string order equals part-number order.

use async_trait::async_trait;
use redb::{ReadableDatabase, ReadableTable};

use tinio_core::{
    CompletedPart, ETag, ListPartsParams, ListUploadsParams, MultipartOps, MultipartUpload,
    PartInfo, PartNumber, PartsListing, UploadsListing, bucket, collect_body, from_nanos,
    group_and_paginate_ordered, now_nanos, object, split_uploads_order, uploads_order,
};
use uuid::Uuid;

use crate::{
    Error,
    error::{
        access_denied, database_storage, invalid_etag, invalid_key, invalid_part, no_parts,
        no_such_bucket,
    },
    storage::{
        BUCKETS, MemoryStorage, OBJECT_META, OBJECTS, PART_META, PARTS, UPLOADS, check_bucket,
        check_upload, object_key, parse_part_number, part_key, remove_all_parts, upload_key,
    },
};

#[async_trait]
impl MultipartOps for MemoryStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
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
            initiated_at: std::time::SystemTime::now(),
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
        }
        txn.commit()?;
        Ok(upload)
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: tinio_core::BodyStream,
    ) -> Result<PartInfo, Error> {
        // Fast-fail on a missing bucket before buffering the body (the write
        // transaction re-checks, closing the race).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        let data = collect_body(body).await?;
        let etag = ETag::from_content(&data);
        let now = now_nanos();
        let txn = self.db.begin_write()?;
        {
            let uploads = txn.open_table(UPLOADS)?;
            check_upload(&uploads, upload_id, bucket, key)?;
            let pk = part_key(upload_id, u32::from(part_number));
            let etag_str = etag.as_str();
            let mut parts = txn.open_table(PARTS)?;
            let mut meta = txn.open_table(PART_META)?;
            parts.insert(pk.as_str(), data.as_slice())?;
            meta.insert(pk.as_str(), (etag_str.as_str(), data.len() as u64, now))?;
        }
        txn.commit()?;
        Ok(PartInfo {
            part_number,
            size: data.len() as u64,
            etag,
            last_modified: from_nanos(now),
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
        let prefix = format!("{}\0", params.upload_id);
        // The zero-padded part keys are string-ordered by number, so the
        // scan starts just after the marker and stops one probe part past
        // the page — a page costs O(page) reads, not O(total parts).
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
                Ok(PartInfo {
                    part_number: part_number.into(),
                    size,
                    etag: etag.parse().map_err(invalid_etag)?,
                    last_modified: from_nanos(mtime),
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
                uploads.remove(
                    upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id).as_str(),
                )?;
            }
            {
                let mut stored_parts = txn.open_table(PARTS)?;
                let mut stored_meta = txn.open_table(PART_META)?;
                let prefix = format!("{upload_id}\0");
                remove_all_parts(&mut stored_parts, &mut stored_meta, &prefix)?;
            }
            (data, etag, now)
        };
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
        {
            {
                let uploads = txn.open_table(UPLOADS)?;
                check_upload(&uploads, upload_id, bucket, key)?;
            }
            {
                let mut uploads = txn.open_table(UPLOADS)?;
                uploads.remove(
                    upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id).as_str(),
                )?;
            }
            {
                let mut stored_parts = txn.open_table(PARTS)?;
                let mut stored_meta = txn.open_table(PART_META)?;
                let prefix = format!("{upload_id}\0");
                remove_all_parts(&mut stored_parts, &mut stored_meta, &prefix)?;
            }
        }
        txn.commit()?;
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
        let bucket_prefix = format!("{}\0", params.bucket.as_ref().as_str());
        let upload_list: Vec<MultipartUpload> = uploads
            .range(bucket_prefix.as_str()..)?
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(k, _)| k.value().starts_with(&bucket_prefix))
                    .unwrap_or(false)
            })
            .filter_map(|entry| match entry {
                Ok((k, v)) => {
                    let rest = &k.value()[bucket_prefix.len()..];
                    let (key, upload_id) = rest.rsplit_once('\0')?;
                    if !key.starts_with(&params.prefix) {
                        return None;
                    }
                    Some(Ok(MultipartUpload {
                        upload_id: upload_id.to_string(),
                        bucket: params.bucket.clone(),
                        key: object::key(key).ok()?,
                        initiated_at: from_nanos(v.value()),
                    }))
                }
                Err(e) => Some(Err(database_storage(e))),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // Compound keys (`bucket\0key\0upload_id`) scan in (key, id) order,
        // so key order — and thus delimiter grouping — needs no re-sort.
        // The resume marker pairs the key with the upload id, so a page
        // can position inside a same-key group (S3 `upload-id-marker`).
        // A bare key marker skips the whole key group (S3: only keys
        // strictly greater than `key-marker` are listed) — the sentinel
        // upload id sorts after every real one.
        let marker = match (&params.key_marker, &params.upload_id_marker) {
            (Some(key), Some(upload_id)) => Some(uploads_order(key, upload_id)),
            (Some(key), None) => Some(uploads_order(key, "\u{10FFFF}")),
            _ => None,
        };
        let (keys, common_prefixes, truncated, next) = group_and_paginate_ordered(
            upload_list,
            &params.prefix,
            params.delimiter.as_deref(),
            marker.as_deref(),
            params.max_uploads,
            |u| u.key.as_ref(),
            |u| uploads_order(&u.key, &u.upload_id),
        );
        let (next_key, next_upload_id) = match next {
            Some(next) => {
                let (key, upload_id) = split_uploads_order(&next);
                (Some(key.to_string()), upload_id.map(str::to_string))
            }
            None => (None, None),
        };
        Ok(UploadsListing {
            uploads: keys,
            common_prefixes,
            truncated,
            next_key_marker: next_key,
            next_upload_id_marker: next_upload_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use tinio_core::{
        BucketOps, CompletedPart, ListPartsParams, ListUploadsParams, MultipartOps, ObjectOps,
        PartInfo, bucket, multipart::part_number, object, storage::Error::*,
    };
    use tinio_util::testing::{body, read_body};

    use super::*;

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
    async fn upload_ids_are_unique() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let a = storage
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        let b = storage
            .create_multipart_upload(&bucket, &key)
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
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        assert!(matches!(
            storage
                .upload_part(
                    &other,
                    &key,
                    &upload.upload_id,
                    1.into(),
                    body(b"x".to_vec())
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
                    body(b"x".to_vec())
                )
                .await
                .unwrap_err(),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            storage
                .upload_part(&bucket, &key, "no-such", 1.into(), body(b"x".to_vec()))
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
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"old".to_vec()),
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
            .create_multipart_upload(&bucket, &key)
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
            .create_multipart_upload(&bucket, &key)
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
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"x".to_vec()),
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
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"x".to_vec()),
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
            .create_multipart_upload(&bucket, &key)
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
                .create_multipart_upload(&bucket, &object::key(key).unwrap())
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
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        storage
            .create_multipart_upload(&bucket, &key)
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
            .create_multipart_upload(&bucket, &key)
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
