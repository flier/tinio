#![cfg(feature = "multipart")]

//! S3 multipart operations of the mapping layer (task T050).
//!
//! CreateMultipartUpload/UploadPart/UploadPartCopy/CompleteMultipartUpload/
//! AbortMultipartUpload/ListParts/ListMultipartUploads over the storage
//! contract. The composed ETag `MD5-of-MD5s-N` comes from the backend
//! (FR-022). Part numbers are validated `1..=10000` (invalid → InvalidPart);
//! UploadPartCopy is additionally gated by the `copy` feature. The
//! `multipart` cargo feature and the runtime `[s3]` toggle answer
//! `NotImplemented` (FR-021).

use s3s::{S3Error, S3Request, S3Response, S3Result, dto, s3_error};

use tinio_core::{
    multipart::{CompletedPart, MIN_PART_BYTES, PartNumber},
    storage::{ByteRange, ListPartsParams, ListUploadsParams, Storage},
};

use crate::backend::{
    ConditionalHeaders, S3Backend, byte_range, map_backend_error, normalize_delimiter,
};

/// A request part number into the validated [`PartNumber`] (invalid →
/// `InvalidPart`).
fn part_number(n: i32) -> S3Result<PartNumber> {
    tinio_core::multipart::part_number(n as u32)
        .map_err(|_| s3_error!(InvalidPart, "invalid part number: {n}"))
}

/// The `x-amz-copy-source-range` header into a [`ByteRange`], parsed by
/// the framework's own range grammar. S3 copy ranges use the strict
/// `bytes=first-last` form only; the suffix/open forms GET accepts answer
/// `InvalidArgument` — the shared [`byte_range`] mapping plus the strict
/// shape gate.
#[cfg(feature = "copy")]
fn copy_source_range(raw: &str) -> Result<ByteRange, S3Error> {
    let invalid = || s3_error!(InvalidArgument, "invalid copy source range: {raw}");
    let range = byte_range(dto::Range::parse(raw).map_err(|_| invalid())?);
    match range {
        ByteRange::Inclusive(_, _) => Ok(range),
        _ => Err(invalid()),
    }
}

impl<S: Storage> S3Backend<S> {
    /// The runtime multipart gate: disabled → `NotImplemented` (FR-021).
    fn require_multipart(&self) -> S3Result<()> {
        if self.caps.multipart {
            Ok(())
        } else {
            Err(s3_error!(NotImplemented, "multipart is disabled"))
        }
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload = self
            .storage
            .create_multipart_upload(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::CreateMultipartUploadOutput {
            bucket: Some(bucket.to_string()),
            key: Some(key.to_string()),
            upload_id: Some(upload.upload_id),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload_id = req.input.upload_id;
        let part_number = part_number(req.input.part_number)?;
        let body = Self::stream_in(req.input.body);
        let part = self
            .storage
            .upload_part(&bucket, &key, &upload_id, part_number, body)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::UploadPartOutput {
            e_tag: Some(Self::etag_wire(&part.etag)),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    #[cfg(feature = "copy")]
    pub(crate) async fn op_upload_part_copy(
        &self,
        req: S3Request<dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<dto::UploadPartCopyOutput>> {
        Self::require_cap(
            self.caps.multipart && self.caps.copy_object,
            "UploadPartCopy",
        )?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload_id = req.input.upload_id;
        let part_number = part_number(req.input.part_number)?;
        let (src_bucket, src_key) = self.copy_source(&req.input.copy_source)?;

        // The copy source's byte range, if any (`x-amz-copy-source-range`).
        let range = req
            .input
            .copy_source_range
            .as_deref()
            .map(copy_source_range)
            .transpose()?;

        // Source conditionals (412 on failure, per S3 copy semantics):
        // the head's info carries the source ETag + mtime (no body — the
        // copy primitive moves the bytes).
        let info = self
            .storage
            .head_object(&src_bucket, &src_key)
            .await
            .map_err(map_backend_error)?;
        ConditionalHeaders::new(
            req.input.copy_source_if_match.as_ref(),
            req.input.copy_source_if_none_match.as_ref(),
            req.input.copy_source_if_modified_since,
            req.input.copy_source_if_unmodified_since,
        )
        .check(&info.etag, info.last_modified, true)?;
        let part = self
            .storage
            .copy_part(
                &src_bucket,
                &src_key,
                &bucket,
                &key,
                &upload_id,
                part_number,
                range,
            )
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::UploadPartCopyOutput {
            copy_part_result: Some(dto::CopyPartResult {
                e_tag: Some(Self::etag_wire(&part.etag)),
                last_modified: Some(Self::last_modified(part.last_modified)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload_id = req.input.upload_id;
        let parts = req
            .input
            .multipart_upload
            .and_then(|m| m.parts)
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                let raw = p
                    .part_number
                    .ok_or_else(|| s3_error!(InvalidArgument, "missing part number"))?;
                let part_number = part_number(raw)?;
                let etag = p
                    .e_tag
                    .ok_or_else(|| s3_error!(InvalidPart, "missing part ETag"))?;
                let etag = etag
                    .into_strong()
                    .ok_or_else(|| s3_error!(InvalidPart, "weak part ETag"))?;
                let etag = tinio_core::ETag::new(&etag)
                    .map_err(|_| s3_error!(InvalidPart, "invalid part ETag"))?;
                Ok(CompletedPart { part_number, etag })
            })
            .collect::<Result<Vec<CompletedPart>, S3Error>>()?;
        // S3 requires every non-final part to be at least 5 MiB
        // (EntityTooSmall); the final part has no minimum. The sizes come
        // from the stored parts (paged listing), so the check reflects the
        // bytes the completion would assemble — a part that does not exist
        // answers InvalidPart, matching the backend's own verification.
        if parts.len() > 1 {
            let mut sizes = std::collections::HashMap::new();
            let mut marker = None;
            loop {
                let page = self
                    .storage
                    .list_parts(ListPartsParams {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        max_parts: 1000,
                        part_number_marker: marker,
                    })
                    .await
                    .map_err(map_backend_error)?;
                for part in page.parts {
                    sizes.insert(u32::from(part.part_number), part.size);
                }
                match page.next_part_number_marker {
                    Some(next) if page.truncated => marker = Some(next),
                    _ => break,
                }
            }
            for (index, part) in parts.iter().enumerate() {
                if index + 1 == parts.len() {
                    continue; // the final part may be smaller than 5 MiB
                }
                let n = u32::from(part.part_number);
                let size = sizes
                    .get(&n)
                    .copied()
                    .ok_or_else(|| s3_error!(InvalidPart, "part {n} was not uploaded"))?;
                if size < MIN_PART_BYTES {
                    return Err(s3_error!(
                        EntityTooSmall,
                        "part {n} is {size} bytes, below the {MIN_PART_BYTES}-byte minimum for non-final parts"
                    ));
                }
            }
        }
        // Serialize with the write lock: the completion writes the
        // object — it must not land between a conditional put's check
        // and commit.
        let _guard = self.lock_object(&bucket, &key).await;
        let info = self
            .storage
            .complete_multipart_upload(&bucket, &key, &upload_id, &parts)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::CompleteMultipartUploadOutput {
            bucket: Some(bucket.to_string()),
            key: Some(key.to_string()),
            e_tag: Some(Self::etag_wire(&info.etag)),
            location: Some(format!("/{bucket}/{}", info.key)),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        self.storage
            .abort_multipart_upload(&bucket, &key, &req.input.upload_id)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::AbortMultipartUploadOutput::default()))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload_id = req.input.upload_id;
        let max_parts = req.input.max_parts.unwrap_or(1000).max(0);
        // A negative marker would wrap to a huge u32 and silently mask
        // every part as "already listed" — reject it like the
        // part-number validation does.
        let part_number_marker = req
            .input
            .part_number_marker
            .map(|n| {
                u32::try_from(n)
                    .map_err(|_| s3_error!(InvalidArgument, "invalid part-number-marker: {n}"))
            })
            .transpose()?;
        let page = self
            .storage
            .list_parts(ListPartsParams {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                max_parts: max_parts as usize,
                part_number_marker,
            })
            .await
            .map_err(map_backend_error)?;
        let parts = page
            .parts
            .into_iter()
            .map(|p| dto::Part {
                e_tag: Some(Self::etag_wire(&p.etag)),
                last_modified: Some(Self::last_modified(p.last_modified)),
                part_number: Some(u32::from(p.part_number) as i32),
                size: Some(p.size as i64),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(dto::ListPartsOutput {
            bucket: Some(bucket.to_string()),
            key: Some(key.to_string()),
            upload_id: Some(upload_id),
            is_truncated: Some(page.truncated),
            max_parts: Some(max_parts),
            next_part_number_marker: page.next_part_number_marker.map(|n| n as i32),
            part_number_marker: req.input.part_number_marker,
            parts: Some(parts),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_list_multipart_uploads(
        &self,
        req: S3Request<dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<dto::ListMultipartUploadsOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let max_uploads = req.input.max_uploads.unwrap_or(1000).max(0);
        // An empty `delimiter=` means "no delimiter" (the shared
        // boundary rule, `normalize_delimiter`).
        let delimiter = normalize_delimiter(req.input.delimiter.clone());
        let page = self
            .storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: bucket.clone(),
                prefix: req.input.prefix.clone().unwrap_or_default(),
                delimiter,
                key_marker: req.input.key_marker.clone(),
                upload_id_marker: req.input.upload_id_marker.clone(),
                max_uploads: max_uploads as usize,
            })
            .await
            .map_err(map_backend_error)?;
        let uploads = page
            .uploads
            .into_iter()
            .map(|u| dto::MultipartUpload {
                initiated: Some(Self::last_modified(u.initiated_at)),
                key: Some(u.key.to_string()),
                upload_id: Some(u.upload_id),
                ..Default::default()
            })
            .collect();
        let common_prefixes = page
            .common_prefixes
            .into_iter()
            .map(|p| dto::CommonPrefix { prefix: Some(p) })
            .collect();
        Ok(S3Response::new(dto::ListMultipartUploadsOutput {
            bucket: Some(bucket.to_string()),
            is_truncated: Some(page.truncated),
            max_uploads: Some(max_uploads),
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            prefix: req.input.prefix,
            key_marker: req.input.key_marker,
            upload_id_marker: req.input.upload_id_marker,
            uploads: Some(uploads),
            common_prefixes: Some(common_prefixes),
            delimiter: req.input.delimiter,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testutil::{s3_request, setup};
    use s3s::{S3, S3ErrorCode};
    use tinio_core::storage::{BucketOps, ObjectOps};
    use tinio_core::{bucket, object};
    use tinio_mem::MemoryStorage;
    #[cfg(feature = "copy")]
    use tinio_util::testing::body;
    use tinio_util::testing::read_body;

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn multipart_lifecycle() {
        let (backend, b) = setup().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();

        // Upload three parts: the two non-final parts must satisfy the
        // S3 5 MiB minimum; the final part may be small.
        let mut etags = Vec::new();
        let min = tinio_core::multipart::MIN_PART_BYTES as usize;
        let parts_data: Vec<Vec<u8>> =
            vec![vec![b'a'; min + 1], vec![b'b'; min + 1], b"tail".to_vec()];
        for (n, data) in parts_data.iter().enumerate() {
            let part = backend
                .upload_part(s3_request(dto::UploadPartInput {
                    bucket: b.clone(),
                    key: "big.bin".into(),
                    upload_id: upload_id.clone(),
                    part_number: (n + 1) as i32,
                    body: Some(dto::StreamingBlob::wrap(futures::stream::iter(vec![Ok::<
                        _,
                        std::io::Error,
                    >(
                        bytes::Bytes::copy_from_slice(data),
                    )]))),
                    ..Default::default()
                }))
                .await
                .unwrap();
            etags.push(part.output.e_tag.unwrap());
        }

        // List parts.
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(listed.output.parts.as_ref().unwrap().len(), 3);

        // Complete → composed ETag MD5-of-MD5s-3.
        let complete = backend
            .complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(
                        etags
                            .into_iter()
                            .enumerate()
                            .map(|(i, e)| dto::CompletedPart {
                                part_number: Some((i + 1) as i32),
                                e_tag: Some(e),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        let etag_owned = complete.output.e_tag.unwrap();
        let etag = etag_owned.as_strong().unwrap().to_string();
        assert!(etag.ends_with("-3"), "composed multipart ETag, got {etag}");
        let got = read_body(
            backend
                .storage()
                .get_object(
                    &bucket::name(&b).unwrap(),
                    &object::key("big.bin").unwrap(),
                    None,
                )
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        let expected: Vec<u8> = parts_data.iter().flatten().copied().collect();
        assert_eq!(got, expected);
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_rejects_non_final_parts_below_5_mib() {
        let (backend, b) = setup().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();

        let mut etags = Vec::new();
        for n in 1..=3 {
            let part = backend
                .upload_part(s3_request(dto::UploadPartInput {
                    bucket: b.clone(),
                    key: "big.bin".into(),
                    upload_id: upload_id.clone(),
                    part_number: n,
                    body: Some(dto::StreamingBlob::wrap(futures::stream::iter(vec![Ok::<
                        _,
                        std::io::Error,
                    >(
                        bytes::Bytes::copy_from_slice(b"tiny"),
                    )]))),
                    ..Default::default()
                }))
                .await
                .unwrap();
            etags.push(part.output.e_tag.unwrap());
        }

        let err = backend
            .complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(
                        etags
                            .into_iter()
                            .enumerate()
                            .map(|(i, e)| dto::CompletedPart {
                                part_number: Some((i + 1) as i32),
                                e_tag: Some(e),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                }),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::EntityTooSmall);
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_multipart_uploads_resumes_inside_a_same_key_group() {
        let (backend, b) = setup().await;
        // Two uploads of the same key: the second is reachable only when
        // the upload-id marker is honored by the pagination.
        let create = |bucket: String| async {
            backend
                .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                    bucket,
                    key: "same.bin".into(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .output
                .upload_id
                .unwrap()
        };
        let u1 = create(b.clone()).await;
        let u2 = create(b.clone()).await;
        assert_ne!(u1, u2);

        let page1 = backend
            .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                bucket: b.clone(),
                max_uploads: Some(1),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(page1.is_truncated, Some(true));
        let page1_id = page1.uploads.as_ref().unwrap()[0]
            .upload_id
            .clone()
            .unwrap();
        let next_key = page1.next_key_marker.clone().unwrap();
        let next_upload_id = page1.next_upload_id_marker.clone().unwrap();

        // Resuming with both markers reaches the upload that page 1
        // truncated away.
        let page2 = backend
            .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                bucket: b.clone(),
                max_uploads: Some(10),
                key_marker: Some(next_key),
                upload_id_marker: Some(next_upload_id),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let page2_ids: Vec<String> = page2
            .uploads
            .as_ref()
            .unwrap()
            .iter()
            .map(|u| u.upload_id.clone().unwrap())
            .collect();
        assert_eq!(page2_ids.len(), 1, "{page2_ids:?}");
        assert_ne!(page2_ids[0], page1_id);
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn abort_removes_upload() {
        let (backend, b) = setup().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        backend
            .abort_multipart_upload(s3_request(dto::AbortMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let err = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchUpload");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn invalid_part_numbers_rejected() {
        let (backend, b) = setup().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 0,
                body: Some(dto::StreamingBlob::wrap(futures::stream::empty::<
                    Result<bytes::Bytes, std::io::Error>,
                >())),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidPart");
        backend
            .abort_multipart_upload(s3_request(dto::AbortMultipartUploadInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    #[cfg(feature = "copy")]
    fn upload_part_copy_input(
        b: &str,
        upload_id: &str,
        part_number: i32,
    ) -> dto::UploadPartCopyInput {
        dto::UploadPartCopyInput::builder()
            .bucket(b.to_string())
            .key("copy.bin".to_string())
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .copy_source(dto::CopySource::parse(&format!("{b}/src.bin")).unwrap())
            .build()
            .unwrap()
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_range_and_conditionals() {
        let (backend, b) = setup().await;
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object::key("src.bin").unwrap(),
                body(b"0123456789"),
            )
            .await
            .unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();

        // A valid byte range copies just that slice.
        let mut input = upload_part_copy_input(&b, &upload_id, 1);
        input.copy_source_range = Some("bytes=2-5".into());
        let part = backend.upload_part_copy(s3_request(input)).await.unwrap();
        let etag = part.output.copy_part_result.unwrap().e_tag.unwrap();
        backend
            .complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(vec![dto::CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(etag),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        let got = read_body(
            backend
                .storage()
                .get_object(
                    &bucket::name(&b).unwrap(),
                    &object::key("copy.bin").unwrap(),
                    None,
                )
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        assert_eq!(got, b"2345");

        // Malformed range → InvalidArgument.
        let mut input = upload_part_copy_input(&b, &upload_id, 2);
        input.copy_source_range = Some("junk".into());
        let err = backend
            .upload_part_copy(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");

        // Source conditional failure → 412 (never 304 on the copy path).
        let src_etag = "781e5e245d69b566979b86e28d23f2c7";
        let mut input = upload_part_copy_input(&b, &upload_id, 2);
        input.copy_source_if_none_match = Some(format!("\"{src_etag}\"").parse().unwrap());
        let err = backend
            .upload_part_copy(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // Invalid part number → InvalidPart.
        let input = upload_part_copy_input(&b, &upload_id, 0);
        let err = backend
            .upload_part_copy(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidPart");
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_respects_copy_object_toggle() {
        let storage = MemoryStorage::new().unwrap();
        let backend = S3Backend::new(
            storage,
            crate::backend::Capabilities {
                copy_object: false,
                ..Default::default()
            },
        );
        let b = "data".to_string();
        backend
            .storage()
            .create_bucket(&bucket::name(&b).unwrap())
            .await
            .unwrap();
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object::key("src.bin").unwrap(),
                body(b"0123456789"),
            )
            .await
            .unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let input = upload_part_copy_input(&b, &upload_id, 1);
        let err = backend
            .upload_part_copy(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }
}
