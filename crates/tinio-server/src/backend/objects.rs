//! Object operations of the S3 mapping layer (task T048).
//!
//! PutObject/GetObject/HeadObject/DeleteObject/DeleteObjects/CopyObject
//! over the storage contract. Range requests seek on the backend and
//! answer `206`/`Content-Range`; conditional headers are evaluated against
//! the meta-store ETag and filesystem mtime (304/412 per RFC 7232).
//! Content-Type is inferred via `mime_guess` (FR-022); `x-amz-meta-*` and
//! `x-amz-checksum-*` are accepted and dropped. CopyObject is gated by the
//! `copy` cargo feature and the runtime `copy_object` toggle (FR-021).

use std::{str::FromStr, time::SystemTime};

use s3s::{
    S3Error, S3Request, S3Response, S3Result,
    dto::{self, DeleteObjectOutput, ETagCondition},
    s3_error,
};

use crate::{
    _core::{
        bucket, object,
        storage::{Error as StorageError, GetObjectResult, Storage},
    },
    backend::{
        ConditionFailure, ConditionalHeaders, S3Backend, byte_range, condition_error,
        map_backend_error,
    },
};

/// Parse an ETag-condition header (`x-amz-if-match`, `x-amz-if-none-match`)
/// into the DTO type when present. CopyObject's destination conditionals
/// are not part of the s3s DTO, so they are read from the headers here.
fn parse_etag_condition_header(
    headers: &http::HeaderMap,
    name: &'static str,
) -> Result<Option<dto::ETagCondition>, S3Error> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))?;
    ETagCondition::from_str(text)
        .map(Some)
        .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))
}

/// The destination-conditional protocol (`x-amz-if-match` /
/// `x-amz-if-none-match`): evaluate against the CURRENT object at
/// (bucket, key), 412 on failure. Shared by the conditional put and the
/// conditional copy — a missing object is the "no current version" case
/// (If-None-Match: *); any real failure must not look like an absent
/// object, or the precondition would pass and overwrite.
impl<S: Storage> S3Backend<S> {
    async fn check_destination_conditions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        if_match: Option<&dto::ETagCondition>,
        if_none_match: Option<&dto::ETagCondition>,
    ) -> S3Result<()> {
        if if_match.is_none() && if_none_match.is_none() {
            return Ok(());
        }
        let current = match self.storage.head_object(bucket, key).await {
            Ok(info) => Some(info),
            Err(err) => {
                let err: StorageError = err.into();
                match err {
                    StorageError::NoSuchKey(_) => None,
                    err => return Err(map_backend_error(err)),
                }
            }
        };
        if let Some(info) = current {
            ConditionalHeaders::new(if_match, if_none_match, None, None).check(
                &info.etag,
                info.last_modified,
                true,
            )?;
        } else if if_match.is_some() {
            // No current version can match an If-Match (412).
            return Err(condition_error(ConditionFailure::Match, true));
        }
        Ok(())
    }
}

/// The batch-delete error entry for a failed key (the S3 error into the
/// wire `dto::Error` shape).
fn delete_error(err: &S3Error, key: String) -> dto::Error {
    dto::Error {
        code: Some(err.code().as_str().into()),
        message: err.message().map(str::to_string),
        key: Some(key),
        version_id: None,
    }
}

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;

        // Stage the body first, outside the write lock: streaming a slow
        // client must not stall other writers. The conditional check and
        // the commit then run under one lock (RFC 7232 exclusivity) — an
        // unconditional put landing between them would pass the
        // precondition against stale state.
        let staged = self
            .storage
            .stage_body(&bucket, &key, Self::stream_in(req.input.body))
            .await
            .map_err(map_backend_error)?;
        let _guard = self.lock_object(&bucket, &key).await;
        // Conditional put: If-Match / If-None-Match against the current
        // object (412 on failure) — the shared destination protocol.
        self.check_destination_conditions(
            &bucket,
            &key,
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )
        .await?;
        let put = self
            .storage
            .commit_object(&bucket, &key, staged)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutObjectOutput {
            e_tag: Some(Self::etag_wire(&put.etag)),
            ..Default::default()
        }))
    }

    pub(crate) async fn op_get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;

        let range = req.input.range.map(byte_range);
        // One resolution: the conditionals evaluate against the object's
        // own info (the body is dropped when a precondition fails).
        let GetObjectResult {
            info,
            body,
            served_range,
        } = self
            .storage
            .get_object(&bucket, &key, range)
            .await
            .map_err(map_backend_error)?;
        ConditionalHeaders::new(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since,
            req.input.if_unmodified_since,
        )
        .check(&info.etag, info.last_modified, false)?;

        let (content_length, content_range) = match served_range {
            Some((start, end)) => (
                end - start + 1,
                Some(format!("bytes {start}-{end}/{}", info.size)),
            ),
            None => (info.size, None),
        };
        let content_type = req
            .input
            .response_content_type
            .or(Some(Self::content_type(info.key.as_ref())));
        Ok(S3Response::new(dto::GetObjectOutput {
            accept_ranges: Some("bytes".into()),
            body: Some(Self::stream_out(body)),
            content_length: Some(content_length as i64),
            content_range,
            content_type,
            e_tag: Some(Self::etag_wire(&info.etag)),
            last_modified: Some(Self::last_modified(info.last_modified)),
            ..Default::default()
        }))
    }

    pub(crate) async fn op_head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let head = self
            .storage
            .head_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        ConditionalHeaders::new(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since,
            req.input.if_unmodified_since,
        )
        .check(&head.etag, head.last_modified, false)?;
        Ok(S3Response::new(dto::HeadObjectOutput {
            accept_ranges: Some("bytes".into()),
            content_length: Some(head.size as i64),
            content_type: Some(Self::content_type(head.key.as_ref())),
            e_tag: Some(Self::etag_wire(&head.etag)),
            last_modified: Some(Self::last_modified(head.last_modified)),
            ..Default::default()
        }))
    }

    pub(crate) async fn op_delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        // Serialize with the write lock: a delete landing between a
        // conditional put's check and commit must not erase the state
        // the precondition was evaluated against.
        let _guard = self.lock_object(&bucket, &key).await;
        // Idempotent per S3 (missing objects still answer 204).
        self.storage
            .delete_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    pub(crate) async fn op_delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        Self::require_cap(self.caps.delete_objects, "DeleteObjects")?;
        let bucket = self.bucket(req.input.bucket)?;
        let quiet = req.input.delete.quiet.unwrap_or(false);
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for id in req.input.delete.objects {
            let key = match self.key(id.key.clone()) {
                Ok(key) => key,
                Err(err) => {
                    errors.push(delete_error(&err, id.key));
                    continue;
                }
            };
            // Per-key write lock, as in `op_delete_object`.
            let _guard = self.lock_object(&bucket, &key).await;
            match self.storage.delete_object(&bucket, &key).await {
                Ok(()) => {
                    if !quiet {
                        deleted.push(dto::DeletedObject {
                            key: Some(key.to_string()),
                            ..Default::default()
                        });
                    }
                }
                Err(err) => {
                    let err = map_backend_error(err);
                    errors.push(delete_error(&err, key.to_string()));
                }
            }
        }
        Ok(S3Response::new(dto::DeleteObjectsOutput {
            deleted: if deleted.is_empty() {
                None
            } else {
                Some(deleted)
            },
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        }))
    }

    /// GetObjectTagging — always an empty tag set (v1 stores no tags;
    /// aws cli invokes this before server-side copies).
    pub(crate) async fn op_get_object_tagging(
        &self,
        req: S3Request<dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::GetObjectTaggingOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        self.storage
            .head_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::GetObjectTaggingOutput {
            tag_set: vec![],
            version_id: None,
        }))
    }

    #[cfg(feature = "copy")]
    pub(crate) async fn op_copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        Self::require_cap(self.caps.copy_object, "CopyObject")?;
        let (src_bucket, src_key) = self.copy_source(&req.input.copy_source)?;
        let dst_bucket = self.bucket(req.input.bucket)?;
        let dst_key = self.key(req.input.key)?;

        // Server-side copy: the contract's copy primitive moves the
        // source bytes into the destination (no client passthrough,
        // FR-015 — a backend may copy them kernel-side). The head's info
        // carries the source ETag + mtime for the conditionals (412 on
        // failure, per S3 copy semantics — no body is streamed to drop).
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
        // The destination write serializes with the write lock, as in
        // `op_put_object` — a copy landing between a conditional put's
        // check and commit would invalidate the precondition.
        let _guard = self.lock_object(&dst_bucket, &dst_key).await;
        // Destination conditionals (`x-amz-if-match` / `x-amz-if-none-match`)
        // evaluate against the CURRENT destination (412 on failure): a
        // conditional copy must not silently overwrite — the shared
        // destination protocol.
        let dest_if_match = parse_etag_condition_header(&req.headers, "x-amz-if-match")?;
        let dest_if_none_match = parse_etag_condition_header(&req.headers, "x-amz-if-none-match")?;
        self.check_destination_conditions(
            &dst_bucket,
            &dst_key,
            dest_if_match.as_ref(),
            dest_if_none_match.as_ref(),
        )
        .await?;
        let put = self
            .storage
            .copy_object(&src_bucket, &src_key, &dst_bucket, &dst_key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::CopyObjectOutput {
            copy_object_result: Some(dto::CopyObjectResult {
                e_tag: Some(Self::etag_wire(&put.etag)),
                last_modified: Some(Self::last_modified(SystemTime::now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use http::HeaderValue;
    use s3s::{
        S3, S3ErrorCode,
        dto::{CopyObjectInput, CopySource, Range, StreamingBlob, Timestamp},
    };
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        _core::{bucket, storage::ObjectOps},
        _mem::MemoryStorage,
        _util::testing::{body, read_body},
        backend::testutil::{s3_request, setup},
    };

    async fn setup_name() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let (backend, b) = setup().await;
        (backend, bucket::name(b.as_str()).unwrap())
    }

    #[tokio::test]
    async fn put_get_head_delete_round_trip() {
        let (backend, b) = setup_name().await;
        let put = backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                ..Default::default()
            }))
            .await
            .unwrap();
        let etag = put.output.e_tag.unwrap();
        assert_eq!(
            etag.as_strong().unwrap(),
            "5d41402abc4b2a76b9719d911017c592"
        );

        let get = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = get.output;
        assert_eq!(out.content_length, Some(5));
        assert_eq!(
            out.e_tag.unwrap().as_strong().unwrap(),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert!(out.content_type.as_deref().unwrap().contains("text/plain"));
        let mut body = out.body.unwrap();
        let mut got = Vec::new();
        while let Some(chunk) = body.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(got, b"hello");

        let head = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(head.output.content_length, Some(5));

        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
    }

    #[tokio::test]
    async fn range_requests() {
        let (backend, b) = setup_name().await;
        backend
            .storage()
            .put_object(&b, &"digits".into(), body(b"0123456789"))
            .await
            .unwrap();
        let get = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "digits".into(),
                range: Some(Range::Int {
                    first: 2,
                    last: Some(5),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(get.output.content_range.as_deref(), Some("bytes 2-5/10"));
        assert_eq!(get.output.content_length, Some(4));
        let mut body = get.output.body.unwrap();
        let mut got = Vec::new();
        while let Some(chunk) = body.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(got, b"2345");

        // Unsatisfiable range → InvalidRange (416).
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "digits".into(),
                range: Some(Range::Int {
                    first: 99,
                    last: None,
                }),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRange");
    }

    #[tokio::test]
    async fn conditional_requests() {
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();

        // If-None-Match matching → 304.
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_none_match: Some(format!("\"{etag}\"").parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotModified");

        // If-Match mismatching → 412.
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some("\"deadbeefdeadbeefdeadbeefdeadbeef\"".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // If-Match matching → 200.
        backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some(format!("\"{etag}\"").parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();

        // If-Match takes precedence over If-Unmodified-Since (RFC 9110
        // §13.1.4): a matching If-Match wins even with a stale date.
        let ok = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some(format!("\"{etag}\"").parse().unwrap()),
                if_unmodified_since: Some(Timestamp::from(
                    OffsetDateTime::from_unix_timestamp(915_148_800).unwrap(),
                )),
                ..Default::default()
            }))
            .await;
        assert!(ok.is_ok(), "If-Match must override If-Unmodified-Since");

        // If-Unmodified-Since takes precedence over If-None-Match (RFC
        // 9110 §13.2.2): a matching If-None-Match with a failing date
        // answers 412, never 304 — a caching client must not reuse a
        // stale body.
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_none_match: Some(format!("\"{etag}\"").parse().unwrap()),
                if_unmodified_since: Some(Timestamp::from(
                    OffsetDateTime::from_unix_timestamp(915_148_800).unwrap(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
    }

    #[tokio::test]
    async fn conditional_put_failures_are_412() {
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        let put = |if_match: Option<dto::IfMatch>, if_none_match: Option<dto::IfNoneMatch>| {
            backend.put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                if_match,
                if_none_match,
                ..Default::default()
            }))
        };
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();

        // If-None-Match matching → 412 (never 304 on the write path).
        let err = put(None, Some(format!("\"{etag}\"").parse().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // If-None-Match: * against an existing object → 412.
        let err = put(None, Some("*".parse().unwrap())).await.unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // If-Match mismatching → 412.
        let err = put(
            Some("\"deadbeefdeadbeefdeadbeefdeadbeef\"".parse().unwrap()),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // If-Match matching → 200.
        put(Some(format!("\"{etag}\"").parse().unwrap()), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_objects_batch() {
        let (backend, b) = setup_name().await;
        let storage = backend.storage();
        storage
            .put_object(&b, &"a.txt".into(), body(b"a"))
            .await
            .unwrap();
        storage
            .put_object(&b, &"b.txt".into(), body(b"b"))
            .await
            .unwrap();
        let out = backend
            .delete_objects(s3_request(dto::DeleteObjectsInput {
                bucket: b.to_string(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: dto::Delete {
                    objects: vec![
                        dto::ObjectIdentifier {
                            key: "a.txt".into(),
                            ..Default::default()
                        },
                        dto::ObjectIdentifier {
                            key: "missing.txt".into(),
                            ..Default::default()
                        },
                        dto::ObjectIdentifier {
                            key: "../evil".into(),
                            ..Default::default()
                        },
                    ],
                    quiet: None,
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            }))
            .await
            .unwrap();
        // Two deleted (incl. the missing one — idempotent), one invalid key.
        assert_eq!(out.output.deleted.as_ref().unwrap().len(), 2);
        let errors = out.output.errors.as_ref().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].key.as_deref(), Some("../evil"));
        // The surviving object is untouched.
        assert_eq!(
            read_body(
                storage
                    .get_object(&b, &"b.txt".into(), None)
                    .await
                    .unwrap()
                    .body
            )
            .await
            .unwrap(),
            b"b"
        );
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn copy_object_server_side() {
        let (backend, b) = setup_name().await;
        let storage = backend.storage();
        storage
            .put_object(&b, &"src.txt".into(), body(b"source data"))
            .await
            .unwrap();
        let out = backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap();
        let result = out.output.copy_object_result.unwrap();
        assert_eq!(
            result.e_tag.unwrap().as_strong().unwrap(),
            "d22c0597587f7fd97ac77ee2fdba689d"
        );
        let got = read_body(
            storage
                .get_object(&b, &"dst.txt".into(), None)
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        assert_eq!(got, b"source data");
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn copy_object_source_condition_failures_are_412() {
        let (backend, b) = setup_name().await;
        let etag = "d22c0597587f7fd97ac77ee2fdba689d";
        backend
            .storage()
            .put_object(&b, &"src.txt".into(), body(b"source data"))
            .await
            .unwrap();

        // x-amz-copy-source-if-none-match matching the source → 412.
        let err = backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                    .copy_source_if_none_match(Some(format!("\"{etag}\"").parse().unwrap()))
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // x-amz-copy-source-if-match mismatching → 412.
        let err = backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                    .copy_source_if_match(Some(
                        "\"deadbeefdeadbeefdeadbeefdeadbeef\"".parse().unwrap(),
                    ))
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn copy_object_destination_conditionals_are_enforced() {
        // x-amz-if-match / x-amz-if-none-match evaluate against the
        // CURRENT destination (s3s omits them from the DTO — the headers
        // are read and enforced here; a conditional copy must not
        // silently overwrite).
        let (backend, b) = setup_name().await;
        let storage = backend.storage();
        storage
            .put_object(&b, &"src.txt".into(), body(b"source data"))
            .await
            .unwrap();
        storage
            .put_object(&b, &"dst.txt".into(), body(b"existing"))
            .await
            .unwrap();
        let dst_etag = storage
            .head_object(&b, &"dst.txt".into())
            .await
            .unwrap()
            .etag
            .as_str();

        // x-amz-if-none-match matching the destination → 412, no write.
        let mut req = s3_request(
            CopyObjectInput::builder()
                .bucket(b.to_string())
                .key("dst.txt".to_string())
                .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                .build()
                .unwrap(),
        );
        req.headers.insert(
            "x-amz-if-none-match",
            HeaderValue::from_str(&format!("\"{dst_etag}\"")).unwrap(),
        );
        let err = backend.copy_object(req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
        let got = read_body(
            storage
                .get_object(&b, &"dst.txt".into(), None)
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        assert_eq!(
            got, b"existing",
            "the failed conditional copy must not overwrite"
        );

        // x-amz-if-match with a mismatching ETag → 412.
        let mut req = s3_request(
            CopyObjectInput::builder()
                .bucket(b.to_string())
                .key("dst.txt".to_string())
                .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                .build()
                .unwrap(),
        );
        req.headers.insert(
            "x-amz-if-match",
            HeaderValue::from_str("\"deadbeefdeadbeefdeadbeefdeadbeef\"").unwrap(),
        );
        let err = backend.copy_object(req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // x-amz-if-match matching the destination → the copy proceeds.
        let mut req = s3_request(
            CopyObjectInput::builder()
                .bucket(b.to_string())
                .key("dst.txt".to_string())
                .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                .build()
                .unwrap(),
        );
        req.headers.insert(
            "x-amz-if-match",
            HeaderValue::from_str(&format!("\"{dst_etag}\"")).unwrap(),
        );
        let out = backend.copy_object(req).await.unwrap();
        assert!(out.output.copy_object_result.is_some());
        let got = read_body(
            storage
                .get_object(&b, &"dst.txt".into(), None)
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        assert_eq!(got, b"source data");
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn copy_source_rejects_an_access_point_arn() {
        // Only the plain `<bucket>/<key>` source is supported (path-style
        // addressing, SC-002); an access-point ARN answers
        // `InvalidArgument`, never a partial parse or a wrong bucket.
        let (backend, b) = setup().await;
        let req = s3_request(
            CopyObjectInput::builder()
                .bucket(b)
                .key("dst.txt".to_string())
                .copy_source(CopySource::AccessPoint {
                    partition: "aws".into(),
                    region: "us-east-1".into(),
                    account_id: "123456789012".into(),
                    access_point_name: "ap".into(),
                    key: "src.txt".into(),
                    version_id: None,
                })
                .build()
                .unwrap(),
        );
        let err = backend.copy_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument, "{err:?}");
        assert!(
            err.message().unwrap().contains("unsupported copy source"),
            "{err:?}"
        );
    }
}
