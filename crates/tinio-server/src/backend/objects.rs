//! Object operations of the S3 mapping layer (task T048).
//!
//! PutObject/GetObject/HeadObject/GetObjectAttributes/DeleteObject/
//! DeleteObjects/CopyObject/RenameObject/Get|Put|DeleteObjectTagging
//! over the storage contract.
//! Range requests seek on the backend and answer `206`/`Content-Range`;
//! conditional headers are evaluated against the meta-store ETag and
//! filesystem mtime (304/412 per RFC 7232). Content-Type is inferred
//! via `mime_guess` (FR-022); `x-amz-meta-*` is accepted and dropped,
//! while `x-amz-checksum-*` and `x-amz-tagging` are real under their
//! runtime toggles (the checksum tee of spec 2026-08-31, the object
//! tagging of the tagging spec — off keeps the established accept-and-
//! drop). The recorded object checksum is echoed on GET/HEAD with its
//! kind and under GetObjectAttributes' Checksum attribute;
//! `x-amz-tagging-count` rides the same responses. CopyObject and
//! RenameObject are gated by the `copy` cargo feature and the runtime
//! `copy_object` toggle (FR-021); the tagging ops gate on `caps.tagging`.

use std::sync::Arc;
/// The copy-only conditionals of [`op_copy_object`]/[`op_rename_object`]
/// (`x-amz-if-match`/`x-amz-if-none-match`, the `x-amz-rename-source-if-*`
/// String fields, and the shared 412 constructor): the destination-put
/// conditional writes read the same header family through the s3s DTO
/// instead.
#[cfg(feature = "copy")]
use std::time::SystemTime;

use http::HeaderValue;
use s3s::{
    S3Error, S3Request, S3Response, S3Result,
    dto::{self, DeleteObjectOutput},
    s3_error,
};

#[cfg(feature = "copy")]
use crate::backend::{
    conditions::{ConditionFailure, condition_error},
    parse_etag_condition_header, parse_etag_condition_value,
};
use crate::{
    _core::{
        bucket,
        checksum::Algorithm,
        object,
        storage::{Error as StorageError, GetObjectResult, Storage},
    },
    backend::{
        ConditionalHeaders, DeleteConditions, S3Backend, byte_range, check_write_shape,
        checked_if_match_size,
        checksum::{
            HasFields, Spec, VerifyState, VerifyStream, echo_recorded, echo_validated,
            map_part_error,
        },
        decide_fetch, decide_range_error, generation_changed, map_backend_error,
        normalize_page_size, parse_if_range,
        tags::{parse_tagging_header, tag_set_from_tags, tags_from_tag_set},
    },
};

/// The destination-conditional protocol (`x-amz-if-match` /
/// `x-amz-if-none-match`): evaluate against the CURRENT object at
/// (bucket, key), 412 on failure. Shared by the conditional put and the
/// conditional copy — a missing object is the "no current version" case
/// (If-None-Match: *); any real failure must not look like an absent
/// object, or the precondition would pass and overwrite. The both-present
/// 400 is NOT here — the callers run `check_write_shape` up front
/// (request-shape error, before the body is staged); this checker keeps
/// only the state-dependent part.
impl<S: Storage> S3Backend<S> {
    /// Head the object at `(bucket, key)`, mapping a missing object to
    /// `None` — the conditional paths' "no current state" case. One home
    /// for the preamble the destination, delete, and complete checks
    /// share.
    pub(crate) async fn head_optional(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> S3Result<Option<object::Info>> {
        match self.storage.head_object(bucket, key).await {
            Ok(info) => Ok(Some(info)),
            Err(err) => {
                let err: StorageError = err.into();
                match err {
                    StorageError::NoSuchKey(_) => Ok(None),
                    err => Err(map_backend_error(err)),
                }
            }
        }
    }

    async fn check_destination_conditions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        if_match: Option<&dto::ETagCondition>,
        if_none_match: Option<&dto::ETagCondition>,
    ) -> S3Result<()> {
        // The destination set is etag-only (AWS conditional writes
        // carry no date headers).
        let conditions = ConditionalHeaders::etag_only(if_match, if_none_match);
        // No conditions ⇒ the fast path (skip the head) — `absent()`
        // folds the old early return into the evaluator.
        if conditions.absent() {
            return Ok(());
        }
        if let Some(info) = self.head_optional(bucket, key).await? {
            conditions.check(&info.etag, info.last_modified, true)?;
        } else {
            conditions.check_missing()?;
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
        // Cloned: the checksum-spec parse below borrows the whole input.
        let bucket = self.bucket(req.input.bucket.clone())?;
        let key = self.key(req.input.key.clone())?;

        // The write-path shape gate (AWS conditional writes): If-Match +
        // If-None-Match together → 400, and If-None-Match accepts `*`
        // only (a specific value → 501 — shared with the copy
        // destination and the complete via [`check_write_shape`]),
        // rejected up front — before the body is staged (a rejected
        // request must not pay the body stream).
        check_write_shape(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )?;

        // Object tagging on the write (spec 2026-08-31): parse the
        // `x-amz-tagging` header into the validated set the commit
        // records — under the toggle only; off keeps the established
        // accept-and-drop, and a malformed value answers `InvalidTag`
        // before any body is staged.
        let tags = if self.caps.tagging {
            parse_tagging_header(req.input.tagging.as_ref())?.unwrap_or_default()
        } else {
            object::Tags::empty()
        };
        // The PUT checksum tee (spec 2026-08-31 — the `upload_part`
        // pattern): parse the request spec and wrap the body BEFORE any
        // storage call. Toggle off ⇒ exactly today's code path.
        let tee: Option<(Arc<VerifyState>, Spec)> = if self.caps.checksum {
            Spec::from_put_object(&req.input, &req.headers)?.map(|spec| {
                // The digest-slot promise of the tee (F05): the backend
                // skips its own MD5 hash when the tee hashes it (the MD5
                // algorithm slot or the Content-MD5 check).
                let state = Arc::new(VerifyState::new(
                    spec.algorithm == Some(Algorithm::Md5) || spec.content_md5.is_some(),
                ));
                (state, spec)
            })
        } else {
            None
        };
        let body = match &tee {
            Some((state, spec)) => VerifyStream::wrap(
                Self::stream_in(req.input.body),
                spec,
                req.trailing_headers.as_ref(),
                state,
            ),
            None => Self::stream_in(req.input.body),
        };
        // Stage the body first, outside the write lock: streaming a slow
        // client must not stall other writers. The tee validates while
        // the body streams — a mismatch fails the staging (BadDigest,
        // the multipart path's mapping) and the validated digest rides
        // into the commit below. The conditional check and the commit
        // then run under one lock (RFC 7232 exclusivity) — an
        // unconditional put landing between them would pass the
        // precondition against stale state.
        let staged = self
            .storage
            .stage_body(
                &bucket,
                &key,
                body,
                tee.as_ref().map(|(state, _)| state.slot()),
            )
            .await
            .map_err(|err| map_part_error(tee.as_ref().map(|(state, _)| state.as_ref()), err))?;
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
            .commit_object(&bucket, &key, staged, tags)
            .await
            .map_err(map_backend_error)?;
        let mut output = dto::PutObjectOutput {
            e_tag: Some(Self::etag_wire(&put.etag)),
            ..Default::default()
        };
        echo_validated(&mut output, tee.as_ref());
        Ok(S3Response::new(output))
    }

    pub(crate) async fn op_get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;

        let range = req.input.range.map(byte_range);
        let conditions = ConditionalHeaders::new(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since,
            req.input.if_unmodified_since,
        );
        // If-Range gates the Range header only (RFC 9110 §13.1.5); a
        // parse failure or a wildcard drops the header. Read-path
        // snapshot policy: ONLY a Range request carrying RFC 7232
        // conditions reads a head first — a precondition 304/412 must
        // answer BEFORE a 416. Everything else — a no-Range conditional
        // GET and a pure If-Range request included — is a single body
        // fetch whose snapshot the conditions / the If-Range validator
        // evaluate against: race-free by construction, and one storage
        // read where the old head-first code took two. A pure If-Range
        // request whose validator turns out stale against the fetched
        // snapshot discards the served range and refetches the full
        // object (below) — a stale validator pays a wasted range read,
        // no more.
        let if_range = parse_if_range(&req.headers);
        let needs_head = range.is_some() && !conditions.absent();
        let head = if needs_head {
            Some(
                self.storage
                    .head_object(&bucket, &key)
                    .await
                    .map_err(map_backend_error)?,
            )
        } else {
            None
        };
        // The RFC 7232 conditions evaluate against the head snapshot —
        // a failed precondition answers 304/412 without the body fetch
        // (a matching If-None-Match answers 304 even over an
        // unsatisfiable Range — the precondition beats the 416). The set
        // re-evaluates against the fetched snapshot when a write raced
        // between the two reads (below). The Range honored by the body
        // fetch: an If-Range mismatch drops it (the full 200 is served).
        let range = if let Some(info) = head.as_ref() {
            conditions.check(&info.etag, info.last_modified, false)?;
            match (&range, &if_range) {
                (Some(_), Some(ir)) if !ir.matches(&info.etag, info.last_modified) => None,
                _ => range,
            }
        } else {
            range
        };
        // The body fetch — the snapshot every answer is evaluated and
        // described against (a single storage read is generation-
        // consistent on both backends). A ranged fetch can fail
        // InvalidRange when the object shrank between the head and the
        // fetch: with an honored If-Range that means the validator went
        // stale mid-flight (RFC 9110 §13.1.5 — the Range must be
        // dropped, the full object served); the 416-vs-full
        // classification needs the CURRENT validator — the head-first
        // flow holds it, and a head-less pure If-Range request takes
        // one lazily, on the error path only.
        let fetched = match self.storage.get_object(&bucket, &key, range).await {
            Ok(result) => result,
            Err(err) => {
                let err: StorageError = err.into();
                // The 416-vs-full decision needs the CURRENT validator:
                // the head-first flow holds the validator the Range was
                // honored under; a head-less pure If-Range request (no
                // RFC 7232 conditions) takes one lazily — on the error
                // path only.
                let lazy_head = if matches!(err, StorageError::InvalidRange { .. })
                    && head.is_none()
                    && if_range.is_some()
                {
                    Some(
                        self.storage
                            .head_object(&bucket, &key)
                            .await
                            .map_err(map_backend_error)?,
                    )
                } else {
                    None
                };
                let head = head.as_ref().or(lazy_head.as_ref());
                match err {
                    StorageError::InvalidRange { size, .. }
                        if decide_range_error(if_range.as_ref(), head, size) =>
                    {
                        self.storage
                            .get_object(&bucket, &key, None)
                            .await
                            .map_err(map_backend_error)?
                    }
                    err => return Err(map_backend_error(err)),
                }
            }
        };
        // No head (a no-Range conditional GET, an unconditional one, or
        // a pure If-Range request): the conditions evaluate against the
        // single fetched snapshot — the 304/412 carries that snapshot's
        // own validators.
        if head.is_none() {
            conditions.check(&fetched.info.etag, fetched.info.last_modified, false)?;
        }
        // The head and the body fetch are two independent reads — no
        // lock serializes them (unlike the conditional put, whose check
        // and commit share the per-key write lock) — so a same-key
        // overwrite between the calls would otherwise wrap the new
        // object's bytes in the old snapshot's metadata. When the
        // fetched snapshot differs from the head's (ETag OR mtime — a
        // byte-identical rewrite changes only the mtime), the gates
        // re-evaluate against the fetched one — the snapshot the bytes
        // actually came from — so a response always describes one
        // object. A failed re-check answers 304/412 (the fs body was
        // never polled; the mem copy is paid only in this rare race). A
        // full object is refetched only when the fetch actually served a
        // range and the If-Range no longer matches the fetched snapshot
        // (`decide_fetch`); the refetched snapshot is itself re-evaluated
        // before serving — bounded, never a loop.
        let fetched = if let Some(head) = head.as_ref()
            && generation_changed(head, &fetched.info)
        {
            conditions.check(&fetched.info.etag, fetched.info.last_modified, false)?;
            if decide_fetch(head, &fetched.info, if_range.as_ref(), fetched.served_range) {
                let result = self
                    .storage
                    .get_object(&bucket, &key, None)
                    .await
                    .map_err(map_backend_error)?;
                conditions.check(&result.info.etag, result.info.last_modified, false)?;
                result
            } else {
                fetched
            }
        } else if head.is_none()
            && let Some(ir) = if_range.as_ref()
            && fetched.served_range.is_some()
            && !ir.matches(&fetched.info.etag, fetched.info.last_modified)
        {
            // A head-less pure If-Range request (no RFC 7232
            // conditions): the Range was fetched blind — a validator
            // that fails the fetched snapshot is stale, so the Range is
            // ignored and the full object served (RFC 9110 §13.1.5).
            // Nothing to re-evaluate: this request carries no
            // conditions.
            self.storage
                .get_object(&bucket, &key, None)
                .await
                .map_err(map_backend_error)?
        } else {
            fetched
        };
        let GetObjectResult {
            body,
            served_range,
            info,
        } = fetched;

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
        let mut output = dto::GetObjectOutput {
            accept_ranges: Some("bytes".into()),
            body: Some(Self::stream_out(body)),
            content_length: Some(content_length as i64),
            content_range,
            content_type,
            e_tag: Some(Self::etag_wire(&info.etag)),
            last_modified: Some(Self::last_modified(info.last_modified)),
            ..Default::default()
        };
        // The recorded checksum echo (spec 2026-08-31 — grilling Q7),
        // whenever a checksum is recorded — unconditional, no
        // `x-amz-checksum-mode` gating (nothing is recorded while the
        // `checksum` toggle is off). A partial (206, `content_range`)
        // response carries no checksum headers: the value is the WHOLE
        // object's, and clients (aws cli crc64nvme) verify each ranged
        // part against it and fail the download (interop 33756495359).
        if output.content_range.is_none()
            && let Some(recorded) = &info.checksum
        {
            echo_recorded(&mut output, recorded);
        }
        // `x-amz-tagging-count` (dto field; AWS): present only when the
        // object carries tags, and only while the tagging toggle is on
        // (off = the tags are not served; the count header is a tagging
        // surface).
        if self.caps.tagging && !info.tags.is_empty() {
            output.tag_count = Some(info.tags.len() as i32);
        }
        Ok(S3Response::new(output))
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
        let mut output = dto::HeadObjectOutput {
            accept_ranges: Some("bytes".into()),
            content_length: Some(head.size as i64),
            content_type: Some(Self::content_type(head.key.as_ref())),
            e_tag: Some(Self::etag_wire(&head.etag)),
            last_modified: Some(Self::last_modified(head.last_modified)),
            ..Default::default()
        };
        // The recorded checksum echo (spec 2026-08-31 — grilling Q7):
        // the same per-algorithm field + `x-amz-checksum-type` as GET.
        if let Some(recorded) = &head.checksum {
            echo_recorded(&mut output, recorded);
        }
        let mut resp = S3Response::new(output);
        // `x-amz-tagging-count`: HeadObjectOutput has no dto field (the
        // GET shape does) — the header is hand-set on the response,
        // present only when the object carries tags and the tagging
        // toggle is on (AWS omits it for a tag-less object).
        if self.caps.tagging && !head.tags.is_empty() {
            // The count is a decimal digit string — the parse cannot
            // fail (the header value rules accept it).
            resp.headers.insert(
                "x-amz-tagging-count",
                HeaderValue::from_str(&head.tags.len().to_string()).unwrap(),
            );
        }
        Ok(resp)
    }

    /// GetObjectAttributes — the requested subset of ETag / ObjectSize
    /// / StorageClass / Checksum / ObjectParts of one object. The head
    /// answers NoSuchKey for a missing object first; the Checksum
    /// attribute echoes the RECORDED object checksum (`Info.checksum`,
    /// recorded at write time under the checksum toggle) and
    /// ObjectParts the retained part list of the object's last
    /// multipart completion, paginated at the interface layer per
    /// `max_parts` (default 1000) and `part_number_marker`
    /// (exclusive — grilling Q2a); a non-multipart object omits the
    /// ObjectParts container (AWS). No capability gate (plan): nothing
    /// is recorded while the checksum toggle is off, so an object
    /// written under it has no checksum or part checksums to echo.
    /// StorageClass is always STANDARD (tinio has no tiers).
    pub(crate) async fn op_get_object_attributes(
        &self,
        req: S3Request<dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let info = self
            .storage
            .head_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        // The requested subset. Each `x-amz-object-attributes` entry
        // names one attribute, but s3s keeps a comma-joined header
        // value as ONE entry — the wire form of the AWS SDKs — so the
        // matcher splits every entry on commas.
        let want = |attr: &str| {
            req.input
                .object_attributes
                .iter()
                .any(|x| x.as_str().split(',').any(|token| token.trim() == attr))
        };
        let mut out = dto::GetObjectAttributesOutput {
            e_tag: want("ETag").then(|| Self::etag_wire(&info.etag)),
            object_size: want("ObjectSize").then_some(info.size as i64),
            ..Default::default()
        };
        if want("StorageClass") {
            out.storage_class = Some(dto::StorageClass::from_static(dto::StorageClass::STANDARD));
        }
        // The recorded-checksum echo (spec 2026-08-31), in the
        // attribute container: the matching per-algorithm field plus
        // `checksum_type` from the recorded kind — the GET/HEAD echo
        // rule, unconditional (nothing is recorded while the toggle is
        // off).
        if want("Checksum")
            && let Some(recorded) = info.checksum.as_ref()
        {
            let mut fields = dto::Checksum::default();
            echo_recorded(&mut fields, recorded);
            out.checksum = Some(fields);
        }
        if want("ObjectParts") {
            let parts = self
                .storage
                .list_object_parts(&bucket, &key)
                .await
                .map_err(map_backend_error)?;
            if !parts.is_empty() {
                // Interface-level pagination (grilling Q2a): slice the
                // retained list after the marker, at most max_parts —
                // the multipart listings' page-size policy (default
                // 1000; <1 rejected, unless the `allow_zero_page_size`
                // escape hatch restores the legacy empty page — never
                // truncated).
                let marker = req
                    .input
                    .part_number_marker
                    .map(|n| {
                        u32::try_from(n).map_err(|_| {
                            s3_error!(InvalidArgument, "invalid part-number-marker: {n}")
                        })
                    })
                    .transpose()?;
                let max = normalize_page_size(
                    req.input.max_parts.unwrap_or(1000),
                    "max-parts",
                    self.caps.allow_zero_page_size,
                )?;
                let after: Vec<_> = parts
                    .iter()
                    .filter(|p| marker.is_none_or(|m| u32::from(p.part_number) > m))
                    .collect();
                let page: Vec<_> = after.iter().take(max).collect();
                let is_truncated = max > 0 && after.len() > page.len();
                out.object_parts = Some(dto::GetObjectAttributesParts {
                    is_truncated: Some(is_truncated),
                    max_parts: Some(max as i32),
                    next_part_number_marker: is_truncated
                        .then(|| page.last().map(|p| u32::from(p.part_number) as i32))
                        .flatten(),
                    part_number_marker: req.input.part_number_marker,
                    parts: Some(
                        page.iter()
                            .map(|p| {
                                let mut part = dto::ObjectPart {
                                    part_number: Some(u32::from(p.part_number) as i32),
                                    size: Some(p.size as i64),
                                    ..Default::default()
                                };
                                // The stored part checksum (spec
                                // 2026-08-31) — recorded at write time,
                                // echoed like the object checksum.
                                if let Some(c) = p.checksum.as_ref() {
                                    part.set_checksum(c.algorithm, c.value.as_str());
                                }
                                part
                            })
                            .collect(),
                    ),
                    total_parts_count: Some(parts.len() as i32),
                });
            }
        }
        Ok(S3Response::new(out))
    }

    pub(crate) async fn op_delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        // The malformed-size rejection is request-shape (a negative
        // value can never match any object) — validated up front,
        // state-independently, like the both-present 400. The validated
        // size then compares in the unsigned domain.
        let conditions = DeleteConditions::new(
            req.input.if_match.as_ref(),
            req.input.if_match_last_modified_time,
            req.input
                .if_match_size
                .map(checked_if_match_size)
                .transpose()?,
        );
        // Serialize with the write lock: a delete landing between a
        // conditional put's check and commit must not erase the state
        // the precondition was evaluated against.
        let _guard = self.lock_object(&bucket, &key).await;
        // Conditional delete: ETag / last-modified-time / size — every
        // provided header must match the object AT the key, else 412.
        // The head-check + delete run in the per-key critical section
        // above. A missing object answers 204 under every conditional
        // header (AWS model text: "if the ETag matches or if the object
        // doesn't exist, the operation will return a 204") — delete is
        // idempotent and the conditions gate an existing object only
        // (the module doc in conditions.rs states the policy centrally).
        if !conditions.absent()
            && let Some(info) = self.head_optional(&bucket, &key).await?
        {
            conditions.check(&info)?;
        }
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

    /// GetObjectTagging — the object's real tag set (spec 2026-08-31).
    /// The existence head answers 404 `NoSuchKey` for a missing object
    /// and doubles as the tag source (`Info.tags` is always populated —
    /// no dedicated tag fetch on the read path); a tag-less object
    /// answers the empty set (aws cli invokes this before server-side
    /// copies).
    pub(crate) async fn op_get_object_tagging(
        &self,
        req: S3Request<dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::GetObjectTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "GetObjectTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let head = self
            .storage
            .head_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::GetObjectTaggingOutput {
            tag_set: tag_set_from_tags(&head.tags),
            version_id: None,
        }))
    }

    /// PutObjectTagging — replace the object's tag set (replace-all, no
    /// merge). The dto `TagSet` is validated through the core type
    /// (duplicate keys, the ≤10 object cap → `InvalidTag` 400) before
    /// the contract call; a missing object answers 404 `NoSuchKey`. No
    /// per-key lock (spec decision — AWS gives no atomicity guarantee
    /// between tag and object writes; last-writer-wins is accepted).
    pub(crate) async fn op_put_object_tagging(
        &self,
        req: S3Request<dto::PutObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::PutObjectTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "PutObjectTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let tags = tags_from_tag_set(&req.input.tagging.tag_set, object::OBJECT_TAGS_MAX)?;
        self.storage
            .put_object_tags(&bucket, &key, &tags)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutObjectTaggingOutput::default()))
    }

    /// DeleteObjectTagging — remove the object's tag set. Idempotent,
    /// like the object delete: a missing object answers 204. No per-key
    /// lock (spec decision).
    pub(crate) async fn op_delete_object_tagging(
        &self,
        req: S3Request<dto::DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "DeleteObjectTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        self.storage
            .delete_object_tags(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::DeleteObjectTaggingOutput::default()))
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

        // The destination conditionals (`x-amz-if-match` /
        // `x-amz-if-none-match`) are parsed FIRST — a pure header parse,
        // no I/O — so the destination write-shape gate (both headers →
        // 400, a specific `If-None-Match` → 501; AWS conditional writes)
        // is rejected before the source head and the destination lock.
        // The copy-source family (`copy_source_if_*`) is NOT subject to
        // the gate — it keeps the RFC 9110 §13.2.2 evaluation order
        // below.
        let dest_if_match = parse_etag_condition_header(&req.headers, "x-amz-if-match")?;
        let dest_if_none_match = parse_etag_condition_header(&req.headers, "x-amz-if-none-match")?;
        check_write_shape(dest_if_match.as_ref(), dest_if_none_match.as_ref())?;

        // `x-amz-tagging-directive` (spec 2026-08-31): COPY and REPLACE
        // are all the wire defines — any other value is a request-shape
        // error (InvalidArgument), rejected before the source head like
        // the write-shape gate (under the toggle only; off keeps the
        // accept-and-drop). REPLACE parses the request's `x-amz-tagging`
        // into the destination's tag set — a malformed value is a
        // request-shape error (InvalidTag) — while COPY (the default)
        // resolves the source's tags after the head below. Under the
        // tagging toggle off the write headers are accept-and-drop: the
        // destination records nothing either way.
        let replace = self.caps.tagging
            && match req.input.tagging_directive.as_ref() {
                Some(d) => match d.as_str() {
                    "COPY" => false,
                    "REPLACE" => true,
                    other => {
                        return Err(s3_error!(
                            InvalidArgument,
                            "invalid x-amz-tagging-directive: {other}"
                        ));
                    }
                },
                None => false,
            };
        let replace_tags = if replace {
            parse_tagging_header(req.input.tagging.as_ref())?
        } else {
            None
        };

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
        // The destination's tag set (spec 2026-08-31): REPLACE uses the
        // `x-amz-tagging` parse result — empty when the header was
        // absent; COPY — the default — carries the source's tags, so
        // the copy never invents tags of its own. The source's recorded
        // checksum is carried regardless of the directive — the copy's
        // bytes are the source's (AWS documents the automatic carry) —
        // gated on the checksum toggle like every recording (off keeps
        // today's accept-and-drop); a client-requested algorithm header
        // stays accept-and-drop (known deviation: AWS recomputes).
        let tags = if !self.caps.tagging {
            object::Tags::empty()
        } else if replace {
            replace_tags.unwrap_or_default()
        } else {
            info.tags.clone()
        };
        let checksum = if self.caps.checksum {
            info.checksum.clone()
        } else {
            None
        };
        // The destination write serializes with the write lock, as in
        // `op_put_object` — a copy landing between a conditional put's
        // check and commit would invalidate the precondition.
        let _guard = self.lock_object(&dst_bucket, &dst_key).await;
        // Destination conditionals (`x-amz-if-match` / `x-amz-if-none-match`)
        // evaluate against the CURRENT destination (412 on failure): a
        // conditional copy must not silently overwrite — the shared
        // destination protocol.
        self.check_destination_conditions(
            &dst_bucket,
            &dst_key,
            dest_if_match.as_ref(),
            dest_if_none_match.as_ref(),
        )
        .await?;
        let put = self
            .storage
            .copy_object(&src_bucket, &src_key, &dst_bucket, &dst_key, tags, checksum)
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

    /// RenameObject — atomically move the source onto the destination
    /// (the AWS directory-bucket op; tinio honors it on the
    /// general-purpose model) with source and destination conditionals.
    /// The s3s model: the URL `key` is the DESTINATION and
    /// `rename_source` (`x-amz-rename-source`) the source; the source
    /// conditions are String-typed (`source_if_*`, parsed with ETag
    /// semantics here) and the destination conditions the plain `If-*`
    /// newtypes — on the wire the plain `If-Match`/`If-None-Match`/
    /// date headers, no `x-amz-rename-object-destination-*`. Dual
    /// per-key locks acquired in sorted order — two concurrent renames
    /// can never deadlock; the source head, the destination head and
    /// the move run inside the critical sections. An existing
    /// destination with no destination conditions is overwritten;
    /// source == destination → 412; a missing source → NoSuchKey
    /// (tinio's choice — AWS silent), and the missing check precedes
    /// the degenerate-key 412 — renaming an absent key onto itself
    /// answers NoSuchKey. Destination conditions follow the
    /// shared missing-object policy: `If-None-Match` passes on an
    /// absent destination (the rename proceeds), `If-Match` fails with
    /// 412.
    #[cfg(feature = "copy")]
    pub(crate) async fn op_rename_object(
        &self,
        req: S3Request<dto::RenameObjectInput>,
    ) -> S3Result<S3Response<dto::RenameObjectOutput>> {
        Self::require_cap(self.caps.copy_object, "RenameObject")?;
        let bucket = self.bucket(req.input.bucket)?;
        let dst = self.key(req.input.key)?;
        let src = self.key(rename_source_key(&req.input.rename_source).to_string())?;
        // The source conditions parse FIRST — a pure parse, no I/O —
        // so a malformed value (400, the `parse_etag_condition_header`
        // rule) is rejected before the degenerate-key check, the
        // locks, and the heads. The source pair keeps the RFC 9110
        // evaluation — the copy-source family is exempt from the
        // destination write-shape gate, and the source If-None-Match
        // wildcard matches any existing source (412).
        let source_if_match = parse_etag_condition_value(
            req.input.source_if_match.as_deref(),
            "x-amz-rename-source-if-match",
        )?;
        let source_if_none_match = parse_etag_condition_value(
            req.input.source_if_none_match.as_deref(),
            "x-amz-rename-source-if-none-match",
        )?;
        // The destination pair is a destination write — the shared
        // write-shape gate (both headers → 400, a specific
        // If-None-Match → 501; AWS conditional writes), rejected
        // before the locks like the put and the copy.
        check_write_shape(
            req.input.destination_if_match.as_ref(),
            req.input.destination_if_none_match.as_ref(),
        )?;
        // Source == destination is degenerate — 412 (grilling Q8c),
        // built with the same 412 constructor the conditional paths
        // use (`condition_error`) — one error shape. The source head
        // runs first: a missing key answers NoSuchKey (the copy-source
        // rule — existence beats the degenerate-key 412), under the
        // single key's lock (the sorted pair below would lock it twice
        // and deadlock).
        if src == dst {
            let _guard = self.lock_object(&bucket, &src).await;
            self.storage
                .head_object(&bucket, &src)
                .await
                .map_err(map_backend_error)?;
            return Err(condition_error(ConditionFailure::Match, true));
        }
        // Sorted lock acquisition (the per-key lock keys, one bucket) —
        // deadlock-free.
        let (first, second) = if src.as_ref() <= dst.as_ref() {
            (src.clone(), dst.clone())
        } else {
            (dst.clone(), src.clone())
        };
        let _guard_a = self.lock_object(&bucket, &first).await;
        let _guard_b = self.lock_object(&bucket, &second).await;
        // Source conditions evaluate against the source head — a
        // missing source answers NoSuchKey (tinio's choice). No
        // conditions ⇒ the head is skipped, the destination fast
        // path's mirror: the backend rename answers NoSuchKey for an
        // absent source, under the same locks.
        let src_conditions = ConditionalHeaders::new(
            source_if_match.as_ref(),
            source_if_none_match.as_ref(),
            req.input.source_if_modified_since,
            req.input.source_if_unmodified_since,
        );
        if !src_conditions.absent() {
            let src_info = self
                .storage
                .head_object(&bucket, &src)
                .await
                .map_err(map_backend_error)?;
            src_conditions.check(&src_info.etag, src_info.last_modified, true)?;
        }
        // Destination conditions evaluate against the CURRENT
        // destination (dates included — the dto models them): no
        // conditions ⇒ the fast path (skip the head — the existing
        // destination is overwritten by the move below), an existing
        // destination evaluates the full RFC 9110 set (412 on
        // failure), and an absent one runs the shared missing-object
        // policy (`check_missing` — only If-Match fails against
        // nothing; If-None-Match passes and the rename proceeds).
        let dst_conditions = ConditionalHeaders::new(
            req.input.destination_if_match.as_ref(),
            req.input.destination_if_none_match.as_ref(),
            req.input.destination_if_modified_since,
            req.input.destination_if_unmodified_since,
        );
        if !dst_conditions.absent() {
            match self.head_optional(&bucket, &dst).await? {
                Some(info) => dst_conditions.check(&info.etag, info.last_modified, true)?,
                None => dst_conditions.check_missing()?,
            }
        }
        let info = self
            .storage
            .rename_object(&bucket, &src, &dst)
            .await
            .map_err(map_backend_error)?;
        // The moved object's ETag echo: the s3s 0.15 `RenameObjectOutput`
        // is an empty struct — the echo rides the response header.
        let mut resp = S3Response::new(dto::RenameObjectOutput::default());
        resp.headers.insert(
            http::header::ETAG,
            Self::etag_wire(&info.etag)
                .to_http_header()
                .map_err(|_| s3_error!(InternalError, "invalid ETag header"))?,
        );
        Ok(resp)
    }
}

/// The RenameObject source key from the `x-amz-rename-source` header:
/// the s3s model keeps the header as a raw String (never parsed), and
/// the value is the source object key of the request's own bucket —
/// optionally with the leading slash of the AWS-documented form
/// (`/src.txt`); a bucket prefix is never part of the value. The key
/// validation below rejects what remains invalid.
#[cfg(feature = "copy")]
fn rename_source_key(raw: &str) -> &str {
    raw.strip_prefix('/').unwrap_or(raw)
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use http::HeaderValue;
    #[cfg(feature = "multipart")]
    use s3s::checksum::ChecksumHasher;
    use s3s::{
        S3,
        dto::{Range, StreamingBlob, Timestamp},
    };
    #[cfg(feature = "copy")]
    use s3s::{
        S3ErrorCode,
        dto::{CopyObjectInput, CopySource},
    };
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        _core::{bucket, checksum, storage::ObjectOps},
        _mem::MemoryStorage,
        _util::testing::{body, read_body},
        backend::{
            Capabilities,
            testutil::{s3_request, setup, setup_with_caps},
        },
    };

    async fn setup_name() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let (backend, b) = setup().await;
        (backend, bucket::name(b.as_str()).unwrap())
    }

    /// A backend with the checksum feature on (the default toggle is
    /// off — the tests must opt in).
    async fn setup_checksum() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let (backend, b) = setup_with_caps(Capabilities {
            checksum: true,
            ..Default::default()
        })
        .await;
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

        // A specific If-None-Match value is not implemented on the write
        // path (AWS) → 501, never a live evaluation — the shared
        // destination shape gate, same as the complete.
        let err = put(None, Some(format!("\"{etag}\"").parse().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");

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

        // If-Match on a MISSING key → 412 (create-if-absent cannot
        // pass an If-Match — the destination 412 that differs from the
        // complete's 404 NoSuchKey and the delete's 204).
        let err = backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "absent.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                if_match: Some("*".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // The shape gate fires on a MISSING key too — before the body is
        // staged: a specific If-None-Match answers 501, never a state
        // check.
        let err = backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "absent.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                if_none_match: Some("\"abc\"".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }

    #[tokio::test]
    async fn destination_conditions_with_both_etag_headers_is_400() {
        let (backend, b) = setup_name().await;
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();
        // AWS conditional writes reject If-Match + If-None-Match together.
        let err = backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                if_match: Some("*".parse().unwrap()),
                if_none_match: Some("*".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    #[tokio::test]
    async fn conditional_delete_enforces_the_trio() {
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();

        // Matching conditions delete (204).
        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some(format!("\"{etag}\"").parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();

        // The object is gone: the conditional delete of a missing key
        // answers 204 under EVERY conditional header (AWS model text:
        // "if the ETag matches or if the object doesn't exist, the
        // operation will return a 204") — including If-Match and the
        // date/size conditions alike.
        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some(format!("\"{etag}\"").parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match_last_modified_time: Some(Timestamp::from(
                    OffsetDateTime::from_unix_timestamp(0).unwrap(),
                )),
                if_match_size: Some(0),
                ..Default::default()
            }))
            .await
            .unwrap();

        // A negative size is malformed, not a precondition failure — and
        // the rejection is request-shape, so it answers 400 on a MISSING
        // key too (never a state-dependent 204).
        let err = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match_size: Some(-1),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");

        // A mismatching size on an existing object → 412.
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();
        let err = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match_size: Some(999),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // A mismatching last-modified-time on an existing object → 412.
        let err = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match_last_modified_time: Some(Timestamp::from(
                    OffsetDateTime::from_unix_timestamp(0).unwrap(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
    }

    #[tokio::test]
    async fn get_object_if_range_gates_the_range() {
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();
        // Matching validator → the Range is honored (206).
        let mut req = s3_request(dto::GetObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            range: Some(Range::Int {
                first: 1,
                last: Some(3),
            }),
            ..Default::default()
        });
        req.headers.insert(
            "if-range",
            HeaderValue::from_str(&format!("\"{etag}\"")).unwrap(),
        );
        let got = backend.get_object(req).await.unwrap();
        assert_eq!(got.output.content_range.as_deref(), Some("bytes 1-3/5"));
        let mut body = got.output.body.unwrap();
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(buf, b"ell");
        // Stale validator → the Range is ignored (full 200).
        let mut req = s3_request(dto::GetObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            range: Some(Range::Int {
                first: 1,
                last: Some(3),
            }),
            ..Default::default()
        });
        req.headers.insert(
            "if-range",
            HeaderValue::from_str("\"deadbeefdeadbeefdeadbeefdeadbeef\"").unwrap(),
        );
        let got = backend.get_object(req).await.unwrap();
        assert_eq!(got.output.content_range, None);
        let mut body = got.output.body.unwrap();
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(buf, b"hello");
    }

    #[tokio::test]
    async fn get_object_conditional_ordering_keeps_outcomes() {
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        backend
            .storage()
            .put_object(&b, &"hello.txt".into(), body(b"hello"))
            .await
            .unwrap();
        // A failing If-None-Match still answers 304 (no body)…
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
        // …and a passing no-Range conditional one (a single fetch + post
        // check, 2026-09-02 #2) still answers 200 with the full body…
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "hello.txt".into(),
                if_match: Some(format!("\"{etag}\"").parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let mut body = got.output.body.unwrap();
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(buf, b"hello");
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

        // x-amz-if-none-match: * against an existing destination → 412,
        // no write.
        let mut req = s3_request(
            CopyObjectInput::builder()
                .bucket(b.to_string())
                .key("dst.txt".to_string())
                .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                .build()
                .unwrap(),
        );
        req.headers
            .insert("x-amz-if-none-match", HeaderValue::from_static("*"));
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

        // A specific x-amz-if-none-match value is not implemented on the
        // destination write path (501 — the shared destination shape
        // gate; AWS does not evaluate specific If-None-Match values on
        // writes, and a non-matching value must never fall through to a
        // silent overwrite).
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
            HeaderValue::from_str("\"deadbeefdeadbeefdeadbeefdeadbeef\"").unwrap(),
        );
        let err = backend.copy_object(req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");

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

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn rename_object_moves_with_conditions() {
        // The s3s model: the URL `key` is the DESTINATION and
        // `rename_source` (the `x-amz-rename-source` header) the source;
        // `source_if_*` are String-typed in the dto (parsed with ETag
        // semantics here), `destination_if_*` the real `If-*` newtypes.
        let (backend, b) = setup_name().await;
        let etag = "5d41402abc4b2a76b9719d911017c592";
        backend
            .storage()
            .put_object(&b, &"src.txt".into(), body(b"hello"))
            .await
            .unwrap();

        // Source mismatch → 412; the object stays put.
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "dst.txt".into(),
                rename_source: "src.txt".into(),
                source_if_match: Some(r#""deadbeef""#.into()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
        assert!(
            backend
                .storage()
                .head_object(&b, &object::key("src.txt").unwrap())
                .await
                .is_ok()
        );
        assert!(matches!(
            backend
                .storage()
                .head_object(&b, &object::key("dst.txt").unwrap())
                .await
                .unwrap_err(),
            _mem::Error::Storage(StorageError::NoSuchKey(_))
        ));

        // A matching source condition moves the object; the response
        // echoes the moved object's ETag (the dto output is empty — the
        // echo rides the `etag` response header).
        let out = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "dst.txt".into(),
                rename_source: "src.txt".into(),
                source_if_match: Some(format!("\"{etag}\"")),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            out.headers.get("etag").unwrap().to_str().unwrap(),
            format!("\"{etag}\"")
        );
        assert!(
            backend
                .storage()
                .head_object(&b, &object::key("dst.txt").unwrap())
                .await
                .is_ok()
        );
        assert!(matches!(
            backend
                .storage()
                .head_object(&b, &object::key("src.txt").unwrap())
                .await
                .unwrap_err(),
            _mem::Error::Storage(StorageError::NoSuchKey(_))
        ));

        // A missing source answers NoSuchKey (404) — tinio's choice.
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "x.txt".into(),
                rename_source: "absent.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");

        // A malformed source condition is a request-shape error (400).
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "x.txt".into(),
                rename_source: "dst.txt".into(),
                source_if_match: Some(r#""unclosed"#.into()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");

        // Destination If-None-Match: * fails against an existing
        // destination.
        backend
            .storage()
            .put_object(&b, &"other.txt".into(), body(b"x"))
            .await
            .unwrap();
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "other.txt".into(),
                rename_source: "dst.txt".into(),
                destination_if_none_match: Some("*".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
        assert_eq!(
            read_body(
                backend
                    .storage()
                    .get_object(&b, &"other.txt".into(), None)
                    .await
                    .unwrap()
                    .body,
            )
            .await
            .unwrap(),
            b"x",
            "the failed conditional rename must not overwrite"
        );

        // If-Match on a MISSING destination → 412 (the shared
        // missing-object policy — create-if-absent).
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "fresh.txt".into(),
                rename_source: "dst.txt".into(),
                destination_if_match: Some("*".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // Destination If-None-Match: * on a missing destination →
        // the rename proceeds.
        backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "fresh.txt".into(),
                rename_source: "dst.txt".into(),
                destination_if_none_match: Some("*".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(matches!(
            backend
                .storage()
                .head_object(&b, &object::key("dst.txt").unwrap())
                .await
                .unwrap_err(),
            _mem::Error::Storage(StorageError::NoSuchKey(_))
        ));

        // An existing destination with NO destination conditions is
        // overwritten.
        backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "other.txt".into(),
                rename_source: "fresh.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            read_body(
                backend
                    .storage()
                    .get_object(&b, &"other.txt".into(), None)
                    .await
                    .unwrap()
                    .body,
            )
            .await
            .unwrap(),
            b"hello",
            "the plain rename overwrites the existing destination"
        );

        // Source == destination → 412.
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "other.txt".into(),
                rename_source: "other.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // A MISSING source renamed onto itself answers NoSuchKey — the
        // missing check precedes the degenerate-key 412.
        let err = backend
            .rename_object(s3_request(dto::RenameObjectInput {
                bucket: b.to_string(),
                key: "never.txt".into(),
                rename_source: "never.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
    }

    #[tokio::test]
    async fn object_tagging_ops_round_trip() {
        let (backend, b) = setup_name().await;
        backend
            .storage()
            .put_object(&b, &"t.txt".into(), body(b"x"))
            .await
            .unwrap();

        // Put → Get round-trip.
        let tags = vec![dto::Tag {
            key: Some("env".into()),
            value: Some("prod".into()),
        }];
        backend
            .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                tagging: dto::Tagging {
                    tag_set: tags.clone(),
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                request_payer: None,
                version_id: None,
            }))
            .await
            .unwrap();
        let got = backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(got.output.tag_set, tags);

        // Replace-all semantics.
        let other = vec![dto::Tag {
            key: Some("a".into()),
            value: Some("1".into()),
        }];
        backend
            .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                tagging: dto::Tagging {
                    tag_set: other.clone(),
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                request_payer: None,
                version_id: None,
            }))
            .await
            .unwrap();
        let got = backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(got.output.tag_set, other);

        // Delete clears; a missing object answers 404 on get/put and
        // 204 on delete.
        backend
            .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let got = backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(got.output.tag_set.is_empty());
        let err = backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: "missing.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
        backend
            .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                bucket: b.to_string(),
                key: "missing.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_object_tagging_validation_rejects_bad_sets() {
        let (backend, b) = setup_name().await;
        backend
            .storage()
            .put_object(&b, &"t.txt".into(), body(b"x"))
            .await
            .unwrap();
        // Duplicate keys → InvalidTag.
        let err = backend
            .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                tagging: dto::Tagging {
                    tag_set: vec![
                        dto::Tag {
                            key: Some("k".into()),
                            value: Some("1".into()),
                        },
                        dto::Tag {
                            key: Some("k".into()),
                            value: Some("2".into()),
                        },
                    ],
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                request_payer: None,
                version_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidTag");
    }

    #[tokio::test]
    async fn put_object_validates_and_records_checksums() {
        // The checksum cap defaults OFF — mirror the multipart checksum
        // tests' fixture (`setup_checksum()`: a caps with `checksum: true`).
        let (backend, b) = setup_checksum().await;
        let put = |crc32: Option<String>| {
            backend.put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "c.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                checksum_crc32: crc32,
                ..Default::default()
            }))
        };
        // A mismatching client checksum fails the put (BadDigest) — the
        // checksum cap is enabled in the fixture (it defaults OFF). (dto
        // values are the base64 wire form; "AAAAAAAAAAA=" is base64 of
        // four zero bytes.)
        let err = put(Some("AAAAAAAAAAA=".into())).await.unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
        // A matching one succeeds and is recorded in the object metadata.
        put(Some("NhCmhg==".into())).await.unwrap(); // crc32("hello"), base64 wire form
        let info = backend
            .storage()
            .head_object(&b, &object::key("c.txt").unwrap())
            .await
            .unwrap();
        let recorded = info.checksum.unwrap();
        assert_eq!(recorded.part.value.0, "NhCmhg==");
        assert_eq!(recorded.kind, checksum::Type::FullObject);

        // GET echoes the recorded checksum (grilling Q7) — assert via the
        // dto newtype's accessor (adjust to its exact shape).
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "c.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(got.output.checksum_crc32.as_deref(), Some("NhCmhg=="));
    }

    #[tokio::test]
    async fn put_tagging_is_counted_on_get_and_head() {
        let (backend, b) = setup_name().await;
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"x"))
                }))),
                tagging: Some("env=prod&a=1".into()),
                ..Default::default()
            }))
            .await
            .unwrap();
        // GET answers `x-amz-tagging-count` (the dto field) with the
        // tag count…
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(got.output.tag_count, Some(2));
        // …and HEAD answers the same count as a hand-set response
        // header (HeadObjectOutput has no dto field for it).
        let head = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(head.headers.get("x-amz-tagging-count").unwrap(), "2");
        // A tag-less object omits the count on both surfaces (AWS).
        backend
            .storage()
            .put_object(&b, &"plain.txt".into(), body(b"x"))
            .await
            .unwrap();
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "plain.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(got.output.tag_count.is_none());
        let head = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.to_string(),
                key: "plain.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(head.headers.get("x-amz-tagging-count").is_none());
    }

    #[tokio::test]
    async fn tagging_toggle_off_drops_the_headers_and_gates_the_ops() {
        let (backend, b) = setup_with_caps(Capabilities {
            tagging: false,
            ..Default::default()
        })
        .await;
        // The write header is accept-and-drop — even a malformed value
        // passes (no validation when the toggle is off).
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"x"))
                }))),
                tagging: Some("no-equals-sign".into()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let head = backend
            .storage()
            .head_object(&bucket::name(&b).unwrap(), &object::key("t.txt").unwrap())
            .await
            .unwrap();
        assert!(head.tags.is_empty());
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(got.output.tag_count.is_none());
        // The three tagging ops answer NotImplemented.
        let err = backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        let err = backend
            .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                tagging: dto::Tagging {
                    tag_set: vec![dto::Tag {
                        key: Some("k".into()),
                        value: Some("1".into()),
                    }],
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                request_payer: None,
                version_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        let err = backend
            .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                bucket: b.to_string(),
                key: "t.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn copy_object_directive_selects_tags_and_carries_the_checksum() {
        let (backend, b) = setup_checksum().await;
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "src.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                tagging: Some("env=prod".into()),
                checksum_crc32: Some("NhCmhg==".into()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let src = CopySource::parse(&format!("{b}/src.txt")).unwrap();
        let copy = |key: &str,
                    tagging_directive: Option<dto::TaggingDirective>,
                    tagging: Option<String>| {
            backend.copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key(key.to_string())
                    .copy_source(src.clone())
                    .tagging_directive(tagging_directive)
                    .tagging(tagging)
                    .build()
                    .unwrap(),
            ))
        };
        // COPY (the default directive) carries the source's tags and
        // recorded checksum into the destination.
        copy("dst1.txt", None, None).await.unwrap();
        let info = head_object(&backend, &b, "dst1.txt").await;
        assert_eq!(info.tags.to_wire(), "env=prod");
        let recorded = info.checksum.unwrap();
        assert_eq!(recorded.part.value.as_str(), "NhCmhg==");
        assert_eq!(recorded.kind, checksum::Type::FullObject);
        // REPLACE with an `x-amz-tagging` header → the new tags; the
        // source's checksum is still carried (the bytes are identical).
        copy(
            "dst2.txt",
            Some("REPLACE".parse().unwrap()),
            Some("env=dev".into()),
        )
        .await
        .unwrap();
        let info = head_object(&backend, &b, "dst2.txt").await;
        assert_eq!(info.tags.to_wire(), "env=dev");
        assert_eq!(
            info.checksum.unwrap().part.value.as_str(),
            "NhCmhg==",
            "REPLACE replaces the tags, never the checksum"
        );
        // REPLACE with no header → no tags.
        copy("dst3.txt", Some("REPLACE".parse().unwrap()), None)
            .await
            .unwrap();
        let info = head_object(&backend, &b, "dst3.txt").await;
        assert!(info.tags.is_empty());
        // A directive that is neither COPY nor REPLACE is a
        // request-shape error — 400 InvalidArgument (AWS), never a
        // silent COPY that would carry the source's tags.
        let err = copy(
            "dst4.txt",
            Some("Replace".parse().unwrap()),
            Some("env=dev".into()),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    /// The head metadata at `key` of the test bucket (the directive-test
    /// assertions).
    #[cfg(feature = "copy")]
    async fn head_object(
        backend: &S3Backend<MemoryStorage>,
        b: &bucket::Name,
        key: &str,
    ) -> crate::_core::object::Info {
        backend
            .storage()
            .head_object(b, &object::key(key).unwrap())
            .await
            .unwrap()
    }

    /// The client-side checksum of `data` over `algo` (the same s3s
    /// hasher the wire uses — the test simulates a real client).
    #[cfg(feature = "multipart")]
    fn client_checksum(algo: Algorithm, data: &[u8]) -> String {
        let mut hasher = ChecksumHasher::default();
        crate::backend::checksum::enable_algo(&mut hasher, algo);
        hasher.update(data);
        crate::backend::checksum::checksum_value_of(&hasher.finalize(), algo)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn get_object_attributes_returns_the_requested_subset() {
        let (backend, b) = setup_name().await;
        backend
            .storage()
            .put_object(&b, &"plain.txt".into(), body(b"hello"))
            .await
            .unwrap();
        // The plain object: every requested attribute answers, and
        // ObjectParts is omitted (a non-multipart object has no parts —
        // AWS). The wire carries the requested list comma-joined in one
        // `x-amz-object-attributes` value, which the s3s parse keeps as
        // one entry — the op must split it.
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "plain.txt".into(),
                object_attributes: vec![
                    "ETag,ObjectSize,StorageClass".parse().unwrap(),
                    "Checksum".parse().unwrap(),
                    "ObjectParts".parse().unwrap(),
                ],
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = got.output;
        assert_eq!(
            out.e_tag.unwrap().as_strong().unwrap(),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(out.object_size, Some(5));
        assert_eq!(
            out.storage_class.as_ref().map(|s| s.as_str()),
            Some(dto::StorageClass::STANDARD)
        );
        assert!(out.checksum.is_none(), "no recorded checksum");
        assert!(out.object_parts.is_none(), "non-multipart omits parts");

        // Only the requested subset is present — request ObjectSize
        // alone and everything else stays absent.
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "plain.txt".into(),
                object_attributes: vec!["ObjectSize".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = got.output;
        assert_eq!(out.object_size, Some(5));
        assert!(out.e_tag.is_none());
        assert!(out.storage_class.is_none());
        assert!(out.checksum.is_none());
        assert!(out.object_parts.is_none());

        // A missing key answers NoSuchKey (the op heads first).
        let err = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "missing.txt".into(),
                object_attributes: vec!["ETag".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
    }

    #[tokio::test]
    async fn get_object_attributes_echoes_the_recorded_full_object_checksum() {
        // A checksummed plain PUT records its client-sent checksum with
        // the FULL_OBJECT kind at write time (Task 6) — the most common
        // object class of the Checksum attribute. The op must echo the
        // recorded value and the FULL_OBJECT wire kind (the `wire_type`
        // arm no unit test reaches through a read surface).
        let (backend, b) = setup_checksum().await;
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "c.txt".into(),
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                checksum_crc32: Some("NhCmhg==".into()), // crc32("hello"), the Task 6 fixture value
                ..Default::default()
            }))
            .await
            .unwrap();
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "c.txt".into(),
                object_attributes: vec!["Checksum".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap();
        let checksum = got.output.checksum.unwrap();
        assert_eq!(checksum.checksum_crc32.as_deref(), Some("NhCmhg=="));
        assert_eq!(
            checksum.checksum_type.as_ref().map(|t| t.as_str()),
            Some("FULL_OBJECT")
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn get_object_attributes_echoes_the_recorded_checksum_and_paginates_parts() {
        // Create (SHA256) → upload two parts with per-part checksums →
        // complete: the completion derives the composite and records it.
        // GetObjectAttributes then echoes the RECORDED composite under
        // the Checksum attribute and the retained part list under
        // ObjectParts, paginated at the interface layer (max_parts /
        // part_number_marker — the multipart scaffold, mirroring the
        // multipart tests).
        let (backend, b) = setup_checksum().await;
        let upload_id = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .upload_id
            .unwrap();
        // Part 1 is non-final → must meet the 5 MiB minimum; part 2 is
        // the final part.
        let parts: Vec<Vec<u8>> = vec![vec![b'a'; 5 * 1024 * 1024 + 1], b"tail".to_vec()];
        let sizes: Vec<i64> = parts.iter().map(|p| p.len() as i64).collect();
        let mut etags = Vec::new();
        let mut values = Vec::new();
        for (i, data) in parts.iter().enumerate() {
            let value = client_checksum(Algorithm::Sha256, data);
            let body = Bytes::copy_from_slice(data);
            let mut input = dto::UploadPartInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: (i + 1) as i32,
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(body)
                }))),
                ..Default::default()
            };
            input.set_checksum(Algorithm::Sha256, &value);
            let part = backend.upload_part(s3_request(input)).await.unwrap();
            etags.push(part.output.e_tag.unwrap());
            values.push(value);
        }
        let complete = backend
            .complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(
                        etags
                            .iter()
                            .enumerate()
                            .map(|(i, e)| dto::CompletedPart {
                                part_number: Some(i as i32 + 1),
                                e_tag: Some(e.clone()),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        let composite = complete.output.checksum_sha256.unwrap();

        // The Checksum attribute echoes the recorded composite with its
        // recorded COMPOSITE kind; ObjectParts page 1 (max_parts = 1)
        // lists part 1, truncated, with the next marker and the total.
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                max_parts: Some(1),
                object_attributes: vec![
                    "Checksum".parse().unwrap(),
                    "ObjectParts".parse().unwrap(),
                ],
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = got.output;
        let checksum = out.checksum.unwrap();
        assert_eq!(
            checksum.checksum_sha256.as_deref(),
            Some(composite.as_str()),
            "the attribute echo must match the recorded composite"
        );
        assert_eq!(
            checksum.checksum_type.as_ref().map(|t| t.as_str()),
            Some("COMPOSITE")
        );
        let parts = out.object_parts.unwrap();
        assert_eq!(parts.total_parts_count, Some(2));
        assert_eq!(parts.is_truncated, Some(true));
        assert_eq!(parts.next_part_number_marker, Some(1));
        assert_eq!(parts.max_parts, Some(1), "the applied cap is echoed");
        assert_eq!(parts.part_number_marker, None);
        let page = parts.parts.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].part_number, Some(1));
        assert_eq!(page[0].size, Some(sizes[0]));
        assert_eq!(
            page[0].checksum_sha256.as_deref(),
            Some(values[0].as_str()),
            "the retained part list carries the stored part checksums"
        );

        // Resuming after part 1 lists the rest — the marker is
        // exclusive — untruncated, no next marker.
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                part_number_marker: Some(1),
                object_attributes: vec!["ObjectParts".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap();
        let parts = got.output.object_parts.unwrap();
        assert_eq!(parts.total_parts_count, Some(2));
        assert_eq!(parts.is_truncated, Some(false));
        assert!(parts.next_part_number_marker.is_none());
        assert_eq!(
            parts.max_parts,
            Some(1000),
            "an absent max_parts defaults to 1000 and is echoed applied"
        );
        assert_eq!(
            parts.part_number_marker,
            Some(1),
            "the request marker is echoed"
        );
        let page = parts.parts.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].part_number, Some(2));
        assert_eq!(page[0].size, Some(sizes[1]));
        assert_eq!(page[0].checksum_sha256.as_deref(), Some(values[1].as_str()));

        // A marker past the last part lists an empty page — the
        // container stays, untruncated, with no next marker.
        let got = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                part_number_marker: Some(2),
                object_attributes: vec!["ObjectParts".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap();
        let parts = got.output.object_parts.unwrap();
        assert_eq!(parts.total_parts_count, Some(2));
        assert!(parts.parts.as_ref().unwrap().is_empty());
        assert_eq!(parts.is_truncated, Some(false));
        assert!(parts.next_part_number_marker.is_none());

        // A negative marker would mask every part as already listed —
        // rejected like the multipart listing's marker.
        let err = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                part_number_marker: Some(-1),
                object_attributes: vec!["ObjectParts".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }
}
