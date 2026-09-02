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

use std::{collections::HashMap, sync::Arc};

use s3s::{
    S3Error, S3Request, S3Response, S3Result,
    dto::{self, AbortMultipartUploadOutput, Range},
    s3_error,
};
use tracing::warn;

use crate::{
    _core::{
        ETag,
        checksum::{Algorithm, Part, Type, Upload, Value},
        multipart::{CompletedPart, MIN_PART_BYTES, PartNumber, part_number as parse_part_number},
        storage::{ByteRange, Error as StorageError, ListPartsParams, ListUploadsParams, Storage},
    },
    backend::{
        ConditionalHeaders, S3Backend, byte_range,
        checksum::{
            self, HasFields, VerifyState, VerifyStream, compose_composite, linearize_full_object,
            single_checksum_value,
        },
        map_backend_error, normalize_delimiter, normalize_page_size,
    },
};

/// A request part number into the validated [`PartNumber`] (invalid →
/// `InvalidPart`).
fn part_number(n: i32) -> S3Result<PartNumber> {
    parse_part_number(n as u32).map_err(|_| s3_error!(InvalidPart, "invalid part number: {n}"))
}

/// The `x-amz-copy-source-range` header into a [`ByteRange`], parsed by
/// the framework's own range grammar. S3 copy ranges use the strict
/// `bytes=first-last` form only; the suffix/open forms GET accepts answer
/// `InvalidArgument` — the shared [`byte_range`] mapping plus the strict
/// shape gate.
#[cfg(feature = "copy")]
fn copy_source_range(raw: &str) -> Result<ByteRange, S3Error> {
    let invalid = || s3_error!(InvalidArgument, "invalid copy source range: {raw}");
    let range = byte_range(Range::parse(raw).map_err(|_| invalid())?);
    match range {
        ByteRange::Inclusive(_, _) => Ok(range),
        _ => Err(invalid()),
    }
}

/// The `upload_part` error mapping of the tee paths: a stream that
/// ended in a checksum mismatch surfaces as `BadDigest` (the part was
/// never committed), anything else as the backend error.
fn map_part_error<E: Into<StorageError>>(state: Option<&VerifyState>, err: E) -> S3Error {
    if state.is_some_and(|state| state.mismatched()) {
        s3_error!(BadDigest, "checksum mismatch")
    } else {
        map_backend_error(err)
    }
}

/// The full-object derivation type of a Complete request: the request's
/// `checksum_type` when present (a conflict with the persisted
/// create-time type → `BadDigest`, R3; an unparseable type is a
/// request-shape violation → `InvalidArgument`), the persisted type
/// otherwise, and the per-algorithm default (CRC64NVME → FULL_OBJECT,
/// else COMPOSITE — R7) when neither was fixed. The wire type is parsed
/// once.
fn resolve_checksum_type(
    input_type: Option<&str>,
    persisted: Option<Type>,
    algo: Algorithm,
) -> S3Result<Type> {
    let parsed = input_type
        .map(|wire| {
            wire.parse()
                .map_err(|_| s3_error!(InvalidArgument, "unsupported checksum type: {wire}"))
        })
        .transpose()?;
    if let (Some(parsed), Some(persisted)) = (parsed, persisted)
        && parsed != persisted
    {
        return Err(s3_error!(
            BadDigest,
            "checksum type conflicts with the upload's"
        ));
    }
    Ok(parsed
        .or(persisted)
        .unwrap_or(if algo == Algorithm::Crc64Nvme {
            Type::FullObject
        } else {
            Type::Composite
        }))
}

/// The full-object derivation of a Complete request, shared by the
/// value-present and compute-and-echo branches: the algorithm × type
/// validity table, the FULL_OBJECT request-shape check (W04 — runs
/// before the D2 gate), the D2 completeness gate (a listed part without
/// a stored checksum skips the derivation), and the compose/linearize.
/// `require_size` gates the `x-amz-mp-object-size` shape check: only
/// the validation branch demands it (the echo branch has no value to
/// compare, spec W04).
fn derive_full_checksum(
    upload_algo: Algorithm,
    checksum_type: Type,
    snapshot: &[(Option<&Part>, u64)],
    mpu_object_size: Option<i64>,
    require_size: bool,
) -> S3Result<Option<Value>> {
    // Algorithm × type validity (spec table).
    validate_supported(upload_algo, checksum_type)?;
    // Request-shape validation runs before the D2 completeness gate
    // (review W04): a FULL_OBJECT value without `x-amz-mp-object-size` —
    // or with a size that does not match the stored part sizes — is
    // InvalidRequest even when some listed part lacks a stored checksum.
    if require_size && checksum_type == Type::FullObject {
        let Some(object_size) = mpu_object_size else {
            return Err(s3_error!(
                InvalidRequest,
                "FULL_OBJECT checksum requires x-amz-mp-object-size"
            ));
        };
        let total: u64 = snapshot.iter().map(|(_, size)| size).sum();
        if object_size != total as i64 {
            return Err(s3_error!(
                InvalidRequest,
                "x-amz-mp-object-size does not match the sum of the part sizes"
            ));
        }
    }
    // The D2 gate: every listed part must carry a stored checksum.
    let checksums: Vec<Option<&Part>> = snapshot.iter().map(|(checksum, _)| *checksum).collect();
    if checksums.iter().any(Option::is_none) {
        warn!(algorithm = ?upload_algo, "complete checksum validation skipped: parts without stored checksums");
        return Ok(None);
    }
    let checksums: Vec<&Part> = checksums.into_iter().flatten().collect();
    let sizes: Vec<u64> = snapshot.iter().map(|(_, size)| *size).collect();
    let computed = match checksum_type {
        Type::Composite => compose_composite(upload_algo, &checksums),
        Type::FullObject => linearize_full_object(upload_algo, &checksums, &sizes),
    };
    if let Some(computed) = computed {
        Ok(Some(computed))
    } else {
        warn!(algorithm = ?upload_algo, "complete checksum validation skipped");
        Ok(None)
    }
}

/// The algorithm × type validity check (one home for the error — the
/// create op and the Complete derivation share it, spec table).
fn validate_supported(algo: Algorithm, checksum_type: Type) -> S3Result<()> {
    if checksum_type.supports(algo) {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidRequest,
            "checksum algorithm {} does not support the {} checksum type",
            algo.to_string(),
            checksum_type.to_string()
        ))
    }
}

/// The upload's persisted create-algorithm.
fn upload_algo(upload: &tinio_core::MultipartUpload) -> Option<Algorithm> {
    upload.checksum.as_ref().map(|c| c.algorithm)
}

/// The stored snapshot of the listed parts, one entry per listed part:
/// the size (0 when the part is outside the snapshot — the W04 size
/// check depends only on it) and the stored checksum (absent when the
/// part has none — the D2 gate). One home for the alignment — the sizes
/// stay in lockstep with the parts.
fn stored_parts<'a>(
    stored: &'a HashMap<u32, (u64, Option<Part>)>,
    parts: &[CompletedPart],
) -> Vec<(Option<&'a Part>, u64)> {
    parts
        .iter()
        .map(|part| {
            stored
                .get(&u32::from(part.part_number))
                .map_or((None, 0), |(size, checksum)| (checksum.as_ref(), *size))
        })
        .collect()
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
        // The create-time checksum spec is persisted and drives the
        // per-part algorithm consistency + the compute-only tee (spec
        // 2026-08-31); the toggle off ⇒ today's accept-and-drop.
        let checksum = if self.caps.checksum {
            match (
                req.input.checksum_algorithm.as_ref().map(|a| a.as_str()),
                req.input.checksum_type.as_ref().map(|t| t.as_str()),
            ) {
                (Some(algo), checksum_type) => {
                    let algorithm = algo.parse().map_err(|_| {
                        s3_error!(InvalidArgument, "unsupported checksum algorithm: {algo}")
                    })?;
                    let checksum_type: Option<Type> = checksum_type
                        .map(|t| {
                            t.parse().map_err(|_| {
                                s3_error!(InvalidArgument, "unsupported checksum type: {t}")
                            })
                        })
                        .transpose()?;
                    // Algorithm × type validity (spec table) — an
                    // invalid combination must be rejected at create,
                    // not silently accepted until Complete (F5). A type
                    // that was not fixed (None) is always valid.
                    if let Some(ty) = checksum_type {
                        validate_supported(algorithm, ty)?;
                    }
                    Some(Upload {
                        algorithm,
                        r#type: checksum_type,
                    })
                }
                // A checksum type without an algorithm has nothing to
                // persist — accept and drop (review C01; the AWS wire
                // behavior for this shape is undocumented, so no
                // invented rejection).
                (None, Some(checksum_type)) => {
                    warn!(
                        checksum_type,
                        "checksum type without a checksum algorithm — accepted, not persisted"
                    );
                    None
                }
                (None, None) => None,
            }
        } else {
            None
        };
        let upload = self
            .storage
            .create_multipart_upload(&bucket, &key, checksum)
            .await
            .map_err(map_backend_error)?;
        // Seed the spec cache (F04): the spec is immutable after create
        // — the first UploadPart pays no read at all. Gated: with the
        // toggle off the entry is never read (the readers are gated the
        // same way), so an unseeded entry would leak for the upload's
        // lifetime.
        if self.caps.checksum {
            self.put_checksum_spec(upload.upload_id.clone(), upload.checksum.clone().map(Arc::new));
        }
        Ok(S3Response::new(dto::CreateMultipartUploadOutput {
            bucket: Some(String::from(bucket)),
            key: Some(String::from(key)),
            upload_id: Some(upload.upload_id),
            checksum_algorithm: upload
                .checksum
                .as_ref()
                .map(|c| checksum::wire_algo(c.algorithm)),
            checksum_type: upload
                .checksum
                .as_ref()
                .and_then(|c| c.r#type)
                .map(checksum::wire_type),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.require_multipart()?;
        // Cloned: the checksum-spec parse below borrows the whole input.
        let bucket = self.bucket(req.input.bucket.clone())?;
        let key = self.key(req.input.key.clone())?;
        let upload_id = req.input.upload_id.clone();
        let part_number = part_number(req.input.part_number)?;
        // The checksum tee (spec 2026-08-31): parse the request spec and
        // wrap the body BEFORE any storage call. The persisted
        // create-algorithm drives the compute-only tee and the
        // algorithm-consistency check (S3: the checksum algorithm must
        // match the one supplied at create). Toggle off ⇒ exactly
        // today's code path.
        let tee: Option<(Arc<VerifyState>, checksum::Spec)> = if self.caps.checksum {
            // Parse the request spec FIRST — a malformed request (two
            // value fields, bare algorithm) answers InvalidRequest
            // without paying the storage read. The upload's spec comes
            // from the read-through cache (F04): immutable after create,
            // so no per-part storage read.
            let spec = checksum::Spec::from_upload_part(&req.input, &req.headers)?;
            let upload = self.upload_checksum_spec(&bucket, &key, &upload_id).await?;
            let upload_algo = upload.as_ref().map(|u| u.algorithm);
            let spec = match spec {
                Some(spec) => {
                    if let (Some(upload_algo), Some(algo)) = (upload_algo, spec.algorithm)
                        && upload_algo != algo
                    {
                        return Err(s3_error!(
                            InvalidRequest,
                            "checksum algorithm {} does not match the upload's {}",
                            algo.to_string(),
                            upload_algo.to_string()
                        ));
                    }
                    // The upload's algorithm applies to every part (S3:
                    // "must be the same for all parts") — a Content-MD5-
                    // only part of an algorithm upload still gets the
                    // upload's checksum computed and persisted.
                    Some(checksum::Spec {
                        algorithm: spec.algorithm.or(upload_algo),
                        ..spec
                    })
                }
                None => upload.map(|u| checksum::Spec::compute_only(u.algorithm)),
            };
            spec.map(|spec| {
                // The tee computes MD5 over the body — for the algorithm
                // slot (a part's ETag IS its content MD5) or for the
                // Content-MD5 check (F05: that digest must not be thrown
                // away) — the backend may use the slot as the part ETag.
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
        // The tee's digest rides into the backend's commit transaction
        // (the part row and its checksum row land atomically — no
        // second call, no CAS).
        let checksum_slot = tee.as_ref().map(|(state, _)| state.slot());
        let part = self
            .storage
            .upload_part(&bucket, &key, &upload_id, part_number, body, checksum_slot)
            .await
            .map_err(|err| map_part_error(tee.as_ref().map(|(state, _)| state.as_ref()), err))?;
        let mut output = dto::UploadPartOutput {
            e_tag: Some(Self::etag_wire(&part.etag)),
            ..Default::default()
        };
        // Response echo only when the request carried a value — a header
        // field or a declared trailer (S3 API docs: "only be present if
        // the checksum was provided in the request"). The validated
        // computed value equals the provided one.
        if let Some((state, spec)) = &tee
            && let Some(algo) = spec.algorithm
            && (spec.expected.is_some() || spec.trailer_algo.is_some())
            && let Some(computed) = state.computed()
        {
            output.set_checksum(algo, computed.as_str());
        }
        Ok(S3Response::new(output))
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
        // copy primitive moves the bytes). The source head and the
        // destination upload's checksum spec are independent reads — run
        // them concurrently (the spec comes from the read-through cache,
        // F04: a 10k-part copy flow pays the same zero storage reads as
        // UploadPart).
        let (head, algo) = tokio::join!(
            self.storage.head_object(&src_bucket, &src_key),
            async {
                if self.caps.checksum {
                    self.upload_checksum_spec(&bucket, &key, &upload_id)
                        .await
                        .map(|spec| spec.map(|c| c.algorithm))
                } else {
                    Ok(None)
                }
            },
        );
        let info = head.map_err(map_backend_error)?;
        let algo = algo?;
        ConditionalHeaders::new(
            req.input.copy_source_if_match.as_ref(),
            req.input.copy_source_if_none_match.as_ref(),
            req.input.copy_source_if_modified_since,
            req.input.copy_source_if_unmodified_since,
        )
        .check(&info.etag, info.last_modified, true)?;
        // The copy path carries no client checksum on the wire (R1). For
        // a create-algorithm upload the server computes the copied part's
        // checksum (spec D5 — what AWS does unconditionally): the source
        // range streams through the compute-only tee, the digest is
        // persisted, and CopyPartResult echoes it. Non-algorithm uploads
        // keep the contract `copy_part` fast path (fs `copy_part_fast` on
        // unix). Toggle off ⇒ exactly today's code path.
        let (part, echo) = if let Some(algo) = algo {
            // The compute-only tee's digest rides into the backend's
            // commit transaction with the part row.
            let state = Arc::new(VerifyState::new(algo == Algorithm::Md5));
            let spec = checksum::Spec::compute_only(algo);
            let get = self
                .storage
                .get_object(&src_bucket, &src_key, range)
                .await
                .map_err(map_backend_error)?;
            let body = VerifyStream::wrap(get.body, &spec, None, &state);
            let part = self
                .storage
                .upload_part(
                    &bucket,
                    &key,
                    &upload_id,
                    part_number,
                    body,
                    Some(state.slot()),
                )
                .await
                .map_err(|err| map_part_error(Some(&state), err))?;
            let echo = state.computed().map(|computed| (algo, computed));
            (part, echo)
        } else {
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
            (part, None)
        };
        let mut result = dto::CopyPartResult {
            e_tag: Some(Self::etag_wire(&part.etag)),
            last_modified: Some(Self::last_modified(part.last_modified)),
            ..Default::default()
        };
        if let Some((algo, computed)) = echo {
            result.set_checksum(algo, computed.as_str());
        }
        Ok(S3Response::new(dto::UploadPartCopyOutput {
            copy_part_result: Some(result),
            ..Default::default()
        }))
    }

    #[cfg(feature = "multipart")]
    pub(crate) async fn op_complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.require_multipart()?;
        // Cloned: the checksum validation below borrows the whole input.
        let bucket = self.bucket(req.input.bucket.clone())?;
        let key = self.key(req.input.key.clone())?;
        let upload_id = req.input.upload_id.clone();
        let input = &req.input;
        let mut parts = Vec::new();
        // The client's per-part checksum entries, parallel to `parts`
        // (the CompletedPart cross-check, spec 2026-08-31).
        let mut client_part_checksums = Vec::new();
        for p in input
            .multipart_upload
            .as_ref()
            .and_then(|m| m.parts.as_ref())
            .into_iter()
            .flatten()
        {
            let raw = p
                .part_number
                .ok_or_else(|| s3_error!(InvalidArgument, "missing part number"))?;
            let part_number = part_number(raw)?;
            let etag = p
                .e_tag
                .clone()
                .ok_or_else(|| s3_error!(InvalidPart, "missing part ETag"))?;
            let etag = etag
                .into_strong()
                .ok_or_else(|| s3_error!(InvalidPart, "weak part ETag"))?;
            let etag = ETag::new(&etag).map_err(|_| s3_error!(InvalidPart, "invalid part ETag"))?;
            // The client's checksum entry of this part: exactly one of
            // the six value fields (a second → `InvalidRequest`,
            // mirroring UploadPart — F6). Gated: with the toggle off the
            // entries are accepted and dropped (F01 — off ⇒ v1's
            // pass-through; the scan trigger below stays false).
            client_part_checksums.push(if self.caps.checksum {
                single_checksum_value(p)?.map(|(algo, value)| Part {
                    algorithm: algo,
                    value: Value(value.to_string()),
                })
            } else {
                None
            });
            parts.push(CompletedPart { part_number, etag });
        }
        // Serialize the whole complete with the per-object write lock
        // (spec R8): paging, the 5 MiB rule, the pre-commit checksum
        // validation, and the commit all run under the lock, so
        // validation always reads the snapshot the commit consumes.
        let _guard = self.lock_object(&bucket, &key).await;
        // The persisted upload state and the request's full-object
        // value, fetched before the scan: they decide whether the
        // snapshot is needed at all (Eff #1 — with the toggle on, a
        // single-part complete with no checksums anywhere skips the
        // full paging loop).
        let upload = if self.caps.checksum {
            Some(
                self.storage
                    .get_multipart_upload(&bucket, &key, &upload_id)
                    .await
                    .map_err(map_backend_error)?,
            )
        } else {
            None
        };
        let full = if self.caps.checksum {
            single_checksum_value(input)?
        } else {
            None
        };
        // The stored sizes + part checksums of the listed parts: the 5
        // MiB minimum-size rule and the pre-commit validation both read
        // this snapshot. The scan runs only when something consumes it
        // (the per-page checksum join itself is the backends' internal
        // probe, F03 — a checksum-less upload pays one probe, not one
        // point read per part).
        let mut stored: HashMap<u32, (u64, Option<Part>)> = HashMap::new();
        if parts.len() > 1
            || upload.as_ref().is_some_and(|u| u.checksum.is_some())
            || full.is_some()
            || client_part_checksums.iter().any(Option::is_some)
        {
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
                    stored.insert(u32::from(part.part_number), (part.size, part.checksum));
                }
                match page.next_part_number_marker {
                    Some(next) if page.truncated => marker = Some(next),
                    _ => break,
                }
            }
        }
        // S3 requires every non-final part to be at least 5 MiB
        // (EntityTooSmall); the final part has no minimum.
        if parts.len() > 1 {
            for (index, part) in parts.iter().enumerate() {
                if index + 1 == parts.len() {
                    continue; // the final part may be smaller than 5 MiB
                }
                let n = u32::from(part.part_number);
                let size = stored
                    .get(&n)
                    .map(|(size, _)| *size)
                    .ok_or_else(|| s3_error!(InvalidPart, "part {n} was not uploaded"))?;
                if size < MIN_PART_BYTES {
                    return Err(s3_error!(
                        EntityTooSmall,
                        "part {n} is {size} bytes, below the {MIN_PART_BYTES}-byte minimum for non-final parts"
                    ));
                }
            }
        }
        // Checksum validation (pre-commit, under the per-object write
        // lock — spec R8): a failed validation returns before any write,
        // so the upload (and any pre-existing object of the same key)
        // is left untouched — matching S3, with no rollback machinery.
        let mut echo_checksum: Option<(Algorithm, Value)> = None;
        if let Some(upload) = upload {
            // 1. CompletedPart cross-check: the client's checksum
            // entries must match the stored values whenever both exist.
            // Only the algorithm-consistency rule is gated on the
            // upload's create-algorithm (S3: "the checksum algorithm
            // must be the same for all parts"); the value-vs-stored
            // comparison also runs for uploads without a create
            // algorithm, whose parts may still carry stored checksums
            // (review W03). A part with no stored value is skipped
            // (D2), and a part outside the stored snapshot was never
            // uploaded — the backend classifies it (InvalidPart, F9).
            for (part, entry) in parts.iter().zip(&client_part_checksums) {
                let Some(entry) = entry else {
                    continue;
                };
                if let Some(upload_algo) = upload_algo(&upload)
                    && entry.algorithm != upload_algo
                {
                    return Err(s3_error!(
                        InvalidRequest,
                        "part {} checksum algorithm does not match the upload's",
                        u32::from(part.part_number)
                    ));
                }
                let Some(stored_value) = stored
                    .get(&u32::from(part.part_number))
                    .and_then(|(_, checksum)| checksum.as_ref())
                else {
                    continue;
                };
                if stored_value.value.as_str() != entry.value.as_str() {
                    return Err(s3_error!(
                        BadDigest,
                        "part {} checksum mismatch",
                        u32::from(part.part_number)
                    ));
                }
            }
            // 2. The full-object value (hoisted above).
            match upload_algo(&upload) {
                None => {
                    // No create-time algorithm: a value is accepted but not
                    // validated (documented AWS behavior for CRC32/CRC32C/
                    // SHA1/SHA256; assumed for CRC64NVME — spec R5), and
                    // nothing is computed or echoed.
                    if let Some((algo, _)) = full {
                        warn!(algorithm = ?algo, "complete checksum value without a create-time algorithm — accepted, not validated");
                    }
                }
                Some(upload_algo) => {
                    // A value's algorithm must match the upload's (S3: "must
                    // be the same for all parts").
                    if let Some((algo, _)) = &full
                        && *algo != upload_algo
                    {
                        return Err(s3_error!(
                            InvalidRequest,
                            "checksum algorithm {} does not match the upload's {}",
                            algo.to_string(),
                            upload_algo.to_string()
                        ));
                    }
                    let checksum_type = resolve_checksum_type(
                        input.checksum_type.as_ref().map(|t| t.as_str()),
                        upload.checksum.as_ref().and_then(|c| c.r#type),
                        upload_algo,
                    )?;
                    // The one derivation for both branches: a value is
                    // validated against the computed digest (require_size:
                    // only the validation branch demands
                    // `x-amz-mp-object-size`, spec W04); without a value the
                    // computed digest is echoed (design Architecture,
                    // Complete step 3).
                    let snapshot = stored_parts(&stored, &parts);
                    let derived = derive_full_checksum(
                        upload_algo,
                        checksum_type,
                        &snapshot,
                        input.mpu_object_size,
                        full.is_some(),
                    )?;
                    match (full, derived) {
                        (Some((_, value)), Some(computed)) if computed.as_str() == value => {
                            echo_checksum = Some((upload_algo, computed));
                        }
                        (Some(_), Some(_)) => {
                            return Err(s3_error!(BadDigest, "checksum mismatch"));
                        }
                        (None, Some(computed)) => {
                            echo_checksum = Some((upload_algo, computed));
                        }
                        // Skipped (D2) — accepted, not validated.
                        (_, None) => {}
                    }
                }
            }
        }
        let info = self
            .storage
            .complete_multipart_upload(&bucket, &key, &upload_id, &parts)
            .await
            .map_err(map_backend_error)?;
        // The upload is finished — its spec entry would otherwise stay
        // in the cache forever (see [`S3Backend::evict_checksum_spec`]).
        self.evict_checksum_spec(&upload_id);
        let location = Some(format!("/{bucket}/{}", info.key));
        let mut output = dto::CompleteMultipartUploadOutput {
            bucket: Some(String::from(bucket)),
            key: Some(String::from(key)),
            e_tag: Some(Self::etag_wire(&info.etag)),
            location,
            ..Default::default()
        };
        if let Some((algo, value)) = echo_checksum {
            output.set_checksum(algo, value.as_str());
        }
        Ok(S3Response::new(output))
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
        // The upload is gone — forget its spec entry (see
        // [`S3Backend::evict_checksum_spec`]).
        self.evict_checksum_spec(&req.input.upload_id);
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
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
        let max_parts = req.input.max_parts.unwrap_or(1000);
        // Unified page-size policy: < 1 is rejected before any storage
        // call unless the `allow_zero_page_size` escape hatch restores
        // the legacy empty page. Multipart listings stay uncapped
        // (AWS documents no cap).
        let max_parts =
            normalize_page_size(max_parts, "max-parts", self.caps.allow_zero_page_size)?;
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
        // The upload's persisted checksum spec (spec 2026-08-31), echoed
        // on the listing — from the read-through cache (F04: immutable
        // after create, no second storage read), gated like the rest of
        // the feature (off ⇒ today's output).
        let upload_checksum = if self.caps.checksum {
            self.upload_checksum_spec(&bucket, &key, &upload_id)
                .await?
                .map(|c| c.as_ref().clone())
        } else {
            None
        };
        let parts = page
            .parts
            .into_iter()
            .map(|p| {
                let mut part = dto::Part {
                    e_tag: Some(Self::etag_wire(&p.etag)),
                    last_modified: Some(Self::last_modified(p.last_modified)),
                    part_number: Some(u32::from(p.part_number) as i32),
                    size: Some(p.size as i64),
                    ..Default::default()
                };
                // The stored part checksum (spec 2026-08-31): the
                // backends store what the server computed — echo it
                // only while the toggle is on (off = accept-and-drop,
                // even for rows persisted by an earlier on-run — F4).
                if self.caps.checksum
                    && let Some(checksum) = p.checksum
                {
                    part.set_checksum(checksum.algorithm, checksum.value.as_str());
                }
                part
            })
            .collect();
        Ok(S3Response::new(dto::ListPartsOutput {
            bucket: Some(String::from(bucket)),
            key: Some(String::from(key)),
            upload_id: Some(upload_id),
            checksum_algorithm: upload_checksum
                .as_ref()
                .map(|c| checksum::wire_algo(c.algorithm)),
            checksum_type: upload_checksum
                .as_ref()
                .and_then(|c| c.r#type)
                .map(checksum::wire_type),
            is_truncated: Some(page.truncated),
            max_parts: Some(max_parts as i32),
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
        let max_uploads = req.input.max_uploads.unwrap_or(1000);
        let max_uploads =
            normalize_page_size(max_uploads, "max-uploads", self.caps.allow_zero_page_size)?;
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
                key: Some(String::from(u.key)),
                upload_id: Some(u.upload_id),
                // The persisted checksum spec is echoed only while the
                // toggle is on — ListParts gates the same data (F4), and
                // the two list ops must agree (F02; off = accept-and-drop,
                // even for rows persisted by an earlier on-run).
                checksum_algorithm: if self.caps.checksum {
                    u.checksum
                        .as_ref()
                        .map(|c| checksum::wire_algo(c.algorithm))
                } else {
                    None
                },
                checksum_type: if self.caps.checksum {
                    u.checksum
                        .as_ref()
                        .and_then(|c| c.r#type)
                        .map(checksum::wire_type)
                } else {
                    None
                },
                ..Default::default()
            })
            .collect();
        let common_prefixes = page
            .common_prefixes
            .into_iter()
            .map(|p| dto::CommonPrefix { prefix: Some(p) })
            .collect();
        Ok(S3Response::new(dto::ListMultipartUploadsOutput {
            bucket: Some(String::from(bucket)),
            is_truncated: Some(page.truncated),
            max_uploads: Some(max_uploads as i32),
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
    use std::io;

    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use bytes::Bytes;
    use futures::stream;
    use s3s::{
        S3,
        checksum::ChecksumHasher,
        crypto::{Checksum as _, Sha256},
        dto::{CopySource, StreamingBlob, UploadPartCopyInput},
    };

    use super::*;
    #[cfg(feature = "copy")]
    use crate::_util::testing::body;
    use crate::{
        _core::{
            bucket, object,
            storage::{BucketOps, MultipartOps, ObjectOps},
        },
        _mem::MemoryStorage,
        backend::{
            Capabilities,
            testutil::{s3_request, setup, setup_with_caps},
        },
    };

    /// A backend with the checksum feature on (the default toggle is
    /// off — the tests must opt in).
    async fn setup_checksum() -> (S3Backend<MemoryStorage>, String) {
        setup_with_caps(Capabilities {
            checksum: true,
            ..Default::default()
        })
        .await
    }

    /// The client-side checksum of `data` (the same s3s primitive the
    /// wire uses — the test simulates a real client).
    fn client_checksum(algo: Algorithm, data: &[u8]) -> String {
        let mut hasher = ChecksumHasher::default();
        checksum::enable_algo(&mut hasher, algo);
        hasher.update(data);
        checksum::checksum_value_of(&hasher.finalize(), algo)
            .unwrap()
            .to_string()
    }

    /// The 5 MiB-compliant test parts: two full-size non-final parts
    /// and a small tail (the S3 minimum applies to non-final parts only).
    fn big_parts() -> Vec<Vec<u8>> {
        let min = MIN_PART_BYTES as usize;
        vec![
            vec![b'a'; min + 1],
            vec![b'b'; min + 1],
            b"part-three".to_vec(),
        ]
    }

    /// Create a multipart upload on `backend` (optionally with the
    /// create-time checksum algorithm/type wire names).
    async fn create_upload(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        algo: Option<&str>,
        ty: Option<&str>,
    ) -> String {
        backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                checksum_algorithm: algo.map(|a| a.parse().unwrap()),
                checksum_type: ty.map(|t| t.parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .upload_id
            .unwrap()
    }

    /// A one-chunk part body.
    fn part_body(data: &[u8]) -> Option<StreamingBlob> {
        Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
            Bytes::copy_from_slice(data),
        )])))
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
        assert_eq!(page1.max_uploads, Some(1));
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

    #[cfg(feature = "copy")]
    fn upload_part_copy_input(
        b: &str,
        upload_id: &str,
        part_number: i32,
    ) -> dto::UploadPartCopyInput {
        UploadPartCopyInput::builder()
            .bucket(b.to_string())
            .key("copy.bin".to_string())
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .copy_source(CopySource::parse(&format!("{b}/src.bin")).unwrap())
            .build()
            .unwrap()
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_computes_and_persists_the_checksum() {
        // A create-algorithm upload's copy carries no client checksum on
        // the wire (R1) — the server computes the copied part's checksum
        // (spec D5) and echoes it in CopyPartResult, matching AWS.
        let (backend, b) = setup_checksum().await;
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
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let expected = client_checksum(Algorithm::Crc32, b"0123"); // range 0-3
        let mut input = upload_part_copy_input(&b, &upload_id, 1);
        input.copy_source_range = Some("bytes=0-3".into());
        let part = backend.upload_part_copy(s3_request(input)).await.unwrap();
        assert_eq!(
            part.output
                .copy_part_result
                .as_ref()
                .unwrap()
                .checksum_crc32
                .as_deref(),
            Some(expected.as_str())
        );
        // Persisted → ListParts echo.
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            listed.output.parts.as_ref().unwrap()[0]
                .checksum_crc32
                .as_deref(),
            Some(expected.as_str())
        );
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_without_create_algorithm_keeps_the_fast_path() {
        // A non-algorithm upload's copy keeps the existing copy_part
        // path: no checksum computed, none echoed, none persisted.
        let (backend, b) = setup_checksum().await;
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
        let part = backend.upload_part_copy(s3_request(input)).await.unwrap();
        assert!(
            part.output
                .copy_part_result
                .as_ref()
                .unwrap()
                .checksum_crc32
                .is_none(),
            "no checksum without a create-time algorithm"
        );
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b,
                key: "copy.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(
            listed.output.parts.as_ref().unwrap()[0]
                .checksum_crc32
                .is_none()
        );
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_respects_copy_object_toggle() {
        let storage = MemoryStorage::new().unwrap();
        let backend = S3Backend::new(
            storage,
            Capabilities {
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

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_parts_allow_zero_page_size_restores_the_legacy_empty_page() {
        // The `[s3] allow_zero_page_size` escape hatch: 0 answers the
        // empty page (the legacy behavior), never InvalidArgument.
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                allow_zero_page_size: true,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: "data".into(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: "data".into(),
                key: "big.bin".into(),
                upload_id: create.output.upload_id.unwrap(),
                max_parts: Some(0),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.max_parts, Some(0));
        assert!(out.parts.as_ref().unwrap().is_empty());
        assert_eq!(out.is_truncated, Some(false));
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_multipart_uploads_allow_zero_page_size_restores_the_legacy_empty_page() {
        // The `[s3] allow_zero_page_size` escape hatch: 0 answers the
        // empty page (the legacy behavior), never InvalidArgument.
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                allow_zero_page_size: true,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        storage
            .create_bucket(&bucket::name("data").unwrap())
            .await
            .unwrap();
        let out = backend
            .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                bucket: "data".into(),
                max_uploads: Some(0),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.max_uploads, Some(0));
        assert!(out.uploads.as_ref().unwrap().is_empty());
        assert_eq!(out.is_truncated, Some(false));
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_computes_and_persists_headerless_parts_of_algorithm_uploads() {
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("CRC32"), None).await;
        // Header-less part: computed + persisted, echoed only by ListParts.
        let data = b"hello";
        let expected = client_checksum(Algorithm::Crc32, data);
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: part_body(data),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(
            part.output.checksum_crc32.is_none(),
            "no value in the request → no response echo"
        );
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            listed.output.parts.as_ref().unwrap()[0]
                .checksum_crc32
                .as_deref(),
            Some(expected.as_str())
        );
    }

    /// Upload `parts` with per-part checksum values of `algo`; return
    /// the (etags, client values) in part order.
    async fn upload_parts_with_checksums(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        upload_id: &str,
        algo: Algorithm,
        parts: &[Vec<u8>],
    ) -> (Vec<dto::ETag>, Vec<String>) {
        let mut etags = Vec::new();
        let mut values = Vec::new();
        for (i, data) in parts.iter().enumerate() {
            let value = client_checksum(algo, data);
            let mut input = dto::UploadPartInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                upload_id: upload_id.to_string(),
                part_number: (i + 1) as i32,
                body: part_body(data),
                ..Default::default()
            };
            // The part's checksum value field of `algo` (the same field
            // mapping as the output echo).
            input.set_checksum(algo, &value);
            let part = backend.upload_part(s3_request(input)).await.unwrap();
            etags.push(part.output.e_tag.unwrap());
            values.push(value);
        }
        (etags, values)
    }

    fn complete_input(upload_id: &str, etags: &[dto::ETag]) -> dto::CompleteMultipartUploadInput {
        dto::CompleteMultipartUploadInput {
            bucket: "data".into(),
            key: "big.bin".into(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(dto::CompletedMultipartUpload {
                parts: Some(
                    etags
                        .iter()
                        .enumerate()
                        .map(|(i, e)| dto::CompletedPart {
                            part_number: Some((i + 1) as i32),
                            e_tag: Some(e.clone()),
                            ..Default::default()
                        })
                        .collect(),
                ),
            }),
            ..Default::default()
        }
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_validates_composite_sha256() {
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("SHA256"), None).await;
        let parts = big_parts();
        let (etags, values) =
            upload_parts_with_checksums(&backend, &b, &upload_id, Algorithm::Sha256, &parts).await;
        // The client's COMPOSITE value: SHA-256 over the concatenated
        // raw part digests (the documented construction).
        let mut raw = Vec::new();
        for v in &values {
            raw.extend_from_slice(&STANDARD.decode(v).unwrap());
        }
        let mut h = ChecksumHasher {
            sha256: Some(Sha256::new()),
            ..Default::default()
        };
        h.update(&raw);
        let composite = h.finalize().checksum_sha256.unwrap();

        let mut input = complete_input(&upload_id, &etags);
        input.checksum_sha256 = Some(composite.clone());
        input.checksum_type = Some("COMPOSITE".parse().unwrap());
        let complete = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
        assert_eq!(
            complete.output.checksum_sha256.as_deref(),
            Some(composite.as_str())
        );
        // The object exists (validated pre-commit).
        assert!(
            backend
                .storage()
                .head_object(&bucket::name(&b).unwrap(), &object::key("big.bin").unwrap())
                .await
                .is_ok()
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_validates_full_object_crc32_linearization() {
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("CRC32"), Some("FULL_OBJECT")).await;
        let parts = big_parts();
        let (etags, _) =
            upload_parts_with_checksums(&backend, &b, &upload_id, Algorithm::Crc32, &parts).await;
        // The client's FULL_OBJECT value: the CRC of the concatenated
        // CONTENT (the linearization oracle — independent of the server
        // helper).
        let mut content = Vec::new();
        for p in &parts {
            content.extend_from_slice(p);
        }
        let full = client_checksum(Algorithm::Crc32, &content);

        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some(full.clone());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(content.len() as i64);
        let complete = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
        assert_eq!(
            complete.output.checksum_crc32.as_deref(),
            Some(full.as_str())
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_rejects_algorithm_type_and_size_mismatches() {
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("SHA256"), None).await;
        let parts: Vec<Vec<u8>> = vec![b"data".to_vec()];
        let (etags, _) =
            upload_parts_with_checksums(&backend, &b, &upload_id, Algorithm::Sha256, &parts).await;
        // Value algorithm ≠ create algorithm → InvalidRequest.
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // SHA with FULL_OBJECT → InvalidRequest (algorithm × type table).
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_sha256 = Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(4);
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // FULL_OBJECT without mpu_object_size → InvalidRequest.
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_cross_checks_completed_part_values_without_a_create_algorithm() {
        // W03: the CompletedPart value-vs-stored comparison runs
        // whenever a stored value exists — not only for uploads with a
        // create-time algorithm. A wrong client entry on an
        // algorithm-less upload must answer BadDigest.
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, None, None).await;
        let min = MIN_PART_BYTES as usize;
        let parts: Vec<Vec<u8>> = vec![vec![b'a'; min + 1], b"tail".to_vec()];
        let (etags, values) =
            upload_parts_with_checksums(&backend, &b, &upload_id, Algorithm::Crc32, &parts).await;
        // A wrong entry → BadDigest.
        let mut input = complete_input(&upload_id, &etags);
        if let Some(parts) = input
            .multipart_upload
            .as_mut()
            .and_then(|m| m.parts.as_mut())
        {
            parts[0].checksum_crc32 = Some("y/Q5Jg==".into());
        }
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
        // The matching entry commits (the failed attempt left the
        // upload untouched).
        let mut input = complete_input(&upload_id, &etags);
        if let Some(parts) = input
            .multipart_upload
            .as_mut()
            .and_then(|m| m.parts.as_mut())
        {
            parts[0].checksum_crc32 = Some(values[0].clone());
        }
        backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_skips_completed_part_entries_without_a_stored_checksum() {
        // D2: a client entry whose part has no stored checksum is
        // skipped (warn), not BadDigest — the value cannot be checked.
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, None, None).await;
        // No checksum header → no stored checksum row.
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: part_body(b"data"),
                ..Default::default()
            }))
            .await
            .unwrap();
        let mut input = complete_input(&upload_id, &[part.output.e_tag.unwrap()]);
        if let Some(parts) = input
            .multipart_upload
            .as_mut()
            .and_then(|m| m.parts.as_mut())
        {
            parts[0].checksum_crc32 = Some("y/Q5Jg==".into()); // unchecked
        }
        backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_full_object_size_check_runs_before_the_d2_skip() {
        // W04: the FULL_OBJECT size requirements depend only on the
        // listed part sizes — they must fire even when a part lacks a
        // stored checksum (the D2 skip must not shadow them).
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("CRC32"), None).await;
        // Upload through the storage directly: no stored checksum row
        // (the D2 scenario).
        let part = backend
            .storage()
            .upload_part(
                &bucket::name(&b).unwrap(),
                &object::key("big.bin").unwrap(),
                &upload_id,
                1.into(),
                body(b"data"),
                None,
            )
            .await
            .unwrap();
        let etag = dto::ETag::Strong(part.etag.as_str());
        // FULL_OBJECT without x-amz-mp-object-size → InvalidRequest,
        // even though the stored-checksum gate would skip validation.
        let mut input = complete_input(&upload_id, std::slice::from_ref(&etag));
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // Wrong size → InvalidRequest too.
        let mut input = complete_input(&upload_id, std::slice::from_ref(&etag));
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(999);
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // Correct size → the D2 skip applies (validation is skipped, the
        // upload commits).
        let mut input = complete_input(&upload_id, &[etag]);
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(4);
        backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn checksum_toggle_off_drops_the_headers() {
        // Default caps (checksum off) = today's behavior: accepted and
        // dropped, no validation, no echo.
        let (backend, b) = setup().await;
        let upload_id = create_upload(&backend, &b, None, None).await;
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                checksum_crc32: Some("y/Q5Jg==".into()), // wrong, but ignored
                body: part_body(b"x"),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(part.output.checksum_crc32.is_none());
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(
            listed.output.parts.as_ref().unwrap()[0]
                .checksum_crc32
                .is_none()
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn checksum_toggle_off_accepts_and_drops_part_checksum_entries() {
        // F01: with the toggle off, a CompletedPart carrying TWO checksum
        // fields is accepted and dropped (v1 pass-through) — it must not
        // answer InvalidRequest, and the single-part complete must not
        // force the full snapshot scan.
        let (backend, b) = setup().await;
        let upload_id = create_upload(&backend, &b, None, None).await;
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: part_body(b"hello"),
                ..Default::default()
            }))
            .await
            .unwrap();
        let mut input = complete_input(
            &upload_id,
            std::slice::from_ref(part.output.e_tag.as_ref().unwrap()),
        );
        if let Some(parts) = input
            .multipart_upload
            .as_mut()
            .and_then(|m| m.parts.as_mut())
        {
            parts[0].checksum_crc32 = Some("y/Q5Jg==".into());
            parts[0].checksum_sha256 = Some("DUoRhQ==".into());
        }
        let complete = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
        assert!(complete.output.checksum_crc32.is_none());
        assert!(complete.output.checksum_sha256.is_none());
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn cached_checksum_specs_never_resurrect_aborted_uploads() {
        // F04 guard: the spec cache must not mask existence — after an
        // abort, a part upload whose spec is still cached answers
        // NoSuchUpload (the storage write txn is the existence
        // authority; a stale entry never resurrects a part).
        let (backend, b) = setup_checksum().await;
        let upload_id = create_upload(&backend, &b, Some("SHA256"), None).await;
        // Warm the cache (this part upload reads the spec through).
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: part_body(b"x"),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(part.output.e_tag.is_some());
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
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                part_number: 2,
                body: part_body(b"y"),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchUpload");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn checksum_toggle_off_list_uploads_drops_a_persisted_spec() {
        // F02: a checksum spec persisted by an earlier on-run must not be
        // echoed by ListMultipartUploads while the toggle is off (off =
        // accept-and-drop) — ListParts already gates its echo, the two
        // list ops must agree.
        let (backend, b) = setup().await; // caps.checksum = false
        backend
            .storage
            .create_multipart_upload(
                &bucket::name(&b).unwrap(),
                &object::key("big.bin").unwrap(),
                Some(crate::_core::checksum::Upload {
                    algorithm: Algorithm::Crc32,
                    r#type: None,
                }),
            )
            .await
            .unwrap();
        let listed = backend
            .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                bucket: b,
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload = &listed.output.uploads.as_ref().unwrap()[0];
        assert!(upload.checksum_algorithm.is_none());
        assert!(upload.checksum_type.is_none());
    }
}
