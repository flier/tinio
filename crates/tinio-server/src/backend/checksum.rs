//! Server-side checksum validation for multipart uploads (spec
//! 2026-08-31-multipart-checksum-validation-design.md).
//!
//! One home for every checksum computation of the mapping layer: the
//! [`VerifyStream`] body tee (s3s `ChecksumHasher`, md5 + the requested
//! algorithm enabled together — one streaming pass with the backend's
//! staging write), the request-spec parsing, and the COMPOSITE /
//! FULL_OBJECT full-object derivations. The storage backends never
//! hash.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::header::HeaderName;
use s3s::{
    S3Result, TrailingHeaders,
    checksum::ChecksumHasher,
    crypto::{
        Checksum as _, Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256, Sha512, XxHash3, XxHash64,
        XxHash128,
    },
    dto, s3_error,
};

use crate::_core::{
    BodyStream,
    checksum::{self, Algorithm},
};

/// The shared outcome of a [`VerifyStream`]: the computed digest (in
/// the storage-commit slot) and whether an expected value failed to
/// match.
#[derive(Debug)]
pub(crate) struct VerifyState {
    /// The digest slot the storage commits atomically with the part row
    /// (written exactly once at stream end — a write-once cell, no
    /// lock). The [`checksum::PartChecksum::etag`] cell supplies the
    /// part ETag when the tee hashes MD5.
    slot: Arc<checksum::PartChecksum>,
    mismatched: AtomicBool,
}

impl VerifyState {
    /// A fresh shared outcome; `etag_md5` when the tee computes MD5 over
    /// the body (a part's ETag IS its content MD5 — the backend skips
    /// its own hash). The raw digest lands in the cell at stream end.
    pub(crate) fn new(etag_md5: bool) -> Self {
        Self {
            slot: Arc::new(checksum::PartChecksum {
                etag: etag_md5.then(OnceLock::new),
                ..Default::default()
            }),
            mismatched: AtomicBool::new(false),
        }
    }

    /// The digest slot — the backend's atomic-commit handle.
    pub(crate) fn slot(&self) -> Arc<checksum::PartChecksum> {
        Arc::clone(&self.slot)
    }

    /// The computed digest of the wrapped stream (finalized at stream
    /// end; `None` until then).
    pub(crate) fn computed(&self) -> Option<checksum::Value> {
        self.slot.digest.get().map(|part| part.value.clone())
    }

    /// Whether the stream ended with a checksum mismatch (the op maps
    /// the backend's wrapped error to `BadDigest` when this is set).
    pub(crate) fn mismatched(&self) -> bool {
        self.mismatched.load(Ordering::Relaxed)
    }
}

/// One checksum specification of an upload-part-like request (spec
/// 2026-08-31).
#[derive(Debug, Clone)]
pub(crate) struct Spec {
    /// The algorithm whose computed value is persisted/echoed. `None`
    /// when only `Content-MD5` is present.
    pub(crate) algorithm: Option<checksum::Algorithm>,
    /// The expected value of `algorithm` from the request headers/fields
    /// (`None` = compute-only, or the value arrives via a trailer).
    pub(crate) expected: Option<checksum::Value>,
    /// The declared aws-chunked trailer carrying the value: the trailer
    /// name fixes the algorithm at parse time, the VALUE is read from
    /// the verified trailing-headers map at stream end (R4).
    pub(crate) trailer_algo: Option<checksum::Algorithm>,
    /// The legacy `Content-MD5` (validated, never persisted).
    pub(crate) content_md5: Option<checksum::Value>,
}

impl Spec {
    /// A compute-only spec: hash with `algo`, validate nothing, echo
    /// nothing — the server-side computation of a create-algorithm
    /// upload (a header-less part, a copy part; spec D5).
    pub(crate) fn compute_only(algo: checksum::Algorithm) -> Spec {
        Spec {
            algorithm: Some(algo),
            expected: None,
            trailer_algo: None,
            content_md5: None,
        }
    }

    /// Parse the checksum sources of an `UploadPart` request: exactly
    /// one `checksum_<algo>` DTO field, or an aws-chunked trailer
    /// declared by `x-amz-trailer` (the algorithm comes from the
    /// declared trailer name; the value arrives at stream end), or
    /// nothing. `Content-MD5` is an independent legacy check that may
    /// coexist. More than one algorithm value source → `InvalidRequest`;
    /// an `x-amz-checksum-algorithm` header with no value source at all
    /// → `InvalidRequest` (S3: 400).
    pub(crate) fn from_upload_part(
        input: &dto::UploadPartInput,
        headers: &http::HeaderMap,
    ) -> S3Result<Option<Spec>> {
        Self::parse(
            single_checksum_value(input)?,
            input.content_md5.as_deref(),
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            declared_trailer(headers)?,
        )
    }

    /// The checksum-value sources of one request: the value fields
    /// (`single_checksum_value` already rejected a second), `Content-MD5`,
    /// the `x-amz-checksum-algorithm` header, and the declared
    /// aws-chunked trailer (the algorithm half only — the value
    /// arrives at stream end).
    fn parse(
        found: Option<(checksum::Algorithm, &str)>,
        content_md5: Option<&str>,
        algorithm_header: Option<&str>,
        trailer_algo: Option<checksum::Algorithm>,
    ) -> S3Result<Option<Spec>> {
        // A value field and a declared trailer are two sources.
        if found.is_some() && trailer_algo.is_some() {
            return Err(s3_error!(
                InvalidRequest,
                "more than one checksum value in one request"
            ));
        }
        let expected = found.map(|(algo, value)| (algo, checksum::Value(value.to_string())));
        // Per AWS, an individual checksum wins over the algorithm
        // header; the trailer name is authoritative for a trailer.
        let algorithm = expected.as_ref().map(|(a, _)| *a).or(trailer_algo);
        // An algorithm header without any value source → InvalidRequest
        // (S3: 400 — "there must be a corresponding x-amz-checksum or
        // x-amz-trailer header").
        if algorithm_header.is_some() && algorithm.is_none() {
            return Err(s3_error!(
                InvalidRequest,
                "checksum algorithm without a checksum value"
            ));
        }
        Ok(if algorithm.is_none() && content_md5.is_none() {
            None
        } else {
            Some(Spec {
                algorithm,
                expected: expected.map(|(_, v)| v),
                trailer_algo,
                content_md5: content_md5.map(|v| checksum::Value(v.to_string())),
            })
        })
    }
}

/// The checksum algorithm declared by the `x-amz-trailer` header of an
/// aws-chunked request (the declaration is a request header; the value
/// arrives in the verified trailing-headers map at stream end). Trailer
/// names are HTTP field names — case-insensitive — and HTTP allows the
/// header to repeat; both are honored (a mixed-case or repeated
/// declaration must not silently disable validation). A second
/// checksum trailer → `InvalidRequest`.
fn declared_trailer(headers: &http::HeaderMap) -> S3Result<Option<checksum::Algorithm>> {
    let mut declared = None;
    let name = HeaderName::from_static("x-amz-trailer");
    for value in headers.get_all(&name) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for trailer in value.split(',').map(str::trim) {
            if let Some(algo) = Algorithm::ALL
                .iter()
                .copied()
                .find(|algo| checksum_header_name(*algo).eq_ignore_ascii_case(trailer))
            {
                if declared.is_some() {
                    return Err(s3_error!(
                        InvalidRequest,
                        "more than one declared checksum trailer"
                    ));
                }
                declared = Some(algo);
            }
        }
    }
    Ok(declared)
}

/// The one-method trait that lets an output set its algorithm's value
/// field across the DTO shapes that carry the checksum value headers
/// (`UploadPartOutput`, `CompleteMultipartUploadOutput`, `dto::Part`,
/// `dto::CopyPartResult`).
pub(crate) trait HasFields {
    fn set_checksum(&mut self, algo: checksum::Algorithm, value: &str);
}

/// The `checksum_<algo>` value fields are identical across the
/// output shapes — one macro, four impls.
macro_rules! impl_checksum_fields {
    ($ty:ty) => {
        impl HasFields for $ty {
            fn set_checksum(&mut self, algo: checksum::Algorithm, value: &str) {
                match algo {
                    Algorithm::Crc32 => self.checksum_crc32 = Some(value.into()),
                    Algorithm::Crc32C => self.checksum_crc32c = Some(value.into()),
                    Algorithm::Crc64Nvme => self.checksum_crc64nvme = Some(value.into()),
                    Algorithm::Sha1 => self.checksum_sha1 = Some(value.into()),
                    Algorithm::Sha256 => self.checksum_sha256 = Some(value.into()),
                    Algorithm::Sha512 => self.checksum_sha512 = Some(value.into()),
                    Algorithm::Md5 => self.checksum_md5 = Some(value.into()),
                    Algorithm::XxHash64 => self.checksum_xxhash64 = Some(value.into()),
                    Algorithm::XxHash3 => self.checksum_xxhash3 = Some(value.into()),
                    Algorithm::XxHash128 => self.checksum_xxhash128 = Some(value.into()),
                }
            }
        }
    };
}

impl_checksum_fields!(dto::UploadPartOutput);
impl_checksum_fields!(dto::CompleteMultipartUploadOutput);
impl_checksum_fields!(dto::Part);
impl_checksum_fields!(dto::CopyPartResult);
// The request DTO carries the same ten value fields — the test helper
// sets the part's field with the same mapping.
impl_checksum_fields!(dto::UploadPartInput);

/// The s3s wire algorithm of a [`checksum::Algorithm`].
pub(crate) fn wire_algo(algo: checksum::Algorithm) -> dto::ChecksumAlgorithm {
    dto::ChecksumAlgorithm::from_static(match algo {
        Algorithm::Crc32 => dto::ChecksumAlgorithm::CRC32,
        Algorithm::Crc32C => dto::ChecksumAlgorithm::CRC32C,
        Algorithm::Crc64Nvme => dto::ChecksumAlgorithm::CRC64NVME,
        Algorithm::Sha1 => dto::ChecksumAlgorithm::SHA1,
        Algorithm::Sha256 => dto::ChecksumAlgorithm::SHA256,
        Algorithm::Sha512 => dto::ChecksumAlgorithm::SHA512,
        Algorithm::Md5 => dto::ChecksumAlgorithm::MD5,
        Algorithm::XxHash64 => dto::ChecksumAlgorithm::XXHASH64,
        Algorithm::XxHash3 => dto::ChecksumAlgorithm::XXHASH3,
        Algorithm::XxHash128 => dto::ChecksumAlgorithm::XXHASH128,
    })
}

/// The s3s wire checksum type of a [`checksum::Type`].
pub(crate) fn wire_type(ty: checksum::Type) -> dto::ChecksumType {
    dto::ChecksumType::from_static(match ty {
        checksum::Type::Composite => dto::ChecksumType::COMPOSITE,
        checksum::Type::FullObject => dto::ChecksumType::FULL_OBJECT,
    })
}

/// The `x-amz-checksum-<algo>` header name of an algorithm.
pub(crate) fn checksum_header_name(algo: checksum::Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32 => "x-amz-checksum-crc32",
        Algorithm::Crc32C => "x-amz-checksum-crc32c",
        Algorithm::Crc64Nvme => "x-amz-checksum-crc64nvme",
        Algorithm::Sha1 => "x-amz-checksum-sha1",
        Algorithm::Sha256 => "x-amz-checksum-sha256",
        Algorithm::Sha512 => "x-amz-checksum-sha512",
        Algorithm::Md5 => "x-amz-checksum-md5",
        Algorithm::XxHash64 => "x-amz-checksum-xxhash64",
        Algorithm::XxHash3 => "x-amz-checksum-xxhash3",
        Algorithm::XxHash128 => "x-amz-checksum-xxhash128",
    }
}

/// The `checksum_<algo>` value fields of a request or part DTO,
/// indexed by algorithm — one impl per DTO shape (the mirror of
/// [`HasFields`]).
pub(crate) trait ValueFields {
    fn checksum_value(&self, algo: checksum::Algorithm) -> Option<&str>;
}

/// The value fields are identical across the request/part shapes —
/// one macro, three impls.
macro_rules! impl_checksum_value_fields {
    ($ty:ty) => {
        impl ValueFields for $ty {
            fn checksum_value(&self, algo: checksum::Algorithm) -> Option<&str> {
                match algo {
                    Algorithm::Crc32 => self.checksum_crc32.as_deref(),
                    Algorithm::Crc32C => self.checksum_crc32c.as_deref(),
                    Algorithm::Crc64Nvme => self.checksum_crc64nvme.as_deref(),
                    Algorithm::Sha1 => self.checksum_sha1.as_deref(),
                    Algorithm::Sha256 => self.checksum_sha256.as_deref(),
                    Algorithm::Sha512 => self.checksum_sha512.as_deref(),
                    Algorithm::Md5 => self.checksum_md5.as_deref(),
                    Algorithm::XxHash64 => self.checksum_xxhash64.as_deref(),
                    Algorithm::XxHash3 => self.checksum_xxhash3.as_deref(),
                    Algorithm::XxHash128 => self.checksum_xxhash128.as_deref(),
                }
            }
        }
    };
}

impl_checksum_value_fields!(dto::UploadPartInput);
impl_checksum_value_fields!(dto::CompletedPart);
impl_checksum_value_fields!(dto::CompleteMultipartUploadInput);
impl_checksum_value_fields!(dto::Checksum);

/// Exactly one checksum value across the algorithms of a request or
/// part DTO (a second → `InvalidRequest`).
pub(crate) fn single_checksum_value<T: ValueFields>(
    input: &T,
) -> S3Result<Option<(checksum::Algorithm, &str)>> {
    let mut found = None;
    for algo in Algorithm::ALL {
        if let Some(value) = input.checksum_value(algo) {
            if found.is_some() {
                return Err(s3_error!(
                    InvalidRequest,
                    "more than one checksum value in one request"
                ));
            }
            found = Some((algo, value));
        }
    }
    Ok(found)
}

/// Enable the algorithm's slot on a hasher (one home for the slot
/// mapping).
pub(crate) fn enable_algo(hasher: &mut ChecksumHasher, algo: checksum::Algorithm) {
    match algo {
        Algorithm::Crc32 => hasher.crc32 = Some(Crc32::new()),
        Algorithm::Crc32C => hasher.crc32c = Some(Crc32c::new()),
        Algorithm::Crc64Nvme => hasher.crc64nvme = Some(Crc64Nvme::new()),
        Algorithm::Sha1 => hasher.sha1 = Some(Sha1::new()),
        Algorithm::Sha256 => hasher.sha256 = Some(Sha256::new()),
        Algorithm::Sha512 => hasher.sha512 = Some(Sha512::new()),
        Algorithm::Md5 => hasher.md5 = Some(Md5::new()),
        Algorithm::XxHash64 => hasher.xxhash64 = Some(XxHash64::new()),
        Algorithm::XxHash3 => hasher.xxhash3 = Some(XxHash3::new()),
        Algorithm::XxHash128 => hasher.xxhash128 = Some(XxHash128::new()),
    }
}

/// The wrapper stream: update the hasher per chunk, finalize at stream
/// end, compare the expected values (the declared trailer's value is
/// read from the verified trailing-headers map there — R4), and fail the
/// stream on mismatch so the consuming backend aborts staging (the part
/// is never committed).
pub(crate) struct VerifyStream {
    inner: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>,
    hasher: ChecksumHasher,
    spec: Spec,
    trailing: Option<TrailingHeaders>,
    state: Arc<VerifyState>,
    finished: bool,
}

impl VerifyStream {
    /// Wrap `body` per `spec`; the shared `state` is finalized at
    /// stream end and readable by the op afterwards. `trailing` is the
    /// aws-chunked trailing-headers handle (read only at stream end).
    pub(crate) fn wrap(
        body: BodyStream,
        spec: &Spec,
        trailing: Option<&TrailingHeaders>,
        state: &Arc<VerifyState>,
    ) -> BodyStream {
        let mut hasher = ChecksumHasher::default();
        if let Some(algo) = spec.algorithm {
            enable_algo(&mut hasher, algo);
        }
        // Content-MD5 and `x-amz-checksum-md5` share the md5 slot; the
        // slot is enabled even when only Content-MD5 is present.
        if spec.content_md5.is_some() && spec.algorithm != Some(Algorithm::Md5) {
            enable_algo(&mut hasher, Algorithm::Md5);
        }
        Box::pin(VerifyStream {
            inner: body,
            hasher,
            spec: spec.clone(),
            trailing: trailing.cloned(),
            state: Arc::clone(state),
            finished: false,
        })
    }

    /// The declared trailer's value, once the body stream delivered it
    /// (verified by s3s's aws-chunked machinery).
    fn trailer_value(&self) -> Option<checksum::Value> {
        let algo = self.spec.trailer_algo?;
        let trailing = self.trailing.as_ref()?;
        trailing
            .read(|map| {
                let name = HeaderName::from_static(checksum_header_name(algo));
                map.get(&name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            })
            .flatten()
            .map(checksum::Value)
    }
}

impl Stream for VerifyStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                self.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                self.finished = true;
                // `finalize` consumes the hasher — take it out of the
                // pinned stream (the stream is done).
                let checksum = std::mem::take(&mut self.hasher).finalize();
                let computed = self
                    .spec
                    .algorithm
                    .and_then(|a| checksum_value_of(&checksum, a))
                    .map(|v| checksum::Value(v.to_string()));
                // The declared trailer's value arrives only at stream
                // end (R4) — read it before the comparison.
                let trailer_value = self.trailer_value();
                let expected = self.spec.expected.as_ref().or(trailer_value.as_ref());
                let algo_ok = match (expected, computed.as_ref()) {
                    (Some(expected), Some(computed)) => expected.as_str() == computed.as_str(),
                    // A declared trailer that never arrived is a
                    // mismatch, not a skip.
                    (None, _) if self.spec.trailer_algo.is_none() => true,
                    (Some(_), None) | (None, _) => false,
                };
                let md5 = checksum.checksum_md5.as_deref();
                let md5_ok = match (&self.spec.content_md5, md5) {
                    (Some(expected), Some(computed)) => expected.as_str() == computed,
                    (None, _) => true,
                    (Some(_), None) => false,
                };
                // Fill the storage-commit slot (the backend persists it
                // in the same transaction as the part row) — move the
                // computed value in, no clone.
                if algo_ok
                    && md5_ok
                    && let (Some(algo), Some(computed)) = (self.spec.algorithm, computed)
                {
                    let _ = self.state.slot.digest.set(checksum::Part {
                        algorithm: algo,
                        value: computed,
                    });
                }
                // The MD5 slot doubles as the part ETag (F05): fill the
                // etag cell whenever the tee hashed MD5 — the Content-MD5
                // check enables the slot even when the algorithm slot
                // holds a different one, and that digest must not be
                // thrown away (the backend would hash the body again).
                if md5_ok
                    && let Some(raw) = md5.and_then(|m| checksum::Value(m.to_string()).md5_raw())
                    && let Some(cell) = self.state.slot.etag.as_ref()
                {
                    let _ = cell.set(raw);
                }
                if algo_ok && md5_ok {
                    Poll::Ready(None)
                } else {
                    self.state.mismatched.store(true, Ordering::Relaxed);
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "checksum mismatch",
                    ))))
                }
            }
        }
    }
}

/// The digest value of one algorithm inside a finalized
/// `s3s::checksum::ChecksumHasher` result (the `dto::Checksum` value
/// fields are the same ten as the request/part DTOs — one macro).
pub(crate) fn checksum_value_of(
    checksum: &dto::Checksum,
    algo: checksum::Algorithm,
) -> Option<&str> {
    checksum.checksum_value(algo)
}

/// COMPOSITE: the algorithm over the concatenation of the raw part
/// digest bytes (the documented S3 construction; the AWS Java example
/// applies it to SHA-256). `None` when any part value is not valid
/// base64 — the caller skips validation (deviation D2).
pub(crate) fn compose_composite(
    algo: checksum::Algorithm,
    parts: &[&checksum::Part],
) -> Option<checksum::Value> {
    // The digest bytes are only consumed in order — update the hasher
    // directly instead of materializing the concatenation.
    let mut hasher = ChecksumHasher::default();
    enable_algo(&mut hasher, algo);
    for part in parts {
        hasher.update(&part.value.raw()?);
    }
    checksum_value_of(&hasher.finalize(), algo).map(|v| checksum::Value(v.to_string()))
}

/// FULL_OBJECT (CRC family only): combine the part CRCs with
/// carryless-multiplication matrices so the result equals the CRC of
/// the concatenated content — the S3 linearization (spec "AWS wire
/// facts": "S3 can compute the checksum of the whole object from the
/// part-level checksums"). `None` on invalid input.
///
/// The reflected polynomial constants: CRC-32 0xEDB88320, CRC-32C
/// 0x82F63B78, CRC-64/NVMe 0x9A6C9329AC4BC9B5 (poly
/// 0xad93d23594c93659 bit-reversed). The zlib-style gf2-matrix combine:
/// `combine(crc_a, crc_b, len_b)` returns the CRC of `a || b` for the
/// algorithm's register semantics (the s3s wire digest is the
/// big-endian register value). The self-validating test
/// (`linearize_matches_the_direct_crc_of_concatenated_content`) is the
/// oracle — a wrong constant or endianness fails it.
pub(crate) fn linearize_full_object(
    algo: checksum::Algorithm,
    parts: &[&checksum::Part],
    part_sizes: &[u64],
) -> Option<checksum::Value> {
    let width: usize = match algo {
        Algorithm::Crc32 | Algorithm::Crc32C => 4,
        Algorithm::Crc64Nvme => 8,
        _ => return None, // SHA/MD5/XXHash have no linearization (S3: FULL_OBJECT is CRC-only)
    };
    // The combine chain starts at the CRC of the empty content: for the
    // all-ones init/xorout convention the empty CRC is `init ^ xorout`
    // = 0, and `combine(0, crc_b, len)` = `crc_b` (advance of 0).
    // The combine matrices are memoized per `(algo, len)`: FULL_OBJECT
    // parts are almost always equal-sized (aws-cli's 8 MiB default
    // chunking), so one slot covers the common case and the per-part
    // rebuild (a few thousand multiply-xors) is skipped.
    let mut combine_cache = None;
    let mut crc = 0u64;
    for (part, size) in parts.iter().zip(part_sizes) {
        let bytes = part.value.raw()?;
        if bytes.len() != width {
            return None;
        }
        // The wire digest is the big-endian register value (s3s).
        let next = bytes.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
        crc = crc_combine(algo, crc, next, *size, &mut combine_cache);
    }
    let digest = match algo {
        Algorithm::Crc32 | Algorithm::Crc32C => (crc as u32).to_be_bytes().to_vec(),
        Algorithm::Crc64Nvme => crc.to_be_bytes().to_vec(),
        _ => unreachable!("linearize is CRC-only"),
    };
    Some(checksum::Value(STANDARD.encode(digest)))
}

/// The memoized combine matrices of one `(algo, len)` — the chain
/// `M_k = square^k(advance-by-one-bit)`, LSB first (the two pre-squares
/// make the first entry the advance-by-one-byte operator, zlib's
/// `crc32_combine` construction).
type CombineCache = Option<((checksum::Algorithm, u64), Vec<[u64; 64]>)>;

/// The matrix chain of `crc_combine` for one `(algo, len)`.
fn combine_matrices(algo: checksum::Algorithm, len: u64) -> Vec<[u64; 64]> {
    let (poly, _) = crc_params(algo);
    // The "advance by one bit" operator of the reflected register
    // domain: row 0 is the reflected polynomial, row n is bit n-1
    // (zlib's `odd` matrix). Squaring doubles the advance.
    let mut m = [0u64; 64];
    m[0] = poly;
    let mut row = 1u64;
    for slot in &mut m[1..] {
        *slot = row;
        row <<= 1;
    }
    // The two pre-squares align the chain with the zlib loop (the first
    // in-loop matrix advances one byte = eight bits); one entry per bit
    // of the byte length follows, LSB first.
    let bitlen = 64 - len.leading_zeros() as usize;
    for _ in 0..2 {
        let mut next = m;
        gf2_matrix_square(&mut next, &m);
        m = next;
    }
    let mut mats = Vec::with_capacity(bitlen);
    for _ in 0..bitlen {
        let mut next = m;
        gf2_matrix_square(&mut next, &m);
        m = next;
        mats.push(m);
    }
    mats
}

/// One step of the carryless-multiplication combine: given the CRC of
/// `a`, the CRC of `b`, and the length of `b`, the CRC of `a || b`
/// (reflected algorithms — matrix exponentiation by length, the zlib
/// `crc32_combine` construction; the constants per algorithm). Advance
/// `crc_a` by `len_b` bytes, one bit of the byte length at a time, then
/// xor `crc_b` (the all-ones init/xorout convention makes the xor the
/// complete combination). The matrix chain comes from `cache` (the
/// caller's single-slot memoization of [`combine_matrices`]).
fn crc_combine(
    algo: checksum::Algorithm,
    crc_a: u64,
    crc_b: u64,
    len_b: u64,
    cache: &mut CombineCache,
) -> u64 {
    if cache.as_ref().is_none_or(|(key, _)| *key != (algo, len_b)) {
        *cache = Some(((algo, len_b), combine_matrices(algo, len_b)));
    }
    let mats = &cache.as_ref().expect("just set").1;
    let (_, mask) = crc_params(algo);
    let mut crc = crc_a;
    let mut len = len_b;
    for m in mats.iter() {
        if len & 1 != 0 {
            crc = gf2_matrix_times(m, crc);
        }
        len >>= 1;
    }
    (crc ^ crc_b) & mask
}

/// Multiply a register value by a gf2 matrix (bits of `vec` select the
/// rows).
fn gf2_matrix_times(mat: &[u64; 64], mut vec: u64) -> u64 {
    let mut sum = 0u64;
    let mut i = 0;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    sum
}

/// Square a gf2 matrix (the doubled advance).
fn gf2_matrix_square(square: &mut [u64; 64], mat: &[u64; 64]) {
    for (n, s) in square.iter_mut().enumerate() {
        *s = gf2_matrix_times(mat, mat[n]);
    }
}

/// The reflected polynomial and register-width mask of a CRC algorithm.
fn crc_params(algo: checksum::Algorithm) -> (u64, u64) {
    match algo {
        Algorithm::Crc32 => (0xEDB88320, u64::MAX >> 32),
        Algorithm::Crc32C => (0x82F63B78, u64::MAX >> 32),
        Algorithm::Crc64Nvme => (0x9A6C9329AC4BC9B5, u64::MAX),
        _ => unreachable!("linearize is CRC-only"),
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    /// The wire base64 of a raw digest.
    fn b64(bytes: &[u8]) -> String {
        STANDARD.encode(bytes)
    }

    /// The raw digest of one CRC algorithm over `data` — the s3s
    /// hasher, independent of the linearization code under test.
    fn crc_raw(algo: checksum::Algorithm, data: &[u8]) -> Vec<u8> {
        let mut h = ChecksumHasher::default();
        enable_algo(&mut h, algo);
        h.update(data);
        let finalized = h.finalize();
        let sum = checksum_value_of(&finalized, algo).unwrap();
        STANDARD.decode(sum).unwrap()
    }

    #[test]
    fn linearize_matches_the_direct_crc_of_concatenated_content() {
        // The self-validating oracle: split random content into random
        // parts, CRC each part, linearize, and compare with the direct
        // CRC of the concatenation — they must be identical for every
        // CRC algorithm. A wrong polynomial constant or endianness
        // convention fails this test.
        let mut content = Vec::new();
        let mut state = 0x9E3779B97F4A7C15u64; // deterministic PRNG
        for _ in 0..4096 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            content.push(state as u8);
        }
        for algo in [Algorithm::Crc32, Algorithm::Crc32C, Algorithm::Crc64Nvme] {
            let mut parts: Vec<checksum::Part> = Vec::new();
            let mut sizes: Vec<u64> = Vec::new();
            let mut cursor = 0usize;
            let mut cut = 0u64;
            while cursor < content.len() {
                cut = cut.wrapping_mul(31).wrapping_add(97) % 700;
                let end = (cursor + cut as usize + 1).min(content.len());
                let part = &content[cursor..end];
                sizes.push((end - cursor) as u64);
                parts.push(checksum::Part {
                    algorithm: algo,
                    value: checksum::Value(b64(&crc_raw(algo, part))),
                });
                cursor = end;
            }
            let parts: Vec<_> = parts.iter().collect();
            let linearized = linearize_full_object(algo, &parts, &sizes).unwrap();
            let direct = b64(&crc_raw(algo, &content));
            assert_eq!(linearized.as_str(), direct, "algorithm {algo}");
        }
    }

    #[test]
    fn known_crc32_check_value() {
        // The standard CRC-32/IEEE check value: crc32("123456789") =
        // 0xCBF43926 → base64 "y/Q5Jg==".
        let mut h = ChecksumHasher {
            crc32: Some(Crc32::new()),
            ..Default::default()
        };
        h.update(b"123456789");
        assert_eq!(h.finalize().checksum_crc32.unwrap(), "y/Q5Jg==");
    }

    #[test]
    fn compose_composite_is_the_algorithm_over_concatenated_digests() {
        let mut h = ChecksumHasher {
            sha256: Some(Sha256::new()),
            ..Default::default()
        };
        h.update(b"alpha");
        let a = h.finalize().checksum_sha256.unwrap();
        let mut h = ChecksumHasher {
            sha256: Some(Sha256::new()),
            ..Default::default()
        };
        h.update(b"beta");
        let b = h.finalize().checksum_sha256.unwrap();
        let parts = [
            checksum::Part {
                algorithm: Algorithm::Sha256,
                value: checksum::Value(a.clone()),
            },
            checksum::Part {
                algorithm: Algorithm::Sha256,
                value: checksum::Value(b.clone()),
            },
        ];
        let parts: Vec<_> = parts.iter().collect();
        let composed = compose_composite(Algorithm::Sha256, &parts).unwrap();
        // The documented construction: SHA-256 over the concatenation of
        // the RAW part digests.
        let mut raw = Vec::new();
        raw.extend_from_slice(&STANDARD.decode(&a).unwrap());
        raw.extend_from_slice(&STANDARD.decode(&b).unwrap());
        let mut h = ChecksumHasher {
            sha256: Some(Sha256::new()),
            ..Default::default()
        };
        h.update(&raw);
        assert_eq!(composed.as_str(), h.finalize().checksum_sha256.unwrap());
    }

    #[test]
    fn parse_rejects_two_value_fields_and_bare_algorithm() {
        use http::HeaderMap;
        use s3s::dto::UploadPartInput;

        // The value-source of the parse under test (the single-value
        // scan already rejected a second).
        fn found<'a>(
            crc32: Option<&'a str>,
            sha256: Option<&'a str>,
        ) -> Option<(checksum::Algorithm, &'a str)> {
            match (crc32, sha256) {
                (Some(v), None) => Some((Algorithm::Crc32, v)),
                (None, Some(v)) => Some((Algorithm::Sha256, v)),
                (None, None) => None,
                (Some(_), Some(_)) => unreachable!("the single-value scan rejects this"),
            }
        }
        // Two value fields → InvalidRequest (the single-value scan).
        let err = Spec::from_upload_part(
            &UploadPartInput {
                checksum_crc32: Some("y/Q5Jg==".into()),
                checksum_sha256: Some("y/Q5Jg==".into()),
                ..Default::default()
            },
            &HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // Algorithm header without any value source → InvalidRequest.
        let err = Spec::parse(found(None, None), None, Some("CRC32"), None).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // Algorithm header + declared trailer → valid (the value comes
        // at stream end).
        let spec = Spec::parse(
            found(None, None),
            None,
            Some("CRC32"),
            Some(Algorithm::Crc32),
        )
        .unwrap()
        .unwrap();
        assert_eq!(spec.algorithm, Some(Algorithm::Crc32));
        assert!(spec.expected.is_none());
        assert_eq!(spec.trailer_algo, Some(Algorithm::Crc32));
        // Nothing at all → no spec.
        assert!(
            Spec::parse(found(None, None), None, None, None)
                .unwrap()
                .is_none()
        );
        // Content-MD5 alone → a spec with no algorithm.
        let spec = Spec::parse(
            found(None, None),
            Some("kAFQmDzST7DWlj99KOF/cg=="),
            None,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(spec.algorithm, None);
        assert_eq!(
            spec.content_md5.as_ref().unwrap().as_str(),
            "kAFQmDzST7DWlj99KOF/cg=="
        );
        // The declared-trailer detection reads `x-amz-trailer`.
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-trailer", "x-amz-checksum-crc32".parse().unwrap());
        let input = UploadPartInput::default();
        let spec = Spec::from_upload_part(&input, &headers).unwrap().unwrap();
        assert_eq!(spec.trailer_algo, Some(Algorithm::Crc32));
    }

    #[test]
    fn declared_trailers_are_case_insensitive_and_may_repeat() {
        // F7/F8: trailer names are HTTP field names (case-insensitive)
        // and the header may repeat — a mixed-case or second-line
        // declaration must not silently disable validation.
        use http::HeaderMap;
        use s3s::dto::UploadPartInput;

        // Mixed case.
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-trailer", "X-Amz-Checksum-Crc32".parse().unwrap());
        let spec = Spec::from_upload_part(&UploadPartInput::default(), &headers)
            .unwrap()
            .unwrap();
        assert_eq!(spec.trailer_algo, Some(Algorithm::Crc32));

        // The checksum trailer on a SECOND x-amz-trailer line (the first
        // declares the signature trailer).
        let mut headers = HeaderMap::new();
        headers.append("x-amz-trailer", "x-amz-checksum-sha256".parse().unwrap());
        headers.append("x-amz-trailer", "x-amz-chunk-signature".parse().unwrap());
        let spec = Spec::from_upload_part(&UploadPartInput::default(), &headers)
            .unwrap()
            .unwrap();
        assert_eq!(spec.trailer_algo, Some(Algorithm::Sha256));

        // Two checksum trailers declared → InvalidRequest.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-trailer",
            "x-amz-checksum-crc32, x-amz-checksum-sha256"
                .parse()
                .unwrap(),
        );
        let err = Spec::from_upload_part(&UploadPartInput::default(), &headers).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    /// The raw 16-byte MD5 of `data` (the s3s hasher — the same digest
    /// the tee computes).
    fn md5_raw(data: &[u8]) -> [u8; 16] {
        let mut h = ChecksumHasher {
            md5: Some(Md5::new()),
            ..Default::default()
        };
        h.update(data);
        checksum::Value(h.finalize().checksum_md5.unwrap())
            .md5_raw()
            .expect("the s3s md5 is valid and 16 bytes")
    }

    /// Drain one wrapped stream to its end (the finalize happens at the
    /// final `None`).
    async fn drain(body: BodyStream) {
        let mut body = body;
        while let Some(chunk) = body.next().await {
            chunk.unwrap();
        }
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn tee_fills_the_etag_cell_whenever_it_hashes_md5() {
        // F05: the tee hashes MD5 for a Content-MD5 check even when the
        // algorithm slot holds a different one — the raw digest must
        // land in the etag cell (the backend reuses it as the part ETag
        // instead of hashing the body a second time), while the digest
        // slot keeps the algorithm's value.
        let state = Arc::new(VerifyState::new(true));
        let spec = Spec {
            algorithm: Some(Algorithm::Sha256),
            expected: None,
            trailer_algo: None,
            content_md5: Some(checksum::Value(b64(&md5_raw(b"hello world")))),
        };
        let body = VerifyStream::wrap(
            Box::pin(stream::iter(vec![Ok::<_, io::Error>(Bytes::from_static(
                b"hello world",
            ))])),
            &spec,
            None,
            &state,
        );
        drain(body).await;
        let slot = state.slot();
        assert_eq!(
            slot.etag.as_ref().and_then(|c| c.get()),
            Some(&md5_raw(b"hello world")),
            "the Content-MD5 digest must be exposed as the part ETag"
        );
        assert_eq!(
            slot.digest.get().map(|p| p.algorithm),
            Some(Algorithm::Sha256),
            "the digest slot keeps the algorithm's value"
        );

        // No Content-MD5 and no MD5 algorithm → no etag promise at all
        // (the backend hashes for the ETag itself).
        let state = Arc::new(VerifyState::new(false));
        let spec = Spec {
            algorithm: Some(Algorithm::Sha256),
            expected: None,
            trailer_algo: None,
            content_md5: None,
        };
        let body = VerifyStream::wrap(
            Box::pin(stream::iter(vec![Ok::<_, io::Error>(Bytes::from_static(
                b"hello world",
            ))])),
            &spec,
            None,
            &state,
        );
        drain(body).await;
        let slot = state.slot();
        assert!(slot.etag.is_none(), "no MD5 hashed → no etag cell");
        assert_eq!(
            slot.digest.get().map(|p| p.algorithm),
            Some(Algorithm::Sha256)
        );
    }
}
