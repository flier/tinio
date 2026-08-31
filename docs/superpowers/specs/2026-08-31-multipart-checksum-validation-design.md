# Design: Multipart upload checksum validation

**Date**: 2026-08-31
**Status**: implemented and verified (2026-08-31) — design review rounds 1–2 incorporated; see [Review log](#review-log-2026-08-31-round-2) before planning
**Scope**: `tinio-core` checksum types + `MultipartOps` deltas (`storage/multipart.rs`), `tinio-fs` redb persistence (`database/tables.rs`, `multipart.rs`), `tinio-mem` redb-backed tables (`storage.rs`, `multipart.rs` — redb over `InMemoryBackend`, nothing persisted), `tinio-server` validation (`backend/checksum.rs` new, `backend/multipart.rs`, `backend/errors.rs`), `tinio-config` capability toggle (`schema/s3.rs`), conformance + e2e tests (`tinio-util/testing.rs`, `tinio-server/tests/`), contract docs (`specs/001-s3-local-server/contracts/s3-surface.md`, `checklists/compatibility.md`).

## Goal

When a multipart upload request specifies a checksum algorithm and value, validate content integrity:

1. **UploadPart** — validate the part content against the request's checksum value (any `ChecksumAlgorithm` variant; `Content-MD5` included), before the part is committed. A mismatching part is never stored. **UploadPartCopy** carries no client checksum value on the S3 wire (R1); the server instead computes and stores the copied part's checksum when the upload was created with an algorithm.
2. **CompleteMultipartUpload** — validate the client's full-object checksum value against the upload's stored part checksums (`COMPOSITE` composition) or the linearized full-object CRC (`FULL_OBJECT`); cross-check `CompletedPart` checksum entries against stored values. Validation runs **pre-commit**: on failure the upload is untouched (including a pre-existing object of the same key).
3. **All checksum computation lives in `tinio-server`** — one streaming pass with body read + disk write, via `s3s::checksum::ChecksumHasher` (enables md5 + the requested algorithm together). The storage backends contain zero checksum logic: they store and return whatever the server hands over.
4. The whole feature is behind a `[s3] checksum` capability toggle, default **off** (off = today's behavior: accept and drop).

## Non-goals

- **No PutObject/PostObject/CopyObject checksum validation.** Single-part uploads keep accepting and dropping `x-amz-checksum-*` (current behavior).
- **No default-checksum auto-attach.** S3 attaches CRC64NVME to objects uploaded without a checksum; tinio does not — a checksum is only computed/stored when the request carries one or the upload was created with an algorithm.
- **No `x-amz-sdk-checksum-algorithm`** (SDK-internal header; S3 ignores it, so do we).
- **No checksum validation at rest** (scanner/repair re-hash of stored objects).
- **No checksum fields on GetObject/HeadObject/GetObjectAttributes** responses (out of scope; only the multipart request/response surface).
- **No re-reading the assembled object** and no assembly-time hashing in the backends. S3 itself derives multipart full-object checksums from stored part checksums (CRC linearization); tinio does the same, server-side.

## AWS wire facts (verified 2026-08-31 against the S3 API docs, the S3 User Guide, and s3s 0.15.0 sources; corrected at review R1–R7)

- **Algorithm × checksum type support, multipart** (User Guide "Full object and composite checksum types" table):

  | Algorithm | FULL_OBJECT | COMPOSITE |
  |-----------|:-----------:|:---------:|
  | CRC64NVME | Yes | **No** |
  | CRC32 / CRC32C | Yes | Yes |
  | SHA1 / SHA256 / SHA512 / MD5 / XXHASH* | **No** | Yes |

- **FULL_OBJECT** values are CRC-only *because they linearize*: "S3 can compute the checksum of the whole object from the part-level checksums" — no re-read, ever. The client sends the full-object value plus `x-amz-mp-object-size` at Complete (header name corrected at review, R2); S3 compares against the linearized value and fails with `BadDigest` on checksum mismatch, `InvalidRequest` on size mismatch.
- **COMPOSITE** values: S3 "uses the stored checksum values of each part to calculate the full object checksum internally, comparing it with the provided checksum value". Official SDK example (SHA-256): the full-object checksum is the algorithm applied to the **concatenation of the raw part digest bytes**.
- **Create-time algorithm requirement**: a checksum value at Complete is validated only when the algorithm was specified at `CreateMultipartUpload`. Without it, S3 accepts-and-does-not-validate the existing algorithms (CRC32, CRC32C, SHA-1, SHA-256) and fails `InvalidRequest` for the newer MD5/XXHash*/SHA-512 set; CRC64NVME is in neither documented category (it is the auto-attached default algorithm) — tinio treats it as accept-without-validation (assumption R5).
- **Algorithm consistency**: "The checksum algorithm must be the same for all parts and it match the checksum value supplied in the `CreateMultipartUpload` request" (UploadPart API docs). "If you provide an individual checksum, Amazon S3 ignores any provided `ChecksumAlgorithm` parameter" — the value field's algorithm is authoritative.
- **Algorithm without value** on UploadPart: "there must be a corresponding `x-amz-checksum` or `x-amz-trailer` header sent. Otherwise, Amazon S3 fails the request with HTTP 400" — tinio maps this to `InvalidRequest`.
- **Default checksum type**: CRC64NVME is *always* FULL_OBJECT ("The CRC64NVME checksum is always a full object checksum"); the other algorithms default to COMPOSITE. `checksum_type` mismatch at Complete → `BadDigest` (CompleteMultipartUpload API ref — R3, R7).
- **Response echo**: the UploadPart response checksum is "only present if the checksum was provided in the request"; stored part checksums (computed for create-algorithm uploads) are returned by ListParts. For UploadPartCopy, the `CopyPartResult` checksum is **server-computed**, "present if the multipart upload request was created with the checksum algorithm" — the copy **request** defines no `x-amz-checksum-*` headers at all (R1).
- **Error codes**: digest mismatch → `BadDigest` (400). Request-shape violations → `InvalidRequest` (400).
- **s3s 0.15.0** parses every `x-amz-checksum-*` header (all ten fields, incl. SHA-512/XXHash*) into the UploadPart/Create/Complete DTOs, plus `checksum_type` and `mpu_object_size` on `CompleteMultipartUploadInput` (tinio drops them today — `backend/objects.rs:8`); `s3s::checksum::ChecksumHasher` computes all supported algorithms in one pass to base64; `S3Request.trailing_headers` is a **shared handle populated only after the body stream is fully consumed** (verified aws-chunked trailers) — trailer values are read at stream end, inside the verifying stream, not at request-parse time (R4). `UploadPartCopyInput` has **no** checksum fields — matching AWS, which defines no checksum request headers for UploadPartCopy (R1). The composition/linearization math is **not** in s3s — it is tinio's own helper.

## Architecture

```
UploadPart (feature on)
  server: get_multipart_upload(bucket, upload_id)      // persisted create-algorithm + type
          ChecksumSpec::from_input(req)                // ≤1 checksum-value source: checksum_<algo>
                                                       //   field or declared trailer; Content-MD5 may
                                                       //   coexist and is also validated (R6);
                                                       //   algorithm-without-value → InvalidRequest
          part algo ≠ upload algo (when upload has one) → InvalidRequest
          wrap body in VerifyStream                    // s3s ChecksumHasher { md5: Some, <algo>: Some };
                                                       //   trailer expectation read from the trailing-
                                                       //   headers handle at stream end (R4)
          each backend read → hasher.update(chunk)     // single pass with staging write
          stream end → finalize → compare expected(s)
              mismatch → stream yields Err → backend aborts staging → part never stored
          upload_part(..., checksum: Option<Arc<PartChecksum>>)   // tee slot: the digest rides
                                                       //   into the part's commit txn — the
                                                       //   checksum row commits atomically with
                                                       //   the part row (no CAS, no second call);
                                                       //   also for header-less parts of
                                                       //   create-algorithm uploads (computing tee);
                                                       //   etag_md5 ⇒ the slot value IS the part
                                                       //   ETag (the backend skips its own hash)
          Err → state.mismatched ? BadDigest : map_backend_error
          echo checksum ONLY when the request carried a value/trailer

UploadPartCopy (feature on) — no client checksum value exists on this wire (R1)
  get_multipart_upload → upload has create-algorithm?
    yes → get_object(src, range) → VerifyStream (compute-only tee) → upload_part(…, Some(slot))
          → echo computed value in CopyPartResult (the digest committed with the part)
    no  → existing copy_part / copy_part_fast path, unchanged

CreateMultipartUpload (feature on)
  create_multipart_upload(..., checksum: Option<UploadChecksum>)   // persisted
  echo algorithm + type

CompleteMultipartUpload (feature on) — pre-commit, per-object write lock held
  NOTE: implemented per R8 — the paging, the 5MiB rule, and the validation all
        run under lock_object (the paging is max_parts=1000, for the 5MiB
        min-part-size rule)
  get_multipart_upload → page list_parts (sizes + stored part checksums)
  1. cross-check CompletedPart.checksum_* vs stored                                → BadDigest
  2. full-object value present?
       value's algorithm vs upload's create-algorithm → mismatch                   → InvalidRequest
       type = input.checksum_type | persisted type | (CRC64NVME ? FULL_OBJECT : COMPOSITE)   (R7)
       create-type vs input-type conflict                                          → BadDigest   (R3)
       algorithm×type validity (table above)                                       → InvalidRequest
       create-algorithm absent → accept, no validation, warn                       (AWS behavior for
                                                                                     CRC32/CRC32C/SHA1/SHA256;
                                                                                     CRC64NVME undocumented → R5)
       any listed part without stored checksum → skip validation, warn             (deviation D2:
                                                                                     possible only for
                                                                                     parts uploaded with
                                                                                     the toggle off — the
                                                                                     checksum row commits
                                                                                     atomically with the
                                                                                     part row, so the old
                                                                                     two-phase crash
                                                                                     window is gone)
       COMPOSITE   → compose_composite(parts)      → compare                       → BadDigest
       FULL_OBJECT → linearize_full_object(parts)  → compare                       → BadDigest
                     + x-amz-mp-object-size vs Σ part sizes                        → InvalidRequest
  3. compute + echo the full-object value whenever the upload has a create-algorithm
     and all listed parts have stored checksums (validated when a value was sent, echo-only otherwise)
  4. complete_multipart_upload → respond

ListParts / ListMultipartUploads (feature on) — echo stored checksum fields
```

**Pre-commit race argument**: validation composes from the `list_parts` snapshot; a concurrent part overwrite after the snapshot cannot be committed behind the client's back — `complete_multipart_upload` re-verifies the client's `CompletedPart` ETags against the stored parts (mem: in one write txn; fs: a one-txn read snapshot plus an assembly-time re-hash of part bytes — the verify-then-copy race closes there), and etags are content-derived (an overwritten part cannot match the client's listed etag), so Ok ⇒ the snapshot's checksum values are exactly the committed parts'. Validation before commit cannot falsely reject a good upload, and on genuine mismatch the upload (and any pre-existing object of the same key) is left untouched — matching S3, with no rollback machinery.

## Contract changes — `tinio-core`

New module `tinio-core/src/checksum.rs` (one home for the shared types; exported from `lib.rs`; no new dependencies — plain types only):

```rust
pub enum Algorithm { Crc32, Crc32C, Crc64Nvme, Sha1, Sha256, Sha512, Md5, XxHash64, XxHash3, XxHash128 }
// Display/FromStr: "CRC32" | "CRC32C" | "CRC64NVME" | "SHA1" | "SHA256" | "SHA512" | "MD5" | "XXHASH64" | "XXHASH3" | "XXHASH128"

pub enum ChecksumType { Composite, FullObject }
// as_str()/FromStr: "COMPOSITE" | "FULL_OBJECT"

pub struct ChecksumValue(pub String);                       // base64, S3 wire format
pub struct PartChecksum { pub algorithm: ChecksumAlgorithm, pub value: ChecksumValue }
pub struct UploadChecksum { pub algorithm: ChecksumAlgorithm,
                            pub checksum_type: Option<ChecksumType> }
```

`MultipartOps` deltas (`tinio-core/src/storage/multipart.rs`):

- `create_multipart_upload(&self, bucket, key, checksum: Option<UploadChecksum>) -> Result<MultipartUpload, …>` — new third param; persisted.
- `get_multipart_upload(&self, bucket, key, upload_id: &str) -> Result<MultipartUpload, …>` — **new**; the upload's persisted checksum spec (needed by upload/copy/complete); `NoSuchUpload` when absent **or the key does not match** (S3 identity is `(bucket, key, uploadId)` — recorded at review N04).
- `upload_part(…, body, checksum: Option<Arc<PartChecksum>>)` — new last param: the server's tee slot (`tinio-core::checksum::PartChecksum` — `digest: OnceLock<Part>`, `etag_md5: bool`). The backend reads the digest **at commit time** and writes the `PART_CHECKSUMS` row in the SAME transaction as the part row (atomic — a re-upload overwrites both, so no CAS is needed; the row is cleared when the slot is empty, so a re-uploaded part never keeps a stale value). With `etag_md5` the slot value also **is** the part ETag (a part's ETag IS its content MD5) — the backend skips its own hash. The backends never hash.
- `PartInfo` += `checksum: Option<Part>` (returned by `upload_part`/`list_parts`; `upload_part` echoes the digest it just committed).
- `MultipartUpload` += `checksum: Option<Upload>` (returned by `create_multipart_upload` / `get_multipart_upload` / `list_multipart_uploads`).
- `complete_multipart_upload` **unchanged** — validation is server-side, from the `list_parts` snapshot the op already pages.
- **No new contract error variant.** Mismatches never surface from the backends: a failing verifying stream aborts staging (existing error path), and the server re-maps via `VerifyState` before backend-error mapping.

## tinio-server

New module `backend/checksum.rs`:

- `VerifyState` — `Arc<VerifyState>` shared between the wrapper stream and the op: `{ slot: Arc<PartChecksum> (the storage-commit handle — filled once at stream end), mismatched: AtomicBool }`.
- `VerifyStream` — wraps `BodyStream` (from `stream_in`); on each chunk: `hasher.update(chunk)` (one `s3s::checksum::ChecksumHasher` with md5 + the requested algorithm enabled — Content-MD5 and `x-amz-checksum-md5` share the md5 slot); yield the chunk untouched. On stream end: `finalize()` → store computed in state; compare every expected value → mismatch records `mismatched` and the stream yields `Err(io::Error::new(InvalidData, …))`. If the backend errors mid-stream, the state never finalizes and the op maps the backend error normally.
- `ChecksumSpec::from_upload_part(input, headers)` — at most one `checksum_<algo>` value source: a DTO field (the field name is the algorithm) or a declared trailing checksum (aws-chunked; the algorithm comes from the declared trailer name). `content_md5` (algorithm Md5) is an **independent second expectation** that may coexist with one checksum value — both validated in the same `ChecksumHasher` pass (R6 — AWS docs are silent on the combination; validating both is free since the hasher computes md5 + the algorithm together). Two checksum-value sources (e.g. field + trailer, or two different `checksum_*` fields) → `InvalidRequest`. `x-amz-checksum-algorithm` is not cross-checked against the value (per AWS, "if you provide an individual checksum, Amazon S3 ignores any provided ChecksumAlgorithm parameter"); an algorithm header with no value and no declared trailer → `InvalidRequest` (S3: 400).
- Trailer values are not available at request-parse time: `req.trailing_headers` is a shared handle filled when the aws-chunked body is fully consumed (s3s verifies the trailer signature). `VerifyStream` carries the handle and reads the expected trailer value at end-of-stream, before the `finalize()` comparison (R4).
- No `from_headers` parser for UploadPartCopy — AWS defines no checksum request headers on that operation (R1); the copy path only *computes*.
- Composition (the only hand-written crypto — one home, byte-exact reference-tested):
  - `compose_composite(algo, parts: &[PartChecksum]) -> ChecksumValue` — decode each value to raw digest bytes, concatenate in ascending part order, run a fresh `s3s::checksum::ChecksumHasher` with `algo` over the concatenation (per the documented "checksum of the concatenation of the part checksums" rule).
  - `linearize_full_object(algo, parts) -> ChecksumValue` — CRC-combine (carryless-multiplication) with the algorithm's parameters: CRC32 / CRC32C / CRC64NVME (poly `0x04C11DB7` / `0x1EDC6F41` / `0xad93d23594c93659`, all reflected, init + xorout all-ones). Self-validating test: random content split into random parts — direct CRC of the concatenated content must equal the linearized value; known check values (e.g. CRC-32 of "123456789" = `CBF43926`) also pinned.

Op changes in `backend/multipart.rs` (all gated on `self.caps.checksum`; off ⇒ exactly today's code path):

- `op_upload_part` — `get_multipart_upload` → parse spec → algorithm-vs-upload consistency check (`InvalidRequest`) → wrap body in `VerifyStream` (validating when a value is present; computing-only when the upload has a create-algorithm and the request has none; no tee when neither) → `upload_part(…, checksum: state.slot())` → error mapping via state → response echo only when the request carried a value (the digest was committed with the part row).
- `op_upload_part_copy` — `get_multipart_upload`; if the upload has a create-algorithm: `get_object(src, range)` → compute-only tee (`VerifyStream` with no expectations) → `upload_part(…, Some(slot))` → echo the computed value in `CopyPartResult` (matches AWS; the digest was committed with the part row). Otherwise the existing `copy_part` path (fs `copy_part_fast` on unix) is unchanged. Trade-off: create-algorithm uploads give up the zero-copy fast path (R1 resolution, deviation D5).
- `op_create_multipart_upload` — pass `UploadChecksum`; echo in output.
- `op_complete_multipart_upload` — pre-commit, under the per-object write lock (the existing `list_parts` paging moves inside the lock, R8): `get_multipart_upload`; the paged `list_parts` collects sizes + stored checksums; `CompletedPart` cross-check; full-object validation per the Architecture section (algorithm/type consistency — type conflict → `BadDigest`, the rest → `InvalidRequest` — algorithm×type validity, COMPOSITE composition, FULL_OBJECT linearization + `mpu_object_size` from the s3s DTO); compute + echo the full-object value whenever the upload has a create-algorithm and all listed parts have stored checksums; then `complete_multipart_upload`.
- `op_list_parts` / `op_list_multipart_uploads` — echo stored values.
- `backend/errors.rs` — `BadDigest` and `InvalidRequest` via the existing `s3_error!` path.

## Persistence (additive redb tables, no migration)

- `tinio-fs/src/database/tables.rs` + `tinio-fs/src/multipart.rs`:
  - `UPLOAD_CHECKSUMS: (bucket, upload_id) → (algorithm, checksum_type)` — written with the `UPLOADS` row in `Store::create`'s txn; read by `get_multipart_upload` / `list_multipart_uploads`; drained with the upload.
  - `PART_CHECKSUMS: (bucket, upload_id, part_number) → (algorithm, value)` — written **or cleared** in `Store::put_part`'s publish txn, from the tee slot (the part row and the checksum row commit atomically — a re-uploaded part's row follows its new content, and a part without a slot clears a stale value — that would otherwise corrupt Complete composition); read by `list_parts`; drained with the parts.
  - `complete` unchanged (doesn't need checksums); `consume`/`abort` drain the new tables in the same txns (`drain_upload`).
- `tinio-mem/src/storage.rs` + `tinio-mem/src/multipart.rs`: two analogous tables and flows (redb over `InMemoryBackend`; key shapes differ from fs — compound strings, zero-padded part keys).
- redb schema versioning — **resolved at review (R10)**: `STATE_VERSION: u64 = 1` (`database/tables.rs:493`); `ensure_version` rejects mismatches and all tables are `ensure`d (created if missing) on open (`database/open.rs`), so purely additive tables need no version bump. Note: no additive-table precedent exists in repo history (all current tables arrived in the initial redb migration commit) — the mechanism is safe regardless.

## Config — `tinio-config`

- `schema/s3.rs` `Capabilities` += `checksum: bool` — plain `#[serde(default)]` + `#[default = false]` (the actual `allow_zero_page_size` pattern; no module-level default fn needed). `From<&Config>` picks it up automatically (flattened). No shared `DEFAULT_*` const needed (config-only, nothing shared with the backends).
- Serve wiring: none — `Capabilities → DataPlane → S3Backend::new` already flows.

## Errors

| Case | Code |
|------|------|
| Part checksum / Content-MD5 mismatch (tee) | `BadDigest` 400 |
| CompletedPart entry vs stored mismatch | `BadDigest` 400 |
| Full-object COMPOSITE / FULL_OBJECT mismatch | `BadDigest` 400 |
| Create-time vs request `checksum_type` conflict at Complete | `BadDigest` 400 (CompleteMultipartUpload API ref — R3) |
| More than one `checksum_<algo>` value in one request | `InvalidRequest` 400 |
| `Content-MD5` + one algorithm value (both validated, one hasher pass — R6) | allowed |
| Checksum algorithm header without a value and no declared trailer | `InvalidRequest` 400 |
| Part algorithm ≠ upload's create-algorithm | `InvalidRequest` 400 |
| Complete value algorithm ≠ upload's create-algorithm | `InvalidRequest` 400 |
| Algorithm × type not supported (e.g. SHA with FULL_OBJECT) | `InvalidRequest` 400 |
| FULL_OBJECT value without `x-amz-mp-object-size`, or size vs Σ parts | `InvalidRequest` 400 |

## Deviations (documented in `s3-surface.md` / `compatibility.md` CHK019)

- **D1** — UploadPart response echo follows AWS (only when the request carried a value); the stored value (ListParts) covers create-algorithm uploads' header-less parts.
- **D2** — Complete validation is skipped (warn) when any listed part lacks a stored checksum. Possible only when the part was uploaded with the toggle off, or uploaded without a value to a non-algorithm upload (the checksum row commits atomically with the part row — the two-phase crash window is gone; copied parts of create-algorithm uploads are server-computed after the R1 redesign).
- **D3** — PutObject/PostObject/CopyObject checksums remain accepted and dropped.
- **D4** — No automatic CRC64NVME attach for uploads without a checksum.
- **D5** — UploadPartCopy of a create-algorithm upload uses the server-streamed compute path, not the zero-copy `copy_part_fast` fast path (the price of computing the copied part's checksum, which AWS does unconditionally). Non-algorithm uploads keep `copy_part_fast`.
- **D6** — the `Algorithm` enum is widened to ten variants (`Sha512`, `XxHash64`, `XxHash3`, `XxHash128` beyond the six original ones) for wire-name parsing and create-algorithm compute paths; their value FIELDS remain accepted-and-dropped (`single_checksum_value` ignores them), COMPOSITE is legal for them (hashes), FULL_OBJECT stays CRC-only.
- **Mostly matches AWS**: a Complete-time checksum value with no create-time algorithm is accepted but not validated — documented AWS behavior for CRC32/CRC32C/SHA-1/SHA-256; for CRC64NVME the behavior is undocumented (it belongs to neither the "existing" nor the "new" algorithm categories), and tinio applies the same accept-without-validation rule (assumption R5).

## Testing

- `backend/checksum.rs` unit tests: hasher selection; VerifyStream ok / mismatch / mid-stream backend error (state semantics); trailer-expectation read at stream end (R4); `compose_composite` and `linearize_full_object` self-validating on randomized part splits (deterministic PRNG) + standard check values per algorithm.
- `backend/multipart.rs` (existing test module): per-algorithm ok/mismatch; Content-MD5 ok/mismatch; Content-MD5 + checksum value together, both validated (R6); trailing-checksum path; multiple-sources → `InvalidRequest`; algorithm-without-value → `InvalidRequest`; part-algorithm-vs-create mismatch → `InvalidRequest`; toggle off ⇒ drop (current behavior); echo only-when-provided on upload; echo on create/complete/list-parts; COMPOSITE ok/mismatch; FULL_OBJECT ok/mismatch; CRC64NVME with no explicit type treated as FULL_OBJECT (R7); checksum-type conflict → `BadDigest` (R3); mp-object-size mismatch; CompletedPart cross-check; missing-stored-value skip; copy on a create-algorithm upload → computed value persisted + echoed in `CopyPartResult`; copy on a non-algorithm upload keeps the fast path (no checksum); pre-commit failure leaves the pre-existing object intact.
- `tinio-fs` / `tinio-mem`: persistence round-trips — create with checksum → `get_multipart_upload`/`list_multipart_uploads` echo; `upload_part` with a tee slot writes the row atomically with the part + returns it in `PartInfo`; re-upload with a new slot overwrites, without a slot clears; `list_parts` echo; complete/abort drain the new tables.
- Conformance (`tinio-util/testing.rs` `conformance_multipart`): `create_multipart_upload` with `UploadChecksum`, the tee-slot flow (write / clear / overwrite), `PartInfo.checksum` echo, drain — through the contract, both backends.
- e2e: boto3 journey already sends checksums (CRC64NVME by default — exercises the R7 type defaulting) — run it with `[s3] checksum = true` (real-client math exercised end-to-end); optional low-level corrupt-part test → `BadDigest`.
- `error_codes.rs`: `BadDigest` / `InvalidRequest` cases.

## Call sites / compatibility

`MultipartOps` implementors to update: `tinio-fs/src/backend/multipart.rs` (upload_part passes the slot through + new `get_multipart_upload`), `tinio-fs/src/multipart.rs` (`Store`: publish-txn write-or-clear from the slot, `drain_upload`, `list_parts`), `tinio-mem/src/multipart.rs` + `storage.rs`, `tinio-util/src/testing.rs` (conformance). `upload_part` gains the `checksum` slot param — the existing call sites pass `None` (incl. the contract `copy_part` default impl and fs's `copy_part_fast`); `complete_multipart_upload` is unchanged. `backend/errors.rs` gains two mappings. `metrics.rs` mock DTO construction is unaffected (verified at review: the literals are s3s DTOs with `checksum_*` already `None`; the new fields land on tinio-core types). New workspace dependencies: `base64` (tinio-core — the wire-value decode helper; `tinio-fs`/`tinio-mem` for the slot-MD5 ETag); hashing stays `s3s::checksum` (server) + the composition helper's carryless math (hand-rolled, reference-tested).

## Review log (2026-08-31, round 2)

Resolutions from the implementation review (docs/superpowers/reviews/2026-08-31-multipart-checksum-validation-review.md), all incorporated above:

- **W02 — R8 implemented in code.** `op_complete_multipart_upload` now runs the `list_parts` paging, the 5 MiB rule, and the pre-commit validation under `lock_object` (the Architecture NOTE is updated to the implemented state).
- **W03 — CompletedPart cross-check scope.** The value-vs-stored comparison runs whenever the client sent an entry and a stored value exists (independent of a create-time algorithm); only the algorithm-consistency `InvalidRequest` is gated on the upload's algorithm; a missing stored value skips (D2), a part outside the stored snapshot is left to the backend's `InvalidPart` classification.
- **W04 — FULL_OBJECT request-shape validation first.** The `x-amz-mp-object-size` presence and size-sum checks run before the D2 stored-checksum completeness gate; only the digest comparison stays under the skip.
- **C01 — `checksum_type` without `checksum_algorithm` at create is accepted and dropped** (warn), not rejected — the AWS wire behavior for this shape is undocumented, so no invented rejection. Recorded as part of the effective-coverage matrix.
- **H03 — wire names via `parse_display`.** `Algorithm`/`Type` use `#[display(style = "UPPERCASE")]`; `as_wire()` is dropped in favor of `to_string()`.
- **Interface — `from_upload_part(input, headers)`.** The declared-trailer detection needs the raw `x-amz-trailer` header (the algorithm comes from the declared trailer name); the trailer VALUE is read from the verified trailing-headers map at stream end (R4). `ChecksumSpec` carries `trailer_algo` for that.
- **D6 — enum widened to ten variants.** `Algorithm` includes `Sha512`/`XxHash64`/`XxHash3`/`XxHash128` for wire-name parsing and create-algorithm compute paths; their value FIELDS remain accepted-and-dropped (`single_checksum_value` ignores them), COMPOSITE is legal for them (hashes), FULL_OBJECT stays CRC-only.
- **M07 — the `""` sentinel stays.** The persisted `UPLOAD_CHECKSUMS` second column encodes `Option<Type>`; no schema change (additive tables, no migration).
- **M08 — validation helpers shared, op stays in place.** `stored_parts`/`resolve_checksum_type` centralize the two Complete branches; a full move of the validation body into `backend/checksum.rs` was not pursued.

## Review log (2026-08-31, round 1)

Design reviewed against the S3 API Reference, the S3 User Guide, docs.rs/s3s 0.15.0, and the current codebase. Findings and their resolutions (all incorporated above):

- **R1 — UploadPartCopy has no client checksum headers.** The AWS request syntax defines no `x-amz-checksum-*` for UploadPartCopy; the original "validate the copy request's checksum value" path implemented a nonexistent wire feature. Resolution: the copy path *computes* — for create-algorithm uploads the server tees the copy stream and echoes the computed value in `CopyPartResult` (which is exactly the documented AWS behavior); non-algorithm uploads keep `copy_part_fast`. D5 rewritten accordingly; copies no longer trigger D2.
- **R2 — Header name.** `x-amz-mp-object-size` (DTO field `MpuObjectSize`), not `x-amz-mpu-object-size`. s3s parses it into `CompleteMultipartUploadInput`, so no raw-header parsing is needed.
- **R3 — checksum-type conflict error code.** A `checksum_type` mismatch at Complete is `BadDigest` per the CompleteMultipartUpload API ref, not `InvalidRequest`.
- **R4 — Trailing-headers timing.** `S3Request::trailing_headers` is a shared handle populated only after the body stream is fully consumed; the trailer expectation is read at stream end inside `VerifyStream`, not at request-parse time.
- **R5 — "New algorithm" set.** AWS's InvalidRequest-on-missing-create-algorithm list is MD5/XXHash*/SHA-512; CRC64NVME belongs to neither documented category. tinio applies accept-without-validation to CRC64NVME and records this as an assumption, not a verified AWS fact.
- **R6 — Content-MD5 + `x-amz-checksum-*` in one request.** Undocumented by AWS; the original "more than one source → InvalidRequest" was an extrapolation. Resolution: Content-MD5 may coexist with one checksum value and both are validated (free — the hasher already computes md5 + the algorithm in one pass); only two checksum-*value* sources conflict.
- **R7 — Default checksum type.** CRC64NVME is always FULL_OBJECT; the original unconditional `| COMPOSITE` fallback would have falsely rejected CRC64NVME requests (boto3's default) via the algorithm×type check. Resolution: per-algorithm default.
- **R8 — Lock ordering (code).** The existing `list_parts` paging in complete runs *before* `lock_object` (`backend/multipart.rs:205-248`); pre-commit validation under the lock requires moving the paging inside it. Also clarified: the paging is `max_parts=1000` for the 5MiB minimum-part-size rule, not a "5MiB paging budget".
- **R9 — checksum/part write atomicity (code, revised at cleanup 2026-08-31).** The original resolution — a separate post-`upload_part` `set_part_checksum` call with a CAS on the returned etag — was superseded by the atomic-commit design: `upload_part` gains the tee-slot param (`Option<Arc<PartChecksum>>`), and the backend writes (or clears) the checksum row in the SAME transaction as the part row. The lost-update race cannot exist (a re-upload overwrites both rows atomically), the extra trait method, the second round trip, and the `set_part_checksum`/CAS machinery are gone, and with `etag_md5` the slot also supplies the part ETag (no second MD5 pass). The D2 deviation now covers only toggle-off / non-algorithm parts — the two-phase crash window is closed.
- **R10 — Codebase phrasing corrections.** `allow_zero_page_size` uses plain `#[serde(default)]` (no default fn); additive redb tables have no repo precedent but are mechanically safe (ensure-on-open, `STATE_VERSION` untouched); the fs etag re-verification is a one-txn snapshot + assembly re-hash, not "one txn"; tinio-mem's redb runs over `InMemoryBackend` (nothing persisted, different key shapes); the call-site list gained `tinio-fs/src/multipart.rs`.
- **R11 — Process.** The compatibility docs (`s3-surface.md` / `compatibility.md`) must state the *effective validation coverage* as an explicit matrix (which request combinations are actually validated), not leave it scattered across D1–D5.

## Decisions (locked 2026-08-31; R-items revised at review)

- **Server-layer validation via a body tee** (`VerifyStream` over `stream_in`), per-read hasher update, single streaming pass with staging — the backends never hash.
- **s3s `ChecksumHasher` with md5 + requested algorithm enabled together** — Content-MD5 / `x-amz-checksum-md5` and the checksum share one pass; zero new deps; no tinio-core hasher re-implementation.
- **Pre-commit Complete validation** from the `list_parts` snapshot (etag check makes it race-free); no rollback; a failed complete leaves the upload and any pre-existing object untouched (matches S3).
- **Atomic checksum commit via the tee slot** — `upload_part(…, checksum: Option<Arc<PartChecksum>>)`; the backend writes (or clears) the checksum row in the part's own commit txn, from the slot the tee filled at stream end; with `etag_md5` the slot value is the part ETag (no second MD5 pass). No CAS, no second round trip. Header-less parts of create-algorithm uploads are computed + persisted (S3-consistent), echoed only in ListParts.
- **`get_multipart_upload` contract lookup** — the persisted create-algorithm/type drives UploadPart/Copy/Complete consistency checks (S3: "must match the checksum value supplied in CreateMultipartUpload") and the compute-only tee.
- **Strict S3 consistency**: algorithm-without-value → `InvalidRequest`; part/Complete algorithm ≠ create-algorithm → `InvalidRequest`; individual checksum fields win over the algorithm header (per AWS); checksum-type conflict at Complete → `BadDigest` (R3).
- **UploadPartCopy in scope (revised, R1)**: the copy request carries no client checksum (AWS defines none); copies of create-algorithm uploads stream through a compute-only server tee (`get_object` range → `VerifyStream` → `upload_part` → `set_part_checksum`) and echo in `CopyPartResult`; non-algorithm uploads keep `copy_part_fast`.
- **Feature toggle `[s3] checksum`, default off.**
- **Additive redb tables** for persisted checksums (no migration; no version bump needed, R10).
- **English-only docs; no auto git commit** (project git rule).
