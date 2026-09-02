# Contract: S3 Data-Plane Surface

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

The S3 protocol surface implemented by the backend over s3s (routing, XML, error codes, SigV4/SigV2 verification handled by the framework). All other operations of the S3 trait (99 total) return standard NotImplemented errors.

## Implemented operations (~30)

| Group | Operations |
|-------|-----------|
| Buckets | `CreateBucket` (FR-002, FR-012), `DeleteBucket` (only empty → else `BucketNotEmpty`), `HeadBucket`, `ListBuckets` (2025-03 pagination semantics: `continuation-token` / `max-buckets` / `prefix`; a `ContinuationToken` is returned when more buckets remain; CreationDate from the `BUCKETS` table of `meta.redb`), `GetBucketLocation` (returns `us-east-1`) |
| Objects | `PutObject` (FR-003, ETag = content MD5), `GetObject` (FR-003; Range → seek + `206`/`Content-Range`; ETag; Content-Type inferred), `HeadObject`, `DeleteObject`, `DeleteObjects` (batch), `CopyObject` (FR-015; server-side file copy, no client passthrough; source conditionals `x-amz-copy-source-if-*` and destination conditionals `x-amz-if-match`/`x-amz-if-none-match` → 412 on failure) |
| Listing | `ListObjects` (V1), `ListObjectsV2` (V2) — prefix filtering, delimiter grouping, pagination per S3 semantics (FR-004) |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload` (composed ETag `MD5-of-MD5s-N`), `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` (FR-014; no 5 MB minimum) |

## Behavior notes

- **ETag**: single uploads `"<md5hex>"`; multipart `"<md5hex>-N"`; served only when meta-store size/mtime matches, else recomputed streaming (FR-022). Listings include ETags — missing/stale entries are recomputed synchronously during the listing (one-time full-content pass over externally-added files, documented cost of SC-006).
- **Range**: framework parses the header; backend seeks and returns the correct range + `Content-Range`.
- **Content-Type**: inferred from extension (`mime_guess`, fallback `application/octet-stream`); user `x-amz-meta-*` accepted and dropped.
- **Checksums** (FR-026; `x-amz-checksum-*`): validated and echoed on multipart uploads behind `[s3] checksum = true` (default `false` = accepted and dropped, the v1 behavior); see [Multipart upload checksum validation design](../../../docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md). Effective validation coverage (which request combinations are actually validated when the toggle is on):
  - `UploadPart` — at most one `x-amz-checksum-<algo>` value (header field or aws-chunked trailer; `Content-MD5` may coexist and is validated in the same pass); a mismatch answers `BadDigest` and the part is never stored. A part whose algorithm differs from the upload's create-time algorithm answers `InvalidRequest`.
  - `UploadPartCopy` — no client checksum value exists on the wire (AWS defines none); for create-algorithm uploads the server computes and persists the copied part's checksum and echoes it in `CopyPartResult`.
  - `CompleteMultipartUpload` — `CompletedPart` checksum entries are cross-checked against the stored values; the full-object value is validated pre-commit (`COMPOSITE` = the algorithm over the concatenated raw part digests; `FULL_OBJECT` = CRC linearization, CRC algorithms only) with algorithm/type/size consistency (`BadDigest` on value or type conflicts, `InvalidRequest` on algorithm/type/size shape violations).
  - `PutObject`/`PostObject`/`CopyObject` checksums stay accepted and dropped; no default CRC64NVME is auto-attached.
  - Deviations: a Complete-time checksum value without a create-time algorithm is accepted but not validated (documented AWS behavior for CRC32/CRC32C/SHA-1/SHA-256; assumed for CRC64NVME); Complete validation is skipped (warn) when any listed part lacks a stored checksum; create-algorithm uploads' `UploadPartCopy` gives up the zero-copy fast path.
- **Error codes** (FR-005): `NoSuchBucket`, `NoSuchKey`, `InvalidBucketName`, `BucketAlreadyExists`, `BucketNotEmpty`, `InvalidAccessKeyId`/`SignatureDoesNotMatch` (framework), `NotImplemented` (disabled/unsupported ops), etc.
- **Traversal** (FR-006): keys containing `..`/absolute/control sequences rejected before any FS access.
- **Conditional requests**: `If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since` honored on Get/Head (`If-None-Match` / `If-Modified-Since` failure → `304 Not Modified`; `If-Match` / `If-Unmodified-Since` failure → `412 Precondition Failed`); on the PUT write path only `If-Match` / `If-None-Match` are evaluated (any precondition failure → `412 Precondition Failed`) — `If-Modified-Since` / `If-Unmodified-Since` on PUT are not supported. The Copy source is evaluated per S3 semantics. Conditional checks reuse the meta-store ETag + filesystem mtime.
- **Folder markers**: keys ending in `/` are not objects. PUT `dir/` creates the directory (idempotent); GET/HEAD on `dir/` return `NoSuchKey`; DELETE `dir/` always returns `204` (idempotent, mirroring AWS DeleteObject marker semantics) and removes the directory only when it is empty — a non-empty directory is left in place.
- **Key charset**: universal rules (all backends): traversal, absolute paths, control characters → rejected. Platform charset restrictions follow the backend: the filesystem backend rejects keys that cannot exist on the host OS (e.g. Windows-invalid characters on Windows); other platforms allow them.
- **Symlinks**: rejected by default (`[storage.fs] follow_symlinks = false`) — access resolving through a symlink is refused and symlink entries are excluded from listings (a link inside a bucket cannot escape the storage root). Opt-in `true` follows links, which may point outside the root (user-owned directory, documented).
- **Capability toggles** (FR-021): runtime `[s3]` section disables groups → `NotImplemented` (`multipart`, `copy_object`, `list_objects_v1`, `list_objects_v2`, `delete_objects`); compile-time default-on features (`multipart`, `copy`, `list-v1`, `list-v2`) strip the code entirely — stripped groups return `NotImplemented` and their `[s3]` keys are silently ignored.
- **Listing page size**: every listing page size < 1 (`max-buckets`, `max-keys`, `max-parts`, `max-uploads`) answers `InvalidArgument`, and `max-buckets` above the AWS-documented 10,000 is rejected too (never a silent clamp). `[s3] max_buckets` (default 10,000; 0 = unlimited) clamps ListBuckets page sizes — including the no-parameter default; `[s3] max_keys` (default 0 = unlimited) clamps ListObjects page sizes; multipart listings are uncapped. The `[s3] allow_zero_page_size = true` escape hatch restores the legacy empty page (negatives clamped to 0) on the pre-existing `max-keys` / `max-parts` / `max-uploads` surfaces; ListBuckets stays strict. V1/V2/ListParts/ListMultipartUploads responses echo the effective (clamped) page size.
- **SigV2**: disabled by default (`[s3] sig_v2 = false`); enable only for legacy clients — aws cli v2 and rclone always use SigV4.
- **DeleteBucket is 204-before-gone** (F07): the name is unpublished (renamed under `.tinio/deleting/`) before the request answers `204 No Content`; the tree is purged asynchronously on the removal lane. A bucket recreated under the same name is live again immediately — it may briefly coexist with the background purge of the old tree. Removal failures surface as an error log plus the scanner summary's `removal_failures`; a graceful shutdown awaits the lane with a bounded timeout, and any interrupted walk is reclaimed at the next startup repair or scanner pass.
- **Read-only mode** (FR-023): all mutating operations (`CreateBucket`, `DeleteBucket`, `PutObject`, `DeleteObject(s)`, `CopyObject`, all multipart ops) return `AccessDenied`; read operations behave identically.
- **Addressing style**: path-style addressing is the supported/tested mode (s3s serves `/<bucket>/<key>`); virtual-hosted addressing is not configured in v1. Interop tests verify aws cli v2 and rclone work against `127.0.0.1` endpoints without client-side addressing overrides.
- **Listing pollution**: reserved `.tinio/` never appears in `ListBuckets`; root-level files are not buckets (only directories). `.tinio` is a reserved path segment at ANY depth (FR-020): keys containing it are rejected on write (`AccessDenied`), return `NoSuchKey` on read, and are skipped in listings — a nested root's state is never served by an outer server.
- **Out-of-band changes** (FR-013, SC-006): files placed/modified on disk are served immediately; Last-Modified always from FS mtime.

## Automated coverage (2026-09-01)

The cucumber suite (`crates/tinio-e2e/tests/features/`) is the executable form of this contract; the `traceability` test target (`cargo test -p tinio-e2e --test traceability`) cross-checks spec IDs and feature tags bidirectionally. Every ID in the tag column appears as a feature tag, and every traceability tag in the features has a spec ID here or in `tasks.md`.

| Operation | Feature | Tag |
|---|---|---|
| `CreateBucket` / `DeleteBucket` (empty-only) / `HeadBucket` / `ListBuckets` / `GetBucketLocation` | `buckets.feature` | `@SC-001 @FR-002` |
| A directory placed directly in the served root is a bucket (US1-AS1, out-of-band mirror); deleted bucket name reusable immediately (F07) | `buckets.feature` | `@fs @SC-001 @FR-002` |
| `ListBuckets` pagination caps (`max-buckets` page cap) | `interop/journey.feature` | `@FR-021 @max-buckets-3` |
| Bucket-name rules (validation matrix) | `error_codes.feature` | `@FR-012` |
| `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` (round-trip, zero-byte, nested keys, Content-Type, overwrite) | `objects.feature` | `@T025 @FR-003` |
| ETag = content MD5, meta-store validation (FR-022) | `objects.feature` | `@FR-022` |
| Range (`206`/`Content-Range`, `416` `InvalidRange`) | `objects.feature` | `@T025` |
| Conditional requests (304/412, weak/wildcard, date-based) | `conditions.feature` | `@FR-003` |
| Folder markers (`dir/` never an object) | `objects.feature` | `@T025` |
| Concurrent writes last-write-wins; interrupted upload leaves no partial object | `objects.feature` | `@FR-011` |
| Out-of-band changes served immediately | `objects.feature` | `@FR-013 @SC-006` |
| `DeleteObjects` (batch, quiet mode), `GetObjectTagging` | `tagging.feature` | `@FR-003` |
| `CopyObject` — same/cross-bucket, overwrite, missing source (`NoSuchKey`), source/destination conditionals → 412 | `objects.feature`, `conditions.feature` | `@FR-015 @FR-003` |
| `ListObjects` (V1) / `ListObjectsV2` (V2) — prefix, delimiter grouping, pagination | `listing.feature` | `@SC-001 @FR-004` |
| `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` (composed ETag) / `AbortMultipartUpload` / `ListParts` / `ListMultipartUploads` — no 5 MB minimum, part-number validation, `NoSuchUpload` identity | `multipart.feature` | `@FR-014` |
| `UploadPartCopy` (ranges, source conditionals, `InvalidPart`) | `conditions.feature` | `@FR-014` |
| Multipart checksum validation behind `[s3] checksum = true` (per-part BadDigest, create-algorithm consistency, pre-commit Complete validation) — FR-026; see the [design doc](../../../docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md) | `error_codes.feature`, `multipart.feature`, `interop/journey.feature` | `@checksum-on` scenarios |
| Error-code set (FR-005), traversal/absolute-path rejection before FS access, capability toggles → `NotImplemented` | `error_codes.feature` | `@SC-004 @FR-005 @FR-006 @FR-021` |
| `.tinio` reservation at any depth; nested-root isolation | `reserved_paths.feature` | `@FR-020` |
| Symlink policy: access through a link refused (`AccessDenied`), link entries excluded from listings (default `follow_symlinks = false`) | `reserved_paths.feature` | `@fs @FR-020` |
| `GET /metrics` (Prometheus text format; the three-layer set — HTTP, S3 ops, storage streaming counters) | `metrics.feature` | `@T075 @SC-008` |
| SigV4 authentication: signed requests succeed; invalid credentials rejected with `InvalidAccessKeyId` and no operation performed (US3-AS1/AS2) | `interop/journey.feature` | `@FR-008 @SC-001 @T032 @FR-025 @SC-002` |
| Interop: mandated clients (aws cli v2, rclone) core journey + ephemeral port (T032); client tiering (FR-025) | `interop/journey.feature` | `@SC-001 @T032 @FR-025` |
| Interop: SC-002 no-client-side-overrides | `interop/journey.feature` | `@SC-002` |
| Interop: boto3 basic journey (best-effort, T034) | `interop/journey.feature` | `@T034` |
| Interop: multipart > 8 MiB composed ETag, server-side copy (FR-015), cold listing with/without scanner (FR-024), edge keys | `interop/advanced.feature` | `@T033 @FR-015 @FR-024` |
| Interop: mc basic journey (best-effort, T035) | `interop/advanced.feature` | `@T035` |

**Not covered by cucumber** (verified by the unit/integration suites that stayed in Rust, the manual perf scripts, or not yet implemented; mirrored in the traceability test's allow-list): FR-001 (meta-requirement), FR-007 (CLI), FR-009 (anonymous-mode configuration semantics — the anonymous request path itself is what the in-process suite exercises), FR-010 (streaming memory property), FR-016 (config precedence), FR-017 (logging), FR-018 (management plane), FR-019 (metric-recording overhead; the storage full-scan gauges are the management plane's T075 work — the streaming byte counters are covered), FR-023 (read-only mode — US2, unimplemented), SC-003 (flat-memory script), SC-005/SC-007 (US2 timing criteria), T010/T018/T023 (foundation/config citations).
