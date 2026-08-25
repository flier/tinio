# Contract: S3 Data-Plane Surface

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

The S3 protocol surface implemented by the backend over s3s (routing, XML, error codes, SigV4/SigV2 verification handled by the framework). All other operations of the S3 trait (99 total) return standard NotImplemented errors.

## Implemented operations (~30)

| Group | Operations |
|-------|-----------|
| Buckets | `CreateBucket` (FR-002, FR-012), `DeleteBucket` (only empty → else `BucketNotEmpty`), `HeadBucket`, `ListBuckets` (CreationDate from the `BUCKETS` table of `meta.redb`), `GetBucketLocation` (returns `us-east-1`) |
| Objects | `PutObject` (FR-003, ETag = content MD5), `GetObject` (FR-003; Range → seek + `206`/`Content-Range`; ETag; Content-Type inferred), `HeadObject`, `DeleteObject`, `DeleteObjects` (batch), `CopyObject` (FR-015; server-side file copy, no client passthrough; source conditionals `x-amz-copy-source-if-*` and destination conditionals `x-amz-if-match`/`x-amz-if-none-match` → 412 on failure) |
| Listing | `ListObjects` (V1), `ListObjectsV2` (V2) — prefix filtering, delimiter grouping, pagination per S3 semantics (FR-004) |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload` (composed ETag `MD5-of-MD5s-N`), `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` (FR-014; no 5 MB minimum) |

## Behavior notes

- **ETag**: single uploads `"<md5hex>"`; multipart `"<md5hex>-N"`; served only when meta-store size/mtime matches, else recomputed streaming (FR-022). Listings include ETags — missing/stale entries are recomputed synchronously during the listing (one-time full-content pass over externally-added files, documented cost of SC-006).
- **Range**: framework parses the header; backend seeks and returns the correct range + `Content-Range`.
- **Content-Type**: inferred from extension (`mime_guess`, fallback `application/octet-stream`); user `x-amz-meta-*` accepted and dropped.
- **Checksums** (`x-amz-checksum-*`): ignored in v1.
- **Error codes** (FR-005): `NoSuchBucket`, `NoSuchKey`, `InvalidBucketName`, `BucketAlreadyExists`, `BucketNotEmpty`, `InvalidAccessKeyId`/`SignatureDoesNotMatch` (framework), `NotImplemented` (disabled/unsupported ops), etc.
- **Traversal** (FR-006): keys containing `..`/absolute/control sequences rejected before any FS access.
- **Conditional requests**: `If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since` honored on Get/Head (`If-None-Match` / `If-Modified-Since` failure → `304 Not Modified`; `If-Match` / `If-Unmodified-Since` failure → `412 Precondition Failed`) and on Put/Copy (any precondition failure → `412 Precondition Failed`); the Copy source is evaluated per S3 semantics. Conditional checks reuse the meta-store ETag + filesystem mtime.
- **Folder markers**: keys ending in `/` are not objects. PUT `dir/` creates the directory (idempotent); GET/HEAD on `dir/` return `NoSuchKey`; DELETE `dir/` always returns `204` (idempotent, mirroring AWS DeleteObject marker semantics) and removes the directory only when it is empty — a non-empty directory is left in place.
- **Key charset**: universal rules (all backends): traversal, absolute paths, control characters → rejected. Platform charset restrictions follow the backend: the filesystem backend rejects keys that cannot exist on the host OS (e.g. Windows-invalid characters on Windows); other platforms allow them.
- **Symlinks**: rejected by default (`[storage.fs] follow_symlinks = false`) — access resolving through a symlink is refused and symlink entries are excluded from listings (a link inside a bucket cannot escape the storage root). Opt-in `true` follows links, which may point outside the root (user-owned directory, documented).
- **Capability toggles** (FR-021): runtime `[s3]` section disables groups → `NotImplemented` (`multipart`, `copy_object`, `list_objects_v1`, `list_objects_v2`, `delete_objects`); compile-time default-on features (`multipart`, `copy`, `list-v1`, `list-v2`) strip the code entirely — stripped groups return `NotImplemented` and their `[s3]` keys are silently ignored.
- **SigV2**: disabled by default (`[s3] sig_v2 = false`); enable only for legacy clients — aws cli v2 and rclone always use SigV4.
- **Read-only mode** (FR-023): all mutating operations (`CreateBucket`, `DeleteBucket`, `PutObject`, `DeleteObject(s)`, `CopyObject`, all multipart ops) return `AccessDenied`; read operations behave identically.
- **Addressing style**: path-style addressing is the supported/tested mode (s3s serves `/<bucket>/<key>`); virtual-hosted addressing is not configured in v1. Interop tests verify aws cli v2 and rclone work against `127.0.0.1` endpoints without client-side addressing overrides.
- **Listing pollution**: reserved `.tinio/` never appears in `ListBuckets`; root-level files are not buckets (only directories). `.tinio` is a reserved path segment at ANY depth (FR-020): keys containing it are rejected on write (`AccessDenied`), return `NoSuchKey` on read, and are skipped in listings — a nested root's state is never served by an outer server.
- **Out-of-band changes** (FR-013, SC-006): files placed/modified on disk are served immediately; Last-Modified always from FS mtime.
