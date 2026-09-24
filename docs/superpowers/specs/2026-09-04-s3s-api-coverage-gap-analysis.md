# Gap analysis: s3s API coverage (tinio-server)

**Date**: 2026-09-04 · **Re-audited**: 2026-09-19 against s3s 0.16.0 (full enumeration) · 2026-09-24 against s3s 0.17.0 (API delta re-check only — see Revision notes)
**Status**: current — gap inventory of the s3s `S3` trait against `tinio-server`'s implementation, with cost analysis per missing method
**Scope**: the protocol surface only — `crates/tinio-server/src/backend/s3.rs` (the `S3` impl) against the pinned `s3s = "0.17"` workspace dep (resolved 0.17.0 in `Cargo.lock`). Not touched: storage semantics, the implemented operations' internals, the server HTTP layer, and the s3s `S3Config` (see Findings).

## Revision notes (2026-09-24, s3s 0.17.0)

**Delta (0.16.0 → 0.17.0): public surface unchanged; tinio unaffected.** Diffed against the local registry sources (`s3s-0.16.0/` vs `s3s-0.17.0/` under `$CARGO_HOME/registry/src/`; no published changelog in the crate). Same `114`-method `S3` trait, still only `post_object` with a non-`NotImplemented` default (`src/s3_trait.rs:5065`), still only one `#[cfg(feature = "minio")]` method (`listen_bucket_notification`, `src/s3_trait.rs:5057`). Byte-identical: `src/s3_trait.rs`, `src/config.rs`, `src/error/*`, `src/dto/*`, `src/ops/generated/*` (`114` trait methods, `118` generated op modules both versions). Feature set unchanged (`minio`, `openssl` only). Every claim below — counts, tiers, gates — therefore still holds; nothing was re-enumerated beyond this delta.

Only deltas, all internal:
- `HttpError` reshaped (`0.16` `src/protocol.rs:34-44` → `0.17` `src/protocol.rs:34-62`): `HttpError::new` → `from_std_error`, `into_std_error` added, `impl From<HttpError> for StdError` → `Display` + `core::error::Error`. `HttpError` still importable and still `NotImplemented`-defaults every uncovered op. tinio has no call site (`HttpError` appears once, in a comment). Note `crates/tinio-server/src/data.rs:458` asserts `HttpError` is not a `std::error::Error` — **stale under 0.17** (left alone: out of this doc's scope).
- `src/http/multipart.rs` delegates POST-Object parsing to the new `s3s-multipart` crate; `s3s-multipart`/`s3s-rfc2047`/`s3s-sigv2`/`s3s-sigv4` bumped `0.16.0` → `0.17.0`. tinio never touches `s3s::http::Multipart`.
- `src/ops/signature.rs` `pub(super)` visibility + envelope-header rules; `src/service.rs` `HttpError` construction. Internal.

`crates/tinio-auth` and `crates/tinio-server` both depend on `s3s.workspace = true` with no s3s feature flags, so none of the above reaches tinio's surface.

## Revision notes (2026-09-19)

- Re-audited against s3s **0.16.0** (the workspace moved 0.15 → 0.16). The trait went **108 (0.15.0) → 113** methods under tinio's feature set (0.16 declares 114; `listen_bucket_notification` is `minio`-gated and not compiled). The "roughly 100" of the original revision was approximate — 0.15.0 already declared 108.
- The declared set grew **30 → 36**; with `post_object` free via the s3s default delegation, **37 operations are reachable** and **76 answer 501** by default.
- The original Tier A's ACL and CORS rows have landed (bucket/object ACL 4 ops + enforcement; bucket CORS 3 ops + the OPTIONS preflight route), as has `select_object_content` from Tier C. Versioning is the only original Tier-A row still open. The original ACL open question ("real enforcement or stored-and-echoed?") is answered: ACLs are enforced — `tinio-auth`'s `AclAccess` on the s3s `S3Access` seam, per `docs/superpowers/specs/2026-09-05-s3-acl-owner-design.md`.
- Adjacent drift found while re-auditing (protocol-visible; details under Findings): `specs/001-s3-local-server/contracts/s3-surface.md` still counts "99 total / ~30 implemented" (s3s 0.15.0 declared 108, 0.16.0 declares 114), and `[s3] sig_v2` is parsed but never wired to s3s (`S3ServiceBuilder::set_config` is never called), so s3s 0.16's `S3Config` defaults are authoritative (`enable_sig_v2: false`).

## Context

- s3s's `S3` trait declares **114** methods (0.16.0 → 0.17.0 unchanged); tinio compiles s3s with its default (empty) feature set, so the `#[cfg(feature = "minio")]` `listen_bucket_notification` is not part of tinio's surface — **113 methods**.
- Exactly **one** of the 113 has a non-`NotImplemented` trait default: `post_object` maps `PostObjectInput` → `PutObjectInput` and delegates to `put_object` (`s3_trait.rs`). Every other undeclared method answers 501 `NotImplemented` (`s3_error!(NotImplemented, "<Operation> is not implemented yet")`).
- tinio-server declares **36** methods in `backend/s3.rs`, all thin forwards to `op_*` handlers:
  - Buckets: `create_bucket`, `delete_bucket`, `head_bucket`, `list_buckets`, `get_bucket_location` (T047).
  - Bucket tagging: `get/put/delete_bucket_tagging` (spec 2026-08-31).
  - Bucket CORS: `get/put/delete_bucket_cors` (spec 2026-09-05, feature `cors`).
  - Bucket/object ACL: `get/put_bucket_acl`, `get/put_object_acl` (spec 2026-09-05, feature `acl`).
  - Objects: `put_object`, `get_object`, `head_object`, `get_object_attributes`, `delete_object`, `delete_objects` (T048).
  - Object tagging: `get/put/delete_object_tagging` (spec 2026-08-31).
  - Copy: `copy_object`, `rename_object` (feature `copy`; `rename_object` is an s3s extension, not a general-purpose AWS API).
  - Listing: `list_objects`, `list_objects_v2` (T049, features `list-v1` / `list-v2`).
  - Multipart: `create_multipart_upload`, `upload_part`, `upload_part_copy`, `complete_multipart_upload`, `abort_multipart_upload`, `list_parts`, `list_multipart_uploads` (T050, feature `multipart`; `upload_part_copy` also needs `copy`).
  - Select: `select_object_content` (spec 2026-09-04, feature `select`).
- `post_object` (browser form upload) is free but not unchecked: the handler-side ACL B1 destination head runs for POST (`objects.rs::post_object_destination_head`), and s3s invokes the `S3Access::put_object` per-op hook with the mapped input before delegating.
- Feature gates: `multipart`, `copy`, `list-v1`, `list-v2`, `cors`, `select`, `acl` are all in `default`, so the default build carries every declared op. `select-parquet` is **not** default (Select accepts JSON/CSV; parquet input needs it).
- Runtime capability toggles (`[s3]`, FR-021): `multipart`, `copy_object`, `list_objects_v1`, `list_objects_v2`, `delete_objects`, `tagging`, `cors`, `select`, `acl`. Off ⇒ the op answers 501 `NotImplemented "<name> is disabled"` (and the CORS preflight route is left unregistered).
- Protocol basics in the implemented ops are already strong: `get_object` Range/If-Range/416, the write-path conditional gate (RFC 9110 §13.2.2), `x-amz-checksum-*` plumbing with validation behind `[s3] checksum` (default off: accept-and-drop), delete-bucket tombstones + mutation locks, listing pagination/continuation tokens, Select conformance (SQL + parquet + JSON streaming), `rename_object` atomicity, and the ACL enforcement surface.

## Coverage by group

| Group | Operations | Gate |
|---|---|---|
| Bucket lifecycle | create, delete, head, list, get-location | base |
| Bucket tagging | get / put / delete | `tagging` cap |
| Bucket CORS | get / put / delete + OPTIONS preflight route | `cors` feature + `cors` cap |
| Bucket + object ACL | 4 ops (+ write-path headers, Owner surface, enforcement) | `acl` feature + `acl` cap |
| Objects | put, get, head, get-attributes, delete, delete-objects | base; `delete_objects` has its own cap |
| Object tagging | get / put / delete | `tagging` cap |
| Copy / rename | copy-object, rename-object | `copy` feature + `copy_object` cap |
| Listing | ListObjects, ListObjectsV2 | `list-v1` / `list-v2` features + caps |
| Multipart | 7 ops | `multipart` feature + cap (`upload_part_copy` also `copy`) |
| Select | SelectObjectContent | `select` feature + cap |
| Browser upload | PostObject | free (s3s default delegates to `put_object`) |

## Missing methods, by cost tier

**76 methods** answer 501 under tinio's feature set (113 effective trait methods − 37 reachable), split **4 / 26 / 3 / 43** across Tier A, Tier B, Tier C and the recommended non-goals.

### Tier A — commonly called by real clients, cheap to implement

| Method | Who calls it | Cost | Notes |
|---|---|---|---|
| `get_bucket_versioning` / `put_bucket_versioning` | migration/validation tooling, `aws s3api get-bucket-versioning`, `mc` probes | Trivial (respond `Disabled` / accept `Suspended`) | Zero-cost compatibility. Real versioning is a storage-layer feature (per-key version rows + `list_object_versions`) and deserves its own design. |
| `get_bucket_encryption` | compliance scanners probing `get-bucket-encryption` | Trivial | Answer an SSE-S3 default (`AES256`) instead of 501. `put`/`delete_bucket_encryption` are Tier B (store config). |
| `restore_object` | GLACIER restore flows | Trivial | No cold tier ⇒ a successful no-op is compatible. |

### Tier B — configuration-class methods: store config, answer defaults

Cheap across the board, and best landed as one per-bucket configuration row set on the shared-store schema rather than one schema change per op.

| Method group | Notes |
|---|---|
| `get/put_bucket_lifecycle_configuration`, `delete_bucket_lifecycle` | `mc`, `s3cmd` common. Store the body; background enforcement is a separate feature. |
| `get/put/delete_bucket_policy` | `mc policy`, FUSE/bridge clients. Policy bodies are opaque JSON to the server — there is no policy evaluator, and ACLs are the enforced model. |
| `put/delete_bucket_encryption` | Store the config (`get` is Tier A). |
| `get/put_bucket_notification_configuration` | `mc notify`, Lambda-style pipelines. Config storage is cheap; delivery is the large part. |
| `get/put/delete_bucket_ownership_controls` | Modern SDK probes; echo/store `ObjectWriter` / `BucketOwnerEnforced`. |
| `get/put_public_access_block`, `delete_public_access_block` | Modern SDK/tooling probes. |
| `get/put_bucket_request_payment` | Echo/store `BucketOwner` / `Requester`. |
| `get/put_bucket_accelerate_configuration` | Answer `Suspended` (or store, with no runtime effect). |
| `get/put_object_lock_configuration`, `get/put_object_legal_hold`, `get/put_object_retention` | Object Lock probes (Veeam-class backup software). When not enabled, answer the AWS-shaped "not enabled" results — hence the separate batch in the recommended order. |

### Tier C — performance-relevant, or niche / new APIs

| Method | Notes |
|---|---|
| `list_object_versions` | Follows the versioning decision (Tier A). Sizeable: version rows + pagination. |
| `get_bucket_policy_status` | Cheap once bucket-policy storage exists (Tier B). |
| `update_object_encryption` | AWS 2025: KMS-keyed object encryption updates. Needs KMS semantics tinio does not have. |

### Recommended non-goals (explicitly skip)

- S3 Tables / metadata configuration: `create_bucket_metadata_configuration`, `get_bucket_metadata_configuration`, `delete_bucket_metadata_configuration`, `create_bucket_metadata_table_configuration`, `get_bucket_metadata_table_configuration`, `delete_bucket_metadata_table_configuration`, `update_bucket_metadata_annotation_table_configuration`, `update_bucket_metadata_inventory_table_configuration`, `update_bucket_metadata_journal_table_configuration`.
- ABAC: `get/put_bucket_abac`.
- Object annotations (new in 0.16): `get/put/delete_object_annotation`, `list_object_annotations`.
- `listen_bucket_notification` — the s3s `minio` feature is not even compiled into tinio's build.
- Analytics configurations: `get_bucket_analytics_configuration`, `put_bucket_analytics_configuration`, `delete_bucket_analytics_configuration`, `list_bucket_analytics_configurations`.
- Metrics configurations: `get_bucket_metrics_configuration`, `put_bucket_metrics_configuration`, `delete_bucket_metrics_configuration`, `list_bucket_metrics_configurations`.
- Inventory configurations: `get_bucket_inventory_configuration`, `put_bucket_inventory_configuration`, `delete_bucket_inventory_configuration`, `list_bucket_inventory_configurations`.
- Intelligent-tiering configurations: `get_bucket_intelligent_tiering_configuration`, `put_bucket_intelligent_tiering_configuration`, `delete_bucket_intelligent_tiering_configuration`, `list_bucket_intelligent_tiering_configurations`.
- `get_object_torrent` — dead ecosystem.
- `create_session` (S3 Express sessions), `list_directory_buckets` (directory buckets).
- `write_get_object_response` — Object Lambda transport; irrelevant.
- `get/put_bucket_logging`, `get/put/delete_bucket_website`, `get/put/delete_bucket_replication` — replicated/website/access-log deployments; no backend supports them and no roadmap implies them.

## Recommended order

1. **Tier-A compatibility batch** — versioning `get`/`put` answering `Disabled`, `get_bucket_encryption` default, `restore_object` no-op. Near-zero cost; unblocks migration, validation and compliance probes.
2. **Configuration-storage batch** — lifecycle, policy, encryption `put`/`delete`, notification, ownership controls, public access block, request payment, accelerate. One shared per-bucket config row set; answer defaults where nothing enforces the config yet.
3. **Object Lock probe answers** — legal hold / retention / lock configuration, as their own batch: the right answers are "not enabled" errors, not empty configs.
4. **`get_bucket_policy_status`** — free once policy storage exists.
5. **Real versioning + `list_object_versions`** — only as a versioning design; the other Tier-A items do not depend on it.

## Findings (adjacent to this doc's scope, but protocol-visible)

- **`[s3] sig_v2` is dead config.** `tinio-config` parses it (deprecated, default `false`), but no crate calls `S3ServiceBuilder::set_config`, so s3s's `S3Config::default()` governs: `enable_sig_v2: false`, `allow_presigned_url: true`, `allow_post_signature: true`, `presigned_url_max_skew_time_secs: 900`. `src/config.rs` is byte-identical 0.16.0 → 0.17.0, so the 0.17 bump leaves this and the stale `default true` in `2026-09-05-s3-acl-owner-design.md` unchanged. Either wire `S3Config` and map `[s3] sig_v2`, or drop the key.
- **`specs/001-s3-local-server/contracts/s3-surface.md` counts are stale** ("99 total", "~30 implemented", s3s 0.15 references). The real numbers are 108 declared in s3s 0.15.0 / 114 in 0.16.0 and 0.17.0 (113 effective, minus the `minio`-gated method) and 37 reachable in tinio. Same refresh needed.
- **s3s feature surface:** tinio compiles s3s with default features only. Enabling `minio` would add `listen_bucket_notification`; `openssl` would swap the SigV4 crypto backend. Neither is enabled today.

## Open questions

- Config-storage batch: store policy bodies while there is no policy evaluator, or reject `put_bucket_policy` to avoid implying enforcement the server does not perform? (ACLs remain the only enforced authorization model.)
- Versioning: answer `Disabled` indefinitely, or design real version rows? Every other Tier-A item is independent of this decision.
- Object Lock: answer AWS-shaped "not enabled" errors, or stay 501 until Object Lock is a real feature?
- Counts need one home: this doc and `contracts/s3-surface.md` both track the same implemented/501 split — pick one as the source of truth so they cannot drift again.
