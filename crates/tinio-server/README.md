# `tinio-server` — the S3 data plane

`tinio-server` is the S3 data plane: the s3s protocol layer over the `tinio-core` `Storage` contract — one mapping for every S3 operation tinio answers, the hyper data plane that serves it, and the startup wiring a served process needs (identity, logging, metrics, the pipeline runtimes). It enforces the `[s3]` section's capability switches on the request path and owns the server-side read-only plane, and it does **not** own the `Storage` contract (`tinio-core`), the authn/authz engine (`tinio-auth`, reached only for the s3s auth and authorization providers it plugs in), the SQL engine behind S3 Select (`tinio-select`), the management plane (`tinio-api`), or any persistence — a backend is whatever type the caller hands it as `S: Storage`, and no backend crate is named in its own code outside `#[cfg(test)]`.

## Features

| Feature | Effect |
|---|---|
| `multipart` | the multipart family (Create, UploadPart, Complete, Abort, ListParts, ListMultipartUploads) |
| `copy` | CopyObject and RenameObject, over `tinio-auth`'s copy/rename source-header parser; one of the two gates on UploadPartCopy |
| `list-v1` | ListObjects |
| `list-v2` | ListObjectsV2 |
| `cors` | the CORS trio and the data plane's browser-preflight route |
| `select` | SelectObjectContent, over the `tinio-select` engine |
| `select-parquet` | `select` plus the Parquet input format |
| `acl` | the four ACL operations and the identity-backed authn/authz stack (`ConfigAuth` + `AclAccess`) |
| `versioning` | the version plane: Get/PutBucketVersioning, ListObjectVersions, the version-addressed object and delete surface, and the guard that refuses a version-addressed request outright |
| `object-lock` | the Object Lock surface over the version plane (implies `versioning`): the bucket configuration and default retention, per-version retention and legal hold, the six `?object-lock`/`?retention`/`?legal-hold` operations, the write headers and the GET/HEAD echo |
| `sandbox` | nothing in the library: it is the `required-features` gate of the confined-server end-to-end targets |
| `api` | the `tinio-api` re-export — a name for the management plane, with no server code behind it |
| `config` | the section types' serde + garde derives (the `[s3]` capability, identity, log and pipeline schemas the CLI composes) |
| `otel` | the OpenTelemetry export layer beside the ordinary logging layers |

## Layout

| File | What it is |
|---|---|
| `src/backend/mod.rs` | `S3Backend`: the type the s3s dispatch and every decorator wrap |
| `src/backend/errors.rs` | a backend failure mapped onto its S3 error code, with client-supplied values made XML-safe |
| `src/backend/ops.rs` | the operation inventory: one row per operation, naming its dto pair, its handler, its mutating kind, its stripping feature and its gating capability |
| `src/backend/s3.rs` | the s3s `S3` dispatch, generated from that inventory |
| `src/backend/buckets.rs`, `objects.rs`, `listing.rs`, `multipart.rs` | the operation groups as inherent methods |
| `src/backend/versioning.rs` | the version operations, and the one guard every version-addressed request passes |
| `src/backend/object_lock.rs` | the six Object Lock operations: the bucket configuration, per-version retention and legal hold, the write-header resolution and the GET/HEAD echo |
| `src/backend/cors.rs` | the CORS trio, and the preflight route the browser asks anonymously |
| `src/backend/acls.rs` | the ACL wire helpers and the write-path grant headers |
| `src/backend/select.rs` | the S3 Select operation: the async bridge over the synchronous engine |
| `src/backend/conditions.rs` | the conditional-header machinery, and the per-operation policies on a missing object |
| `src/backend/checksum.rs` | server-side checksum validation: the body tee and the full-object derivations (the backends never hash) |
| `src/backend/tags.rs` | the `x-amz-tagging` header and the tag conversions |
| `src/backend/locks.rs` | the per-object write lock the check-then-commit paths take |
| `src/backend/testutil.rs` | the backend modules' shared test fixtures (`#[cfg(test)]`) |
| `src/capability.rs` | the `[s3]` capability gate as a decorator |
| `src/readonly.rs` | the read-only plane as a decorator: reads forward, writes refuse |
| `src/data.rs` | the hyper data plane: `DataPlane`, `PlaneConfig`, the decorator stack and graceful shutdown |
| `src/pipeline.rs` | the pipeline runtime: the IO, removal and DB pipelines over their own runtimes |
| `src/identity.rs` | the config's auth/owner/user sections into the identity map and the owner-uid map |
| `src/identity/{auth,owner,users}.rs` | the `[auth]`, `[owner]` and `[[users]]` section types with their per-field rules (serde behind `config`) |
| `src/s3.rs` | the `[s3]` section's capability half — the runtime toggles and page-size caps (serde behind `config`) |
| `src/metrics.rs` | the Prometheus families, the metrics/access-log wrapper, and the refresh hook the management plane's scrape reaches through the CLI |
| `src/log.rs` | the access-log format and the operational logging layers |
| `src/error.rs` | the server's own startup and mapping failures |

## The `[s3]` capability switches (FR-021)

A disabled toggle answers `NotImplemented`, named by the **operation's** wire name (`"PutBucketTagging is disabled"`), never by the toggle. The refusals are one column of the operation inventory, so the answer and the operation list cannot drift; the switches themselves are this crate's `[s3]` section (`s3::Capabilities`), composed into the root document by the CLI. A group whose cargo feature has the same name is also stripped from the build, and the strip wins where the two disagree — an absent operation answers s3s's own default instead.

| Toggle | A disabled toggle refuses |
|---|---|
| `multipart` | the multipart family |
| `copy_object` | CopyObject, RenameObject; UploadPartCopy needs it **and** `multipart` |
| `list_objects_v1` | ListObjects |
| `list_objects_v2` | ListObjectsV2 |
| `delete_objects` | DeleteObjects |
| `tagging` | the six bucket and object tagging operations |
| `cors` | the three CORS operations (the preflight route is armed with the toggle) |
| `select` | SelectObjectContent |
| `acl` | the four ACL operations |
| `versioning` | Get/PutBucketVersioning, ListObjectVersions — and every version-addressed request is refused outright rather than answered from the current object |
| `object_lock` | the six Object Lock operations; the management plane's Object Lock routes are not registered and the GET/HEAD lock headers are skipped, while stored configurations, default retention and delete protection stay in force |

The toggles above gate an **operation**, which is what lets them buy a refusal. The section's other keys gate nothing: they are limits and durations (`max_buckets`, `max_keys`, `select_concurrency`, `select_stack_bytes`, and the sweep and retention numbers the CLI hands to a backend), a relaxation (`allow_zero_page_size`), and further booleans — `checksum`, an opt-in that validates rather than refuses, and `sig_v2`, deprecated and off by default. None of them answers `NotImplemented`.

## Gates

The confined-server targets spawn the real `tinio` binary, so build it first, in the same profile as the test run — a filtered `cargo test --test …` builds test targets only. Each confined target is compiled only on its own platform and carries `required-features = ["sandbox"]`, so a default build runs just the one its host can.

```console
cargo build -p tinio
cargo test -p tinio-server
cargo test -p tinio-server --features select-parquet
cargo bench -p tinio-server -- --test
```
