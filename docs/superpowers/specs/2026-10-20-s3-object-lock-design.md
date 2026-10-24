# Design: S3 Object Lock on the version model (FR-043)

**Date**: 2026-10-20 (revised after the four-agent audit)
**Status**: revised and binding. The grilling and the four-agent audit settled the operator choices: capability-off keeps enforcement; existing buckets may be enabled without a token; the governance bypass is the addressed version's owner/FullControl and is **false** when ACL is off; GET/HEAD echo the lock headers behind a per-bucket negative cache; no new crate; the full S3 surface plus an mc-aligned CLI and management plane; the lock table is keyed by version identity; deletion is enforced in the storage primitives **and every backend's current-version delete path**; the bucket config is an enabled bit plus a rule wire; AWS error codes; the marker mix (405 explicit / 404 `NoSuchKey` latest / 404 `NoSuchObjectLockConfiguration` non-marker); the past date is `InvalidRequest`; the headers follow s3s's bool parser; the checksum keeps the verifying tee and is presence-only for the two XML operations; lifecycle/sweeper skip and count; the management plane is lifecycle-gated with a distinguishing 404 body; pg ships in the same batch with **no old-database migration**; one plan, one operator commit; a protected-deletion counter; the capability defaults on. The post-audit revisions: `_with` contract methods; required fail-closed methods; the addressed version's ACL; CLI auto-enables versioning; the fs create compensation + startup repair; doctor reports corrupt/orphan rows; `expire_many_report` is one set-based transaction.
**Scope**: `tinio-core` (the model, the pure rules, the bucket/version/upload metadata contract, the storage error variants), `tinio-store-redb` (three additive tables, shared by fs and mem), `tinio-store-pg` (the same three tables in `0001_init.sql`, inline/file statements, the `.sqlx` cache), `tinio-ops` (the bodies, the write-path resolution, the lock-aware delete primitives), `tinio-fs` / `tinio-mem` / `tinio-pg` (the backend overrides, including `tinio-fs/src/version_adapter.rs`), `tinio-server` (six S3 operations, headers, capability), `tinio-auth` (the six operations' rows, the addressed-version authority), `tinio-lifecycle` (a protected-version skip), `tinio-api` (two management routes and the admin trait), `tinio-cli` (the `retention` / `legalhold` / `object-lock` groups, the doctor check, the `[s3] object_lock` key, the `MetricsStorage` forwards, the admin impl in `start/run.rs`), `tinio-fs` (the startup repair), `tinio` (the feature in `full`), `tinio-util` (the conformance legs and the metric), `tinio-e2e` (the feature, CLI and boto3 journeys), and the corpus files under **Ownership**. Not touched: the version state machine, the lifecycle rule engine, the block store, and the sweeper's scheduling.
**Spec**: `specs/001-s3-local-server/spec.md` FR-043 — the authoritative requirement. This document states every decision the requirement leaves open.
**Reference**: AWS S3 Object Lock (userguide and six API pages), MinIO's lock package and handlers, the `mc` command surface, RustFS's storage-level lock module. Citations live in `docs/superpowers/object-lock-prior-art-research.md`; §Prior art states only what the design does with them.

## Background

Object Lock lands on the existing version model (three bucket states, version rows, the row-less current slot, null versions, delete markers, `VersionMeta::expire`/`expire_many`/`advance_slot`). Lifecycle already deletes through the same `Storage` methods. Three facts from the audit bound the shape:

### Delete paths

- **fs delete path.** `delete_object_version`'s `Found::Current`/`Found::Marker` arms remove/restore without `expire`.
- **fs path.** `crates/tinio-fs/src/backend/version.rs:1148-1194`.
- **mem delete path.** `drop_current` (`crates/tinio-mem/src/version.rs:1748-1760`) does likewise.
- **Enforcement.** The primitive and every backend's current/marker delete arm.
- **Row-less current is hard.** No version row; identity in slot. `expire` resolves slot, deletes its lock row, one transaction.

### Serving path

- **The serving path is decorated.** `MetricsStorage` manually implements all four ops traits.
- **Decorator path.** `crates/tinio-cli/src/commands/start/write_metrics.rs`.
- **Forwarding.** Every new method must be forwarded there; a security read must never default to "unlocked".

## Goal

The AWS Object Lock surface on tinio's version model: bucket configuration and default retention, per-version GOVERNANCE/COMPLIANCE retention and legal holds, write-time lock headers, the governance bypass, delete protection inherited by S3, bulk delete, lifecycle and the sweeper, the versioning-state rules, the S3 wire and error shapes, and an mc-aligned local CLI over a management plane — proven per backend by the conformance harness and end to end by cucumber, the CLI and boto3.

## Non-goals

### Not modeled

- **Variable retention** (`EventHold`/`EventHoldDuration`): `s3s` 0.17's `ObjectLockRetention` has no such fields.
- **Bucket-policy condition keys**: tinio has no IAM policy engine.
- **Replication, S3 Inventory, access-log restrictions, Storage Lens**: no subsystem.
- **MFA delete**: already a versioning non-goal.
- **Force delete**: the bucket must be empty; no force path.

### Not provided

- **A new background pass**: Object Lock is enforced at write/delete time.
- **A configuration-file seed**: the stored configuration is written through the S3 API or the plane.
- **mc's `--recursive`, `--versions`, `--rewind`**: one object/version per invocation.

## Decisions

| # | Decision |
|---|---|
| OL1 | **The full AWS surface the framework models.** Bucket `ObjectLockConfiguration` (`Enabled` + one optional `DefaultRetention`), per-version `Retention`/`LegalHold`, lock headers on `PutObject`/`CopyObject`/`CreateMultipartUpload`, the bypass header on `DeleteObject`/`DeleteObjects`/`PutObjectRetention`, lock headers on `GetObject`/`HeadObject`, the `CreateBucket` header, and the versioning-suspension refusal. `PostObject` has no row of its own: `s3s` maps its form fields into `PutObjectInput` (including the three lock fields), so it reaches the same write path. |
| OL2 | **The model and enforcement are always compiled; the cargo feature gates the surfaces only.** `tinio-core::object_lock`, the contract, the tables and the primitives are ungated. `object-lock` gates the six S3 operations, the plane routes and the CLI groups; `[s3] object_lock` (default true) is the runtime capability. Off answers `NotImplemented`/`404` on the surfaces while stored configs, default retention and delete protection stay in force. **A security read never defaults to unlocked**: every lock method is **required** (no default body); the `--all-targets` gate enumerates every implementor. |
| OL3 | **Two management surfaces, one stored fact.** The S3 API is the six operations. The plane carries a bucket-config route and one object route (key/version in the JSON body, never a path segment). The CLI drives the plane, never a store handle and never a new S3 client. There is no configuration-file seed. |
| OL4 | **The bucket configuration lives in its own table, never in the bucket row tuple, with a non-corruptible enabled bit.** `bucket_object_lock(bucket → (enabled, rule wire))` in the shared redb layer and in pg's `0001_init.sql`; additive; `FORMAT_VERSION` stays `1`; the rule wire's `None` is the empty string (pg `rule text not null`, empty = no rule). The row is cleared by **every** bucket-row writer: `create`, `record`, `record_if_absent`, `record_full`, the lazy create, `remove`, `remove_if_empty`, and the fs/mem/pg teardowns (the LC19 rule). pg's `REFERENCES buckets(name) ON DELETE CASCADE` covers the cascade; redb needs the explicit clears. **Read-only compatibility**: the three new redb tables are `MAY_BE_ABSENT`, so a root written before the feature serves with no locks instead of failing; the writable open still `ensure_all`s them. **pg migration**: `0001_init.sql` is edited and `SCHEMA_VERSION` stays `1`; **no old-database migration path is offered** — a database created by an older build is not supported and is recreated fresh from the new initial schema (the operator's 2026-10-20 ruling). The contract states "new databases only"; no dump/restore runbook. |
| OL5 | **Per-version lock state lives in its own table keyed by the version identity.** `version_object_lock(bucket, key, version_id wire)` → `(mode, retain_until unix nanos, legal_hold)`, the null version's wire being the literal `null`; a lock-free version has no row. The identity is stable from current to history, so the row follows the version without a move. Markers never get a row. `VersionMeta` gains `version_lock`, `get_object_retention`, `get_object_legal_hold`, `set_retention`, `set_legal_hold`, `set_lock`, plus the test-only `set_version_lock` (gated behind `cfg(any(test, feature = "testing"))` or an `Unchecked` token; it is the only raw writer and may never be used by the API/plane). `set_retention`/`set_legal_hold` do the read-modify-write in **one transaction**, and `set_lock` does the retention member and the legal-hold member as one decision and one row write in the same transaction; all three assert the identity exists (the current slot or a version row of `Kind::Object`; a marker/absent key refuses), require the bucket's configuration to be enabled and versioning to be `Enabled`, and fail closed on a corrupt mode, retain-until, legal-hold or a partial mode/date pair. Every read fails closed too: a corrupt/partial row is `Corrupt` → `500 InternalError`, **never** an omitted header or an "unlocked" answer (R28). |
| OL6 | **Multipart lock intent is resolved at create and written atomically with the upload row.** `UploadMeta::create_with_lock` (or `create_guarded_with_lock`) writes the upload row and `upload_object_lock(bucket, upload_id)` in one transaction; the effective lock (explicit or the bucket default evaluated at create) is fixed then, so a mid-upload config change cannot alter it. Complete applies the stored intent; abort/expiry/teardown clear it. `CompleteMultipartUpload` rejects any lock/bypass header in the raw map (`InvalidRequest`). |
| OL7 | **The lock is committed with the current version, and a slot advance never clears it.** The versioned write's commit is `advance_slot_with(bucket, key, id, seq, lock: Option<&Lock>)`: it writes the row only when `lock` is `Some` and **never removes an existing row**. Clearing happens only in the removal primitives. A null in-place replacement of an identity that already owns an active lock is refused (a protected overwrite); an ordinary overwrite creates a new identity and leaves the old lock intact. The effective lock is resolved in the backend write path, where `BucketMeta` is in hand (the `VersionPlane` bounds carry only `Metadata + VersionMeta`); the resolved `Option<&Lock>` is passed into `put_object_versioned`/`write_object_recording_versions`/`publish_versioned` and then `advance_slot_with`. The old `advance_slot` delegates with no lock and never clears. |
| OL8 | **Deletion enforcement lives in the primitive and in every backend's current/marker delete arm.** `expire_with(bucket, key, seq, bypass)` refuses a protected identity and removes row, index entry and lock row in one transaction, answering `Expired::{Removed, Nothing}` (a refusal stays `Err(Locked)`, so it never reaches the verdict); its no-row fallback judges the current slot even when the addressed sequence differs from the slot's, so the fs degraded-null delete is judged by the store. `expire_many_report` skips protected sequences and reports `(removed, protected-with-reason)`, judging a repeated sequence once (first occurrence); the old `expire`/`expire_many` delegate (no bypass / discarded report). fs `Found::Current`/`Found::Marker` and mem's `drop_current` gain the same check and lock-row removal before the file/row removal; the fs current arm unlinks the key-path file after any successful verdict (the degraded-null file is reclaimed there — the sweep only reclaims unreferenced blocks). `drain_bucket` **refuses only when a protected version exists**; an orphan lock row with no version row is removed in the drain, and the bucket-emptiness decision includes version rows and lock rows. The server-callable read is `VersionOps::get_object_retention`/`get_object_legal_hold` (the server cannot see `VersionMeta`), and the same methods feed the GET/HEAD echo. |
| OL9 | **The plain DELETE is never blocked; markers are never protected.** A DELETE without a version ID writes a delete marker. A lock-enabled bucket cannot be unversioned or suspended. Permanent deletes of protected versions are refused. Marker addressing follows the version model: an **explicitly addressed** marker → `405 MethodNotAllowed`; no version ID and the current version is a marker → `404 NoSuchKey`; a non-marker version with no lock → `404 NoSuchObjectLockConfiguration`. |
| OL10 | **The rules are the model's pure functions, with a `Bypass` newtype.** `write_lock(explicit, rule, now)`, `apply_retention(current, requested, bypass, now)`, `may_delete(lock, now, bypass)`, `default_retention_at(rule, now)`. `Bypass::{None, Governance}` is a contract newtype, not a bare bool. A `Years` default is calendar years with the Feb-29 → Feb-28 fallback; days `1..=36_500`, years `1..=100`. Any invalid stored element or partial pair fails closed. |
| OL11 | **Compliance and legal holds cannot be bypassed; expired retention is inert.** The bypass is consulted only for an unexpired GOVERNANCE retention; COMPLIANCE refuses deletion, shortening, mode change and removal even with the header; a legal hold refuses deletion in either mode. Bypassing governance never changes a legal hold. |
| OL12 | **A write to a locked key creates a new version; an in-place replacement is the exception.** `CopyObject` does not inherit the source lock; the destination's headers/default apply. `RenameObject` is already refused on a versioned bucket. A suspended in-place null write that would replace a locked identity is refused; a lock cannot exist on a bucket without a config (OL13), so this is reachable only through the audited hole. |
| OL13 | **The versioning-state and per-object rules are the store's, under one row lock.** `PutBucketVersioning` to `Suspended` is refused (`InvalidBucketState`, 409) when a lock config exists; the first `PutObjectLockConfiguration` requires versioning `Enabled`; only `Enabled` is accepted and it can never be disabled. `set_retention`/`set_legal_hold` require the bucket config enabled and versioning `Enabled`. On pg, both cross-checks first `SELECT ... FOR UPDATE` the **bucket row** (or the `bucket_versioning` row) in the same transaction, closing the Read Committed write skew; redb's single writer serializes. `CreateBucket` with the header writes bucket + versioning + config in one step (the default implementation compensates; redb/pg override atomically). fs's three-commit path is compensated on error, and a crash residue (a lock-enabled bucket with one of the three facts missing) is completed or cleared by the startup repair (OL31). |
| OL14 | **Existing-bucket enablement is supported, without a token.** Unlike MinIO (creation-only), an existing bucket whose versioning is already enabled may be enabled. AWS's support token has no local issuer; the request is accepted without it and the deviation is recorded. RustFS follows the same versioned-existing path. |
| OL15 | **The governance-bypass authority is the addressed version's owner/FullControl.** The S3 layer resolves `may_bypass` for the **version the request addresses**: `version_acl` for a version-addressed delete/retention/legal-hold, the current object's ACL only for an unaddressed request, and bucket owner parity. The bit rides the s3s request extensions into the handler. With `[s3] acl = false` or no identity plane there is no owner, so **`may_bypass` is false** (a governance lock cannot be bypassed); ACL-off still permits ordinary deletes. The plane's token is the local operator's authority; the CLI's implicit clear bypass is authorized by that token, not an ACL. Legal hold has no bypass. The **base operation gate** of the version-addressed object operations — `DeleteObject`, each `DeleteObjects` entry, `Get/PutObjectRetention`, `Get/PutObjectLegalHold`, and the tagging/attributes class `Get/Put/DeleteObjectTagging` and `GetObjectAttributes`, whose handlers likewise serve the version the request names (R31) — also authorizes the addressed version's ACL with the same permission class as the current-object gate: a malformed address fails closed, a well-formed address naming no version passes through to the handler's `404 NoSuchVersion`, and a delete-marker address (the stores report it with the same `NoSuchVersion` class) keeps the deny-first fail-closed posture, so a non-owner's marker delete stays 403. |
| OL16 | **A write that results in a retention requires a checksum.** `PutObject`/`PostObject`/`CopyObject`/`CreateMultipartUpload` require `Content-MD5` or a checksum header/trailer when the effective lock carries a retention; missing is `400 InvalidRequest`, a malformed `Content-MD5` is `400 InvalidDigest`. The claim is scoped: `PutObject`/multipart already have a verifying tee when `[s3] checksum = true`; only `PutObjectRetention`/`PutObjectLegalHold` are presence-only (the XML body is consumed). The effective lock/rule is resolved **once** and the same fact drives the checksum decision and the write, so a concurrent config change cannot make them disagree; a per-bucket negative cache is the upgrade path. `PostObject` reads the mapped `PutObjectInput` fields, not only headers. |
| OL17 | **The wire and error surface follow AWS, with the documented calls.** Protected permanent delete → `403 AccessDenied`; missing per-object lock → `404 NoSuchObjectLockConfiguration`; missing bucket config → `404 ObjectLockConfigurationNotFoundError`; `InvalidBucketState` → 409; lock headers on a bucket without a config → `400 InvalidRequest`; a past `RetainUntilDate` → `400 InvalidRequest` (AWS does not name the code; MinIO uses `MalformedXML` for the XML body, RustFS `InvalidArgument`); a missing pair on the retention XML → `400 MalformedXML`; `CompleteMultipartUpload` lock headers → `400 InvalidRequest`; `CreateBucket`'s `x-amz-bucket-object-lock-enabled` follows s3s's bool parser (`true`/`True`/`false`/`False`; anything else is the framework's `400 InvalidArgument`). A protected entry in `DeleteObjects` is a per-key `AccessDenied`, the rest proceeds. |
| OL18 | **GET/HEAD echo the lock headers, keyed by the identity wire, not by `Option`, behind a per-bucket negative cache.** The handlers use `(bucket_records_versions, wire_of(version_id))` — the same rule the version-id header uses — so a locked null version (`version_id = None`) is not skipped. The lookup is skipped when the capability is off and only runs for a versioned bucket. A process-local `has_lock_config` cache keyed by bucket makes the first GET/HEAD per bucket read the config once; a `false` entry then skips the lock table for every subsequent GET/HEAD/range. Both writers insert the resulting fact (a config write → `true`, a bucket delete → `false`) instead of bumping a generation, so a stale reader can never hide a new configuration; the miss path is a plain `entry().or_insert(read)` behind an `RwLock` (the map is read-mostly). A bucket with a config still pays the point read — one read: the echo, the legal-hold operation and the plane admin use `VersionOps::object_lock`. |
| OL19 | **Lifecycle and the sweeper skip protected versions silently.** A `Locked` in lifecycle's noncurrent expiration is a counted skip, not an error; current-version expiration still writes a marker; `ExpiredObjectDeleteMarker` removes markers (not protected); the sweeper's `expire_many_report` skips and reports. `DeleteAllVersions`/`DelMarkerExpiration` are not in tinio's lifecycle action set; no new validation is needed. |
| OL20 | **Read-only and capability-off are distinct.** Read-only refuses the three lock writes through the operation table and serves the three reads; enforcement is not a write path and stays. Capability off refuses the six operations and the plane (`404`), while stored configs, default retention and delete protection stay; GET/HEAD lock headers are skipped. A stripped build is the capability-off shape at compile time and still refuses protected deletes; a runtime feature-off test proves it. |
| OL21 | **The conformance harness is the oracle, per backend.** The legs cover bucket config, the version lock on current/history/null, the **current-version delete by id and null**, markers, corrupt rule/mode/retain-until/partial pairs, governance bypass, compliance refusal, expiry, `expire_with`/`expire_many_report`, drain refusal, read-only pre-feature roots, and the upload intent. redb proves fs/mem through `RedbMetadata` **and** the fs `version_adapter` adapter; pg proves itself under `TINIO_PG_DSN`. The harness's corrupt-row legs live in the per-store tests (raw access), not the shared suite. |
| OL22 | **The corpus owns the facts, once.** `contracts/s3-surface.md` owns the six operations and wires; `contracts/config.md` owns `[s3] object_lock`; `contracts/management-api.md` owns the two plane routes; `contracts/cli.md` owns the three groups and FR-007's **second** exception (FR-042 is the first); `data-model.md` owns the three tables and the metric; `spec.md` owns FR-043; `tasks.md` owns T176+. |
| OL23 | **The management plane follows the lifecycle shape, with explicit updates.** `tinio-api` gains `ObjectLockAdmin` (`bucket_config`, `put_bucket_config`, `object_lock`, `put_retention`, `put_legal_hold`, `update_lock`) injected by the CLI (the `LifecycleAdmin` precedent). Two routes: `GET/PUT /buckets/{bucket}/object-lock` and `GET/PUT /buckets/{bucket}/object-lock/objects`, the key and a `VersionAddress` (`current`/`null`/id) in the JSON body. `put_bucket_config` rejects `enabled:false` (the setter only enables or updates the rule); the object route's retention update distinguishes **set / clear / leave** and legal-hold set/clear, so clearing one never clears the other. **The object `PUT` applies its present members in one store transaction** (`VersionOps::update_lock` → `VersionMeta::set_lock`): a refused retention answers its `Locked` status and applies neither member, and the legal hold has no bypass. Errors map `Locked` → 403, `InvalidBucketState` → 409, `NoSuchBucket` → 404, `NoSuchVersion`/`NoSuchObjectLockConfiguration` → 404; the 404 body distinguishes "no such bucket" from "not enabled"/"no retention". Routes register only when the feature is on, the capability is on and the admin is injected; otherwise `404`; read-only serves GET and answers 403 for PUT. The admin impl/injection lives in `start/run.rs`. |
| OL24 | **The CLI mirrors `mc`, minus the alias layer, and can reach the state its commands need.** `tinio retention set|info|clear`, `tinio legalhold set|info|clear`, `tinio object-lock enable`. `object-lock enable` and `retention set --default` **enable versioning first** when the bucket is unversioned, then write the lock config (a CLI convenience; the S3 `PutObjectLockConfiguration` still requires versioning). `retention clear --default` only clears the rule, keeps `enabled`, and reports "not enabled" without writing on a non-enabled bucket. `legalhold set`/`clear` are ON/OFF; `VALIDITY` is `Nd`/`Ny`; `--bypass` only on `retention set`; `retention clear` carries the implicit governance bypass under the plane token. `retention info <bucket>` without a key reads the bucket config; `set`/`clear` without `--default` need a key. `--json` per subcommand mirrors `mc`'s fields. |
| OL25 | **One metric family, two bounded labels.** `tinio_object_lock_protected_deletions_total{operation, reason}`, `operation ∈ {delete_object, delete_objects, lifecycle, sweeper}`, `reason ∈ {legal_hold, compliance, governance}`, registered once in `tinio-util::metrics`; no key/version/bucket label. |
| OL26 | **pg ships in the same batch; one plan; one operator commit.** redb (fs + mem) and pg prove the same legs; pg e2e is `TINIO_PG_DSN`-gated. `0001_init.sql` is edited and `SCHEMA_VERSION` stays 1 (the operator ruling); **no old-database migration path is offered** — new databases only. |
| OL27 | **Contract evolution is additive, and the old methods delegate.** New `expire_with` (returns its `Expired` verdict), `expire_many_report`, `advance_slot_with`, `set_retention`, `set_legal_hold`, `get_object_retention`, `get_object_legal_hold`, `create_with_lock`, and the `VersionOps::object_lock` single-read seam (composing default; every store overrides it with one read); the old `expire`/`expire_many`/`advance_slot` keep their signatures and delegate (no bypass / discarded report / no lock). The new methods are **required, with no default body** — never "unlocked". `set_version_lock` is compiled only under `cfg(any(test, feature = "testing"))`. This keeps Task 2 compiling while every real store and the `MetricsStorage` decorator forward. |
| OL28 | **The serving decorators forward; the server reads through `VersionOps`.** `MetricsStorage` (`write_metrics.rs`) implements every new method by forwarding; the `StorageAdmin<S: Storage>` uses the `VersionOps` read/write methods, not `VersionMeta`. The server's `GetObjectRetention`/`GetObjectLegalHold`/echo use the same `VersionOps` reads, and the echo's whole lock is the one `VersionOps::object_lock` read. |
| OL29 | **One version-address model for S3 and the plane.** `version::Address { Current, Null, Id }` is the S3 mapping; the plane's JSON spells the same three states (`current`/`null`/an id), never an `Option<Id>` where absent and null collapse. `Address::Current` resolves to the current slot's identity (which may be `None`), `Null` to `None`, `Id` to the id. |
| OL30 | **The CLI's `--json` is an output shape, not a wire.** The plane's JSON is the stored model (snake_case); the CLI's `--json` mirrors `mc`'s field names. There is no third stored wiring. |
| OL31 | **doctor reports; the fs startup repair completes.** `tinio doctor` scans the three tables and reports a corrupt rule/mode/retain-until/partial pair and an orphan lock row, dry-run-aware and **report-only** (no repair). The fs startup repair completes or clears a partial lock-enabled bucket left by a crash; redb/pg create atomically. |

## Design

### The model (`tinio-core/src/object_lock.rs`)

```rust
pub struct Config { pub enabled: bool, pub rule: Option<Rule> }
pub struct Rule { pub default_retention: DefaultRetention }
pub struct DefaultRetention { pub mode: Mode, pub period: Period }
pub enum Period { Days(u32), Years(u32) }
pub enum Mode { Governance, Compliance }
pub struct Retention { pub mode: Mode, pub retain_until: SystemTime }
pub enum LegalHold { On, Off }
pub struct Lock { pub retention: Option<Retention>, pub legal_hold: LegalHold }
pub enum Bypass { None, Governance }
pub struct Refusal { pub reason: RefusalReason, pub detail: &'static str }
pub enum RefusalReason { LegalHold, Compliance, Governance }
```

`Config` is the stored bucket value, split in the table into the `enabled` bit and the rule wire; the rule wire's `None` is the empty string. `Lock` is the per-version value, split into three columns/elements. Decoding never errors: a corrupt rule is `None` with a warning while `enabled` survives; any invalid per-version element (mode, retain-until, legal-hold) or a partial pair fails **closed** (the delete path refuses; a read answers `500 InternalError`, never an omitted header that would read as "unlocked"). Validation is the request path's: only `Enabled`; mode + exactly one period; days `1..=36_500`, years `1..=100`; a future date; legal-hold `ON`/`OFF`.

The pure rules: `write_lock`, `apply_retention`, `may_delete`, `default_retention_at`; `Bypass` is the newtype. A legal-hold-only write still receives the default; explicit retention overrides; `Years` is calendar years with the Feb-29 fallback.

### The storage model

| Layer | Where |
|---|---|
| Bucket config | `bucket_object_lock(bucket → (enabled, rule wire))`, one row per bucket; cleared by every bucket-row writer and every teardown; pg cascade; redb tables `MAY_BE_ABSENT` |
| Version lock | `version_object_lock(bucket, key, version_id wire) → (mode, retain_until, legal_hold)`; the null wire `null`; no row = unlocked; removed with the version; a corrupt/partial row fails closed |
| Multipart intent | `upload_object_lock(bucket, upload_id)`, written by `UploadMeta::create_with_lock` in the upload row's transaction; cleared at complete/abort/expiry/teardown |

The redb tables join the shared layer; pg's join `0001_init.sql`. `FORMAT_VERSION` stays 1; the table-list pins grow by three; the version-row 11-tuple and bucket-row 5-tuple pins stay untouched. The `drain_bucket` and bucket teardowns remove the lock rows; the bucket-emptiness decision includes version/lock rows.

`VersionMeta` gains:

```rust
async fn version_lock(...) -> Result<Option<Lock>, Self::Error>;
async fn get_object_retention(...) -> Result<Option<Retention>, Self::Error>;
async fn get_object_legal_hold(...) -> Result<LegalHold, Self::Error>;
async fn set_retention(..., requested: Option<&Retention>, bypass: Bypass, now: SystemTime) -> Result<Option<Lock>, Self::Error>;
async fn set_legal_hold(..., hold: LegalHold) -> Result<Lock, Self::Error>;
async fn expire_with(..., bypass: Bypass) -> Result<Expired, Self::Error>;
async fn expire_many_report(...) -> Result<ExpireReport, Self::Error>;
async fn advance_slot_with(..., lock: Option<&Lock>) -> Result<(), Self::Error>;
```

The old methods delegate. `set_retention`/`set_legal_hold` assert the identity and the bucket config in the transaction; `set_version_lock` is test-only. On pg, the retention write, `expire_with` and the bucket cross-checks lock the version row / `current_slots` row / bucket row `FOR UPDATE`; redb's single writer serializes.

### The write path

The effective lock is resolved **once** in the backend write path (where `BucketMeta` is in hand), from the bucket config and the explicit headers. It is passed into the actual seams: `ObjectOps::commit_object`, `ObjectOps::copy_object` (and the fs `publish_versioned` fast path), `MultipartOps::create_multipart_upload` (which stores the effective lock through `create_with_lock`), and the mem `write_object_recording_versions`. The versioned commit calls `advance_slot_with`; a null in-place replacement of a locked identity is refused; `advance_slot_with(lock: None)` never clears. The checksum decision uses the same resolved lock. `CopyObject` does not inherit; `PostObject` reaches the same handler.

### The delete path

- Plain DELETE: a marker; no lock check.
- `DeleteObject?versionId`: `bypass = header && may_bypass`; `delete_object_version` forwards. fs/mem arms check; pg/ops use `expire_with`.
- `DeleteObjects`: per-entry; a protected entry is `AccessDenied`, the rest proceed.
- Lifecycle: a `Locked` is a counted skip; markers still expire.
- Sweeper: `expire_many_report` skips and counts.
- `drain_bucket`: refuses while a lock row exists; the emptiness probes include version/lock rows.

### Retention writes

- `PutObjectRetention`/`LegalHold`: resolve `Address`; config + versioning required; atomic store.
- Marker 405; unlocked non-marker 404 `NoSuchObjectLockConfiguration`.

### The S3 surface

Six rows in `tinio_s3_ops!` with `#[gated(object_lock: "object-lock")]`; the three GETs are read rows, the three PUTs write rows. `CreateBucket` reads the header (s3s's bool parser). `PutBucketVersioning(Suspended)` is refused with 409. The write headers enforce the checksum; `CompleteMultipartUpload` rejects lock/bypass headers. GET/HEAD echo the lock headers keyed by `(records_versions, wire_of(version_id))`. A missing per-object lock is 404 `NoSuchObjectLockConfiguration`; a missing bucket config is 404 `ObjectLockConfigurationNotFoundError`; a protected delete is 403; `InvalidBucketState` is 409. The marker rules are OL9.

### Auth

The six matrix rows; `may_bypass` resolves the **addressed version's** ACL (`version_acl`) for version-addressed requests, the current object only for unaddressed, and bucket owner parity. With ACL off/no identity, `may_bypass = false`. The bit rides the s3s extensions; the handler passes `header && may_bypass` as `Bypass::Governance`.

### Management plane and CLI

`ObjectLockAdmin` with `bucket_config`, `put_bucket_config`, `object_lock`, `put_retention`, `put_legal_hold`; two routes; the version address in the body; `enabled:false` refused; retention set/clear/leave and legal-hold set/clear are distinct; errors map 403/404/409; the 404 body distinguishes causes; the admin impl/injection in `start/run.rs`; only when the feature and capability are on. The CLI groups and flags are OL24; `enable`/`set --default` enable versioning first; `clear --default` never enables; `--json` mirrors `mc`; the CLI drives the plane.

### Lifecycle, sweeper, observability, gating

Lifecycle's runner counts a skip; the sweeper consumes `expire_many_report`; the metric records each protected refusal with the operation/reason labels. Read-only serves the GETs; capability off answers `NotImplemented`/`404` but enforcement stays; a stripped build is tested at runtime. The stores and the model are ungated; the surfaces are gated per task.

## Testing

| Level | What proves it |
|---|---|
| Model | validation matrix; wire round trip; corrupt/partial fail-closed; calendar years/Feb-29 |
| Pure rules | default/explicit/legal-hold independence; governance/compliance/expiry; `Bypass`; reason |
| Contract | redb and pg: bucket config, current/history/null locks, **current-version delete by id and null**, markers 405/404, corrupt/partial, `expire_with`, `expire_many_report`, drain refusal, upload intent atomicity; fs `version_adapter` adapter; read-only pre-feature root |
| Engine | actual write seams (`commit_object`, `copy_object`, `publish_versioned`, `create_multipart_upload`), default/explicit, copy no inherit, multipart fixed at create, null overwrite refusal, checksum decision |
| S3 | `object_lock.feature` (`@object-lock`, `@FR-043`): six round trips, create header, suspension, write headers, checksum, protected 403, bulk per-key, legal hold, compliance, bypass, GET/HEAD echo, null version echo, capability off, read-only |
| Plane | two routes, JSON, `enabled:false` refusal, set/clear/leave, version address, 403/404/409, 404 body, admin gate, read-only |
| CLI | seven commands, `--json`, auto-enable versioning, `clear --default` no-op on disabled, `Nd`/`Ny`, no-server refusal |
| Doctor/repair | doctor reports a corrupt rule/mode/retain-until/partial pair and an orphan lock row (dry-run, no repair); the fs startup repair completes/clears a partial lock-enabled bucket |
| Cache | the GET/HEAD `has_lock_config` cache skips the lock table for a bucket with no config and is invalidated by `put_bucket_object_lock`/bucket delete |
| Metric | one registration; both labels; S3/lifecycle/sweeper feed it |
| Auth | addressed-version ACL; current-owner cannot bypass an older owner's lock; ACL-off no bypass; compliance refused |
| Builds | each feature-gated task runs `--features object-lock`; stripped build runtime refusal; `cargo hack --skip otel` |
| Interop | boto3 journey |

## Review Focus

### Store and enforcement

- Current-version permanent delete on fs/mem bypasses `expire`; the arm itself must check and remove the lock row.
- `advance_slot_with(None)` must never clear; a null in-place replacement of a locked identity is refused.
- Per-object writes on a bucket without a config; the suspension cycle that would overwrite a locked null version.
- The addressed version's ACL, not the current object's.
- Corrupt/partial rows fail closed; required methods never answer unlocked.
- The plane must not disable; the address must distinguish current/null; the 404 must distinguish causes.

### Surfaces and delivery

- Read-only over a pre-feature root; the three tables `MAY_BE_ABSENT`.
- Bucket-row writers and drains; stale/orphan rows; the drain removes orphans but refuses a protected version.
- The checksum decision and the applied lock are one fact; the tee stays for PutObject/multipart.
- The GET/HEAD cache is invalidated by `put_bucket_object_lock` and bucket delete; a stale `false` must not hide a new config.
- The fs startup repair completes/clears a partial lock-enabled bucket; doctor reports corrupt/orphan rows.
- Feature-gated tasks actually compile/run the new code.

## Accepted limits

### Scope and cost

- No variable retention; no IAM condition keys.
- `PutObjectRetention`/`LegalHold` are presence-only for the checksum; PutObject/multipart keep the verifying tee.
- Configured bucket: **two** point reads per GET/HEAD/range (retention, legal hold separate); no config: one cached read.
- pg `0001` edit + `SCHEMA_VERSION=1`: no old-database migration; new databases only.
- A corrupt rule loses the default, while the enabled bit survives; a corrupt/partial per-version row fails closed.

### Wire and compatibility

- Corrupt/partial row → `500 InternalError` on any read (GET/HEAD echo, `?retention`/`?legal-hold`); fail closed, no omitted header (R28).
- Existing-bucket enablement deviates from MinIO.
- `InvalidBucketState` is 409 (MinIO 400).
- The past-date code is `InvalidRequest` (MinIO `MalformedXML` on the XML body; RustFS `InvalidArgument`).
- Marker mix (version model): 405 explicit / 404 `NoSuchKey` latest / 404 `NoSuchObjectLockConfiguration` non-marker; ports differ.
- The fs `create_bucket_with_lock` is three commits + compensation + startup repair; redb/pg are atomic.

## Ownership

- `contracts/s3-surface.md`, `contracts/config.md`, `contracts/management-api.md`, `contracts/cli.md` own OL22's surface facts.
- `data-model.md`, `spec.md`, `tasks.md` own OL22's data, requirement and task facts.
- `tinio-core` model/rules; `tinio-ops` bodies; stores transactions; `tinio-server` wire.
- `tinio-auth` authority; `tinio-api` plane; `tinio-cli` verbs/decorators; `tinio-util` harness/metric.
- This document owns the decisions and the storage model, not the trait signatures the plan pins.

## Prior art (what the research changed)

AWS: the modes, bypass, markers, default independence, checksum, wires/errors. MinIO: multipart create-time lock, lifecycle skip, Complete header rejection, mc surface; rejected: creation-only enablement, 400 protected delete, 400 missing per-object config, legal-hold suppression of the default. RustFS: storage-level model, legal-hold-first, enabled-bit fail-closed, malformed fail-closed, Feb-29, multipart create-time default; rejected: partial pairs, empty GetObjectRetention, GetObject header gap, replication bypass. The audit's code findings (fs/mem current delete, `advance_slot` clear, `MAY_BE_ABSENT`, `MetricsStorage`, version ACL) are the reasons for OL7/OL8/OL15/OL21/OL27/OL28.

## My calls to check

The operator selected every previously open call: the marker mix (405/404/404), past-date `InvalidRequest`, ACL-off `may_bypass=false`, s3s header parsing, the per-bucket GET/HEAD cache, the verifying checksum tee, the fs create compensation + startup repair, the drain orphan rule, required fail-closed methods, `set_version_lock` under `cfg(test)`/`testing`, the distinguishing 404 body, the `retention info <bucket>` fallback, the doctor report, no pg old-DB migration, one set-based `expire_many` transaction, and PostObject support. What remains is implementation detail the plan pins: the exact cache eviction sites, the startup-repair ordering, and the doctor report shape.
