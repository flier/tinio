# S3 versioning, plan 3 — the S3 surface: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The wire — the three bucket/listing operations, `versionId` threaded through every operation that carries one, the response headers and status codes AWS documents (and no `Allow`), the three-level capability gating with its fail-open defense, the config keys, the read-only-mode amendment, and the cucumber journeys.

**Architecture:** Three new rows in the `tinio_s3_ops!` table give the operations their capability gate, their metrics and their read-only refusal for free — the table drives five consumers, so the rows are the whole wiring. Handlers thread `versionId` into the `VersionOps` calls plan 2 pins, and a single helper refuses any version-addressed request when versioning is not available, so a stripped build or a server with the toggle off can never answer a version request from the current version.

**Tech Stack:** Rust (edition 2024), `s3s` 0.17 (the trait and its DTOs), `axum`/`hyper` underneath, cucumber for the journeys.

**Spec:** `docs/superpowers/specs/2026-10-03-s3-versioning-design.md` — this plan implements its **Error surface**, **Feature gating and configuration**, **Interaction with existing behavior** (the read-only amendment) and the wire half of **Read paths**, plus the corpus updates its **Ownership** section names.

**Depends on:** plan 1 (`tinio-core::blob`, `tinio-blob`) and plan 2 (`tinio-core::version`, `VersionOps`, the version tables, the fs/mem implementations). **Amendment 2026-10-03:** plan 1 was executed and then revised on operator rulings — `tinio-blob`'s `versioning` feature and its crate-root gate were **removed** (the feature had exactly one referent, and its off-state compiled an empty crate), and `BlockStore` gained a required `walk`. Neither alters this plan's surface, but the compile-time level of the three-level gating model no longer includes plan 1's crate, and the gating prose in Task 8 below has been corrected to match. `tinio-server`'s own `versioning` feature — the one this plan is about — is unaffected.
>
> **Execution note (2026-10-06, review round).** This plan landed as one commit, `ba5004a6` (2026-10-06): every non-commit step below was verified against that tree and is ticked; the eight Commit steps are unticked because they were not performed as written (the tree landed as one bundled commit). Three steps are landed-as-amended: Task 2 Step 3 (one `require_versioning` body using `cfg!`, not the sketch's two `#[cfg]` arms), Task 3 Step 3 (the marker `405` goes through the error route — the landed note sits beside the sketch), and Task 5 Step 4 (the metric families are `tinio-fs`'s, per plan 2's wave F4 — corrected in place below).

## Global Constraints

- English only in repo artifacts; no hard-wrapped lines in prose (`docs/compress.md`). Public items carry runnable doc examples; gate `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- The ops table is the single place an operation is declared: `crates/tinio-server/src/backend/ops.rs`'s `tinio_s3_ops!` feeds **five** consumers (`crates/tinio-server/src/metrics.rs:1260`, `crates/tinio-server/src/capability.rs:72`, `crates/tinio-server/src/readonly.rs:55` and `:223`, `crates/tinio-server/src/backend/ops.rs:216` — the decorators sit beside `metrics.rs` at the crate root, not under `backend/`), and `ops.rs:288`'s `table_matches_the_s3_dispatch` test cross-checks the table against the `S3` impl in `s3.rs` — a row without a dispatch method fails that test.
- Refusals keep their families' exact text: a disabled capability answers `NotImplemented "<Operation> is disabled"` (`capability.rs:53`), the read-only plane answers `AccessDenied "Access Denied"` (`crates/tinio-server/src/readonly.rs:48`), and an operation this workspace has not implemented answers s3s's trait default (`NotImplemented "<Operation> is not implemented yet"`, `s3s` `src/s3_trait.rs:3372`).
- Wire shape is AWS's, verified 2026-10-03 (the design's **Evidence**): `x-amz-version-id` on the responses AWS carries it on; `x-amz-delete-marker` on deletes and on marker hits; the delete-marker `405` carries `Last-Modified` and **no `Allow`**; an unknown version is `404 NoSuchVersion`; a malformed one is `400 InvalidArgument`.
- cucumber: `Given`/`When` act, `Then` asserts; one behavior per scenario; spec tags `@FR-xxx`; the allow-list `NOT_COVERED_BY_CUCUMBER` (`crates/tinio-e2e/tests/traceability.rs:73`) is the only place a spec id may be exempted (`docs/tests.md`).
- `docs/tests.md`'s fail-not-skip rule: a suite that cannot certify fails; `TINIO_SANDBOX_ALLOW_SKIP` is the only opt-out and applies to confinement legs only.
- Git: every commit step asks the operator first (`docs/git.md`); commit messages carry no AI/agent trailers.
- Gate commands: `cargo test -p tinio-config -p tinio-server`, `cargo test -p tinio-e2e`, `cargo hack check --package tinio --each-feature`, `cargo hack check --package tinio-server --each-feature`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo fmt`, `cargo clippy`.

## Review Focus

1. **A version-addressed request on a server that has no versioning.** With the cargo feature stripped, or the `[s3] versioning` toggle off, a `GET ?versionId=…` must answer `NotImplemented` — never the current object's bytes. This is the fail-open hazard the whole gating design exists for; the handler-level helper is its only guard, because `get_object` itself is not gated.
2. **An unknown version id versus a malformed one.** `?versionId=zzz` (well-formed, unknown) is `404 NoSuchVersion`; a value that cannot be an id at all is `400 InvalidArgument`. Collapsing them breaks clients that distinguish "deleted long ago" from "you built the URL wrong".
3. **`GET`/`HEAD` of a delete marker by version id.** `405` with `Last-Modified` and `x-amz-delete-marker: true`, no body, no `Allow` — and the same request on a *non-marker* version must not be a 405.
4. **A plain `GET` after repeated plain deletes.** Each `DELETE` without a version id inserts another marker (AWS documents the stacking); a plain `GET` answers `404` with `x-amz-delete-marker: true` every time, and no version's content is lost.
5. **Read-only mode with a root whose database cannot be opened.** History serves when the root's `meta.redb` opens read-only; when it does not (absent, corrupt, a format this build refuses, a read-write server holding the root), the run degrades to "no history" with a warning and **still refuses every write** — a degrade that accidentally served a write would break FR-023's core promise.

---

### Task 1: The config keys

**Files:**
- Modify: `crates/tinio-config/src/schema/s3.rs` (`Capabilities` + `Config` + the default fns + tests)
- Modify: `specs/001-s3-local-server/contracts/config.md` (the three keys)

**Interfaces:**
- Consumes: nothing from plans 1–2.
- Produces: `Capabilities::versioning: bool` (default `true`, `#[serde(default = "versioning")]`, `#[default = true]`, default fn `fn versioning() -> bool { Capabilities::default().versioning }`); `Config::noncurrent_keep: u64` (default 0) and `Config::noncurrent_expire_days: u64` (default 0), each with its default fn in the established shape (`crates/tinio-config/src/schema/s3.rs:171-221`); and `Config::block_grace_minutes: u64` (default 10, `#[garde(range(min = 1))]` — the floor makes a zero a startup error rather than a silent disarm of the block sweep's race guard, decided with the operator on 2026-10-03).

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-config/src/schema/s3.rs`'s tests, in the file's existing pattern:

```rust
    #[test]
    fn versioning_defaults_on_and_the_retention_knobs_default_to_keep_everything() {
        let config = RootConfig::parse("version = 1\n[s3]").unwrap();
        let s3 = config.s3.clone().unwrap();
        assert!(s3.capabilities.versioning, "a core-surface capability defaults on, like tagging");
        assert_eq!(s3.noncurrent_keep, 0, "0 = keep every noncurrent version");
        assert_eq!(s3.noncurrent_expire_days, 0, "0 = never expire by age");
        assert_eq!(s3.block_grace_minutes, 10, "the block sweep's grace defaults to ten minutes");

        let off = RootConfig::parse("version = 1\n[s3]\nversioning = false\nnoncurrent_keep = 3\nnoncurrent_expire_days = 30\nblock_grace_minutes = 45").unwrap();
        let s3 = off.s3.unwrap();
        assert!(!s3.capabilities.versioning);
        assert_eq!(s3.noncurrent_keep, 3);
        assert_eq!(s3.noncurrent_expire_days, 30);
        assert_eq!(s3.block_grace_minutes, 45);
    }

    #[test]
    fn a_zero_block_grace_is_refused() {
        let err = RootConfig::parse("version = 1\n[s3]\nblock_grace_minutes = 0").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "got {err:?}");
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-config --lib schema::s3`
Expected: FAIL — `no field versioning`.

- [x] **Step 3: Add the keys**

`Capabilities` (beside `tagging`, `cors`, `acl`) and `Config` (beside `temp_ttl_hours`, `multipart_expire_days`); the default fns beside the existing ones. The flattening is unchanged, so the keys stay under `[s3]` (`s3.rs:144-169`).

- [x] **Step 4: Update the config contract**

`specs/001-s3-local-server/contracts/config.md`: the `[s3]` example block gains the four keys with their defaults and one line each — `versioning` (capability toggle, default on), `noncurrent_keep` (retain the newest N noncurrent versions; `0` = unlimited), `noncurrent_expire_days` (expire a noncurrent version D days after it stopped being current; `0` = never), `block_grace_minutes` (the minimum age before an unreferenced block may be reclaimed; default 10, minimum 1). These four lines are the contract's; plan 2's sweep reads them as `sweep::Options` fields.

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-config`
Expected: PASS.

- [ ] **Step 6: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-config specs/001-s3-local-server/contracts/config.md
git commit -m "feat(config): add the versioning toggle and the retention keys"
```

---

### Task 2: The ops table rows, the dispatch, and the fail-open defense

**Files:**
- Modify: `crates/tinio-server/src/backend/ops.rs` (three rows)
- Modify: `crates/tinio-server/src/backend/s3.rs` (three dispatch methods, in a new banner section)
- Create: `crates/tinio-server/src/backend/versioning.rs` (the handlers + `require_versioning`)
- Modify: `crates/tinio-server/src/backend/errors.rs` (map plan 2's two new storage errors)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` (the toggle-off startup warning: one bucket-versioning-table scan)

**Interfaces:**
- Consumes: `Capabilities::versioning` (Task 1), `VersionOps` (plan 2), the decorator stack (`crates/tinio-server/src/data.rs:270-285`).
- Produces: table rows `#[gated(versioning: "versioning")] fn get_bucket_versioning (GetBucketVersioningInput) -> GetBucketVersioningOutput;`, the same for `put_bucket_versioning` and `list_object_versions`. The groups follow the surface, not convenience: `get_bucket_versioning` and `list_object_versions` are `read` rows and `put_bucket_versioning` is a `write` row, so the read-only plane (`crates/tinio-server/src/readonly.rs`, the crate-root decorator beside `metrics.rs` — `AccessDenied` at `:48`) forwards the reads and refuses the write through the existing macro, exactly as it does for every other operation; `pub(crate) fn require_versioning<S: Storage>(backend: &S3Backend<S>, version: Option<&str>) -> S3Result<()>`; the error mapping arms `StorageError::NoSuchVersion(_) => s3_error!(NoSuchVersion)` and `StorageError::InvalidVersion(_) => s3_error!(InvalidArgument, …)`.

- [x] **Step 1: Write the failing test**

The table's own cross-check test already fails once a row exists without a dispatch method (`ops.rs:288`), so the first step is the row; the fail-open helper gets its own test in `versioning.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    use crate::backend::{testutil::setup_with_caps, Capabilities};

    /// The helper is the guard the un-gated handlers call: it must refuse a
    /// version-addressed request whenever versioning is not available.
    #[tokio::test]
    async fn a_version_addressed_request_is_refused_when_the_toggle_is_off() {
        let (backend, _bucket) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        let err = require_versioning(&backend, Some("v1")).unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert!(require_versioning(&backend, None).is_ok(), "a plain request is untouched");
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server --lib versioning`
Expected: FAIL — `cannot find function require_versioning`.

- [x] **Step 3: Write the helper and the rows**

`crates/tinio-server/src/backend/versioning.rs`:

```rust
//! The versioning operations and the one guard every version-addressed
//! request passes (the design's fail-open rule: a request naming a version is
//! refused outright when versioning is unavailable, never answered from the
//! current object).

use s3s::{S3Result, s3_error};
use tinio_core::storage::Storage;

use super::S3Backend;

/// Refuse a version-addressed request when this server does not offer
/// versioning. The two arms differ only in *why* it is unavailable: with the
/// feature compiled in, the `[s3] versioning` toggle decides;
/// without it, every such request is refused.
#[cfg(feature = "versioning")]
pub(crate) fn require_versioning<S: Storage>(backend: &S3Backend<S>, version: Option<&str>) -> S3Result<()> {
    if version.is_some() && !backend.capabilities().versioning {
        return Err(s3_error!(NotImplemented, "versioning is disabled"));
    }
    Ok(())
}

#[cfg(not(feature = "versioning"))]
pub(crate) fn require_versioning<S: Storage>(_backend: &S3Backend<S>, version: Option<&str>) -> S3Result<()> {
    if version.is_some() {
        return Err(s3_error!(NotImplemented, "versioning is not compiled in"));
    }
    Ok(())
}
```

`S3Backend` needs a `capabilities()` accessor if it has none (`S3Backend::new_shared(Arc::clone(&storage), plane.caps)` already receives them — `crates/tinio-server/src/data.rs:171`).

The three rows in `ops.rs` (read group after `get_object_attributes`; write group after `delete_objects`):

```text
      #[gated(versioning: "versioning")]
      fn get_bucket_versioning (GetBucketVersioningInput) -> GetBucketVersioningOutput;
      #[gated(versioning: "versioning")]
      fn list_object_versions (ListObjectVersionsInput) -> ListObjectVersionsOutput;
```
```text
      #[gated(versioning: "versioning")]
      fn put_bucket_versioning (PutBucketVersioningInput) -> PutBucketVersioningOutput;
```

and the three `s3.rs` dispatch methods under a `// --- versioning (spec 2026-10-03) ---` banner.

The cargo feature `versioning` is declared by `tinio-server` (`[features] versioning = []`) and by the facade `tinio`'s `full` set, so `--no-default-features` ships without it. The rest of the boundary is already in place, decided with the operator on 2026-10-03: **plan 1 declared it for `tinio-blob` and that was removed after plan 1 was executed** (operator ruling 2026-10-03 — the feature had exactly one referent, the crate root, and its off-state compiled an empty crate, so the compile-time level now excludes plan 1's crate; plan 2's `tinio-version` gate, drafted on that analogy, needs re-deciding rather than inheriting) and plan 2 declares it for `tinio-fs` and `tinio-mem`, where it gates their version machinery (no hand-written refusal twin: with the feature off the trait's own default bodies answer the refusal — amended 2026-10-04, wave 3, which deleted the twins and their `unsupported()` helpers); **`tinio-core` is deliberately ungated** — its `version` types and `VersionOps` are inert, and the ungated `object::Info` carries a `version_id` — and the config keys stay ungated in `tinio-config` (a config that names them must parse in every build, since an unknown key is a startup error). This task adds `tinio-server`'s gate and the facade's wiring.

The **toggle-off startup warning** is this task's second obligation from the design's **Feature gating and configuration**: with the feature compiled in but `[s3] versioning` off, a root that holds version data still starts — current versions keep serving, every version operation answers `NotImplemented` — and startup logs exactly one warning naming how many buckets hold version data. The count is one range scan of the bucket-versioning table (`tinio_store::bucket_versioning`, plan 2's reader — never a per-bucket lookup, so a large root stays cheap), the log site is the run's startup (`crates/tinio-cli/src/commands/start/serve.rs`, where the parsed config and the storage are both in hand), and the test pins the warning's count on a root with two versioned buckets plus its absence when the toggle is on or no bucket holds version data.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server --lib && cargo hack check --package tinio-server --each-feature`
Expected: PASS — including `table_matches_the_s3_dispatch` and the write-group pin `WRITE_OPS` (`ops.rs:317`), which gains `put_bucket_versioning`.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-server Cargo.toml
git commit -m "feat(server): declare the versioning operations and the fail-open guard"
```

---

### Task 3: `versionId` through the object operations, and the response headers

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs` (`op_get_object`, `op_head_object`, `op_delete_object`, `op_put_object`, `op_copy_object`)
- Modify: `crates/tinio-server/src/backend/mod.rs` (the header helper)
- Modify: `crates/tinio-server/src/backend/multipart.rs` (`op_complete_multipart_upload`)

**Interfaces:**
- Consumes: `VersionOps::{get_object_version, head_object_version, delete_object_version}`, `version::{Id, Marker, ReadResult, HeadResult}`, `require_versioning` (Task 2).
- Produces: `pub(crate) fn set_version_headers(out: &mut http::HeaderMap, id: Option<&version::Id>, is_delete_marker: bool)` — sets `x-amz-version-id` (`"null"` when `id` is `None`) and, when `is_delete_marker`, `x-amz-delete-marker: true`. It takes the identity, not a `Marker`, because the write path has none: `op_put_object` and its siblings answer from the `Info` the storage returned (`info.version_id: Option<version::Id>`), and only the delete path holds a `Marker` (`marker.id`, `marker.is_delete_marker`).

- [x] **Step 1: Write the failing test**

`crates/tinio-server/src/backend/versioning.rs`'s tests gain the wire journeys (the handler-level tests the repo already writes for ops — `crate::backend::testutil`'s request helpers):

```rust
    #[tokio::test]
    async fn a_get_addressed_at_a_delete_marker_answers_405_with_the_marker_headers() {
        /* enable versioning; put; delete (no version id); capture the marker id
           from the delete response's x-amz-version-id;
           GET without a version id -> 404 + x-amz-delete-marker: true;
           GET with the marker id -> 405 + Last-Modified + x-amz-delete-marker: true,
             and NO Allow header (Review Focus 3) */
    }

    #[tokio::test]
    async fn an_unknown_version_is_no_such_version_and_a_malformed_one_is_invalid_argument() {
        /* GET ?versionId=zzz -> 404 NoSuchVersion;
           GET ?versionId=<1025 bytes> -> 400 InvalidArgument (Review Focus 2) */
    }

    #[tokio::test]
    async fn every_version_creating_write_reports_its_version_id() {
        /* PUT, COPY and CompleteMultipartUpload each answer x-amz-version-id
           on an enabled bucket and x-amz-version-id: null on a suspended one */
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server --lib versioning`
Expected: FAIL.

- [x] **Step 3: Thread the reads and the delete**

**The parse.** Every version value on the wire is turned into an `Option<version::Id>` by one step before it reaches a call, and the step has an owner: `version::Id::from_wire(value)` owns the literal `"null"` (`"null"` → `None`), and any other value is validated by the checked constructor `version::Id::new(value)` (the 1024-byte cap, `version::MAX_ID_LEN`) — a malformed value answers `400 InvalidArgument` through plan 2's `StorageError::InvalidVersion` arm, and a well-formed id that names no version answers `404 NoSuchVersion` through `StorageError::NoSuchVersion`. `from_wire` alone also answers `None` for a malformed string (it validates and drops the error), which is exactly the collapse Review Focus 2 forbids, so it is never the only parse. The sites are `GET`/`HEAD`/`DELETE`'s `versionId`, `CopySource`'s source version, and Task 6's tagging/ACL `versionId`.

- `op_get_object` / `op_head_object`: first `require_versioning(self, req.input.version_id.as_deref())?`; when the input carries an id, call `storage.get_object_version(…, version, range)`; a `ReadResult::Marker { id, .. }` answers `s3_error!(MethodNotAllowed)` **with** the response's headers set — s3s's `S3Error` carries no headers, so the marker case builds the response directly: `S3Response::new(GetObjectOutput { last_modified: …, ..Default::default() })` with the 405 status set on `resp` and the two headers inserted through `set_version_headers(resp.headers_mut(), id.as_ref(), true)` (the pattern `op_head_object` already uses for `x-amz-tagging-count`, `crates/tinio-server/src/backend/objects.rs:566-578`). The status override is `resp.status = Some(http_modified_405)` — s3s's `S3Response` carries `status: Option<StatusCode>` precisely to override the output's implied code (`src/protocol.rs:183-185`) — and s3s renders it when set. **Landed 2026-10-06 — superseded.** s3s 0.17 never reads `S3Response::status`; the shipped handler answers the marker `405` through the error route with headers (`crates/tinio-server/src/backend/versioning.rs`'s `marker_error`), and the contract records it as ruling R11. **The plain, non-version-addressed branch carries the header too**, through the same helper (`info.version_id.as_ref(), false`): decided with the operator on 2026-10-03 — AWS returns `x-amz-version-id` on every `GET`/`HEAD` of a versioned bucket, and a client records the current version's id from exactly that header, which is what makes a later `?versionId=` request possible without a listing first. An unversioned bucket answers without it.
- `op_delete_object`: `require_versioning(...)`, then `delete_object_version` when an id is present and plain `delete_object` otherwise; the resulting `Marker` goes through `set_version_headers(resp.headers_mut(), marker.id.as_ref(), marker.is_delete_marker)` into the response, and the `DELETE` of a marker answers with `x-amz-delete-marker: true`.
- `op_put_object` / `op_copy_object` / `op_complete_multipart_upload`: no input version, but their responses carry `x-amz-version-id` — one `set_version_headers(resp.headers_mut(), info.version_id.as_ref(), false)` call each, from the `Info`/`PutObjectResult` the storage returned. `CopyObject`'s source `versionId` (inside `CopySource`, `s3s` `src/dto/copy_source.rs:23-63`) threads into a version-addressed read of the source; `UploadPartCopy` the same.
- **Conditional writes.** The existing conditionals handling (`crates/tinio-server/src/backend/objects.rs`'s write path and `crates/tinio-server/src/backend/conditions.rs`) gains the two versioning arms the design's **version model** pins: `If-None-Match: *` applies to the **current version only** and succeeds when there is no current version or the current version is a delete marker; `If-Match` fails with `404 Not Found` (not `412`) in those same two cases. A test beside the existing conditionals tests drives both arms — on an enabled bucket with a live current version, on a key whose current version is a marker, and on a key with no current version.
- **`DeleteObjects` (batch) with version ids.** `op_delete_objects` threads each `ObjectIdentifier.version_id` through the same delete path (a version-addressed delete per entry, the plain delete when the id is absent) and fills each `DeletedObject { version_id, delete_marker, delete_marker_version_id }` from the `Marker` that path returned — the same triple the single-object `DELETE` renders as `x-amz-version-id`/`x-amz-delete-marker` headers. This is the design's Goal 4 (bounded disk by explicit version deletion) reached through the batch surface; a test deletes two version ids and one plain key in one request and asserts the three entries.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS — the new journeys plus every existing handler test.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-server
git commit -m "feat(server): address object versions on the wire"
```

---

### Task 4: `ListObjectVersions`

**Files:**
- Modify: `crates/tinio-server/src/backend/versioning.rs` (the handler)
- Modify: `crates/tinio-server/src/backend/s3.rs` (the dispatch, already declared in Task 2)

**Interfaces:**
- Consumes: `VersionOps::list_object_versions`, `version::{Entry, ListParams, Listing, IdMarker}`, the existing listing helpers (`crates/tinio-server/src/backend/buckets.rs`'s `op_list_objects_v2` is the pagination template).
- Produces: `op_list_object_versions` filling `ListObjectVersionsOutput { versions: Some(ObjectVersionList), delete_markers: Some(DeleteMarkers), is_truncated, key_marker, version_id_marker, next_key_marker, next_version_id_marker, name, prefix, delimiter, max_keys, common_prefixes, .. }`.

- [x] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn list_object_versions_separates_versions_from_markers_newest_first() {
        /* enable; two puts of the same key; one delete;
           GET /bucket?versions -> versions: [newest object version, older object version]
             newest first, each carrying x-amz-version-id and is-latest on the first;
             delete_markers: [the marker]; a page size of 2 truncates with both cursors set
           (Review Focus 4's listing half) */
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server --lib versioning`
Expected: FAIL.

- [x] **Step 3: Write the handler**

Map `ListParams` from the input (`prefix`, `delimiter`, `key_marker`, `version_id_marker` — the wire's raw string wrapped in `version::IdMarker`, plan 2's newtype; the cursor is never parsed into a `version::Id`, because the wire may send any id and the backend compares it to the listing's own cursors — and `max_keys`, the capability's cap applying as it does for the object listings (`crates/tinio-config/src/schema/s3.rs`'s `max_keys`) plus this listing's own hard ceiling of 1,000 — AWS's documented maximum for the operation, landed by the 2026-10-06 review as the walk's memory bound), call `list_object_versions`, and split the entries: markers into `delete_markers` as `DeleteMarkerEntry`, objects into `versions` as `ObjectVersion`; `is_latest` is `true` for the first entry of each key (the backend returns each key's versions newest first, so the handler derives it from position and `Entry` carries no flag). Set `name` to the bucket, echo `prefix`/`delimiter`/`max_keys`, and set the four cursors when `is_truncated`.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-server
git commit -m "feat(server): list object versions"
```

---

### Task 5: `GetBucketVersioning` / `PutBucketVersioning`, and the storage metrics

**Files:**
- Modify: `crates/tinio-server/src/backend/versioning.rs`
- Modify: `crates/tinio-server/src/metrics.rs` (the version families)

**Interfaces:**
- Consumes: `VersionOps::{bucket_versioning, set_bucket_versioning}`, `version::State`.
- Produces: `op_get_bucket_versioning` (fills `status` only — `mfa_delete` stays `None`, per the design: `GetBucketVersioning` never reports MFA delete) and `op_put_bucket_versioning` (reads `VersioningConfiguration.status`, refuses `mfa_delete: Some(MFADelete::ENABLED)` with `NotImplemented`, and otherwise calls `set_bucket_versioning`). Two rules decided with the operator on 2026-10-03: **authorization goes through the existing ACL layer** — the same `AclAccess` that decides every other bucket write (`crates/tinio-server/src/data.rs:246-268`) — rather than an operation-level owner check, because `PutBucketAcl` (the plan's earlier model) has no requester-owner check at all and a single op-level exception would be the only one in the workspace; the corpus says so in one clause, translating AWS's owner-only rule into this workspace's model. And **a request with neither `Status` nor `MfaDelete` answers `200` with no state change** — an empty request changes nothing, so it is a no-op rather than a `400`.

- [x] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn the_versioning_state_round_trips_and_mfa_delete_is_refused() {
        /* GET on a fresh bucket -> no Status element (unversioned);
           PUT <VersioningConfiguration><Status>Enabled</Status> -> 200; GET -> Enabled;
           PUT Suspended -> 200; GET -> Suspended;
           PUT with <MfaDelete>Enabled</MfaDelete> -> NotImplemented, state unchanged */
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server --lib versioning`
Expected: FAIL.

- [x] **Step 3: Write the handlers**

`VersioningConfiguration` is a DTO (`s3s` `src/dto/generated.rs:33498`): `status: Option<BucketVersioningStatus>`, `mfa_delete: Option<MFADelete>`. `BucketVersioningStatus` is a `Cow<'static, str>` newtype with `ENABLED`/`SUSPENDED` consts (`:1882`) — compare against those, never against a re-typed literal. The unversioned state is the **absence** of `Status` in the response, which is what makes the round-trip test's first assertion meaningful.

- [x] **Step 4: Add the storage metric families**

**Landed 2026-10-06 — superseded as drafted.** The families are declared by the crate that observes the sweep, `crates/tinio-fs/src/metrics.rs`: `tinio_storage_blocks_reclaimed_total`, `tinio_storage_bytes_reclaimed_total` and `tinio_storage_noncurrent_versions_total` (plan 2's code-review fix wave F4, 2026-10-04), owned by `specs/001-s3-local-server/data-model.md`'s `## Metrics` list; `crates/tinio-server/src/metrics.rs` gained only the ops-label pin over the new rows. The retention pass reports through its own type, plan 2's `tinio_fs::sweep::Summary { temp_files, uploads, expired_versions, reclaimed_blocks, reclaimed_bytes }` — the two reports stay distinct, because plan 1's `SweepReport` counts blocks and plan 2's `Summary` counts the retention run's work. No dedup-ratio family exists: the design dropped it on 2026-10-03 (the ratio would need write-versus-stored byte counters for a low-information number in a whole-blob design), so the versioning metrics keep exactly what the observability ruling asks for — the versions count, blocks reclaimed, bytes reclaimed — and these three families are the only new facts about it a reader of `/metrics` cannot otherwise see.

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-server && cargo test -p tinio-config`
Expected: PASS.

- [ ] **Step 6: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-server
git commit -m "feat(server): read and write the bucket versioning state"
```

---

### Task 6: Per-version tagging and ACL

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs` (the tagging and ACL handlers — `backend/tags.rs` holds only the Tags↔core conversion and was not touched)

**Interfaces:**
- Consumes: `VersionOps::{version_tags, set_version_tags, version_acl, set_version_acl}` (plan 2).
- Produces: the six operations (`get`/`put`/`delete_object_tagging`, `get`/`put_object_acl`; `delete_object_tagging` clears the version's tags) honouring `req.input.version_id`.

- [x] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn tags_and_acls_are_version_scoped() {
        /* enable; put with tags A; overwrite with tags B;
           GET ?tagging&versionId=<v1> -> A; GET ?tagging (current) -> B;
           PUT ?tagging&versionId=<v1> replaces that version's tags only */
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server --lib versioning`
Expected: FAIL.

- [x] **Step 3: Thread the version**

Each handler: `require_versioning(...)`, then route to the version-scoped accessor when the input carries an id and to the existing current-version path otherwise. The capability gates are unchanged (`tagging`, `acl`), so an operator may run versioning with tagging off — the version rows carry the wires either way (plan 2 stores them), and the handlers simply never read them.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-server
git commit -m "feat(server): scope tags and ACLs to a version"
```

---

### Task 7: The read-only-mode amendment (FR-023)

**Files:**
- Modify: `crates/tinio-fs/src/database/open.rs` (a read-only open)
- Modify: `crates/tinio-fs/src/database/handle.rs` (a read-only handle)
- Modify: `crates/tinio-store/src/table.rs` (`open_readonly` on a missing table reads as empty)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`FsStorage::resolve`: the read-only rules at `:434-438`, `:460-470`)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` (`scanner_and_sweeper` at `:434-450`)
- Modify: `crates/tinio-e2e/tests/features/read_only.feature:247-273` (the scenario is inverted)
- Modify: `specs/001-s3-local-server/data-model.md` and `fs-backend.md` (the amended sentences)

**Interfaces:**
- Consumes: `redb::ReadOnlyDatabase` (4.2; `db.rs:427`, `open` at `:458`), `AccessMode` (`crates/tinio-core/src/mode.rs:21-45`), the scanner's `ScannerOptions.access` (`crates/tinio-fs/src/scanner.rs:82-85`).
- Produces: `pub fn open_read_only(state_dir: &Path) -> Result<ReadOnlyDatabase, Error>` (no create, no `ensure_version`, no `ensure_all`, no compaction — reads only); `Handle`'s database becomes an enum `Db::Writable(redb::Database) | Db::ReadOnly(redb::ReadOnlyDatabase)` with `Handle::open_read_only(state_dir)`; a write attempted on a read-only handle is an error (a programming error — the read-only policy refuses writes upstream, and this design disables the scanner in that mode); in read-only mode the **metadata source is the root's own `.tinio/`**, opened read-only, and the relocated home dir holds tool state only (socket, state file, logs).

**Why the root's own `.tinio/`**: the root's database is where the version rows and the object rows live; reading it read-only is what makes history (and tags/ACLs/CORS) visible to a read-only run. The relocation keeps its purpose — the tool state a run *writes* (socket, `state`, logs) must not land in a root that may be genuinely read-only — and the one rule that changes is which database the *storage* reads. Redb documents the constraint that shapes the failure path: a `ReadOnlyDatabase` may not be opened concurrently with a `Database` (`redb` `db.rs:390-391`), so a read-write server holding the same root makes the open fail, and that failure is the degrade path — serve objects from their files with an empty metadata view and a warning naming the reason, which is exactly today's behavior.

- [x] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn a_read_only_store_serves_the_metadata_a_read_write_run_wrote() {
    /* build a root with a read-write FsStorage: create a bucket, put an object
       with a tag; drop it; open the same root read-only; assert the tag reads
       back and the object serves (the amendment's core claim) */
}

#[tokio::test]
async fn a_read_only_store_over_a_root_with_no_database_degrades_to_an_empty_view() {
    /* a root with objects but no .tinio/: open read-only; objects serve, the
       metadata view is empty, and the open error is what the run reports */
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib read_only`
Expected: FAIL — there is no read-only open anywhere in the workspace.

- [x] **Step 3: Implement the read-only open and the handle**

`crates/tinio-fs/src/database/open.rs`: `open_read_only(state_dir)` opens `<state_dir>/meta.redb` with `ReadOnlyDatabase::open` and maps every failure (absent file, corrupt, format the build refuses — checked by reading `state::Table::version`, a missing table included) into the crate error, without creating or writing anything.

`crates/tinio-fs/src/database/handle.rs`: the `db` field becomes the shared `tinio_store::store::Handle`, which owns the `Db` arm enum (writable / read-only / empty) and the read-only refusal — the fs handle keeps only the blocking-pool hop and the write-lock histograms (round B2, one database-handle shape); `read`/`read_blocking` work on both arms; `write`/`write_if` on the read-only arm return an error (`Error::ReadOnly`), which the fs's own tests assert is unreachable in a read-only run's flows.

`crates/tinio-store/src/table.rs`: `open_readonly` currently propagates redb's "table does not exist" error; a read-only handle must treat a missing table as empty, because a root written by a build without versioning has no version tables — that is the difference between "no history" and "this root cannot be read".

- [x] **Step 4: Point the read-only backend at the root's database**

`FsStorage::resolve` (`crates/tinio-fs/src/backend/mod.rs:432-476`): in read-only mode the metadata/state dir is `root.join(RESERVED_SEGMENT)` — opened read-only — and the refusal at `:460-470` (an explicit state dir inside the root) becomes the opposite rule: a read-only run's *storage* dir must be the root's own. Decided with the operator on 2026-10-03, so nothing is left to the executor: `FsStorage` keeps **one** state dir per run — the root's own `.tinio/` in read-only mode, the configured one otherwise — and it never holds the CLI's tool-state directory, because a read-only run writes nothing through the storage (every mutating operation is refused upstream, the scanner is off, and blocks are read from `<root>/.tinio/blobs`) and the tool state (socket, `state`, logs) lives where `resolve_state_dir` put it.

- [x] **Step 5: Disable the scanner in read-only mode**

`crates/tinio-cli/src/commands/start/serve.rs:434-450`: `Scanner::new(storage.clone(), ScannerOptions { enabled: !access.is_read_only(), …, access })`. The scanner's purpose is to pre-warm ETags by *writing* rows; with the rows readable, listings recompute on demand and write nothing (`crates/tinio-fs/src/listing.rs`'s existing claim path), so a read-only run writes no database at all.

- [x] **Step 6: Invert the scenario and update the corpus**

`crates/tinio-e2e/tests/features/read_only.feature:247-273` becomes "The in-root meta database is read, read-only": the read-write run plants a tag, the read-only run **serves** it, and the root's snapshot after the run is unchanged (the existing `state_lands_under_the_injected_home` leg, `crates/tinio-server/tests/read_only.rs:189-223`, stays and keeps asserting the root is untouched). A second leg covers the degrade: a root with no `.tinio/` serves objects with an empty metadata view.

`data-model.md` (Read-only mode's paragraph and the state-dir row) and `fs-backend.md` §9 (the scanner's read-only note) lose their "not read" sentences and state the amended rule; the design's **Read paths** owns the reasoning and the corpus states the rule only.

- [x] **Step 7: Run the gates**

Run: `cargo test -p tinio-fs -p tinio-cli -p tinio-server && cargo test -p tinio-e2e` (plus the `@cli` read-only scenarios).
Expected: PASS — every read-only scenario against the amended rule.

- [ ] **Step 8: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-fs crates/tinio-cli crates/tinio-store crates/tinio-e2e specs/001-s3-local-server
git commit -m "feat(read-only): serve the root's metadata, read-only"
```

---

### Task 8: The journeys, the traceability rows and the corpus

**Files:**
- Create: `crates/tinio-e2e/tests/features/versioning.feature`
- Create: `crates/tinio-e2e/tests/steps/versioning.rs` (+ register it in `tests/steps/mod.rs`)
- Modify: `crates/tinio-e2e/tests/steps/mod.rs` (`config_from_tags`: a `versioning-off` tag → `caps.versioning = false`)
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (the versioning operations, headers, status codes, error rows)
- Modify: `specs/001-s3-local-server/data-model.md` (the version tables, the `object_meta` tuple, the state-dir `blobs/` subtree)
- Modify: `specs/001-s3-local-server/fs-backend.md` (the supersede/materialize protocols, the sweep/cleanup passes)
- Modify: `specs/001-s3-local-server/tasks.md` (rows T152–T153)

**Interfaces:**
- Consumes: everything above; the harness's `config_from_tags` (`crates/tinio-e2e/tests/steps/mod.rs:112`) and `World` (`:56`).
- Produces: the feature file with `@FR-037`-class tags (the corpus's FR-035 and FR-036 are the service verbs and the confinement channels, so this feature's requirements are FR-037 onward), and the corpus text that stops being plan-owned the moment it lands.

- [x] **Step 1: Write the feature file**

> **Landed 2026-10-06 — this sketch is superseded wherever it differs**: the authoritative record is the landed `crates/tinio-e2e/tests/features/versioning.feature` together with the corpus it drives (`contracts/s3-surface.md` §Object versioning, `data-model.md`, `fs-backend.md`, `tasks.md`), and the sketch below is kept as the plan's own record rather than rewritten — it still shows presence-form header steps where the landed file asserts values, an object-addressed `?versions` where the contract's one listing shape is bucket-addressed (`GET /<bucket>?versions&prefix=…`), and it lacks the landed version-addressed delete journey.

```gherkin
Feature: Object versioning

  @FR-037
  Scenario: An overwrite keeps the previous version retrievable
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    And I upload "data/notes.txt" with body "second"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 200
    And the response body is "second"
    When I send a "GET" request to "/data/notes.txt?versions"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers
    And the first version in the listing is the latest

  @FR-038
  Scenario: A delete marker hides the object but keeps every version
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    When I send a "DELETE" request to "/data/notes.txt"
    Then the response status is 204
    And the response has a "x-amz-delete-marker" header
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 404
    And the response has a "x-amz-delete-marker" header

  @FR-039
  Scenario: A version-addressed request is refused when versioning is off
    Given I create bucket "data"
    And I upload "data/notes.txt" with body "first"
    When I send a "GET" request to "/data/notes.txt?versionId=v1"
    Then the response status is 501
    And the error code is "NotImplemented"

  @FR-040
  Scenario: A suspended bucket writes the null version in place
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    And I suspend versioning on "data"
    And I upload "data/notes.txt" with body "second"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 200
    And the response body is "second"
    And the response has a "x-amz-version-id" header
    When I send a "GET" request to "/data/notes.txt?versions"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers

  @FR-041
  Scenario: The null version is addressable by its literal
    Given I create bucket "data"
    And I upload "data/notes.txt" with body "before"
    When I send a "GET" request to "/data/notes.txt?versionId=null"
    Then the response status is 200
    And the response body is "before"
    And the response has a "x-amz-version-id" header

  @cli @FR-023
  Scenario: A read-only run serves the history it can read
    Given a storage root
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    And I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    And I upload "data/notes.txt" with body "second"
    When I stop the server through its control channel
    When I run "tinio server {root} --port 0 --read-only"
    And the client is bound to the run's endpoint
    When I send a "GET" request to "/data?versions"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers
```

The third scenario runs under a `@versioning-off` tag whose `config_from_tags` arm sets `capabilities.versioning = false`; it is the Review Focus 1 leg on the real surface. The suspended scenario pins the write-in-place rule (the pre-suspension version stays retrievable, the suspended write answers a version id), the `?versionId=null` scenario pins null-version addressing by its literal, and the read-only leg rides the `@cli` read-only harness `read_only.feature` already uses (Task 7 re-points it), so history is proven served by a read-only run rather than only unit-tested.

The external-client journeys run at their existing tiers, not a fourth harness: boto3 at the `@boto3` tier (the checked-in scripts, `crates/tinio-e2e/tests/steps/clients.rs`; the journey gains the versioning legs) and aws cli v2 at the `@interop` tier. Both are opt-in legs, so the in-process scenarios above stay the always-on proof.

The feature matrix the design names (feature on/off × toggle on/off × bucket state × object kind) is stated here rather than enumerated as scenarios. **Covered cells**: feature on × toggle on × {unversioned, enabled, suspended} × {object, delete marker} — the scenarios above plus the handler tests of Tasks 3–6 (an unversioned bucket is today's behavior, which the existing suites already pin); and feature on × toggle off × a `versionId` request — the `@versioning-off` leg. **Derivable cells, no scenario**: feature off × every toggle and bucket-state cell (a stripped build has no rows and no handlers, so no feature-off server exists to run — `cargo hack check --package tinio --each-feature` plus the `#[cfg(not(feature = "versioning"))]` arm's own test prove the refusal at compile level); toggle off × bucket state (the refusal precedes any state read, so the cell collapses into the row above); unversioned × object kind (today's behavior, covered by the suites that keep passing unchanged). Folder markers sit outside the matrix by the design's non-goal.

- [x] **Step 2: Write the step definitions**

`tests/steps/versioning.rs`: `I enable versioning on {string}` and `I suspend versioning on {string}` (a `PUT /<bucket>?versioning` with the `VersioningConfiguration` body — `Enabled` and `Suspended`), and two listing steps the versioning scenarios need — `the version listing has {int} versions and {int} delete markers` (counts the `<Version>` and `<DeleteMarker>` elements in the body; the listing carries metadata, never an object's bytes, which is why the scenarios assert on these counts and never on content) and `the first version in the listing is the latest` (the first `<IsLatest>` element in document order is `true`, which is how "newest first" is pinned without parsing ids). Reuse the existing steps for the rest (`I upload`, `I send a "GET" request to`, `the response body contains`, `the response has a "…" header`, plus the `@cli` read-only harness's run/bind steps); register the module in `steps/mod.rs`, and add the `versioning-off` arm to `config_from_tags`.

- [x] **Step 3: Run the journeys**

Run: `cargo test -p tinio-e2e` and, for the mem pass, `TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e`.
Expected: PASS on both backends.

- [x] **Step 4: Update the corpus**

- `contracts/s3-surface.md`: the three operations' rows, the `x-amz-version-id`/`x-amz-delete-marker` headers on every response AWS carries them on (**including the plain, non-version-addressed `GET`/`HEAD` of a versioned bucket**), the `404`/`405` delete-marker rules (with the no-`Allow` ruling and its evidence), `NoSuchVersion`/`InvalidArgument` rows, the suspended-write and marker-stacking rules, **the `PutBucketVersioning` authorization rule** (the ACL layer decides, per the 2026-10-03 ruling that translates AWS's owner-only wording into this workspace's model) and **the empty-configuration no-op** (`200` when neither `Status` nor `MfaDelete` is given), and the FR rows the feature file's `@FR-0xx` tags resolve to (the traceability gate in Step 5 reads them both ways).
- `data-model.md`: the version tables under their module names — `tinio_store::{version, version_index, bucket_versioning}` — and the 10-tuple `object_meta` value; `blobs/` in the state-dir table; the version columns' meaning.
- `fs-backend.md`: the supersede and materialize protocols (the ordering from the design's **Write paths**), the retention pass, the block pass, the startup repair's new stage, the doctor checks, the state dir's closed set (`blobs/` joins `tmp/` and `multipart/`; the closed-set test at `crates/tinio-fs/src/backend/tests.rs:791` moves with it), and §9's scanner note (the scanner is disabled in read-only mode — the rule Task 7's amendment lands).
- `tasks.md`: `T152` (the surface) and `T153` (the journeys and the corpus).

- [x] **Step 5: Run the traceability gate**

Run: `cargo test -p tinio-e2e --test traceability`
Expected: PASS — every new `@FR-` tag resolves both ways; nothing new lands in `NOT_COVERED_BY_CUCUMBER`.

- [x] **Step 6: Run the full gate**

Run each, expecting success:
- `cargo test -p tinio-config -p tinio-server -p tinio-e2e -p tinio-core -p tinio-fs -p tinio-mem`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- `cargo hack check --package tinio --each-feature`
- `cargo hack check --package tinio-server --each-feature`

- [ ] **Step 7: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-e2e specs/001-s3-local-server
git commit -m "feat(e2e): drive versioning through the surface and land the corpus"
```

---

## Plan-set ownership

- **This plan owns**: the three ops-table rows, every handler's version-address handling, the response-header helper, the config keys' names and defaults, the fail-open guard, the read-only amendment's wire-visible consequences, and the cucumber/corpus updates.
- **Plan 2 owns**: the `VersionOps` signatures, the version tables, the state machine, the retention arithmetic.
- **Plan 1 owns**: `tinio_core::blob` and `tinio_blob`.
- **The spec owns**: the storage model, the version state machine, and the three-level gating model.

## Notes for the executor

- The three ops-table rows are the whole gate: capability, metrics and read-only come from the table, so do not add hand-written checks for those three — the only hand-written guard is `require_versioning`, and it exists for the *un-gated* operations that merely carry a `versionId`.
- `s3s`'s `S3Error` cannot carry response headers; the delete-marker `405` therefore sets them on the `S3Response` it builds, the way `op_head_object` already sets `x-amz-tagging-count`. If a later refactor centralizes header setting, this is the case it must not break.
- The versioning cargo feature reaches the facade through `tinio`'s `full` set: `--no-default-features` must still compile and serve, and its `GET ?versionId=…` answers `NotImplemented` through the `#[cfg(not(feature = "versioning"))]` arm.
