# S3 Object Lock — thermos review

Phase record for the uncommitted Object Lock change on `dev`. The design owns intent (`docs/superpowers/specs/2026-10-20-s3-object-lock-design.md`); the plan owns tasks (`docs/superpowers/plans/2026-10-20-s3-object-lock.md`). This file owns the review's findings and the changes those findings ask for.

**Scope.** Staged tree against `HEAD`, plus the unstaged remainder the correctness pass found (not empty). `.sqlx/` query-cache JSON and `Cargo.lock` were not treated as feature diff. No pull request exists; no review discussion was read.

**Method.** Two read-only passes over the same change. One audited bugs, breakage, security, developer-experience regressions, and feature-gate leaks, and only in added or modified code. The other audited maintainability: file size, duplicated policy, spaghetti branches, and whether a restructuring would delete complexity. Line numbers below were re-checked against the tree when this record was written. File lengths are raw line counts.

**Verdict.** Correctness passes. Structure does not. The retention rules live in `tinio-core`'s `may_delete` / `apply_retention` / `Lock::carries_fact`, and every enforcement path the correctness pass traced agrees with the spec. The same "read the lock, refuse, roll back" sequence is still pasted into each store's delete path, and several files crossed 1000 lines because of that paste. One structure blocker is already met in this tree: `ObjectLockCache` is `crates/tinio-server/src/backend/object_lock_cache.rs`, and `backend/mod.rs` is 995 lines.

## Correctness

No high or medium finding.

Cleared, and consistent with the design (OL8, OL9, OL11, OL13, OL15, OL20, OL26), so not recorded as defects:

- **Mode and bypass.** `may_delete` checks legal hold first. COMPLIANCE ignores bypass. `apply_retention` refuses to shorten or downgrade COMPLIANCE. GOVERNANCE may shorten or clear only with `Bypass::Governance`. `bypass_of` ANDs the header with the ACL extension bit; no ACL or no identity yields `Bypass::None`. `DeleteObjects` recomputes permission per entry. The bucket-owner short-circuit is the OL15 peer rule. A FullControl grantee fails `can_delete` (owner / bucket owner): a tightening, not a hole.
- **Delete and overwrite.** A delete that names a version id goes through `expire_with` / `drop_current` inside one transaction: `FOR UPDATE` on pg, one write transaction on redb and mem. fs judges `may_delete` under the bucket mutation lock before it unlinks. A plain DELETE writes a delete marker and leaves the lock on the historical version. A null in-place overwrite happens only for Suspended or unversioned buckets, and `set_bucket_state_guarded` refuses Suspended once lock is enabled.
- **Multipart and default retention.** The intent is stored at create via `create_with_lock`. fs `complete_object_state` writes that intent onto the new version in the same transaction that consumes the upload. `resolve_write_lock` carries an explicit legal hold and the bucket default retention.
- **Lifecycle and sweep.** Expiring the current version calls `delete_object` (a marker). A noncurrent version calls `delete_object_version(..., Bypass::None)`. `Locked` is a skip, not a successful delete. The fs sweep folds protected sequences back into the live set and does not reclaim their blocks.
- **Bucket delete.** pg `remove_if_empty` and mem `bucket_occupied` count version rows and lock rows as occupied. fs refuses via `has_objects` while object rows remain. `VersionMeta::drain_bucket` refuses the whole drain when a protected version exists; only an orphan lock row is cleared.
- **Surface gate.** The six S3 operations are `#[gated(object_lock: "object-lock")]`. Turning the capability off skips GET/HEAD echo and does not skip the delete check. A stripped build does not mount the six methods. `object_lock_stripped.rs` is the `cfg(test)` refusal suite, not a second handler.
- **Migration `0001`.** `SCHEMA_VERSION` stays 1. The schema gate checksums an already-applied migration and returns `SchemaChecksum` on mismatch, so a database missing the new columns does not silently look unlocked. OL26 accepts serving new databases only.
- **Errors.** `StorageError::Locked` maps to 403 `AccessDenied`, not a retryable successful delete.
- **Clock.** `in_force` is `retain_until > now` (equal means expired, as the spec says). `Retention::validate` rejects a date in the past.

Unstaged remainder, also cleared: `check_writable` defaults to `Ok` and both PUTs call it before the body is parsed (`crates/tinio-api/src/object_lock.rs`); fs startup sweeps, then deletes orphan lock rows (`crates/tinio-fs/src/backend/mod.rs`); wording in the spec, `docs/tests.md`, and `specs/001-s3-local-server/contracts/cli.md`.

No correctness change is required by this pass.

## Structure

Do not approve. The policy functions can stay. What fails the bar is the copied delete path, the missing legal-hold writer, and the files that grew past 1000 lines.

### 1. Delete protection is pasted, not owned

**Blocker.** `crates/tinio-store-pg/src/meta/version.rs` is 3779 lines (production grew 1288 → 1928; tests grew 1083 → 1851). The refuse arm is six copies of the same sequence, not one function:

```1085:1096:crates/tinio-store-pg/src/meta/version.rs
        if id.is_none()
            && let Some(slot) = current.as_ref().filter(|slot| !slot.is_delete_marker)
        {
            let stored = lock_for_update(&mut tx, bucket, key, &slot.wire()).await?;
            if let Err(refusal) = object_lock::may_delete(
                stored.as_ref(),
                SystemTime::now(),
                object_lock::Bypass::None,
            ) {
                tx.rollback().await?;
                return Err(Error::Storage(storage::locked(refusal.reason)));
            }
        }
```

The same `may_delete` → rollback → `storage::locked` sits in `expire_with` (1475, 1522), `expire_many_report` (1639, 1668), and `drain_bucket` (1771). `expire_with` and `expire_many_report` each restate "an object row is judged, a marker is skipped, a missing row falls through to the current slot".

redb already has the shape to copy:

```1022:1037:crates/tinio-store-redb/src/meta/version.rs
fn expire_row(
    txn: &mut redb::WriteTransaction,
    ...
) -> Result<Expired, Error> {
    let row = VersionTable::open(txn)?.get(bucket, key, seq)?;
    match row {
        Some(row) => {
            if row.kind == RowKind::Object {
                let lock = LockTable::open(txn)?.get(bucket, key, &row.id)?;
                if let Err(refusal) = object_lock::may_delete(lock.as_ref(), now, bypass) {
                    return Ok(Expired::Protected(refusal.reason));
                }
            }
```

**Change.** Give pg one `expire_row`. The batch path loops it. Move lock read/write into `meta/version_lock.rs` so `version.rs` does not grow another 600 lines for Object Lock. `expire_with`'s two `match row` arms and `expire_many_report`'s row-present / row-less arms become one `Expired::{Removed, Cleared, Absent, Protected}` enum, which is what redb already returns.

The null in-place overwrite in `crates/tinio-mem/src/object.rs` (387–405) is the same rule inlined inside another transaction. It calls the same helper.

### 2. fs judges the lock twice, then unlinks between the two

**Blocker.** `crates/tinio-fs/src/backend/version.rs` 1187–1251 walks identities, calls `version_lock` + `may_delete`, then `remove_object_file`, then `expire_with`, which judges again. The comment says both sites must judge the same identities; a wrong set unlinks a protected file before the store answers. That is a second delete policy. The correctness pass found the two sets agree today, under the bucket mutation lock, with every lock writer taking that same lock. The agreement is a comment, not a type.

**Change.** Unlink after the store's authoritative check. Delete the pre-check loop.

### 3. Legal hold has no writer next to `apply_retention`

**Blocker.** Retention updates go through `apply_retention`. Legal hold is assembled at each writer:

- `crates/tinio-store-pg/src/meta/version.rs` 1918–1922: `unwrap_or(empty lock)`, assign `legal_hold`, then `write_lock_row`, which uses `carries_fact` to decide whether to delete the row.
- `crates/tinio-store-redb/src/meta/version.rs` 933–938: the same assignment, then a local `if carries_fact { put } else { remove }`.
- `crates/tinio-version/src/testutil.rs` 1497–1510: does not call `carries_fact`; it hand-writes `retention.is_none() && legal_hold == Off`.

`Lock::carries_fact` (`crates/tinio-core/src/object_lock.rs` 522) is already the definition of "an empty lock is not a row".

**Change.** Add `Lock::apply_legal_hold(current, hold) -> Option<Lock>`. The three writers only load and store. The `None` arm is `!carries_fact`.

Column coding is the same gap. `encode_lock` (`crates/tinio-store-pg/src/meta/mod.rs` 164) and `version_object_lock::encode` (`crates/tinio-store-redb/src/version_object_lock.rs` 67) are both the inverse of `from_columns`.

**Change.** One `Lock::to_columns`. Both stores call it.

`Option<Option<Retention>>` (leave / clear / set) is a deliberate three-state. Leave it.

### 4. Three `cfg` pairs do one job

**Strong.** The write path states the same call twice. Put:

```286:324:crates/tinio-server/src/backend/objects.rs
        #[cfg(feature = "object-lock")]
        let write_lock = self
            .resolve_request_lock(...)
            .await?;
        #[cfg(not(feature = "object-lock"))]
        let write_lock = {
            self.write_lock(
                &bucket,
                None,
                object_lock::LegalHold::Off,
                false,
                super::LockChecksum { ... },
            )
            .await?
        };
```

Copy repeats it at 1738 and 1765. `CreateMultipartUpload` repeats it in `crates/tinio-server/src/backend/multipart.rs` at 264 and 291. A stripped build only needs "no request headers".

**Change.** One `LockHeaders`: filled from the input when the feature is on, `None` / `Off` / `false` when it is off. Each of the three call sites keeps a single `write_lock`. These three pairs are what will drift. `object_lock_stripped.rs` will not.

Not a copy of the policy, so not part of this change: the three redb table modules (`bucket_object_lock`, `upload_object_lock`, `version_object_lock`). `upload_object_lock::get` already uses `version_object_lock::decode`. Lifecycle only passes `Bypass::None` into the existing `delete_object_version` (`crates/tinio-lifecycle/src/runner.rs` 742, 813).

CLI `object_lock` / `retention` / `legalhold` each keep `plan` / `connect` / `send`, but HTTP is already one `Plane` plus `object_update` inside the `object_lock` module. Verb boilerplate. Not a blocker.

### 5. Files past 1000 lines

Lengths are the raw counts at the time of this record. The review's HEAD → staged deltas are in the second column.

| File | Review's HEAD → staged | Now | Ask |
|---|---|---|---|
| `crates/tinio-server/src/backend/mod.rs` | 781 → 1112 | 995 | Met. `ObjectLockCache` is `object_lock_cache.rs` (128). The file sits on the line; do not put the cache back. |
| `crates/tinio-store-redb/src/meta/version.rs` | production 720 → 1102; file 1811 → 2594 | 2594 | **Blocker.** Lock RMW and `expire_row` move to `version_lock.rs`. |
| `crates/tinio-store-pg/src/meta/version.rs` | 2371 → 3779 | 3779 | **Blocker.** See §1. Tests move out with the helper. |
| `crates/tinio-auth/src/access.rs` | production 763 → 1054; file 1481 → 2346 | 2346 | **Blocker.** Bypass (`MayBypass` at 147, `may_bypass_authority` at 199, `is_bypass_authority_op` at 951) is its own concept. Move it to `bypass.rs`. Tests grew 718 → 1292; they leave the ACL gate. |
| `crates/tinio-util/src/testing/metadata.rs` | 3169 → 4320 | 4320 | **Blocker.** Lock conformance cases become `testing/object_lock.rs`. |
| `crates/tinio-mem/src/version.rs` | 3628 → 4524 | 4524 | Production grew by about 160. Tests 1566 → 2302, `object_lock_tests` at 3821. Move the tests. |
| `crates/tinio-fs/src/version/tests.rs` | 3245 → 4176 | 4176 | Same: lock cases leave this file. |
| `crates/tinio-pg/src/storage.rs` | 998 → 1090 | 1090 | **Strong.** `resolve_write_lock` (638) is the right forwarder. Split bucket-lock methods out with the rest of the bucket impl. |
| `crates/tinio-store-redb/src/meta/bucket.rs` | 862 → 1318 | (over 1000) | **Strong.** Production is about 691. The tests (about 627) move. |
| `crates/tinio-store-pg/src/meta/bucket.rs` | 759 → 1077 | (over 1000) | Production stops near 555. Tests from line 556 move. |
| `crates/tinio-mem/src/bucket.rs` | 905 → 1083 | (over 1000) | Production is about 277. `mod tests` starts at 278. |
| `crates/tinio-core/src/object_lock.rs` | new, 1108 | 1108 | Keep. This is the policy object. The extra hundred lines are docs. Tests are already `object_lock/tests.rs`. |
| `crates/tinio-server/src/backend/object_lock/tests.rs` | new, 2139 | 2139 | **Strong.** Split by configuration / retention / legal hold / delete. |
| `crates/tinio-cli/src/commands/object_lock/tests.rs` | new, 1292 | 1292 | **Strong.** Split by verb. |
| `crates/tinio-pg/src/version.rs` | 1041 → 1171 | 1171 | `set_retention` / `set_legal_hold` (489–518) are pure forwards. They can stay. No further logic goes in this file. |

## What to change, in order

The first item is the one that makes the next lock rule a one-line edit. Later items are the file splits that item makes possible. Behavior stays. No correctness fix is in this list.

1. **`Lock::apply_legal_hold`.** `current` plus the new hold returns `Option<Lock>`. `None` when `!carries_fact`. pg `set_legal_hold` (1918), redb's matching writer (933), and the version test double (1497) only load and store. Add `Lock::to_columns` and delete `encode_lock` plus `version_object_lock::encode`.
2. **One `expire_row` per store.** Match redb `meta/version.rs:1022`, including `Expired::Protected`. pg's six `may_delete` arms, the fs identity pre-check (1187–1251), and mem's in-place overwrite (387–405) call it. Put the helper in `version_lock.rs` for pg and redb. On fs, unlink only after that call returns removed.
3. **`LockHeaders`.** Collapse the three `cfg` pairs in `objects.rs` (Put, Copy) and `multipart.rs` (`CreateMultipartUpload`) into one `write_lock` call.
4. **Split the files §5 marks Blocker**, in this order: `access.rs` → `bypass.rs`; `testing/metadata.rs` lock cases → `testing/object_lock.rs`; pg and redb `version.rs` tests out of the production file; mem and fs version lock tests out of `version.rs` / `version/tests.rs`. Then the strong splits: server and CLI object-lock tests by operation, and the bucket test modules out of `meta/bucket.rs` and `tinio-mem`'s `bucket.rs`.
5. **Leave `backend/mod.rs` alone** apart from not growing it. The cache extraction the review asked for is already in the tree.
