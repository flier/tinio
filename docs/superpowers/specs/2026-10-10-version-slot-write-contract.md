# The version slot-write contract — D37's restore instance: Design

**Status:** design, **binding on** `docs/superpowers/plans/2026-10-10-version-slot-write-contract.md` (that plan may not contradict this document; where it needs a value this document does not fix, it makes an explicit decision and records it). Nothing in this document is landed: the wave it designs is not executed at the time of writing.

**Filed from:** `docs/superpowers/specs/2026-10-03-ops-engine-design.md` → `## Filed at close-out` → `### Contract gaps found and not closed` → **D37**'s **version restore** bullet (`docs/superpowers/specs/2026-10-03-ops-engine-design.md:174`), the ops version group's reasoned refusal (`crates/tinio-ops/src/version.rs:1-18`), the fs backend's two reasoned refusals (`crates/tinio-fs/src/backend/version.rs:22-26` and `:1098-1121`), and the T4 fix round's finding F1 (2026-10-10 SDD ledger: "the missing contract call is *new* — a backward-real-identity variant bound atomically to the object-row write and the row/index drop, or a combined restore writer").

**One sentence:** the restore's commit — a backwards slot move to a real identity, bound atomically to the object row it installs and the version row it consumes — has no contract call and no composable call sequence, so `tinio-fs` commits it itself; this document specifies the minimal new call that makes it expressible, and states what each of the other two D37 sites needs instead.

## 1. The gap, in contract terms

### 1.1 What a caller can express today

Five calls touch this ground, and their exact signatures are the whole statement of the gap:

```rust
// crates/tinio-core/src/meta/version.rs
async fn advance_slot(&self, bucket: &bucket::Name, key: &object::Key, id: Option<version::Id>, seq: u64) -> Result<(), Self::Error>;   // :500
async fn put_version(&self, bucket: &bucket::Name, key: &object::Key, row: &Stored) -> Result<(), Self::Error>;                        // :538
async fn expire(&self, bucket: &bucket::Name, key: &object::Key, seq: u64) -> Result<(), Self::Error>;                                // :588
async fn current_slot(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Option<Current>, Self::Error>;                       // :446

// crates/tinio-core/src/meta/object.rs
async fn put_with_parts(&self, bucket: &bucket::Name, key: &object::Key, row: &Row, parts: &[PartRow]) -> Result<(), Self::Error>;     // :356
```

- **`advance_slot` is the only contract writer of a key's current slot** (its doc says so), and its landed rule is RC1's: it **refuses a lower sequence when the incoming identity is real** (`id.is_some() && seq < row.version_seq`, `crates/tinio-store-redb/src/meta/version.rs:263`), admits a move to the same sequence (the crash window's re-run), and admits an incoming `None` in place (the suspended-bucket PUT). It also stamps `became_noncurrent` on the row it moves off, in the same commit (RG5, `crates/tinio-store-redb/src/meta/version.rs:279-298`), derives the marker bit from the version row at the slot's sequence (never from the caller), materializes an object row for a key with none, and asserts the bucket row (`NoSuchBucket`).
- **`put_with_parts` is one transaction, but it cannot move the slot**: the contract's `meta::object::Row` carries no version columns, and every ordinary object-row writer **preserves** the stored version columns ("a content rewrite can never silently un-version an object", `advance_slot`'s doc). `put` also clears the key's retained part list in that same transaction.
- **`put_version`** writes one row and its index entry, refuses a row another version already occupies, and displaces another null-id row (the one-null rule).
- **`expire`** removes a row and its index entry in one transaction, idempotently.

### 1.2 What the restore needs, and why no composition reaches it

The restore (S3's version-addressed delete of the **current** version, or of the current marker) makes the newest remaining version current again. On the D37 row-slot model — the current version's identity lives in the slot, version rows are history — the commit is **five facts in one transaction**:

1. the object row: the successor's content elements (ETag, size, mtime, file identity) and recorded elements (tags, checksum, owner, ACL), plus the slot columns naming `(id, seq)`;
2. the slot moved **backwards to a real identity** — the successor's own sequence, below the version being removed;
3. the restored version's own row and its index entry consumed (a current version is not addressable as a history row);
4. the key's retained part list cleared (the row is a fresh object's record);
5. all-or-nothing, because the intermediate is observable: with the row dropped or the slot left naming the removed version, the key would list its current version twice or serve a version whose content was already removed.

The three-call composition fails on more than width:

- `put_with_parts` **cannot move the slot** (fact 2 needs `advance_slot`), and `advance_slot` **refuses the move** (RC1's guard admits only the null identity in place) — so the composition is not merely wider, it is **refused at the call that would move the slot**. This is the "not expressible *at all*" of the T4 fix round's F1, and it is why no ordering of today's calls works.
- The two-transaction split that the versioned PUT accepted (RI3: `put_with_parts` then `advance_slot`, whose `became_noncurrent` stamping is atomic with the slot move) is **not** available here: its accepted cost ("a superseded version's parts uncleared — a stale retained list, never a lost object") has no analogue in a restore, where the same split leaves the restored version's row alive while the slot still names the removed version.

The site, exactly: `crates/tinio-fs/src/backend/version.rs`'s `restore` (`:850`) and `commit_restore` (`:982`), reached from `delete_object_version` (`:1122`) under the bucket mutation lock. `commit_restore`'s body is the five facts: `object_meta::Table::put`, `object_part::Table::remove_key`, `rows::Table::remove(seq)`, `version_index::Table::remove_pointing_at(id, seq)` — one raw redb write transaction.

Two consequences already stated in the tree bind this design: the fs module doc calls the move "a move `advance_slot` refuses" (`crates/tinio-fs/src/backend/version.rs:22-26`), and the ops module doc states the same blocker as the reason the version group writes eight of its nine bodies (`crates/tinio-ops/src/version.rs:7-18`).

### 1.3 The other two D37 sites are not this gap

They are analyzed in §4 (a second capability, named and deferred) and §5 (a model difference, not a call). This document's capability is scoped to the restore, and deliberately to nothing else.

## 2. The capability: `VersionMeta::restore_slot`

### 2.1 The signature

One new method on `VersionMeta` (`crates/tinio-core/src/meta/version.rs`), the trait that owns the version rows and the current slot (`VersionMeta` is standalone, never a supertrait of `Metadata`, D20):

```rust
    /// Make a version current again — the restore's commit, in one
    /// transaction: the object row the restored version becomes, the slot
    /// moved back to the version's own sequence, the version row and index
    /// entry the restore consumes, and the key's cleared retained part list.
    ///
    /// The slot move is deliberate and is the one move `advance_slot` refuses:
    /// a restore moves the slot **backwards to a real identity**, so this call
    /// applies no monotonic guard — the guard stays absolute for `advance_slot`
    /// (the exception is the call, never a parameter).
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object::Row,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error>;
```

Naming: the verb mirrors `advance_slot` (the slot is the subject, `restore` is the domain act). The alternative `restore_version` is rejected because the call is not the whole restore — the content publish (`Versions::materialize`), the file removal and the successor scan stay the caller's — and `restore_version` would read as an operation body rather than a store primitive.

Home: `VersionMeta`, not `ObjectMeta`. The call writes an object row and consumes a version row, and the slot it moves is version-domain state whose only other writer (`advance_slot`) already lives here and already writes object rows (it materializes one). Putting it on `ObjectMeta` would give the object plane a slot writer, contradicting the rule `advance_slot`'s doc states and D37's model.

### 2.2 Semantics — the transaction

Everything below commits together or not at all. Each rule is stated as an observable, so the conformance suite can pin it (§7).

- **S1 — the bucket row is asserted inside this transaction.** A bucket with no row refuses, projecting to `storage::Error::NoSuchBucket` (the guarded-writer precedent: `put_guarded`, `put_with_parts`, `advance_slot`).
- **S2 — the version row at `seq` is read in this transaction, and it is the call's premise.** It must exist and its identity wire must equal `id`'s wire (`None` is the `null` literal). It supplies the **marker bit**: the row's `Kind` is what the written object row's marker bit records — the caller never carries a kind (`advance_slot`'s rule: "the slot is a version-row fact, not a kind this call carries").
- **S3 — the object row is written wholesale**: the caller's content and recorded elements, the slot columns naming `(id, seq)`, and the marker bit derived in S2. The write **clears the key's retained part list** in the same transaction (`ObjectMeta::put`'s rule — an overwrite is a fresh object).
- **S4 — the consumed version's row at `seq` and its index entry are removed** in the same transaction. The restored version is now the current version: on the D37 model that is exactly the state with no row of its own, and leaving the row would make one version both current and addressable as history.
- **S5 — the row the slot moves off is stamped `became_noncurrent`** in the same commit, under RG5's rule and its two exemptions exactly as `advance_slot` applies them: a move at the **same** sequence moves off nothing, and a sequence holding no row has nothing to stamp. A row that already carries a stamp keeps it (the first stamp wins).
- **S6 — no monotonic guard.** The call exists to move the slot backwards to a real identity; the guard's invariant is not restorable by definition here. The caller has established, under its own serialization (fs: the bucket mutation lock held across the whole removal; a store-side engine body: its own read), which version becomes current.
- **S7 — the derived marker bit survives the row it came from.** After the call `current_slot(bucket, key)` answers `Current { exists: true, version: id, is_delete_marker: <the consumed row's kind == Kind::Marker> }`. The implementation records the bit where the slot's facts live (see §6: on redb, the object row's existing `is_delete_marker` column; on pg, its slot record) — a store whose slot cannot answer the bit must not silently answer `false`; it files the gap and answers today's behaviour (§7's pinning rule applies to the assertion, §6's pg item to the implementation).
- **S8 — the call is not idempotent, and a false premise is an error.** It is written once per restore, not re-run by a crash-window adoption path: a second run finds the row consumed and answers R2.

### 2.3 Refusal conditions

- **R1 — `NoSuchBucket`** (S1): the bucket row is absent, asserted inside the transaction, projecting to `storage::Error::NoSuchBucket`.
- **R2 — the row's own refusal**: no version row at `seq`, or a row whose identity wire differs from `id`. The refusal is the implementation's own error, projecting through the contract's `Error: Into<storage::Error>` (the `put_version` same-sequence conflict's precedent), never `Ok`, never a panic. The caller named a version it had just read; a row that vanished or changed identity is a bug or a lost race, and the store must not write a slot naming a version it no longer holds.
- **R3 — the store's own I/O errors**, as every contract method's.

### 2.4 What the call deliberately does not do

- **It publishes nothing.** The content half stays the engine's `Versions::materialize` (the caller streams the version's bytes back onto the key path **before** this call, so a crash between the two leaves the version's row — and so its content — recoverable; that ordering is fs's `restore` today and stays).
- **It writes no part list** — it clears it (S3). A completed multipart object's retained list is `put_with_parts`' business, and the restore's successor carries no parts to retain (`commit_restore` clears them).
- **It touches no upload records.**
- **It applies no guard and takes no `backwards: bool`** (S6): the exception is the call, so `advance_slot`'s rule stays absolute for every forward writer instead of growing a parameter every implementor must honour.
- **It does not decide which version becomes current.** The rule that picks the successor ("the newest row that is history, judged by the removed version's own slot fact") is the contract's `Stored::is_history(Slot)` (`crates/tinio-core/src/meta/version.rs`), and the scan is the caller's.
- **It does not create the version's row when absent** — it consumes it.

### 2.5 Illustration (non-binding)

The fs re-point (§8): `restore`'s scan picks `(seq, row)`; the marker arm builds the row from the marker's own row, the object arm materializes through `Versions::materialize`, stats the landed file and builds the row from `row.stored(seq, published.size, mtime, file_identity)`; then, instead of `commit_restore`, one call:

```rust
Rows::new(self.clone())
    .restore_slot(bucket, key, &row, id_of(row.id.as_str()), seq)
    .await
```

## 3. `tinio-version`'s state engine: unchanged, and why

**Nothing changes in `crates/tinio-version/`.** `plan_write`'s decision (whether to supersede, which identity and sequence the incoming version takes, the one-null-row convergence), `Versions::supersede_with`, `Versions::materialize`, `Versions::expire`/`expire_many` and the one port (`CurrentRows`, the gated read) are untouched, and no new port is declared.

Why the state machine stays the owner of the rules, and this call is not one of them:

- The new call **decides nothing**. Everything it writes was decided by the caller: which version becomes current (the successor rule, the contract's own `Stored::is_history`), which identity and sequence the slot records (the caller's read of the row), and what the row's elements are (the caller's materialize/stat). It is a **store primitive** — one transaction over the store's own tables — exactly as `advance_slot`, `put_version` and `expire` are. Its home is the contract crate, beside them.
- The engine owns **policy**, and the policy for a restore is "the newest remaining version becomes current", which is `plan_write`'s neighbour but not its subject: `plan_write` answers what a **write** records, and a restore is not a write of new content.
- Had the engine been asked to own the restore, the successor scan — on fs a bounded two-row prefix read over raw rows, judged by the wire-level `is_history` — would have to move into a crate that must not know a backend's layout, or the engine would need a second gated port. Neither is warranted by one body.
- The composition a later wave may add — an ops engine body that runs `materialize` + `restore_slot` — is the **ops engine's** (a body over contracts), not the state machine's, and it composes the same pure decision the other eight bodies take from `plan_write`. The ninth body is therefore expressible after this call lands (§8, follow-on 1) without any `tinio-version` change.

## 4. The multipart completion: a second capability, named and deferred

**The finding:** `complete_multipart_upload` is absent from `crates/tinio-ops/src/multipart.rs` because its commit half is not a contract call sequence in either backend (`crates/tinio-ops/src/multipart.rs:38-49`). The fs body's commit is **one** state transaction — `complete_object_state` (`crates/tinio-fs/src/backend/mod.rs:1048`): the upload row read (the create-time tags/owner/ACL), `drain_upload`, the object row **with its version columns** (the D37 slot move rides the row write on fs's model), the retained part list replaced, and `stamp_superseded` — under the bucket mutation lock, after `publish_versioned` committed the supersede's row before the rename. mem decides the version outcome inside one transaction and credits the consumed parts' bytes in the publish (`crates/tinio-mem/src/multipart.rs:514-601`).

**Why the first capability does not close it:** the completion's one-transaction core is {the object row + the slot, **and** the retained part list, **and** the upload row with its part rows}. `restore_slot` carries the first pair only; `put_with_parts` cannot carry the slot and preserves it; `drain_upload` is a third transaction. The offered sequence — `put_with_parts` → `advance_slot` → `drain_upload` — is three transactions where the backend runs one, and the difference is observable on an **error path**: a refused `drain_upload` after the row and slot committed would leave the object current and the upload listed, where today the whole completion rolls back. So: **not expressible already once the first lands.**

**The decision: a second capability, deliberately not landed in this wave.** Its irreducible core is a **slot-carrying parts write** — the same shape as `restore_slot`'s row half with the parts retained instead of cleared, no version row consumed:

```rust
    // Sketch only; the multipart wave rules the exact shape and its home.
    async fn commit_completion(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object::Row,
        parts: &[PartRow],
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error>;
```

with `drain_upload` split after it as a **filable residual** — a shape mem already ships ("Known deviation: the drain is a contract transaction of its own, so a completion is not one transaction end to end; the object write has already committed, and the in-memory backend has no crash surface between it and the consume", `crates/tinio-mem/src/multipart.rs:597-600`).

Reasons to defer, not to design it badly here: (a) no landed site needs it — fs's and mem's completions are correct today, so deferring is zero observable change; (b) its **home** is a real design question (a slot-and-parts writer on `VersionMeta`, where the slot's writer lives, versus a new cross-category contract) that the consumer's wave must rule with its own evidence; (c) this plan already carries one new contract method and its four-implementor ripple, and the operator's minimality rule is explicit that this wave is the extraction's leftover, not a redesign.

Cost if this decision is wrong: fs's `complete_multipart_upload` and mem's stay backend-side, and the multipart group's engine body stays unlanded — the exact status quo, filed with a sharper reason.

## 5. The suspended DELETE: a model difference, not a call

The D37 filing's third instance: fs keeps `delete_versioned` (`crates/tinio-fs/src/backend/version.rs:612`) because a **shared body that wrote the delete marker as a version row** would let the store's one-null rule displace the superseded null version's own row (`crates/tinio-fs/src/backend/objects.rs:1292-1302`), and the shared storage journey pins that row's survival ("the superseded null version must survive a suspended DELETE", `crates/tinio-util/src/testing/mod.rs:3230`).

This is **not** a missing call, and no call this document adds closes it:

- The suspended DELETE's marker carries the **null identity**. Under `put_version`'s one-null rule ("committing the null identity displaces any other null-id row of the key", `crates/tinio-core/src/meta/version.rs:527-532`), a null-identity marker can **never** be a version row — that is the model's own rule, not an implementation limit. So a shared body cannot write the marker as a version row for any store.
- fs's model puts the marker in the **object row** (no version row at all), while the contract's `current_slot` derives the marker bit "from the version row at its sequence" (`crates/tinio-core/src/meta/version.rs:434-440`). A call that moved the slot for fs's suspended delete would therefore record a marker the contract's own read answers as "a recorded slot that is not a marker".
- Closing it needs a **model decision**, one of: (i) a marker bit on the written row (a new element in `meta::object::Row` and in every store's object-row codec), or (ii) a `current_slot` rule change (the object row's own marker bit as the read source), or (iii) a marker version row for a **non-null** identity only, with the null case staying object-row state. All three touch every implementor's row shape or read rule, and none is the extraction's leftover.

**Decision: not generalised here; filed as the third capability ("the object-row marker"), with the reason above.** fs's `delete_versioned` keeps its private path.

**Recorded uncertainty (pre-existing, unchanged by this wave):** on the fs plane `VersionMeta::current_slot` delegates to `RedbMetadata` (`crates/tinio-fs/src/version_adapter.rs:755-761`), which derives the marker bit from the version row at the slot's sequence (`crates/tinio-store-redb/src/meta/version.rs:189-199`), while a current marker on fs's model has no version row — so `current_slot` answers `is_delete_marker: false` for a current marker on that plane, and fs's own gate (which reads the object row) answers `Marker`. No production caller has been found for that path on the fs plane (the gate is the engine's read port), and the shared suites do not catch it; it is recorded here because the restore's marker clause (S7) touches the same seam. The plan's redb task resolves the seam for `restore_slot`'s written rows (§6) and leaves the older fs marker path exactly as it is.

## 6. Implementors that must change

Adding one method to `VersionMeta` breaks every implementor. The tree has **four**, plus two delegating adapters, plus a fifth that is **gone**:

| # | Implementor | File | Change |
|---|---|---|---|
| 1 | `RedbMetadata` (the reference; fs's and mem's planes both run it) | `crates/tinio-store-redb/src/meta/version.rs` | The implementation: one write transaction over `ObjectTable` + `VersionTable` + the version index + the part table — the tables `advance_slot` and `expire` already open. Plus S7: `current_slot` must answer the bit `restore_slot` writes (the object row's existing `is_delete_marker` column becomes the read source when a row exists, with the version row at the slot's sequence kept as it is — every landed pin stays green, because every contract slot move writes the column from the same source). |
| 2 | `PgStore` | `crates/tinio-store-pg/src/meta/version.rs` + `sql/meta/version/*` + `sql/migrations/*` | The implementation as its own SQL transaction (bucket row asserted, the version row at `seq` read `FOR UPDATE` for S2's identity and kind, the row and its index entry deleted, the `object_meta` row upserted, the parts cleared, `current_slots` upserted). pg's slot table (`current_slots(bucket, key, version_id, seq)`) carries **no marker bit** today, so S7 needs one column there (or an equivalent), written by this call; that migration, the statement files and the `.sqlx` cache regeneration (`cargo xtask sqlx update`, `docs/cargo.md` §sqlx cache) are this implementor's cost and are named as pg's item in §7. |
| 3 | `Fake` (the `tinio-version` engine's own double) | `crates/tinio-version/src/testutil.rs` | The implementation over its own slot/row/index tables (`:824`). |
| 4 | `TestMeta` (the ops engine's double) | `crates/tinio-ops/src/testutil.rs` | A delegation to `RedbMetadata`, like every other `VersionMeta` method there (`:691`). |
| 5 | `Rows` (fs's metadata adapter) | `crates/tinio-fs/src/version_adapter.rs` | A delegation to `RedbMetadata` (`:724`), like `advance_slot`'s (`:765`). |
| — | `MemoryStorage` | `crates/tinio-mem/src/storage.rs` | **No new code**: mem holds `Arc<RedbMetadata>` and its version group is mem-local (E16). Verify, do not assume: the compile is the check. |
| — | `spike/pg-meta` | `spike/pg-meta/**` | **Retired, not merely out of workspace.** It was deleted by `5304d33b` ("feat(pg): implement the persistence contracts over PostgreSQL", the newest commit on `dev`): `git show --name-status 5304d33b -- spike` lists the seven removed files (`Cargo.toml`, `Cargo.lock`, `README.md`, `docker-compose.yml`, `schema.sql`, `src/lib.rs`, `src/main.rs`). The standing rule ("adding one contract method breaks four implementors and the spike is invisible to every gate, so run `cargo check --manifest-path spike/pg-meta/Cargo.toml` after any contract edit") therefore **has no manifest left to check**; the fifth implementor now lives in-workspace as `crates/tinio-store-pg`, whose check every workspace gate sees. The plan records the retirement and replaces the spike check with `cargo check -p tinio-store-pg`; re-materializing the spike (`git checkout 5304d33b^ -- spike/pg-meta`, a tree write the operator owns) is optional and its check command is unchanged. |

## 7. What the conformance suite can and cannot pin

Three suites are in play, and the filing's platform rule binds the choice ("a rule pg cannot satisfy must be pinned at the store's own suite, not the shared one").

- **The shared contract leg** — `assert_version_conformance` (`crates/tinio-util/src/testing/metadata.rs:293`), run by redb's in-crate tests (`crates/tinio-store-redb/src/meta/version.rs:597`) and by pg's conformance test (`crates/tinio-store-pg/src/tests/conformance.rs:43`) — **only those two**: `assert_metadata_conformance` does not call it (`crates/tinio-util/src/testing/metadata.rs:204-213` runs the bucket, object, traversal, upload and refusal legs and no version leg), so mem's plane is covered by redb's run of the same `RedbMetadata` implementation, not by a leg of its own. It **can** pin: S1/R1 (`NoSuchBucket`), S3 (the row's elements + the cleared part list, read back through `ObjectMeta::get`/`parts`), S4 (`get_version(seq)` is `None`, `version_seq_of(id)` is `None`), S5 (the moved-off row's `became_noncurrent`, through `Stored::became_noncurrent`), S6 (a **backwards real-identity move lands**), R2 (an absent row and an identity mismatch are refused), and S2/S7's **object-version** arm. It **cannot** pin the **marker** arm of S2/S7: `PgStore`'s slot record carries no bit until its named item lands, and a shared leg may not redden pg for a rule its implementation cannot yet satisfy.
- **The store's own suite** — redb's in-crate test module. It pins the marker arm (S2/S7: a restored marker reads back as a marker through `current_slot`), the same-sequence re-run (S5's exemption), and both refusal arms' concrete shapes.
- **The shared storage journey** — `assert_conformance` (`crates/tinio-util/src/testing/mod.rs:260`), run by fs, mem and (P5) pg. Its parked journey already exercises the restore end to end for an object version: it deletes the current real version (`:3281`) and asserts the surviving null version becomes readable again with its own identity (`:3263-3272`). That journey is this capability's **wire-level acceptance** for fs's re-point (zero observable change) and needs no new leg.
- **Not pinnable, stated rather than gapped:**
  - the intermediate state a split would produce (the key listing its current version twice) — no test can observe a state the contract refuses to reach;
  - pg's marker arm (filed, §6.2);
  - the **marker-successor** restore on the fs wire: the tree's journeys pin the marker *removal* restoring an **object** version (`crates/tinio-fs/src/version/tests.rs:343`) and the shared journey's restores are object versions; whether fs's marker-successor arm (`restore`'s `row.kind == rows::Kind::Marker` branch) had a pin was **not established while writing this document** — **resolved by the plan's Task 2 Step 1: it had none**, and the pin (`removing_a_version_restores_the_marker_it_superseded`, `crates/tinio-fs/src/version/tests.rs`) was written **before** the re-point and stayed green unmodified through it (§10).

## 8. The migration, in order

1. **This wave (the plan that binds on this document).** The contract method, the four implementors, the delegations, the shared leg and redb's own pins. Ends green with **no behaviour change anywhere**: no caller moves yet, so every existing suite and both cucumber passes are unchanged.
2. **fs's restore commit (the same plan's last code task).** `commit_restore` and `marker_row` retire; `restore`'s two arms call `restore_slot` instead, keeping the scan, the materialize, the stat and the prune. This is where site 1 **loses its private path** — the site's transaction moves from `crates/tinio-fs/src/backend/version.rs` into the contract. Zero observable change is the acceptance: `cargo test -p tinio-fs`, the shared storage journey, and the fs cucumber versioning pass stay green with no test edit (a test edit is a finding, not a step).
3. **Named follow-ons, each with its own plan** (none is this document's to land):
   - **the ops engine's ninth body** — `ObjectOps::delete_object_version` over the contracts: the successor scan becomes `list_versions` + `Stored::is_history(Slot::At(removed_seq))` (a decode cost where fs's raw two-row prefix read is cheap; a stated cost, fs keeps its adapter scan until it re-points), the content removal is `ObjectContent::remove`, and the commit is `restore_slot`. This call removed the "does not exist at all" blocker; the body's authoring is the version group's next wave (and `tinio-pg`'s `VersionOps` consumer, P5);
   - **the completion writer** (§4) with the multipart group;
   - **the object-row marker** (§5), a model decision.
4. **What is preserved throughout:** fs's and mem's observable behaviour (their suites, `assert_conformance`, both cucumber passes, the benches), the guard on `advance_slot` (absolute, no parameter), the `became_noncurrent` stamping rule (extended to the new call, not weakened), the one-null rule, and `tinio-version`'s ownership of the decisions.

## 9. Open questions for the operator

1. **The marker clause's scope (S7).** This document requires the restored marker's bit to survive; that obliges a pg column (§6.2) and a redb read-source clause (§6.1). The alternative is to refuse a `Marker` row in `restore_slot`, leaving fs's marker arm private — smaller, but it makes the contract's shape follow one store's slot table. **Recommendation: keep S7 as written** (the repo's precedent is to fix the store for the rule — RC1, RG5), and let pg's column ride its own wave.
2. **The refusal class for R2.** Which `storage::Error` a vanished/mismatched restored row should project to at the API layer (the not-found class naming the version, or the internal class) is the ninth body's decision. **Recommendation:** the store answers its own refusal (as `put_version` does); the projection is ruled with the ninth body.
3. **The successor scan's cost.** If the ninth body uses `list_versions`, a version delete decodes every row of the key (tags, checksum, owner, ACL) where fs's raw scan reads two. **Recommendation:** keep fs's adapter scan and file the body's cost when the body lands; do not add a contract call for it now.
4. **The completion writer's home** (§4): a slot-carrying parts writer on `VersionMeta`, or a new cross-category contract. **Recommendation:** rule it in the multipart wave with fs's and mem's bodies in view; this document's sketch is not binding.

## 10. Uncertainties recorded (what the tree does not tell)

- **Resolved (the plan's Task 2 Step 1, 2026-10-10):** fs's marker-successor restore arm had no test (§7's last bullet). The pin was written before the re-point — `removing_a_version_restores_the_marker_it_superseded` (`crates/tinio-fs/src/version/tests.rs`) — and stayed green unmodified through it; its first RED was a test-authoring error in the stamp capture (the marker's own object-row stamp was read after the superseding put), not a product defect, and the arm was already correct under the private path.
- Whether `ObjectMeta::remove`'s retained-part-list clear is contract text or only redb's implementation detail: the doc says only "Remove one row" (`crates/tinio-core/src/meta/object.rs:461-468`) while `RedbMetadata::remove` clears the parts in the same transaction (`crates/tinio-store-redb/src/meta/object.rs:582`). The restore's parts clear (S3) rides `put`'s **documented** rule instead, so this uncertainty does not bind this design; it matters to the `no successor remains` arm of a restore, which is `ObjectMeta::remove`'s and stays the backend's.
- The exact read-source clause for redb's `current_slot` (§6.1) is this document's proposal, not a settled rule: the plan's redb step writes the failing test first (a restored marker reads back as a marker) and lets the implementation choose between "the column wins" and "the column OR the version row"; both keep every landed pin green.
