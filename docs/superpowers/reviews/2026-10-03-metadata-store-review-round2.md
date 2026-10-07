# Review record: metadata-store plan set, round 2 (post-rewrite)

**Date**: 2026-10-03
**Method**: five read-only review lanes (three `reviewer`, one `oracle`, one `scout`) over the nine documents, scoped to the sections rewritten after round 1 — the three specs' §1/decision tables, P1's method block and task interfaces, P4 and P5 in full, P2/P3's edited steps, and the three feature edits in the versioning plan. Findings only, each falsifiable; no edits by the lanes.
**Result**: 68 findings (lane counts 15/12/18/15/8), of which 5 blockers. Two lanes independently found the same read-heal defect — the strongest signal in the round.

## What this record is

A work list, not a design. Each item names the document that owns the fix, the change, and the ruling it comes from. The nine documents are otherwise consistent; the items below are the difference between "consistent" and "implementable".

## Blockers

| # | Finding | Fix (owner) | Status |
|---|---|---|---|
| B1 | **The read-heal's inputs are not in the contract.** `stat -> Option<ContentEntry{key,len}>` cannot supply the size/mtime/identity that fs's `entry_matches`/`composed_keep` and the listing's per-entry staleness gate need (round-2 lanes 1 and 2, independently). | `ContentEntry` becomes `{ key, len, identity: u64, mtime: Option<SystemTime> }`, one type for `scan` and `stat`; a store without a timestamp answers `None` and never fabricates (D42 amended, operator ruling round 39). Document: contract spec §1/D23/D42 + P1 Task 1's block. | ruled, not applied |
| B2 | **pg's `open`/`stat` semantics are undefined**, so the unconditional heal either resurrects residue as an object (a PUT whose row write failed) or, on a read-only instance, recomputes and tries to persist on every read. | pg design §5: "`open`/`stat` resolve the key through the object row's live generation; a key with no live row has no content — residue is never content"; engine spec E13/obligation 7 states the heal is the content-truth case (dead code on pg by construction) and that **a heal write failure never fails the read** (log, serve the computed value — operator ruling round 39, no third policy field). | ruled, not applied |
| B3 | **pg has no engine-sequence lock.** The sequence supersede → publish → row → slot advance spans awaited content I/O, so no `SELECT … FOR UPDATE` can cover it; two concurrent same-key PUTs mint colliding version rows. | pg design §6 + P5 Task 2: `tinio-pg` takes an **in-process per-bucket mutex** around the engine call (sufficient under PG12's single writer), as fs's `Storage` impl does; the store's row locks stay inside the contract methods. | pending |
| B4 | **No `CurrentRows` implementation for pg.** The engine's `Ops.versions` needs a type implementing `Metadata + VersionMeta + CurrentRows`; P3 produces `impl VersionMeta for PgStore` only and P5 names neither. | pg design §6 + P5 Task 2's Produces: name `impl CurrentRows for PgStore`, with its gate stated (the `object_meta` slot is the truth — nothing outside the database can move the current object). | pending |
| B5 | **The completed multipart's part chunks have no owner.** PG21's two permitted forms need opposite reclamation rules: with a part-list indirection, a cleanup keyed on "rows whose upload is gone" reclaims the live object's bytes; with copy-under-a-new-generation, those chunks leak. | pg design PG21: name the post-completion owner and match `Cleanup`'s predicate to it; add the form to P1 Task 2 Step 2's pinned list (the spike pins it) and give it an owner in P3 Task 2 Step 4 with its leg. | pending |
| B6 | **`Arc<dyn Metadata<…>>` does not compile** (found by probe A, not by any review lane — no lane could compile). The sub-trait method style (`where Self: Metadata`, `<Self as Metadata>::Error`) makes `Metadata` **not dyn compatible** (E0038), and `dyn ObjectContent` additionally needs `Staged` named (E0191). The workspace's `Storage` survives the same style only because it is **never used as `dyn`**. | Reverse D6: the engine's `Ops<M, V, R, C, B, H>` is **generic over concrete backend types** (`Ops<RedbMetadata, Versions<…>, RedbMetadata, FsContent, LocalBlocks, FsHooks>`), and every backend holds concrete handles (`Arc<RedbMetadata>`, `Arc<FsContent>`, `Arc<PgStore>`) — the workspace's own style. Touch points: D6, D17/D26 (ports), D35, E1/§1's instantiations, P1 Task 1's blanket sentence + Task 5's pin, P2's handle spelling, P3/P5's pg instantiations. Probe A2 proves this shape compiles. | ruled by probe, not applied |

## Fix list by document

**`specs/2026-10-03-metadata-store-contract-design.md`** (§1, D-table, Verification)
1. B1's `ContentEntry` widening; `stat` returns the same type.
2. `VersionMeta::advance_slot(bucket, key, id: Option<version::Id>, seq: u64)` + the property "the object-row writers (`put`/`put_batch`/`put_preserving`) preserve the stored version columns; only `advance_slot` and the delete-marker path change them" (round 39; the write path had no home).
3. `current_slot` returns the port's `Current { exists, version, is_delete_marker }` shape, not `Option<(Id, u64)>` (the null version and a delete marker are both legitimate currents).
4. `StagedObject` gets its own `type Error: Into<storage::Error>` and `ObjectContent::Staged: StagedObject<Error = Self::Error>` (the pinned `<Self as ObjectContent>::Error` does not resolve).
5. Property 1: `put`/`put_batch` **clear** a key's retained parts (an overwrite is a fresh object), `put_with_parts` writes them, `put_preserving` leaves them — and the heal therefore persists through `put_preserving`, not `put`.
6. Property 4: qualify "never fails" — a corrupt row never fails `walk_healing`, but a **heal write failure is logged and the read still succeeds** (round 39).
7. `walk_healing -> Vec<(Key, Option<Row>)>`; `walk -> Vec<(Key, Row)>` (the gated traversal's `Option` is the heal decision the caller needs; the strict one keeps failing on a corrupt row).
8. One pair for the retained part list (`parts`/`put_with_parts`); delete the duplicate trio from P1's `UploadMeta` block; `list_object_parts` projects `PartRow` → `ObjectPart`.
9. Existence-conditioned setters carry a signal: `BucketMeta::{set_tags, clear_tags, set_cors, set_acl}` and `ObjectMeta::set_acl` return `Result<bool>` (or a `NoSuch…`-projecting error), so the refusal happens in the contract's transaction rather than as a pre-read (D8/D22).
10. `BucketMeta::create(bucket, row)` — insert or refuse in one transaction (`create_bucket`'s `AlreadyExists` has no atomic home today).
11. Property 3: `scan`'s `prefix` is a bound the implementation **must not scan past** (not "may prune with").
12. Property 9 + §4: `is_empty` counts **object content** (fs includes folder-marker directories); uploads stay a separate `has_uploads` probe; the engine's `delete_bucket` refusal is the content-side check, with `remove_if_empty`/`remove` as the removal step (one authority, stated).
13. D23: scope `copy` — same-bucket object-to-object only; cross-bucket and ranged copies stream.
14. D8/D22: keep them, pointing at item 9's signal rule.

**`specs/2026-10-03-ops-engine-design.md`**
15. E13/obligation 7: the heal reads its inputs from the widened `ContentEntry`; its write goes through **the path `bucket_row` selects** (`put`/`put_guarded`), and its failure is logged, never fatal (B2/B1/B3 above).
16. Obligation 6: the listing's staleness gate uses the walked entry's `identity`/`mtime` (B1); the enrichment stays one `ObjectMeta::load` per page.
17. New obligation: **the engine sequence lock is the backend's in-process lock** (fs's bucket-mutation lock, pg's per-bucket mutex) — the store's own locks stay inside contract methods (B3).
18. Risk row: the composed-ETag keep rule and its jitter constant live **once**, in `tinio-ops`; fs's `meta.rs` and `etag.rs` (the scanner's compute task) call it (round-2 lane 3; the two copies would resurrect the F04/F14 stale-ETag class).
19. Verification 4: scope the independence check to **non-test code** (the dev-dependency's path contains `redb::`).

**`specs/2026-10-03-pg-design.md`**
20. §5: `open`/`stat` resolve through the live generation; residue is never content (B2).
21. §3/§5: the **generation marker lives in the `content` relation**, not in `object_meta` (an `object_meta` marker forces `publish` to write the metadata plane — a double write and a stale row); §3's `object_meta` cell keeps only the version slot (D37) (round-2 lane 2).
22. §6: the two `Ops` policy values (`metadata_failure = Fail`, `bucket_row = Guaranteed`) and the `Hooks` implementation with its no-op `lock_part` guard, with the reason written (part bytes are keyed `(upload_id, number)`; the object becomes live through the row update).
23. §6/PG16: `impl CurrentRows for PgStore` and its gate (B4).
24. §6: the in-process per-bucket mutex around the engine call (B3).
25. PG21: the part-chunk ownership after completion + the matching `Cleanup` predicate (B5).

**`plans/2026-10-03-metadata-store-contract-p1-contract-and-fs.md`**
26. Task 1's block: the widened `ContentEntry`, `advance_slot`/`Current`, the `StagedObject` error, `BucketMeta::create`, the setters' signal, and **`VersionMeta`'s full method block pinned here** (the gate in Task 2 implements it before Task 8 "derives" it — the proof cannot precede the signature).
27. Task 1's blanket sentence: scope "`where Self: Metadata`" to the three category traits (`VersionMeta`/`ObjectContent` use their own `Error`).
28. Task 5 → Task 6: `crates/tinio-fs/src/database/tables.rs` belongs to the object traversal's task.
29. Task 6 Step 4: keep only the stat plumbing and `md5_of_file`; the composed-keep rule and its constant **move to `tinio-ops`** in P4, and both fs call sites (`meta.rs`, `etag.rs`) call that copy.
30. Task 9 Files: add `crates/tinio-fs/src/{etag.rs,backend/multipart.rs}` (production `_store_redb::` sites the check would otherwise fail on) and scope the invariant grep to name the `database/mod.rs` shim as the exception.
31. Task 10: one content-error rule (`C::Error: Into<storage::Error>`; backends pass their own) — delete `Error = storage::Error`.
32. Task 8 Step 3: delete "`scan` delegating to the same walk the fs scanner uses" (that is Task 9's fs impl, not `RedbContent`'s).
33. Task 2 Step 2/Step 3: the spike covers **all eleven** D23 calls; add PG21's completion form to the pinned list.
34. Plan-set ownership: the 2026-09-03 arity correction is P2's (see item 40).

**`plans/2026-10-03-metadata-store-contract-p2-mem-and-conformance.md`**
35. Task 1 Interfaces: `&Arc<RedbMetadata>` (or `&Arc<dyn Metadata<Error = tinio_store_redb::Error>>`) — `Arc<RedbMetadata>` cannot coerce to a `Metadata<Error = tinio_mem::Error>` object.

**`plans/2026-10-03-tinio-store-pg-p3.md`**
36. Every DB-backed gate command becomes `--features pg,pg-tests` (three places; `--features pg` alone skips the conformance suite and the PG18–PG24 legs).
37. Task 2 Step 4: consume the same coverage claim P1 pins (the spike covered all eleven) — delete the "three it did not" list.
38. Task 3 Interfaces: `VersionMeta`'s row shape (P1 deletes `VersionRows`).
39. Task 2 Step 4 + Task 4: the completion form (B5) and its leg.

**`plans/2026-10-03-tinio-ops-p4.md`**
40. Task 1 Step 4: assign **both** fs policy values (`MetadataFailure::Warn`, `BucketRow::Lazy`) beside mem's.
41. Task 3 Step 3: delete "`pre_publish` is gone" (no such construct exists).
42. Task 4 Files: align mem's `VersionOps` impl file with the versioning plan's landing (`bucket.rs` there; settle one owner).

**`plans/2026-10-03-tinio-pg-p5.md`**
43. Task 1 Files: add `docs/tests.md` (the state-dir class list) and `specs/001-s3-local-server/data-model.md` — the synthetic root `~/.tinio/pg/<hash>/` is a new state-dir class, and the repo's rule is that a class is recorded with the closed set.
44. Task 5 Files: swap the parentheticals — the `Backend` enum lives in `crates/tinio-e2e/tests/steps/common.rs`, `config_from_tags` and the `World` dispatch in `mod.rs`.
45. Task 2: the per-bucket mutex (B3) and `impl CurrentRows for PgStore` (B4).

**`plans/2026-10-03-metadata-store-contract-p2-mem-and-conformance.md` / 2026-09-03 spec**
46. P2 Task 3: add the arity-correction line beside the 2026-09-03 design's stale table (5/8/5 tuples vs the landed 10-tuple `object_meta`), as P1's ownership note already delegates.

## Already applied in this round

- `Hooks` reads **two methods** everywhere (D27/§2/§8) — the pre-fix one-method copies are gone.
- **B6 (round 40)**: the `dyn` holding shape is gone — D6 says concrete handles + a generic engine, D35/E1/§1 spell `Ops<M, V, R, C, B, H>`, the master spec's §3/§4 name `Arc<RedbMetadata>`/`Arc<FsContent>` with the error mapped at the facade, P1's Goal/Architecture/constraints/shape-probe and P4 Task 1 Step 3 follow, and P2's handle is `&Arc<RedbMetadata>`.
- D35's version fields are **unconditional** (no `cfg`, no feature).
- The contract's held type is `Arc<dyn Metadata<Error = tinio_store_redb::Error>>` with each facade mapping at its boundary.
- "eleven **operations**" vs "eleven missing **calls**" are distinguished; "named steps" replaces "named tasks"; Verification 6 points at the enumerated form.
- D15 states the principle instead of pointing; a **provenance-vs-ownership rule** was added to the decision table ("a summary names its owner; the owner wins").
- D31/PG5 say the config contract **reserves** `type` (it is future work there); PG25 and the pg design's verification quote P3's **scoped** credential grep.
- The reversals paragraph and Non-goals say plainly that **nothing from the versioning set is in today's tree**; the rename count is ~22 files.

## Probe results (round 39, run after this record's fix list)

Two feasibility probes ran against a throwaway crate under `target/ops-probe/` (outside the workspace, untracked) and a `postgres:18-alpine` container. They exist because no review lane could compile or execute anything, and both found things the documents could not.

**Probe A — the contract's holding shape.** The shape the documents pin (`Arc<dyn Metadata<Error = E>>` with category-trait methods `where Self: Metadata`) **does not compile**: `Metadata` is not dyn compatible (E0038 — its sub-trait methods reference `Self` in their `where` clause and return type), and `dyn ObjectContent` also requires `Staged` to be named (E0191). The workspace's `Storage` uses the same method style safely only because nothing ever builds a `dyn Storage`. The **generic** shape — `Ops<M: Metadata, V: VersionMeta, R: VersionMeta, C: ObjectContent, B, H>` with a concrete backend store implementing all four — **compiles clean** (only dead-code warnings), including `Staged: StagedObject<Error = Self::Error>`, `advance_slot`, `current_slot`/`Current`, the widened `ContentEntry` and the two policy fields. → **B6**, and the fix is the workspace's own style.

**Probe B — the pg content model** (PostgreSQL 18.4, `postgres:18-alpine`, `server_encoding = UTF8`, database default collation `en_US.utf8`):

| Check | Result |
|---|---|
| A read at offset 2.5 MiB resolves through the row's live generation | hits `seq = 2` |
| After a 3 MiB → 1 MiB shrink-overwrite, the live generation serves the new bytes only | 1 row, first bytes `BB`; 3 residue rows unreferenced; **0 rows past the new size** |
| A partial publish (generation 3, 1 of 3 chunks, row not advanced) | **0 rows visible** to a row-keyed read — the row update is a working commit point |
| `kind = 'object'` excludes a `kind = 'part'` row | excluded |
| `ORDER BY key COLLATE "C"` | `data`, `data-2`, `data/`, `daß` — byte order, as the contract demands (the database's own `en_US.utf8` default would not) |
| `ALTER COLUMN chunk SET STORAGE EXTERNAL` | accepted |
| Session `default_transaction_read_only = on`, then `BEGIN; SET TRANSACTION READ WRITE;` + `UPDATE` | **the write succeeded** — the session default is not a guard |
| A role with `SELECT` only, attempting `UPDATE` | refused (`insufficient_privilege`) — the role is the real second guard |

Conclusions for the work list: PG18's generation model and PG4's explicit collation are **validated by execution, not argument**; PG6's "the session default is a second guard" is **false as stated** and the role requirement is load-bearing (the documents already say so — the probe is the evidence); PG19's completeness rule and PG22's bounded publish remain the spike's to pin, but the commit-point mechanism they need is proven.



## Rulings on this work list (round 40)

Taken after the probes, with the operator:

1. **B6 is fixed by generics** — `Ops<M, V, R, C, B, H>` is generic over concrete backend types; backends hold concrete handles (`Arc<RedbMetadata>`, `Arc<FsContent>`, `Arc<PgStore>`); **the contract method style (`where Self: Metadata`, `<Self as Metadata>::Error`) is unchanged**. Probe A2 proves the shape compiles, and it is the workspace's own style (`Storage` is never a `dyn`). Touch points: D6, D17/D26 (the ports' bounds), D35, E1/§1's instantiation examples, P1 Task 1's blanket sentence + Task 5's error pin, P2's handle spelling, P3/P5's pg instantiations, and every `dyn …` spelling in the nine documents.
2. **Order: B6 first**, then B1–B5, then the remaining 41 items (document order), then a per-item mapping table, then a **diff-only** third review.
3. **Progress lives in this record**, and the three specs' Verification sections gain one line pointing at it ("the set's unexecuted list is `reviews/2026-10-03-metadata-store-review-round2.md`; a ticked item has landed, an unticked one has not").
4. **Git: nothing is committed now.** When the pass lands, a two-commit plan is put to the operator for approval (① `reviews/` + the three specs; ② the five plans + the versioning plan 2 amendment), and the four staged `s3-versioning-*` files' index state is settled then.

**Execution note.** This pass is a ~46-item text refactor across ten documents, and rounds 1–2 showed that large rewrites introduce their own defects. Run it with room to be careful — a fresh context (or a delegated writer with this record as its brief) followed by the diff-only review is the cheap order; a half-applied pass in a nearly-full context is how B6/B1/B2 were introduced in the first place.

## Convergence

Round 1 found 60+ defects in the first drafts; round 2 found 68 in the *rewrite* — mostly of a different kind (incoherences the rewrite introduced, and second-order facts it left unpinned). Both rounds agree on the pattern: a large rewrite needs a re-read, and the cheapest convergence is to apply the list above in one pass and then review **only that diff**. This record is the list.
