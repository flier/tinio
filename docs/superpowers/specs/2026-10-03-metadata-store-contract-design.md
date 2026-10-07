# Design: the backend persistence contracts (`tinio-core`) + the redb reference implementation

**Date**: 2026-10-03
**Status**: draft — operator review. Every decision below was taken with the operator across rounds 0–38 on 2026-10-03 (each decision is tagged with its round). Rounds 28–38 are post-review: an independent six-lane audit of this document set (2026-10-03) found structural gaps, and its findings' rulings are folded in here.
**Scope**: the contract set in `tinio-core` (metadata rows, version rows, object content, plus the existing block store), the `tinio-store` → `tinio-store-redb` rename and the relocation of the redb transaction machinery into it, that crate's implementations of the metadata/version/content contracts, the re-pointing of `tinio-version`'s narrow ports onto the contracts, the migration of `tinio-fs` and `tinio-mem` onto the contracts' concrete handles with zero observable behavior change, and one PostgreSQL spike that validates the contracts before they are relied on (D21–D25). Not touched: the `Storage` (S3) contract and the server surface, the version state machine's behaviour (spec `2026-10-03-s3-versioning-design.md`), the sandbox crates, `tinio-auth`, `tinio-select`, and every line of *production* PostgreSQL code.

## Background

Derived metadata is one shared redb table layer (`tinio-store`) with two consumers; both reach *through* it into redb from every call site, closing over table handles inside `handle.read`/`handle.write`. redb lives in two places: the table layer (definitions, typed handles, scans, the redb error core, **and the `STATE` table with `FORMAT_VERSION`**, `crates/tinio-store/src/state.rs`) and `tinio-fs/src/database/` (the async driver with its blocking-pool hop and write-lock histograms, the open-time version gate, compaction evaluation).

The workspace already has the shape this design copies — `tinio_core::blob::BlockStore` is the contract, `tinio-blob` is the engine and the local implementation — and it now has the fact that justified deferring the work: **a second, non-redb backend is known** (`tinio-pg`, a peer of `tinio-fs` providing metadata, object content, blobs and versions over PostgreSQL).

**What the seam shares is semantics, not code bodies** (round 1). A second implementation writes its own method bodies against its own transport; what it inherits is the row model, the decode rules, the ordering and atomicity rules, the visibility rule — and the conformance suite that proves it.

**The engine is the strictest consumer** (round 38): every contract call below exists because an operation of the shared ops engine (`docs/superpowers/specs/2026-10-03-ops-engine-design.md`) needs it. The audit found **eleven `Storage` operations** the first draft's surface could not express; a call nobody needs is not in the surface.

## Goal

1. **Four contracts in one home.** `tinio-core` owns `meta` (rows + `Metadata`/`BucketMeta`/`ObjectMeta`/`UploadMeta` + `VersionMeta`), `content` (`ObjectContent`) and `blob` (`BlockStore`, already there). No redb type, table or key encoding appears in any of them.
2. **One redb implementation, one crate.** `tinio-store` is renamed `tinio-store-redb` and absorbs the driver, the open/version-gate machinery, compaction evaluation and the histograms; it implements `BucketMeta`, `ObjectMeta`, `UploadMeta`, `VersionMeta` and the redb byte-table `ObjectContent`.
3. **A decoupled version engine.** `tinio-version` holds the version state machine, born on narrow ports it declares itself; this design's plan set deletes those ports and re-points the engine onto the contracts, so `tinio-pg` inherits the same engine.
4. **Two migrating consumers, no observable change.** `tinio-fs` and `tinio-mem` hold the shared implementation's **concrete handle** (`Arc<RedbMetadata>`, plus their own content handle); the S3 surface, the error paths and text, the table names and arities as they stand after the versioning set, `FORMAT_VERSION` and the DB row shapes are unchanged; the fs suite takes import/await edits only, plus one committed pre-change database fixture (D43).
5. **A third implementation is a backend, not a research project.** `tinio-pg` (with `tinio-store-pg` as its persistence implementation) implements the four contracts plus `Storage`, and runs the same conformance suite.

## Non-goals

- **No production PostgreSQL code inside P1 and P2.** The pg work is the later phases of §8 (the spike is P1's gate); no pg crate ships before P3.
- **No pg-specific persistence contract.** Where PostgreSQL cannot satisfy a rule, that is a scope finding (the contract changes, not the schema).
- **No `Storage` redesign in P1/P2.** The S3 contract is untouched until the ops engine (P4) extracts its shared bodies — whose only acceptance is that same behavior.
- **No version-semantics rewrite.** The state machine's behaviour stays as `2026-10-03-s3-versioning-design.md` specifies; only its home and its dependencies change.
- **No error-model change.** Per-crate `Error` enums, the `From<Error> for storage::Error` projections, and the `Redb`/`DatabaseError` names survive.
- **No redb row-format change of our own.** The tables keep the names and arities the versioning set leaves (D7); that set's own `object_meta` tuple change arrives **with** it (it is not in today's tree — `crates/tinio-store/src/meta.rs` still carries the 8-tuple), so P1's first task re-baselines `meta::object::Row` against whatever that tree then holds.
- **No crate-root feature gates.** A feature exists only where it gates real code or an optional dependency (D36).

## Decisions

| # | Decision | Round |
|---|---|---|
| D1 | The seam is the **domain service trait**; a method is one atomic metadata operation and exposes neither a table nor a transaction | 0 |
| D2 | The contracts are **async** (`async-trait`); the transaction handle is the implementation's internal shape | 0 |
| D3 | The contracts live in **`tinio-core`** (`meta/`, `content.rs`, `blob.rs`) — one backend-contract crate, no redb dependency | 0, 8 |
| D4 | Scope: **metadata + content + version rows**, with the content plane as the existing `BlockStore` plus the new `ObjectContent` | 1, 6, 7 |
| D5 | The redb machinery moves into a crate named **`tinio-store-redb`, which absorbs `tinio-store`** (rename + extend); the `_store_redb` alias follows `docs/style.md`'s rule | 1, 2, 3 |
| D6 | Backends hold **concrete handles** to the contracts and the engine is **generic** over them (`Ops<RedbMetadata, …>`); a `dyn` aggregate is impossible — the category methods' `where Self: Metadata` clause and `<Self as Metadata>::Error` return make `Metadata` non-dyn-compatible (probe A, round 40; the workspace's `Storage` is never `dyn` either). No aggregation trait, no generic `FsStorage<M>` | 3, 8, 40 |
| D7 | Contract **rows carry decoded domain types**; `identity` is an opaque `u64`; the bucket/object/upload rows carry **no content reference** (the version rows carry their `BlobRef` **by design** — `VersionMeta`'s row is the version row type) | 3, 4, 38 |
| D8 | **`put_guarded` and `remove_if_empty` exist** (mem's TOCTOU-free delete); whether the engine uses `put` or `put_guarded` is a per-backend policy it declares (D40) | 3, 38 |
| D9 | The traversal rule is **two methods**: `walk` fails on a corrupt row, `walk_healing` heals it (both behaviours exist in the tree today — `for_bucket_strict` and `for_bucket_gated` — and the contract keeps both) | 4, 38 |
| D10 | The **method list is a working surface**; the lock is §1's properties plus the conformance suite | 4 |
| D11 | The conformance suite lives in **`tinio-util::testing`** (`testing.rs` becomes `testing/`); `tinio-store-redb`'s tests run the reference legs, `tinio-mem`'s the second consumer's | 5 |
| D12 | The implementation work is **two plans** — P1 contract + rename/driver + redb impl + fs; P2 mem + suite + cleanup | 5 |
| D13 | Acceptance is **zero observable behaviour change**; the enumerated internal changes are `load_bucket` becoming async (its `read_blocking` hop stays), mem's write order, the driver's relocation, and the fs `ObjectContent` implementation's shape | 5, 38 |
| D14 | Doc examples follow the **`Storage` precedent**: a trait-level `ignore`d example, doc comments on methods; the conformance suite is the behavioral proof | 5 |
| D15 | (superseded) `ObjectContent` is the **staged-publish seam**: its method list is D23's and nowhere else — this row records only the principle (a staged write, a visibility point, reads by key), and a reader implementing the surface follows D23 | 7, 38 |
| D16 | The version state machine moves to **`tinio-version`**; `2026-10-03-s3-versioning-p2-versioned-store.md` is amended in place | 7, 8 |
| D17 | `tinio-version` is **born against narrow ports it declares** (`VersionRows`, `CurrentRows`, `ContentPort`) with an fs adapter; this design's plan set deletes `VersionRows` and `ContentPort` and re-points the engine | 9, 15 |
| D18 | The future pg work is **`tinio-pg` + `tinio-store-pg`**, symmetric with `tinio-fs` + `tinio-store-redb` | 6 |
| D19 | Sequencing: the plan set executes **after versioning plan 3**; P1's first task re-baselines against the landed tree | 1, 2 |
| D20 | `VersionMeta` is a **standalone trait**, never a supertrait of `Metadata` | 4 |
| D21 | A **PostgreSQL spike is a hard gate** before the rest of P1: it implements the metadata contracts and `ObjectContent` over `postgres:18-alpine` (docker compose) and runs the conformance suite; if no PostgreSQL is reachable the work **stops** | 10, 11, 12 |
| D22 | A refusal is **visible as a projection**: `put_guarded` → `storage::Error`'s `NoSuchBucket`, `remove_if_empty` → `NoSuchBucket` / `BucketNotEmpty`, pinned by a conformance leg | 11 |
| D23 | **`ObjectContent`'s surface** (the single definer): `stage(bucket, key)`, `publish(bucket, staged, key)` (no return value), `open(bucket, key, range)`, `remove(bucket, key)`, `rename(bucket, from, to)`, `copy(bucket, from, to)`, `scan(bucket, prefix, after, limit)`, `stat(bucket, key)`, `create_bucket(bucket)`, `remove_bucket(bucket)`, `is_empty(bucket)`; keys are `ContentKey::{Object, Part { upload_id, number }}`. **Amended 2026-10-03 (rounds 28/38):** the original row excluded `scan`, the part key and any move primitive on the premise that no shared consumer reads part bytes or traverses content — the ops engine is that consumer, so those came back; the audit then showed the engine also needs `stat` (cold reads/listings and the supersede fingerprint), `copy` (fs's `copy_file_range` fast path), the bucket-scoped calls (fs creates a directory as the bucket, and a folder-marker directory is content), `is_empty` (a bucket's emptiness must see directories, not only object keys) and the `prefix` bound on `scan` (a prefix listing must not rescan the bucket) | 10, 28, 38 |
| D24 | In the spike, **shape problems are the executor's** (contract edit + suite leg + a finding) and **scope problems stop the work** | 12 |
| D25 | The spike lives at `spike/pg-meta/`, **excluded from the workspace and from CI** while P1 runs, and is **promoted** into `crates/tinio-store-pg` by P3 — it is the seed, not a discard | 11, 17, 19 |
| D26 | The engine's ports **mirror the future contracts**; port errors are `storage::Error`; `CurrentRows` has no contract counterpart (the F01/F04 gate is the backend's read-path rule) so it survives the swap as a backend port; the block plane needs no port (the engine holds `tinio_blob::Store<B>`), and the bucket's versioning state belongs to `VersionMeta` | 13, 14, 15 |
| D27 | A **shared ops engine** is a subsystem of this work line: crate `tinio-ops`, carrying the whole `Storage` surface as shared operation bodies over the contracts, with a **two-method `Hooks` trait** (`before_bucket_delete`, `lock_part`), two policy fields (D39/D40) and its operations returning `storage::Error`. It gets its own spec | 21, 22, 25, 38 |
| D28 | The spike is **promoted**, not discarded: P3 moves it to `crates/tinio-store-pg` (behind a `pg` feature) and completes it — the four contracts including `VersionMeta`, `PgBlocks`, and object content as chunked `bytea` | 19, 21, 23 |
| D29 | The phases: **P3** = `tinio-store-pg`; **P4** = the ops engine with `tinio-fs`/`tinio-mem` re-pointed onto it (zero observable change); **P5** = `tinio-pg` over the engine | 21, 22 |
| D30 | One **pg design document** (`docs/superpowers/specs/2026-10-03-pg-design.md`) owns the schema, connection, content and backend-surface models | 24 |
| D31 | pg configuration: a **`[storage.pg]`** section (`dsn`, `pool`) beside the existing `[storage.fs]`/`[storage.mem]` sections, and the backend selector is **`[storage] type`** — the key `specs/001-s3-local-server/contracts/config.md:152` **plans** for the second backend (P5 adds it to that contract and to the schema, which today carries `fs`/`mem` only) | 26, 33, 38 |
| D32 | pg read-only (FR-023): a session-level read-only connection **and** a role with no DML grants, mutating operations answering `AccessDenied`, no served-root or state-dir relocation concept | 26, 38 |
| D33 | pg implements `Cleanup` fully, and the `@pg` cucumber pass runs in the CI PostgreSQL job (serialized — one shared database) | 21, 26, 38 |
| D34 | **All five plans are written now**, each carrying a re-baseline step against the tree it lands on | 23, 24 |
| D35 | The engine's shape: `Ops<M, V, R, C, B, H>` — **generic over the four contracts' concrete types**, no `?Sized` and no `dyn` (probe A) — holds `Arc<M>` (the metadata implementation), `Arc<V>` (the `tinio-version` state engine) and `Arc<R>` (`VersionMeta`) — **the version group needs both handles** — plus `Arc<C>`, `tinio_blob::Store<B>`, `H` and the two policy fields; **the version fields are unconditional** (`tinio-ops` carries no feature, D36 — the group is always compiled); **the locks are the backend's**; `delete_bucket` decides emptiness content-side; once `before_bucket_delete` returns the operation has happened and later drain failures are logged, not returned; the engine owns the **whole** surface (it defines `StagedBody`, and owns listing through `scan` + batch enrichment + the core `UnorderedPager`) except `list_object_versions` (E14). Owner of the detail: `2026-10-03-ops-engine-design.md` §1 | 30, 31, 37, 38, 40 |
| D36 | **Feature level** (round 38 ruling): only where a feature gates real code or an optional dependency. `tinio-fs`/`tinio-mem` carry `versioning` (their version modules and the `VersionOps` twins) and `tinio-server` gates its operation rows; **`tinio-version` and `tinio-ops` carry no `versioning` feature at all** (they are always compiled; the rule that removed `tinio-blob`'s crate-root gate applies to them). `tinio-store-pg`/`tinio-pg` carry `pg` (optional dependencies + code) and a non-default `pg-tests` (the DB-backed tests); `pg` joins `crates/tinio`'s `full` set | 38 |
| D37 | The current slot — the key's current version id and sequence — is exposed by **`VersionMeta::current_slot(bucket, key) -> Result<Option<(version::Id, u64)>>`**, not by an extra column on `meta::object::Row` (version identity is version-domain state, like the bucket's versioning state) | 38 |
| D38 | **`ObjectContent::stage` takes the target key** (`stage(bucket, key)`), and `StagedObject::write` returns the **contract's `Error`** — a chunked store must be able to insert staged chunks before `publish`, and a typed refusal (mem's `EntityTooLarge`) must reach the wire | 38 |
| D39 | **`Ops.metadata_failure: MetadataFailure::{Warn, Fail}`** — the policy for a metadata-row write that fails after the content is published: `Warn` for fs (the object is committed; the row is recomputed on the next read, FR-022), `Fail` for mem/pg (no row means no object; the content is left to `Cleanup`/compensation) | 38 |
| D40 | **`Ops.bucket_row: BucketRow::{Guaranteed, Lazy}`** — whether the engine writes object rows with `put_guarded` (mem/pg: the bucket row must exist, preserving the TOCTOU-free delete) or with `put` (fs: bucket rows are recorded lazily) | 38 |
| D41 | **The pre-change database fixture** (`crates/tinio-store-redb/tests/fixtures/pre-change-meta.redb` + a test that opens it) is committed by P1 — the claim "a database written before the change still opens" needs a real artifact, since the test target the plans used to cite no longer exists | 38 |
| D42 | **`ContentEntry` is `{ key, len }`** — no timestamp: a listing's timestamps come from the metadata rows, and a content store that has none (mem's byte table) must not fabricate one | 38 |
| D43 | **The suite's legs are owned** (round 38): the 8 MiB staged leg (64 KiB chunks, full + ranged read) and the corrupt-row traversal legs (raw-access, per consumer) are **named steps** in P1 Task 1/P1 Task 6 and P2 Task 2, and the pg backend's `>100 MiB` streaming leg is a named step in P5 — an unowned leg is a leg nobody writes | 38 |
| D44 | **`list_object_versions` stays backend-side** (fs's and pg's own modules): it needs the backend's "what is the current object" truth plus the F01/F04 gate, which `Metadata::get` deliberately does not apply. The engine's version group is therefore **nine** operations, and the `VersionOps` bodies the engine owns are the version-addressed reads/writes/deletes plus the versioning-state calls | 38 |

Two earlier rulings are **reversed, deliberately and on the record**: the 2026-10-03 versioning ruling that the seam's machinery crate waits for a second backend (the second backend is known, so this plan set creates `tinio-store-redb` — by renaming `tinio-store` — and the versioning set creates `tinio-version`; **neither exists in today's tree**, and both are targets of the phases that follow), and the same ruling's placement of the version state machine inside `tinio-fs` (it is `tinio-version`). A third reversal is `tinio-blob`'s removed crate-root gate — applied to `tinio-version`/`tinio-ops` (D36).

**Provenance vs ownership in this table.** A row whose subject a satellite spec owns names that owner (e.g. D35's "Owner of the detail") — the row records the ruling and its round, the satellite owns the contract. Where a summary and its owner disagree, **the owner wins**; a summary is corrected here, never the reverse.

## Design

### §1 The contract set

`crates/tinio-core/src/`: `meta/` (`mod.rs`, `bucket.rs`, `object.rs`, `upload.rs`, `version.rs`), `content.rs`, `blob.rs` (existing).

```rust
pub trait Metadata: Send + Sync + 'static + BucketMeta + ObjectMeta + UploadMeta {
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
}

#[async_trait]
pub trait VersionMeta: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
    // bucket_state / set_bucket_state
    // current_slot(bucket, key) -> Option<(version::Id, u64)>          (D37)
    // put_version / get_version / list_versions / expire / index lookup
}

#[async_trait]
pub trait ObjectContent: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
    type Staged: StagedObject;
    // stage(bucket, key) -> Staged                                     (D38)
    // publish(bucket, staged, key) -> Result<()>
    // open(bucket, key, range) -> BodyStream
    // remove(bucket, key) / rename(bucket, from, to) / copy(bucket, from, to)
    // scan(bucket, prefix, after, limit) -> Vec<ContentEntry>          (D23)
    // stat(bucket, key) -> Option<ContentEntry>                        (D23)
    // create_bucket(bucket) / remove_bucket(bucket) / is_empty(bucket) (D23)
}
```

`Metadata`'s three category traits carry the operation surface (the method block is pinned by **P1 Task 1**, which is the plan that creates it); `ObjectMeta` exposes **two traversals** (D9) — `walk` (a corrupt row fails the traversal) and `walk_healing` (the row is healed, the traversal never fails) — because both behaviours are live in the tree today. `VersionMeta` is a **standalone trait** (D20).

**Rows** (decoded, redb-free): `meta::bucket::Row`, `meta::object::Row` (etag, size, mtime, `identity: u64`, tags, checksum, owner, acl — **no version columns**, D37), `meta::upload::{Row, PartRow, PartRecord}`, and `meta::object::PartRow` for the **retained part list** (`OBJECT_PARTS`) with the two calls that read and write it atomically beside the object row (`parts(bucket, key)`, `put_with_parts(bucket, key, row, parts)`), plus `ObjectMeta::move_(bucket, from, to)` for fs's atomic rename (row + retained parts in one transaction). `ContentEntry` is `{ key, len }` (D42). Existing core types are reused rather than re-declared (`multipart::{MultipartUpload, ObjectPart, PartNumber}`).

**Properties the contracts carry** — each a rule a second implementation must satisfy and the conformance suite pins:

1. **Atomicity per method.** `Ok` means every row of the operation committed; `Err` means none did. A `put_with_parts` writes the object row and its retained parts together; `move_` moves the row and its parts together.
2. **Ordering.** Ascending by the key's UTF-8 byte sequence (a SQL implementation pins `COLLATE "C"` **and** verifies the database encoding is UTF-8); part numbers order numerically.
3. **Prefix boundaries.** A per-bucket traversal never yields another bucket's row; `scan`'s `prefix` is a lower bound the implementation may prune with, and `scan` yields **object keys only** (part keys are never enumerated — the content implementation must be able to exclude them, D23).
4. **The self-heal asymmetry, both halves** (D9): a point read treats a corrupt row as absent and heals its elements; `walk` fails on a corrupt row; `walk_healing` never fails and heals.
5. **Idempotence.** Drains, clears, removals and `remove_bucket` are no-ops on absent state and report success.
6. **The visibility point.** `ObjectContent::publish` returns only when the bytes are visible; the metadata row describing them is written **after** it. **Nothing observes a half-published object** — an implementation whose publish is not atomic (a chunked store) must make its partial state invisible (a generation/base marker, all-or-nothing rows) and re-publishable, and this is the pg store's own model (pg design §5).
7. **Error projection.** Each contract's `Error` converts into `storage::Error`; the per-crate enums and their lift rules are unchanged.
8. **Refusals are visible.** A `put_guarded` refusal projects to `storage::Error`'s `NoSuchBucket` and `remove_if_empty`'s two outcomes to `NoSuchBucket` / `BucketNotEmpty`.
9. **Buckets are content-side too.** `create_bucket`/`remove_bucket` are the content plane's bucket lifecycle (fs creates/moves a directory; a store without directories makes them no-ops or row ops), and `is_empty` sees **everything the backend counts as bucket content** — for fs that includes folder-marker directories, which carry no metadata row.

### §2 Crate topology

| Crate | Owns |
|---|---|
| `tinio-core` | the four contracts, the row types, the wire decoders, `Storage` and the domain newtypes |
| `tinio-store-redb` | today's `tinio-store` (table definitions, codecs, per-table handles, scan helpers, the redb error core, `state`/`FORMAT_VERSION`) **plus** the relocated driver (async read/write with the blocking-pool hop, `write_if`, `WriteLockSnapshot` and the histograms), the open-time version gate, compaction evaluation, and the implementations of `BucketMeta`, `ObjectMeta`, `UploadMeta`, `VersionMeta` and the redb byte-table `ObjectContent` |
| `tinio-version` | the version state machine, born on three narrow ports (D17/D26); **no feature of its own** (D36) |
| `tinio-ops` | the shared operation bodies and the **two-method** `Hooks` + two policies; **no feature of its own** (D36) |
| `tinio-fs` | path/layout/policy, the file `ObjectContent` implementation (including the directory-walk window `scan` uses and the F01/F04 gate `CurrentRows` applies), the atomic writer, tombstones, scanner, sweep, cleanup, the fs facades, and the engine adapter |
| `tinio-mem` | the in-memory `Storage` implementation over `tinio-store-redb`'s redb byte tables, the shared metadata implementation, and its `CountingContent` decorator |
| *(future)* `tinio-pg` + `tinio-store-pg` | the Storage implementation + the PostgreSQL implementations of the four contracts (`pg` feature, `pg-tests` for the DB-backed tests, `pg` in `tinio`'s `full` set, D36) |

`tinio-fs` keeps re-exporting `WriteLockSnapshot` and `Driver` so the metrics consumer (`crates/tinio-cli/src/commands/start/metrics.rs`) and `tinio-server` need no edit. The rename is mechanical: `tinio_store` → `tinio_store_redb`, alias `_store` → `_store_redb` per `docs/style.md`; the crate-path occurrences in `docs/style.md`'s error rule are updated with the alias rule.

### §3 `tinio-fs`

- The facades (`bucket::Store`, `meta::Store`, `multipart::Store`) keep their names, signatures and visibility; their bodies delegate to the shared implementation's **concrete handle** (`Arc<RedbMetadata>`) and **map `tinio_store_redb::Error` into the crate's own `Error`** at their boundary (fs: `Error::Redb(e.into())`; mem: its `DatabaseError` alias, which *is* that type); the crate-level lift is unchanged.
- `ObjectContent` for fs implements **the whole D23 surface** over the existing atomic writer, file layout and directory walk: `stage` = the temp file (the parent-symlink policy check and the folder-marker case live here), `publish` = the rename (the unix directory hardening included), `open` = the range reader, `remove` = the unlink, `rename` = the file move, `copy` = `copy_file_range` where available, `scan` = the directory-walk window with the prefix pruning and the reserved-path filters, `stat` = the key's file metadata, `create_bucket`/`remove_bucket`/`is_empty` = the directory operations — including the **folder-marker directories** `is_empty` must see.
- **The walk stays one source of truth**: `scan` delegates to the same streaming walk the scanner uses (the batch accumulator stays in fs), so `scan` keeps its bounded memory and the scanner keeps its per-file path/identity feed; the engine's listing therefore needs no fs-specific walk.
- `load_bucket` becomes `async` **and keeps the `read_blocking` hop** (a full-bucket traversal never runs inline on a runtime worker); its callers await.
- The metadata-row write policy is `Warn` (D39): a row write that fails after the rename is logged, the PUT still succeeds, and the row is recomputed on the next read.
- `FsStorage` holds its concrete handles (`Arc<RedbMetadata>`, `Arc<FsContent>`) beside the `Driver` clone; `write_lock_stats`, compaction evaluation and the doctor surface keep their call sites.

### §4 `tinio-mem`

- Metadata and version rows go through the shared `tinio-store-redb` implementations; object and part bytes go through `RedbContent` wrapped in mem's `CountingContent` decorator (D38's typed refusals included).
- The decorator accounts **by delta**: `publish` reads the key's current length and adds only the difference, subtracts on `remove`/`rename`, and refuses `max_object_bytes`/`max_total_bytes` at the point the bytes are accepted; a compensation that itself fails logs and subtracts anyway (the bytes are unreachable by key).
- Write order is bytes first, metadata row last, with `bucket_row = Guaranteed` (D40) so a concurrent `delete_bucket` cannot be raced; a rejected guarded write removes what it staged.
- `delete_bucket` keeps its TOCTOU-free property through `remove_if_empty`; `MemoryOptions` accounting, `MemoryCleanup` (a documented no-op — in-memory state dies with the process) and the ops modules stay mem-side.

### §5 The version engine and plan 2

`2026-10-03-s3-versioning-p2-versioned-store.md` is amended in place (D16): its Goal and Architecture name `tinio-version`, its Depends-on names this document, its state-machine task writes the engine against `VersionRows`/`CurrentRows`/`ContentPort` plus the blob engine and the fs adapter, and `list_object_versions` stays fs-side (D44). Its behaviour, its version tables and its protocol are unchanged; the engine's API takes content keys, never `&Path`.

This plan set then deletes `VersionRows` and `ContentPort` (their call sites become `VersionMeta`/`ObjectContent` with one `.map_err(Into::into)` each, plus the content-side calls the audit added), and `CurrentRows` remains as the backend's gated current view (D26). The engine's logic does not change in that step — that is the property the port discipline buys.

### §5a The PostgreSQL spike gate

The contracts are derived from two same-source consumers (both redb) and would otherwise first meet a different transport years later. P1 therefore stops after the contract and its conformance suite exist, and the spike proves them: `spike/pg-meta/` (excluded from the workspace, never in CI) implements the metadata contracts, `VersionMeta` and **the whole D23 content surface** over `postgres:18-alpine` (a `docker compose` file ships beside it) and runs the suite. The gate is **hard** (D21): no reachable PostgreSQL means the work stops and asks. Shape findings are fixed in-task with their suite leg and recorded; scope findings stop the work for a ruling (D24). The spike's schema is the seed for `tinio-store-pg` and its verdict is written to `docs/superpowers/pg-meta-spike-findings.md`; the spike is promoted by P3 (D25/D28) and its gate is one command (`cargo run --manifest-path spike/pg-meta/Cargo.toml`, exit `0`).

### §6 Testing and acceptance

- **A contract conformance suite** in `crates/tinio-util/src/testing/metadata.rs`: public entry points (the spike is a consumer), driving the contracts' own methods only. It pins §1's properties — byte ordering (an adversarial pair a locale collation would order differently), per-bucket boundaries, numeric part order, both traversal rules, per-method atomicity (a failing multi-row method leaves nothing), drain idempotence, batch input order, the `put_guarded`/`remove_if_empty` refusals and their projections, `publish` visibility, `open` range semantics, `rename`/`copy`/`stat`/`is_empty`/`create_bucket`/`remove_bucket`, the part key shape, and the **8 MiB staged leg** (64 KiB chunks, full + ranged read — D43).
- **The suite plants no state**: rules that need a pre-existing corrupt row are pinned by whichever layer has raw access (`tinio-store-redb`'s `contract.rs` for redb, the spike's raw-SQL leg for pg, mem's tables) — and those legs are **named tasks** in P1/P2 (D43).
- **The pre-change fixture** (D41) is P1's: `crates/tinio-store-redb/tests/fixtures/pre-change-meta.redb` plus a test that opens it and reads a row through the public implementations. `FORMAT_VERSION` stays `1` and the shared tables keep the arities the versioning set left.
- **Acceptance is zero observable behaviour change** (D13) for fs/mem: their suites, `assert_conformance`, both cucumber passes, plus a **listing benchmark** and the existing fs benchmarks, which must not regress beyond noise (constitution V) — the listing path is rewritten by P4, so it needs a benchmark of its own.
- **Gates**: `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check` per feature-bearing crate, the cucumber passes.

### §7 Sequencing

The plan set executes **after versioning plan 3** (D19). P1's first task re-baselines it against the landed tree: the version tables and their row shapes, the `object_meta` value tuple as that tree left it, mem's content tables, and `tinio-version`'s ports as actually written.

### §8 Roadmap (P3–P5)

| Phase | Delivers | Depends on | Acceptance |
|---|---|---|---|
| **P3** — `tinio-store-pg` | the promoted spike as a workspace member behind `pg`/`pg-tests`: the four contracts, schema versioning and migrations, the pool and TLS, the pg design document §1–§5 | P1 (the contracts, the suite, the spike's findings) | the conformance suite green from the crate's own tests against `postgres:18-alpine`; `cargo test --workspace` unaffected |
| **P4** — `tinio-ops` | the whole `Storage` surface as shared operation bodies (44 operations: `list_object_versions` stays backend-side, D44), the **two-method** `Hooks`, the two policies, and `tinio-fs`/`tinio-mem` re-pointed onto it | P2 (both backends on the contracts) | zero observable change: the fs/mem suites, `assert_conformance`, both cucumber passes and the listing benchmark green |
| **P5** — `tinio-pg` | the `Storage`/`VersionOps`/`Cleanup` implementation over `tinio-ops` and `tinio-store-pg`, the `[storage] type`/`[storage.pg]` configuration, TLS, the read-only mode with the restricted role, the writable-ownership guard, doctor checks, the `@pg` pass (serialized) and the CI PostgreSQL job | P3 + P4 + the pg design document §6 | `assert_conformance` and the `@pg` pass green in CI; `@fs`/`@mem` untouched |

Each phase's plan carries a re-baseline step against the tree it lands on (as P1's Task 1 does). The engine's spec and the pg design document own their phases' models; this document owns the contracts, the decisions and this roadmap.

## Risks

| Risk | Mitigation |
|---|---|
| The grown contract surface (`ObjectContent`'s eleven calls, `ObjectMeta`'s parts/move) is bigger than the two backends need | Every call exists because an operation of the engine needs it (D23's reason column); a call nobody needs is removed rather than kept "for symmetry", and the pg spike exercises the whole surface before it freezes (D21) |
| The engine's acceptance cannot see a rewritten listing's cost | The listing benchmark and the `>100 MiB` leg are named tasks (D43), and P4's acceptance names the benchmark explicitly (§6) |
| A feature gate reappears on a library crate | D36 records the ruling and the reason (a gate whose off-state is an empty crate is not a gate) |
| `tinio-fs`'s cold-read/list/rename/copy behaviours are subtle enough to be lost in the move | Each has a named contract call (D23) and an existing test (the `@cold-listing` family, the rename/parts tests, the copy fast path) that P4 must keep green |
| The rename touches ~33 `_store::` sites across ~22 files | Mechanical, one task, suites green at its end (P1 Task 3) |
| A future pg implementation finds a missing operation | The method list is a working surface (D10); the spike is the falsifier (D24) |

## Out of scope (recorded for later)

- The PostgreSQL phases: everything is in §8's roadmap and the pg design document — no production pg code exists before P3/P5.
- Lifting the version state machine further (it is shared, not yet generic over transports).
- Any change to the live-object layout of `tinio-fs`, or routing its objects through `BlockStore`.
- The S3 surface, the capability gate, the server pipeline, the sandbox crates, `tinio-auth`, `tinio-select`.

## Verification

1. `cargo test -p tinio-core -p tinio-store-redb -p tinio-version -p tinio-ops -p tinio-fs -p tinio-mem -p tinio-util` green.
2. The §5a gate: `cargo run --manifest-path spike/pg-meta/Cargo.toml` against `postgres:18-alpine` exits `0` (or the work has stopped for a ruling), and `docs/superpowers/pg-meta-spike-findings.md` records the verdict, every shape finding, the walkthrough for the version rows, the `COLLATE "C"`/encoding pin, and the raw-SQL self-heal result.
3. `assert_metadata_conformance` (and the version/content legs, the 8 MiB leg, both traversal legs) run from `tinio-store-redb`'s and `tinio-mem`'s tests and pass; the pre-change fixture opens (D41).
4. `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` clean.
5. Cucumber `@fs` and `@mem` passes green; `cargo hack check` per feature-bearing crate clean (no crate-root gates, D36).
6. `rg 'redb::' crates/tinio-core/src` returns nothing; `rg '_store_redb::' crates/tinio-fs/src crates/tinio-mem/src` returns nothing outside the contract implementations (the module list is **enumerated** in P1 Task 9 Step 4 — the prefix alone would miss a module whose name does not start with `version`, e.g. `bucket_versioning`).
7. `tinio-version` and `tinio-ops` carry no `versioning` feature and no backend dependency; the fs/mem suites and both cucumber passes stay green through P4 (including the listing benchmark, within noise).
8. Per the roadmap: P3's suite green from `tinio-store-pg`'s tests (a single `PgStore::connect`/`migrate` surface, the TLS modes, the ownership guard), P4's zero-change acceptance on both migrated backends, P5's `assert_conformance` + serialized `@pg` pass green in the CI PostgreSQL job.
