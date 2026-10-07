# Design: the shared ops engine (`tinio-ops`)

**Date**: 2026-10-03
**Status**: draft — operator review. Decisions taken with the operator on 2026-10-03 (rounds 21–38 of the metadata-store contract design's question set; each is tagged). Round 38 is post-audit: an independent six-lane review found that the engine could not be written against the contracts as they stood, and its rulings — eleven added contract calls, two policy fields, a second `Hooks` method and the read-heal's home — are folded in.
**Scope**: a new workspace crate `tinio-ops` holding the S3 storage semantics shared by the backends — the operation bodies behind the `tinio-core` `Storage` categories — the `Hooks` seam a backend implements for the steps only it can take, the two per-backend policies, and the migration of `tinio-fs` and `tinio-mem` onto it with zero observable behavior change. Not touched: the `Storage` contract and its signatures, the persistence contracts (`tinio-core::meta`/`content`/`blob`), the version state machine (`tinio-version`), `list_object_versions` (backend-side, E14), the server and CLI surfaces, the sandbox crates. `tinio-pg` is the engine's third consumer and is designed separately.

## Background

Three backends are now in view — `tinio-fs`, `tinio-mem` and `tinio-pg` — and each implements the same `Storage` categories over its own persistence. Today that means the same operations are written twice (~6 200 lines under `crates/tinio-fs/src/backend/`, ~3 200 lines in `tinio-mem`'s modules) and would be written a third time. The persistence contracts remove the *storage* duplication; they do not remove the *semantics* duplication: how a `put_object` sequences a staged write, a publish, the metadata row and the version supersede; how a conditional write is evaluated; how a listing page is assembled; how a copy differs from a read-then-write.

`tinio-ops` is that second extraction. It is deliberately **not** a `Storage` implementation and **not** a framework: it is a set of operation bodies over the contracts that each backend calls from its own `Storage` implementation, plus a two-method `Hooks` seam and two policy fields.

**The audit's finding** (round 38) is the reason this spec is longer than the first draft: the phrase "the whole surface over the four contracts" was false for eleven operations. Each needed a contract call the persistence spec did not have (`stat` for cold reads and listings, `copy` for fs's zero-copy fast path, the bucket-scoped content calls, `is_empty` that sees folder-marker directories, a `prefix`-bounded `scan`, the metadata `move_` with retained parts, the retained-parts row) — those calls are now in `2026-10-03-metadata-store-contract-design.md` D23/§1, and this document assumes them.

## Goal

1. **One home for the shared operation bodies.** 44 of the 45 `Storage` operations — everything except `list_object_versions` (E14) — are written once over `Metadata`, `VersionMeta`, `ObjectContent`, `BlockStore` and the version engine.
2. **A small, honest seam for the rest.** `Hooks` has two methods: the bucket-delete action and the part-write lock guard.
3. **Two per-backend policies, declared not inferred**: what a failed metadata-row write means (E11) and whether object rows require a bucket row (E12).
4. **Backends keep their own surface.** `tinio-fs`, `tinio-mem` and `tinio-pg` keep their `Storage`, `VersionOps` and `Cleanup` implementations, their error enums and their crate APIs.
5. **Zero observable change in P4.** The migration of `tinio-fs` and `tinio-mem` onto the engine changes no S3 behavior, and the rewritten listing path is covered by a benchmark of its own.

## Non-goals

- **No `Storage` signature change.** The engine fills in the categories; it does not reshape them.
- **No `Cleanup` extraction.** The repair pipeline is backend policy over backend storage.
- **No persistence abstraction.** The engine knows the four contracts only; it never sees a table, a path or SQL.
- **No `Hooks` growth beyond two methods.** A third needs a named consumer, a spec amendment and the operator's ruling.
- **No version state machine move.** The engine calls `tinio-version`; it does not absorb it.
- **No `list_object_versions`.** It needs the backend's "what is the current object" truth plus the F01/F04 gate, which `Metadata::get` deliberately does not apply (E14).
- **No behavior redesign.** Where fs and mem disagree today on an observable detail, the migration preserves each backend's behavior; the disagreement is filed, not silently unified.

## Decisions

| # | Decision | Round |
|---|---|---|
| E1 | The engine is a crate, **`tinio-ops`**, depending on `tinio-core`, `tinio-version` and `tinio-blob` — and on **no feature of its own** (the version group is always compiled, since `tinio-version` always is; the removed `tinio-blob` gate's rule applies). It is **generic over the backends' concrete contract types**, never `dyn` (probe A, round 40) | 25, 38, 40 |
| E2 | It carries **44 of the 45** `Storage` operations (12 bucket + 15 object + 8 multipart + 9 version); `list_object_versions` stays backend-side | 25, 38 |
| E3 | Its operations return **`storage::Error`**; a backend's own variants are produced around the call | 25 |
| E4 | The seam is `Hooks` with **two methods**: `before_bucket_delete` (the bucket-delete action) and `lock_part(bucket, upload_id) -> Box<dyn Guard>` (a guard held across a part's commit phase only) | 25, 31, 38 |
| E5 | Backends keep their `Storage`/`VersionOps`/`Cleanup` implementations; the engine is called from them | 22 |
| E6 | The engine is P4 of the metadata-store plan set | 21, 22 |
| E7 | The engine's tests run on the **in-memory reference implementation** (`RedbMetadata::in_memory()` + `RedbContent`) with one in-memory `BlockStore` double; `tinio-store-redb` is a **dev-dependency** | 32 |
| E8 | The engine defines the **staged body** (`tinio_ops::StagedBody`), so `stage_body`/`commit_object` are engine-owned; both backends spell `ObjectOps::StagedBody = tinio_ops::StagedBody` | 37 |
| E9 | The engine owns **listing** (content scan → batch row enrichment → the core `UnorderedPager`), with fs's directory-walk window behind its `scan` and the scanner's batch accumulator left fs-side | 37 |
| E10 | (**superseded by E1**: `tinio-ops` carries no `versioning` feature — the version group is always compiled) | 37, 38 |
| E11 | **`Ops.metadata_failure: MetadataFailure::{Warn, Fail}`**: what a metadata-row write that fails after the content is published means. fs = `Warn` (the object is committed; the row is recomputed on the next read), mem = `Fail` (the caller compensates), pg = `Fail` (the content is left to `Cleanup`) | 38 |
| E12 | **`Ops.bucket_row: BucketRow::{Guaranteed, Lazy}`**: whether object rows go through `put_guarded` (mem/pg — the bucket row must exist, preserving the TOCTOU-free delete) or `put` (fs — bucket rows are recorded lazily) | 38 |
| E13 | **The read-heal is the engine's**: a read or listing whose row is absent or stale against `ObjectContent::stat` recomputes the ETag from the content (`open`) and persists the row (`ObjectMeta::put`), including fs's composed-ETag keep rule when the stored etag is composed and the platform identity is unavailable — the fs `ensure_row`/cold-listing behaviour, now shared and reachable through contract calls only. The mtime-jitter constant that rule needs moves to `tinio-ops` | 38 |
| E14 | The version group is **nine** operations over **two handles** — the `tinio-version` state engine and `VersionMeta` — and `list_object_versions` is the backend's | 38 |
| E15 | The per-upload part lock is held by the engine through `Hooks::lock_part`'s guard across the **commit phase only** (publish + part row), so a slow body stream does not block another part write of the same upload | 38 |

## Design

### §1 Shape

```rust
/// The two steps the engine cannot take itself.
#[async_trait]
pub trait Hooks: Send + Sync + 'static {
    /// The backend's action before a bucket's row and metadata are removed
    /// (fs: the tombstone move; mem and pg: nothing).
    async fn before_bucket_delete(&self, bucket: &bucket::Name) -> Result<(), storage::Error>;
    /// A guard held across one part's commit phase (fs: the per-upload
    /// part lock; mem and pg: a no-op guard).
    async fn lock_part(&self, bucket: &bucket::Name, upload_id: &str) -> Box<dyn Guard>;
}

/// What an operation body needs: the contracts, the version handles, the
/// blob engine, the hooks and the policies.
pub struct Ops<M, V, R, C, B, H> {
    pub meta: Arc<M>,          // Metadata
    pub versions: Arc<V>,      // tinio-version's state engine
    pub version_rows: Arc<R>,  // VersionMeta (E14)
    pub content: Arc<C>,       // ObjectContent
    pub blobs: tinio_blob::Store<B>,
    pub hooks: H,
    pub metadata_failure: MetadataFailure, // E11
    pub bucket_row: BucketRow,             // E12
}

/// A body staged by `stage_body` for a later `commit_object`.
pub struct StagedBody { /* ObjectContent::Staged + key, tags, len, the running hash */ }
```

Operation bodies are methods on `Ops` (the struct is the receiver), e.g. `ops.put_object(bucket, key, body, params) -> Result<PutObjectResult, storage::Error>`. They take a `Storage` method's parameters and return its success type, minus `&self` — the `tinio-core` category signatures are the contract for every op's shape. **The six parameters are concrete types**, e.g. `Ops<RedbMetadata, Versions<…>, RedbMetadata, FsContent, LocalBlocks, FsHooks>`; a `dyn` form is impossible by construction — the category methods carry a `where Self: Metadata` clause and return `<Self as Metadata>::Error`, which makes `Metadata` non-dyn-compatible (probe A, round 40: E0038, plus E0191 for `ObjectContent::Staged`). This is the workspace's own style: `Storage` is never used as `dyn` either, so the same method pattern is safe here.

**What stays with the backend** (E5): the `Storage`/`VersionOps`/`Cleanup` trait impls; every backend-specific error variant and its projection; the `Hooks` implementation; **the locks** — the bucket-mutation lock acquired by the trait impl around the engine call, the part lock through `Hooks::lock_part` (E15); `tinio-fs`'s path mapping, atomic writer, tombstones, scanner, sweep, the streaming batch accumulator it shares with the scanner, and `list_object_versions`; `tinio-mem`'s byte limits (in its `CountingContent` decorator) and content tables; `tinio-pg`'s SQL, `list_object_versions` and `Cleanup`.

### §2 The operation bodies

| Group | Operations |
|---|---|
| bucket (12) | `create_bucket`, `delete_bucket`, `head_bucket`, `list_buckets`, the tag/CORS/ACL reads and writes |
| object (15) | `put_object`, `stage_body`, `commit_object`, `get_object`, `copy_object`, `rename_object`, `head_object`, `delete_object`, the tag/ACL pairs, `list_objects`, `list_object_parts` |
| multipart (8) | `create_multipart_upload`, `get_multipart_upload`, `upload_part`, `copy_part`, `list_parts`, `complete_multipart_upload`, `abort_multipart_upload`, `list_multipart_uploads` |
| version (9) | the versioning-state reads/writes, the version-addressed reads and deletes, the version tag/ACL pairs (`list_object_versions` is the backend's, E14) |

The engine's obligations, each a rule the migration must not lose:

1. **Supersede before publish, row last**: when a write creates a version, the outgoing content is moved into the block plane **before** the content publish overwrites it; then the content is published, then the metadata row is written, then the current slot advances. (The first draft had the order reversed — the audit's blocker: publishing first destroys the bytes the supersede needs.)
2. **One contract call per atomic step**; the engine never decomposes a contract method's atomicity into several calls. The object row and its retained parts are written by `put_with_parts`; a rename moves them by `move_`.
3. **Conditionals and ranges** are evaluated by the engine over the rows and content (`If-Match`/`If-None-Match`, byte ranges, `list_object_parts`).
4. **Streaming** (constitution V): streams pass through; the engine buffers only where the S3 semantics require it.
5. **Feature boundary**: the version group is always compiled (E1); a backend without versioning refuses through its own `#[cfg]` twin, so the engine never answers for it.
6. **Listings are content-side, enriched in one batch**: `list_objects` enumerates keys through `ObjectContent::scan(bucket, prefix, after, limit)` (the prefix bound is what keeps a prefixed page from rescanning the bucket), enriches them through **`ObjectMeta::load`** — one call per page, never a point read per key — and paginates with `tinio_core::storage::UnorderedPager`. A key whose row is absent or stale is **healed** (E13) — that is what preserves fs's cold-listing behavior, where a file the scanner has not recorded still appears with its content ETag.
7. **Reads heal, they do not refuse**: `get_object`/`head_object` compare the row against `ObjectContent::stat`; an absent or stale row is recomputed from the content and persisted (E13), so a hand-dropped file keeps being served. A conditional write never heals: it compares against the row it reads.
8. **Moves stay moves, copies stay copies**: `rename_object` calls `ObjectContent::rename` **and** `ObjectMeta::move_` (one contract call each, so the bytes and the metadata move together); `copy_object` calls `ObjectContent::copy` (fs's `copy_file_range` fast path) and writes the destination row.
9. **Buckets are content-side**: `create_bucket` calls `ObjectContent::create_bucket` then `BucketMeta::put`; `delete_bucket` checks emptiness with **`ObjectContent::is_empty` + `UploadMeta::has_uploads`** (a folder-marker directory is content, so a key-only scan would wrongly report empty), then runs `before_bucket_delete`, then drains and removes.
10. **The destructive step is the commit point**: once `before_bucket_delete` returns, the operation has happened — the drains and the row removal that follow are best-effort (a failure is logged, the operation still answers success), exactly as fs behaves today.
11. **Multipart bytes cross the content seam** through `ContentKey::Part`; the completion assembles through the content plane (read the parts, publish the object) unless the implementation's own layout offers a cheaper path, and its transaction/visibility rule is stated per backend (the pg design owns pg's).
12. **The staged body is the engine's** (E8), and `stage` is keyed (the persistence spec's D38) so a chunked store can insert before publish.
13. **The part lock spans the commit phase only** (E15): the engine streams the body into the staged value, takes `Hooks::lock_part`'s guard, publishes, writes the part row, drops the guard.
14. **The policies are read, not inferred**: a metadata-row failure follows `metadata_failure` (E11), and an object row is written guarded or not per `bucket_row` (E12).

### §3 Acceptance and testing

- **P4's acceptance is zero observable change** for `tinio-fs` and `tinio-mem`: their suites, `assert_conformance` (both legs), the `@fs` and `@mem` cucumber passes, the fs benchmarks **and a new listing benchmark** (paginated full listing over 10⁴–10⁶ entries) pass with adapter-only edits — the listing path is rewritten here, so it needs a cost pin of its own (round 38, finding on benchmarks).
- **The engine's own tests** run per operation group on the in-memory reference base (E7) plus the port-level tests for the version group.
- **`tinio-pg` is the engine's third consumer** (P5) and what validates the `Hooks` seam beyond two points; a third hook needs the operator's ruling (a non-goal).
- **Gates**: the workspace gates plus `cargo hack check --package tinio-ops --each-feature` (which, with no feature, is the default graph) and both cucumber passes.

## Risks

| Risk | Mitigation |
|---|---|
| The extraction changes fs behavior somewhere the suites do not pin | P4's acceptance is the existing suites plus both cucumber passes plus `assert_conformance` plus the listing benchmark; each subtle behavior the audit named (cold read/list heal, rename's part move, the copy fast path, the part lock's width, the folder-marker emptiness) now has a named contract call and an existing test |
| The read-heal (E13) moved fs's `ensure_row` policy into the engine and gets the composed-ETag rule subtly wrong | The rule's inputs are contract data only (the stored row + `stat`), the jitter constant moves with it, and the fs tests that pin the composed-ETag cases must stay green; if they cannot, the alternative is a per-backend policy field and a spec amendment |
| `Hooks` grows a third method | E4's non-goal makes it a ruling, and P5 is where a need would surface |
| The engine becomes a `Storage` framework by accretion | Its non-goals are explicit; the operation inventory in §2 is closed and a new operation is a `Storage` contract change first |
| The listing path's cost regresses without any test noticing | The listing benchmark is part of P4's acceptance; the engine's enrichment is a batch call (obligation 6), and `scan` carries the prefix bound (obligation 6 + the persistence spec's D23) |

## Verification

1. `cargo test -p tinio-ops` green (the per-group engine tests).
2. `cargo test -p tinio-fs -p tinio-mem` green with adapter-only edits; `assert_conformance` green on both.
3. Cucumber `@fs` and `@mem` passes green; the fs benchmarks and the new listing benchmark unchanged within noise.
4. `rg -n 'redb::|tokio::fs|sqlx|postgres|tinio_fs|tinio_mem' crates/tinio-ops/src` returns nothing; `cargo tree -p tinio-ops` shows `tinio-core`, `tinio-version`, `tinio-blob` in its production graph and no backend crate.
5. `rg -c 'async fn' crates/tinio-ops/src/hooks.rs` is 2 (E4); the version group is nine operations and `list_object_versions` is not among them (E14).
