# Metadata store contract, plan 1 — the contracts, the PostgreSQL gate, the redb implementation and the fs migration: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the four persistence contracts in `tinio-core` **with their method block written out here** (this task is the single definer of the signatures) plus their conformance suite, prove them against a PostgreSQL spike (the gate, which P3 later promotes), then rename `tinio-store` to `tinio-store-redb`, move the redb transaction machinery into it, implement the metadata/version/content contracts there, re-point `tinio-version`'s ports onto the contracts, and migrate `tinio-fs` onto the contracts' concrete handles — with the fs suite and the `@fs` cucumber pass green and the S3 surface unchanged.

**Architecture:** `tinio-core` owns decoded row types and async traits; a workspace-excluded spike proves the contracts from a non-redb transport before anything depends on them; `tinio-store-redb` owns the tables, the codecs, the driver and the implementations; `tinio-version` owns the version state machine and (after Task 10) depends only on the contracts; `tinio-fs` keeps its paths, files and policy and consumes the contracts through facades that keep their names.

**Tech Stack:** Rust (edition 2024), `async-trait`, `redb` (via `tinio-store-redb`), `tokio`, `tinio-blob`, `tinio-util`'s `testing` harness; for the spike: `tokio-postgres` + `postgres:18-alpine` (docker compose), excluded from the workspace and from CI (2026-10-07: the operator ruled a compile-only CI job `pg-meta-check` in, superseding "from CI" for a compile check only; the spike stays outside the workspace).

**Spec:** `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md` — this plan implements its **§1 The contract set** (its method block is pinned here, in Task 1), **§2 Crate topology**, **§3 `tinio-fs`**, **§5 The version engine and plan 2**, **§5a The PostgreSQL spike gate** and the fs half of **§6 Testing and acceptance**.

**Depends on:** `docs/superpowers/plans/2026-10-03-s3-versioning-p3-s3-surface.md` (the whole versioning set lands first), and `docs/superpowers/plans/2026-10-03-s3-versioning-p2-versioned-store.md` as amended. Plan 2 (`…-p2-mem-and-conformance.md`) migrates `tinio-mem`.

## Global Constraints

- English only in repo artifacts; no hard-wrapped prose (`docs/compress.md`). Public items carry doc comments; the four contract traits carry a trait-level `ignore`d `rustdoc` example (spec D14). Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` (this gate renders doctests but never compiles them — a doctest is only checked by `cargo test`, per `docs/tests.md`).
- `tinio-core` gains **no** `redb`, `tokio`, `tinio-store-redb` or `tinio-fs` dependency.
- **Task 2 is a hard gate** (D21): if no PostgreSQL is reachable, stop and ask.
- Per-crate `Error` with `impl From<Error> for tinio_core::storage::Error`; the fs `Error::Redb` lift and the mem `DatabaseError` name survive. **`RedbMetadata`'s associated error is `tinio_store_redb::Error`** (one impl, two consumers); each backend maps it at its facade (Task 5 Step 1). **The mapping must unwrap the refusal, not flatten it**: `tinio-store-redb` carries a refusal as `Error::Refused(storage::Error)`, and a naive `From` would degrade it to `Io` — the fs shim's `map_contract_error` is the reference and the mem backend owes the same unwrap in P2. A `put_guarded` refusal projects to `storage::Error::NoSuchBucket` and `remove_if_empty`'s outcomes to `NoSuchBucket`/`BucketNotEmpty` (D22).
- redb schema: the tables keep the names and value arities the versioning set left; `FORMAT_VERSION` stays `1` and lives in `crates/tinio-store-redb/src/state.rs` — the table crate already owns it, so Task 4 **moves the driver around it**, never re-declares it.
- Every multi-table operation is one write transaction; a contract method that mutates rows commits them together (a `put_with_parts` writes the object row and its retained parts together; `move_` moves both).
- The contract traits are used **generically**, never as `dyn` (round 40's probe: the category methods' `where Self: Metadata` clause makes `Metadata` non-dyn-compatible); methods still take slices and argument structs rather than closures or generic parameters, so the surface stays uniform.
- Ordering is ascending by UTF-8 byte sequence; the spike pins `COLLATE "C"` **and** verifies the database encoding.
- Unit tests inline (`#[cfg(test)] mod tests`); conformance legs live in `tinio-util`'s `testing` module, whose entry points are public API for consumers outside the workspace (the spike).
- **P1 changes only the metadata-access layer of `tinio-fs`** — function boundaries, branches and error variants stay as they are, **except `load_bucket`**, whose signature becomes `async` (and which keeps its `read_blocking` hop); moving the operation bodies into `tinio-ops` is P4's.
- Git: every commit step asks the operator first (`docs/git.md`); no AI/agent trailers. The spike is never a workspace member and never in CI (2026-10-07: the operator ruled a compile-only CI job `pg-meta-check` in, superseding "never in CI" for a compile check only; it remains never a workspace member).
- Gates: `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check --package tinio-fs --each-feature`, the `@fs` cucumber pass.

## Review Focus

1. **A contract the spike never exercised being relied on.** The gate covers the metadata contracts, `VersionMeta` and the **whole content surface** (D23) — including the calls the audit added (`stat`, `copy`, the bucket-scoped calls, `is_empty`, the `prefix`-bounded `scan`) — before Task 3 starts.
2. **A metadata write that moved relative to the content it describes.** Publish first, row last (spec §1 property 6). Exception, recorded: fs's row write is best-effort (`Warn`, spec D39) — the PUT succeeds and the row is recomputed on the next read.
3. **A multi-row operation that became a loop of atomic calls.** Drains, `put_batch`, `part_records`, `put_with_parts`, `move_` and the version-row-plus-index writes stay one transaction.
4. **Boundary keys and the database encoding.** `data` vs `data-2`, an empty key part, a NUL-bearing key, part 10 vs part 2; the spike's `COLLATE "C"` **plus** a non-UTF8 database refusal.
5. **A behavior the fs suite does not pin.** The audit named five: the cold read/list heal (E13 in the engine spec — here: `stat` + the row it needs), rename's part move (`move_`), the copy fast path, the folder-marker emptiness (`is_empty`), and `walk`'s strict/healing split. Each has a named call and must stay green.

---

### Task 1: `tinio-core` — the contract set, and the conformance suite

**Files:**
- Create: `crates/tinio-core/src/meta/mod.rs`, `meta/bucket.rs`, `meta/object.rs`, `meta/upload.rs`, `meta/version.rs`, `crates/tinio-core/src/content.rs`
- Move: `crates/tinio-util/src/testing.rs` → `crates/tinio-util/src/testing/mod.rs`
- Create: `crates/tinio-util/src/testing/metadata.rs`
- Modify: `crates/tinio-core/src/lib.rs`, `crates/tinio-util/src/lib.rs`
- Test: the `#[cfg(test)] mod tests` in the created `tinio-core` files

**Interfaces:**
- Consumes: `bucket::Name`, `object::{Key, Tags}`, `etag::ETag`, `acl::{Acl, AclGrants, OwnerId}`, `cors::Config`, `checksum::{Recorded, Upload, PartChecksum}`, `multipart::{MultipartUpload, ObjectPart, PartNumber}`, `ByteRange`, `storage::{Error, BodyStream}`, `version`.
- Produces (every later task and the spike consume these names) — **this is the single definer of the method block**; the traits are used generically (never `dyn` — round 40), and each category method carries `where Self: Metadata` and returns `<Self as Metadata>::Error`:

> **Correction (P1 execution, 2026-10-07).** The block below is a pre-ruling record and is kept as written. Round 39/40 (`docs/superpowers/reviews/2026-10-03-metadata-store-review-round2.md`) amends it where marked there: `BucketMeta::{set_tags,clear_tags,set_acl}` and `ObjectMeta::set_acl` return `Result<bool>` (item 9); `walk_healing` returns `Vec<(object::Key, Option<object::Row>)>` (item 7); the UploadMeta retained-parts trio is deleted (item 8); `BucketMeta::create` inserts-or-refuses (item 10); `StagedObject` carries `Error` and `Staged: StagedObject<Error = Self::Error>` (item 4); `put`/`put_batch` clear a key's retained parts (item 5). The landed code is `crates/tinio-core/src/meta/`.

```rust
pub trait Metadata: Send + Sync + 'static + BucketMeta + ObjectMeta + UploadMeta {
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
}

#[async_trait]
pub trait BucketMeta: Send + Sync + 'static {
    async fn created_at(&self, bucket: &bucket::Name) -> Result<Option<SystemTime>, <Self as Metadata>::Error> where Self: Metadata;
    async fn load_many(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<SystemTime>>, <Self as Metadata>::Error> where Self: Metadata;
    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, <Self as Metadata>::Error> where Self: Metadata;
    async fn row(&self, bucket: &bucket::Name) -> Result<Option<bucket::Row>, <Self as Metadata>::Error> where Self: Metadata;
    async fn owners(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<acl::OwnerId>>, <Self as Metadata>::Error> where Self: Metadata;
    async fn record(&self, bucket: &bucket::Name, created: SystemTime) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn record_if_absent(&self, bucket: &bucket::Name, now: SystemTime) -> Result<SystemTime, <Self as Metadata>::Error> where Self: Metadata;
    async fn record_full(&self, bucket: &bucket::Name, row: &bucket::Row) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn set_tags(&self, bucket: &bucket::Name, tags: &object::Tags) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn set_acl(&self, bucket: &bucket::Name, owner: Option<&acl::OwnerId>, grants: &AclGrants) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn set_cors(&self, bucket: &bucket::Name, config: &cors::Config) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn remove(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
}

#[async_trait]
pub trait ObjectMeta: Send + Sync + 'static {
    async fn get(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Option<object::Row>, <Self as Metadata>::Error> where Self: Metadata;
    async fn put(&self, bucket: &bucket::Name, key: &object::Key, row: &object::Row) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn put_guarded(&self, bucket: &bucket::Name, key: &object::Key, row: &object::Row) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    /// The object row **and its retained part list** in one transaction.
    async fn put_with_parts(&self, bucket: &bucket::Name, key: &object::Key, row: &object::Row, parts: &[object::PartRow]) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    /// The retained part list (OBJECT_PARTS) of a completed multipart object.
    async fn parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Vec<object::PartRow>, <Self as Metadata>::Error> where Self: Metadata;
    /// The row's content-derived elements refreshed, the recorded elements carried over.
    async fn put_preserving(&self, bucket: &bucket::Name, key: &object::Key, identity: &object::Identity) -> Result<object::Preserved, <Self as Metadata>::Error> where Self: Metadata;
    /// The row **and its retained parts** moved in one transaction.
    async fn move_(&self, bucket: &bucket::Name, from: &object::Key, to: &object::Key) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn set_tags(&self, bucket: &bucket::Name, key: &object::Key, tags: &object::Tags) -> Result<bool, <Self as Metadata>::Error> where Self: Metadata;
    async fn clear_tags(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, <Self as Metadata>::Error> where Self: Metadata;
    async fn ensure_tags(&self, bucket: &bucket::Name, key: &object::Key, tags: &object::Tags) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn set_acl(&self, bucket: &bucket::Name, key: &object::Key, owner: Option<&acl::OwnerId>, grants: &AclGrants) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn put_batch(&self, bucket: &bucket::Name, rows: &[(object::Key, object::Row)]) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, <Self as Metadata>::Error> where Self: Metadata;
    async fn remove_many(&self, bucket: &bucket::Name, keys: &[object::Key]) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    /// One call per listing page (never a point read per key).
    async fn load(&self, bucket: &bucket::Name, keys: &[object::Key]) -> Result<Vec<Option<object::Row>>, <Self as Metadata>::Error> where Self: Metadata;
    /// Strict traversal: a corrupt row fails it.
    async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<(object::Key, object::Row)>, <Self as Metadata>::Error> where Self: Metadata;
    /// Healing traversal: a corrupt row is healed and never fails the walk.
    async fn walk_healing(&self, bucket: &bucket::Name) -> Result<Vec<(object::Key, object::Row)>, <Self as Metadata>::Error> where Self: Metadata;
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
}

#[async_trait]
pub trait UploadMeta: Send + Sync + 'static {
    async fn create(&self, bucket: &bucket::Name, upload_id: &str, row: &upload::Row) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn get(&self, bucket: &bucket::Name, key: &object::Key, upload_id: &str) -> Result<Option<upload::Row>, <Self as Metadata>::Error> where Self: Metadata;
    async fn put_part(&self, bucket: &bucket::Name, key: &object::Key, upload_id: &str, number: PartNumber, etag: &ETag, checksum: Option<&checksum::PartChecksum>) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn list_parts(&self, bucket: &bucket::Name, upload_id: &str, from: PartNumber) -> Result<Vec<upload::PartRow>, <Self as Metadata>::Error> where Self: Metadata;
    async fn part_records(&self, bucket: &bucket::Name, key: &object::Key, upload_id: &str, numbers: &[PartNumber]) -> Result<Option<Vec<Option<upload::PartRecord>>>, <Self as Metadata>::Error> where Self: Metadata;
    async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> where Self: Metadata;
    async fn list_uploads_page(&self, bucket: &bucket::Name, prefix: &str, delimiter: Option<&str>, marker: Option<&str>, max_uploads: usize) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), <Self as Metadata>::Error> where Self: Metadata;
    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> where Self: Metadata;
    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> where Self: Metadata;
    async fn drain_upload(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn put_object_parts(&self, bucket: &bucket::Name, key: &object::Key, parts: &[ObjectPart]) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
    async fn object_parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Vec<ObjectPart>, <Self as Metadata>::Error> where Self: Metadata;
    async fn drain_object_parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), <Self as Metadata>::Error> where Self: Metadata;
}
```

`VersionMeta` (declared in `meta/version.rs`, never a supertrait of `Metadata` — D20) carries `bucket_state`/`set_bucket_state`, **`current_slot(bucket, key) -> Option<(version::Id, u64)>`** (D37), the version row reads/writes (`put_version`, `get_version`, `list_versions`, `expire`) and the index lookup. `VersionMeta::current_slot` returns `Option<Current>` where `Current { exists, version, is_delete_marker }` (round-2 item 3; a `seq` field was ruled out), and `advance_slot(bucket, key, id, seq)` is the write that moves a slot (item 2). `ObjectContent` (in `content.rs`) carries the surface fixed by the persistence spec's D23 — **the single definer is that spec's §1**; this plan implements it and the suite tests it:

> **Correction (P1 execution, 2026-10-07).** The block below is a pre-ruling record and is kept as written. Round-2 blocker B1 amends `ContentEntry` to `{ key: object::Key, len: u64, identity: u64, mtime: Option<SystemTime> }` (its `key` stays an object key — `scan` never yields a part key), and item 4 gives `StagedObject` an `Error` associated type with `Staged: StagedObject<Error = Self::Error>`. The landed code is `crates/tinio-core/src/content.rs`.

```rust
pub enum ContentKey { Object(object::Key), Part { upload_id: String, number: PartNumber } }
pub struct ContentEntry { pub key: object::Key, pub len: u64 }   // no timestamp (D42)

#[async_trait]
pub trait StagedObject: Send + Sync + 'static {
    async fn write(&mut self, chunk: Bytes) -> Result<(), <Self as ObjectContent>::Error>;   // typed (D38)
}

#[async_trait]
pub trait ObjectContent: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
    type Staged: StagedObject;
    async fn stage(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<Self::Staged, Self::Error>;
    async fn publish(&self, bucket: &bucket::Name, staged: Self::Staged, key: &ContentKey) -> Result<(), Self::Error>;
    async fn open(&self, bucket: &bucket::Name, key: &ContentKey, range: Option<ByteRange>) -> Result<BodyStream, Self::Error>;
    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error>;
    async fn rename(&self, bucket: &bucket::Name, from: &object::Key, to: &object::Key) -> Result<(), Self::Error>;
    async fn copy(&self, bucket: &bucket::Name, from: &object::Key, to: &object::Key) -> Result<(), Self::Error>;
    async fn scan(&self, bucket: &bucket::Name, prefix: &str, after: Option<&object::Key>, limit: usize) -> Result<Vec<ContentEntry>, Self::Error>;
    async fn stat(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Option<ContentEntry>, Self::Error>;
    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;
    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;
    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error>;
}
```

Two argument/return shapes the block names but does not define:

```rust
pub struct Identity { pub etag: ETag, pub size: u64, pub mtime: SystemTime, pub file_identity: u64 }
pub struct Preserved { pub tags: Tags, pub checksum: Option<checksum::Recorded>, pub owner: Option<acl::OwnerId>, pub acl: Acl }
```

The method list is a working surface (D10): a later task adds a method when a migrating call site needs one, and adds its conformance leg in the same task.

- [x] **Step 1: Re-baseline** — read the landed tree (the version tables and row shapes, the `object_meta` value tuple as the versioning set left it, mem's content tables, `tinio-version`'s ports) and record the findings in this task's notes; adjust Tasks 8 and 10 in place if they differ. The contracts' shape does not change.
- [x] **Step 2: Write the failing contract tests** — in `meta/mod.rs`: a **shape probe** that instantiates the engine's generic form with one concrete store (`Ops<Store, Store, Store, Store, Blocks, Hooks>` with `Store` implementing `Metadata`/`VersionMeta`/`ObjectContent` — the shape probe A compiled) and the ordering pin (`"data" < "data-2" < "data/"`); in `meta/object.rs`: the moved self-heal pins for **both** traversals. Do **not** write a `dyn` probe: round 40's probe proved no `dyn Metadata` form exists.
- [x] **Step 3: Run to verify they fail** — `cargo test -p tinio-core --lib meta`.
- [x] **Step 4: Implement the contracts** — move the decode helpers out of the table crate into `meta`, add the row types (`meta::object::PartRow` included) with their conversions, and the traits above with a trait-level `ignore`d example each.
- [x] **Step 5: Write the conformance suite** — `crates/tinio-util/src/testing/metadata.rs`, public entry points, the legs of spec §6: byte ordering (the adversarial pair), per-bucket boundaries, numeric part order, **both** traversal rules, per-method atomicity, drain idempotence, batch input order (`load`), the `put_guarded`/`remove_if_empty` refusals and projections, `publish` visibility, `open` range semantics, `rename`/`copy`/`stat`/`is_empty`/`create_bucket`/`remove_bucket`, the part key shape, `put_with_parts`/`parts`/`move_`, and **the 8 MiB staged leg** (64 KiB chunks, full + ranged read — D43).
- [x] **Step 6: Run to verify they pass** — `cargo test -p tinio-core --lib`, then `cargo build -p tinio-util --features testing` (the suite compiles; the spike and Tasks 5–8 supply consumers).
- [x] **Step 7: Gate and commit** — clippy, the doc gate, `cargo fmt`. Ask the operator, then `git commit -m "feat(core): add the persistence contracts and their conformance suite"`.

---

### Task 2: the PostgreSQL spike gate

**Files:**
- Create: `spike/pg-meta/{Cargo.toml,src/lib.rs,src/main.rs,schema.sql,docker-compose.yml,README.md}` (the README states that P3 promotes this directory into `crates/tinio-store-pg`)
- Create: `docs/superpowers/pg-meta-spike-findings.md`
- Modify: root `Cargo.toml` (`[workspace] exclude = ["vendor/soft-canonicalize", "spike"]`), `.gitignore` (`spike/**/target/`)
- Modify: `crates/tinio-core`/`crates/tinio-util` where a shape finding demands it (D24), each with its suite leg in the same edit

**Interfaces:**
- Consumes: Task 1's contracts and the conformance suite, as path dependencies from a crate outside the workspace (`tinio-core`, `tinio-util` with `features = ["testing"]`).
- Produces: the gate's verdict; `schema.sql` as the seed for `tinio-store-pg`; the findings document; the spike itself (promoted by P3, D25/D28).

- [x] **Step 1: Bring up PostgreSQL 18** — `docker-compose.yml` pins `postgres:18-alpine` with a named volume and a healthcheck; document `docker compose up -d` + `psql` verification in the README. If no PostgreSQL is reachable, **stop and ask** (D21).
- [x] **Step 2: Write the schema** — key columns `text COLLATE "C"`, part numbers bigint, one row per bucket/upload/part, the version tables, the bucket versioning state, and the content/block tables. **Pin the forms the pg design defers to this spike** (PG18–PG23): the content `kind` discriminator and the live-generation marker, the collision-proof part namespace, the `blocks` `len`/`published` columns and the publish-completeness rule, and the bounded publish transaction (batching + incompleteness marker, or a declared cap). Record each choice in the findings.
- [x] **Step 3: Implement the contracts in a library** — `src/lib.rs` holds the `tokio-postgres` implementation of the metadata contracts, `VersionMeta` and **the whole content surface (D23)**, plus the crate's `Error` (redacting the dsn); one statement per point operation, one transaction per mutating method, one statement for `remove_if_empty`'s probe-and-delete, `COLLATE "C"` on every ordered read, **bound parameters only**. `src/main.rs` is the gate driver.
- [x] **Step 4: Run the gate** — `src/main.rs` runs `assert_metadata_conformance`, the content legs (including the 8 MiB leg) and its own raw-SQL self-heal leg (a garbage etag inserted directly reads as absent by key and fails a strict `walk`). All green, exit `0`; the version-row walkthrough stays a human-review item (Step 6).
- [x] **Step 5: Resolve findings** — shape versus scope (D24); shape problems are fixed in `tinio-core` plus their suite leg in the same edit.
- [x] **Step 6: Write the version-row walkthrough** — a per-method table (method, SQL shape, which property it satisfies) in the findings.
- [x] **Step 7: Record the verdict** — `docs/superpowers/pg-meta-spike-findings.md`: the suite output, the schema seed, each finding with its resolution, the walkthrough, the collation/encoding pin, and every pg-design form this step pinned (PG18–PG23).
- [x] **Step 8: Gate and commit** — confirm the spike is outside the workspace (`cargo metadata --no-deps --format-version 1 | rg 'pg-meta'` finds nothing) and in no CI job; ask the operator, then commit.

> **Dated note (2026-10-07, after execution).** Step 8's first half still holds — the spike is outside the workspace and `cargo metadata --no-deps --format-version 1 | rg 'pg-meta'` still finds nothing — but "and in no CI job" was superseded by the operator's 2026-10-07 ruling, which added a **compile-only** job `pg-meta-check` (`.github/workflows/ci.yml`, `cargo check --manifest-path spike/pg-meta/Cargo.toml`). The spike stays outside every gate that runs the conformance suite or the full gate.

---

### Task 3: rename `tinio-store` → `tinio-store-redb`

**Files:**
- Modify: root `Cargo.toml`, `crates/tinio-store/Cargo.toml` → `crates/tinio-store-redb/Cargo.toml`, `crates/tinio-fs/src/lib.rs`, `crates/tinio-mem/src/lib.rs` (`pub extern crate tinio_store_redb as _store_redb;`), every `_store::` site (~35 across 14 files), **`docs/style.md` (both the alias rule's example *and* the error rule's crate path)**, `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md` (a one-line pointer; its record is not rewritten)

**Interfaces:**
- Produces: the crate `tinio-store-redb` with today's `tinio-store` public surface; alias `_store_redb` in both backends.

- [x] **Step 1: Move and rename** — `git mv`, rename the package, update manifests and alias sites.
- [x] **Step 2: Verify** — `rg -n 'tinio_store\b|_store::' crates` shows only intended hits; `cargo test -p tinio-store-redb -p tinio-fs -p tinio-mem` PASS, assertion-free.
- [x] **Step 3: Gate and commit.**

---

### Task 4: the redb driver relocates into `tinio-store-redb`, and the pre-change fixture

**Files:**
- Create: `crates/tinio-store-redb/src/driver.rs`, `open.rs`, `compact.rs`, `crates/tinio-store-redb/tests/fixtures/pre-change-meta.redb`, `crates/tinio-store-redb/tests/open_pre_change_database.rs`
- Delete: `crates/tinio-fs/src/database/{handle,open,compact}.rs` (their tests move with them); `crates/tinio-fs/src/database/mod.rs` becomes a re-export shim
- Modify: `crates/tinio-store-redb/src/lib.rs`, `Cargo.toml` (`tokio`), `crates/tinio-fs/src/lib.rs` and the call sites importing `database::{Handle, open, compact}`

**Interfaces:**
- Produces: `tinio_store_redb::Driver` (today's fs `Handle`), `WriteLockSnapshot`, `open::open(state_dir)`, `compact::compact_if_needed`; fs re-exports them; **`crates/tinio-store-redb/src/state.rs` keeps `FORMAT_VERSION`** (the driver reads it; nothing re-declares it).

- [x] **Step 1: Move the code and its tests** — split `open.rs` along the policy line (state-dir layout, permissions and read-only relocation stay in `tinio-fs`; the redb half moves).
- [x] **Step 2: Run the moved tests** — `cargo test -p tinio-store-redb`, then `-p tinio-fs`.
- [x] **Step 3: Verify the error surface** — the fs/mem error tests and the conversion pins; identical lifts, paths and text.
- [x] **Step 4: Verify the metrics surface** — the histogram tests; re-exported, not reimplemented.
- [x] **Step 5: Commit the pre-change fixture (D41)** — generate a `meta.redb` with the **pre-change** binary (the commit before this plan's first task), copy it to `tests/fixtures/pre-change-meta.redb`, and write `open_pre_change_database.rs` to open it and read a row through the public implementations. This is the only artifact that proves "a database written before the change still opens" — the `--test layout` target the earlier drafts cited no longer exists.
- [x] **Step 6: Gate and commit.**

---

### Task 5: `BucketMeta` over redb + the fs bucket facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/bucket.rs`
- Modify: `crates/tinio-store-redb/src/lib.rs` (`RedbMetadata` + constructors + the error pin), `Cargo.toml` (dev-dependency `tinio-util` with `testing`), `crates/tinio-store-redb/src/contract.rs`, `crates/tinio-fs/src/bucket.rs`, `crates/tinio-fs/src/database/tables.rs` (the strict traversal moves to the new impl's `walk`; the file is named by this task)

**Interfaces:**
- Consumes: Task 1's `BucketMeta`; Task 4's `Driver`.
- Produces: `tinio_store_redb::RedbMetadata { driver: Arc<Driver> }` with `open(state_dir)`, `in_memory()`, `driver(&self) -> Arc<Driver>`, **`type Error = tinio_store_redb::Error`** — implementing `Metadata` (this task the `BucketMeta` half) — and the resolution of `database/tables.rs`'s strict walk into that impl.

- [x] **Step 1: Pin the error and wire the leg** — `RedbMetadata::Error = tinio_store_redb::Error`; each consumer maps it at its facade (fs: `Error::Redb(e.into())`; mem: its `DatabaseError` alias). Call `assert_bucket_meta_conformance` from `contract.rs`; run it (fails: no impl).
- [x] **Step 2: Implement `BucketMeta`** — one read or write transaction per method; `remove_if_empty` probes `object_meta` and `uploads` in the transaction that removes the row, its refusals projecting to `NoSuchBucket`/`BucketNotEmpty`.
- [x] **Step 3: Run to verify it passes.**
- [x] **Step 4: Migrate the fs bucket facade** — bodies delegate; `cors`/`acl`/`tags` derive from `row`; the bucket-mutation lock stays in fs's `Storage` impl around the engine call (P4's rule, and today's lock placement is unchanged). `cargo test -p tinio-fs --lib bucket` PASS.
- [x] **Step 5: Gate and commit.**

---

### Task 6: `ObjectMeta` over redb + the fs meta facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/object.rs`
- Modify: `crates/tinio-store-redb/src/contract.rs`, `crates/tinio-fs/src/{meta.rs,scanner.rs}` (the `load_bucket` await ripple)

**Interfaces:**
- Produces: the `ObjectMeta` half of the implementation, including `put_with_parts`/`parts`/`move_` (one transaction each) and **both traversals** (`walk` strict, `walk_healing`).

- [x] **Step 1: Wire the legs** — `assert_object_meta_conformance`; the corrupt-row traversal legs from `contract.rs` (they plant a corrupt row through the raw table handles, which is why they live here).
- [x] **Step 2: Implement `ObjectMeta`** — one transaction per method; `walk` and `walk_healing` (the strict and gated traversals the tree already has); `put_preserving` is today's `rewrite_preserving`; `put_guarded` reads the `buckets` row in the same transaction; `move_` moves the row and its parts together.
- [x] **Step 3: Run to verify it passes.**
- [x] **Step 4: Migrate the fs meta facade** — the accessors delegate; `ensure_row`'s stat gate, `md5_of_file` and `composed_keep` **stay in fs** (the engine calls them through `stat`/`open` in P4; here they keep working); `load_bucket` becomes `async` **and keeps its `read_blocking` hop** (a full-bucket traversal never runs inline on a runtime worker). `cargo test -p tinio-fs --lib meta` PASS.
- [x] **Step 5: Gate and commit.**

---

### Task 7: `UploadMeta` over redb + the fs multipart facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/upload.rs`
- Modify: `crates/tinio-store-redb/src/contract.rs` (the upload legs + the aggregating `assert_metadata_conformance`), `crates/tinio-fs/src/multipart.rs`

**Interfaces:**
- Produces: the `UploadMeta` half of the implementation; the aggregating metadata leg.

- [x] **Step 1: Wire the legs** — run, expected FAIL.
- [x] **Step 2: Implement `UploadMeta`** — one transaction per method; `put_part` writes the `PARTS` row and its `PART_CHECKSUMS` row after the key-matching check; `create` writes the `UPLOADS` row and its `UPLOAD_CHECKSUMS` row together; `part_records` is the completion-time join in one read transaction.
- [x] **Step 3: Run to verify it passes** — including the full `assert_metadata_conformance`.
- [x] **Step 4: Migrate the fs multipart facade** — record access delegates; part files, the per-upload part lock, the assembly, the atomic writer and `idle_since` stay. The part lock's placement is P4's (through `Hooks::lock_part`), so here it keeps today's shape. `cargo test -p tinio-fs --lib multipart` PASS.
- [x] **Step 5: Gate and commit.**

---

### Task 8: `VersionMeta` and the redb `ObjectContent`

**Files:**
- Create: `crates/tinio-store-redb/src/impls/version.rs`, `crates/tinio-store-redb/src/impls/content.rs`
- Modify: `crates/tinio-core/src/meta/version.rs` (the method list, derived from `tinio-version`'s `VersionRows` port and the landed tables), `crates/tinio-util/src/testing/metadata.rs`, `crates/tinio-store-redb/src/{contract.rs,lib.rs}` (the byte-table modules gain `#[doc(hidden)]`; their paths stay while `tinio-mem` still uses them — P2 privatizes them)

**Interfaces:**
- Produces: `impl VersionMeta for RedbMetadata` (including `current_slot`); `tinio_store_redb::RedbContent` implementing **the whole D23 content surface** (object and part keys, `rename`, `copy`, `scan` with the prefix bound, `stat`, `create_bucket`, `remove_bucket`, `is_empty`); the two legs.

- [x] **Step 1: Derive `VersionMeta`'s method list** from the port and the landed tables, including `bucket_state`/`set_bucket_state`/`current_slot`; record it.
- [x] **Step 2: Write/extend the failing legs** — version rows (put/get, order, lookup by id, delete-marker row, drain, `current_slot`); content (`publish` visibility, ranges, an overwrite, `remove`, a part round-trip, `rename`, `copy`, `scan` order + prefix bound + limit, `stat`, the bucket calls, `is_empty` with a directory-only bucket, and the 8 MiB staged leg).
- [x] **Step 3: Implement** — one transaction per version-row method; `RedbContent` over the byte tables, `scan` delegating to the same walk the fs scanner uses (Task 9 wires fs's impl; here the redb one is the mem-side table scan).
- [x] **Step 4: Run to verify they pass.**
- [x] **Step 5: Gate and commit.**

---

### Task 9: `tinio-fs` migration and wiring

**Files:**
- Create: `crates/tinio-fs/src/content.rs` — the file `ObjectContent` implementing **all eleven** D23 calls: `stage` (temp file; the parent-symlink policy check and the folder-marker case), `publish` (the atomic rename; the unix directory hardening), `open` (the range reader), `remove` (the unlink), `rename` (the file move), `copy` (`copy_file_range` where available), `scan` (**delegating to the same streaming walk the scanner uses**, with the prefix pruning and the reserved-path filters — the batch accumulator stays in fs), `stat` (the key's file metadata), `create_bucket`/`remove_bucket`/`is_empty` (the directory operations; `is_empty` counts folder-marker directories)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`FsStorage` holds the concrete handles (`Arc<RedbMetadata>`, `Arc<FsContent>`) beside the `Driver` clone; the row-write policy is `Warn` — spec D39), the three facades' wiring, `crates/tinio-fs/src/{write,write_task,listing,cleanup,scanner,sweep}.rs` for call sites that touched a table or the raw handle
- Test: a facade-level test that a completed multipart object's metadata is present exactly when its bytes are served; a cold-listing test that a content key with no row still appears with its content ETag source available (`stat`)

**Interfaces:**
- Produces: `FsStorage`'s contract handles; the file `ObjectContent`; no fs module outside the facades touching `tinio-store-redb`'s table handles.

- [x] **Step 1: Write the failing tests** (2026-10-07, follow-up wave) — the visibility test and the cold-listing test above. Both landed in `crates/tinio-fs/src/content.rs` and were mutation-verified red-then-green. The visibility test pins the metadata half through `list_object_parts`: withholding only the `object_meta` row leaves the facade green, because fs reads are file-truth, so the row-only assertion is the honest one.
- [x] **Step 2: Implement `ObjectContent` for fs** — the eleven calls over the existing `AtomicWriter`, layout and walk; the scanner's feed is unchanged (it keeps `walk_files_streaming`).
- [x] **Step 3: Wire and migrate the call sites** — the metadata-access layer only, by domain; the bucket-mutation lock keeps today's placement.
- [ ] **Step 4: Prove no table access remains** — `rg -n '_store_redb::' crates/tinio-fs/src crates/tinio-mem/src` outside the implementations (enumerate the crate's public modules; the prefix `version` misses `bucket_versioning`).
- [x] **Step 5: Full fs suite and the `@fs` pass** — PASS with import/await edits only.
- [x] **Step 6: Gate and commit.**

> Task 9 passed review on this reduced deliverable: `list_parts_page` end to end, the named `FsContent` over its extracted primitives, and the completed `_store_redb::` census. Step 3's remaining call-site wiring and Step 5's full-suite re-run moved to Task 10, because every remaining site rides a transaction that also carries version-domain rows in the same commit — that is the version re-point, not a call swap. Step 1's visibility test was not landed; the fs-side coverage it would have added is covered by `shared_content_conformance_is_green`.
>
> Re-measured on 2026-10-07, three claims of that note do not hold and are corrected here rather than rewritten in it: the census is 58 `_store_redb::` lines across 18 files, not "57 hits / 19 files"; Step 1's two named tests did land (`crates/tinio-fs/src/content.rs`), so `shared_content_conformance_is_green` is a third test, not their substitute; and "every remaining site rides a transaction that also carries version-domain rows" is too strong. See the Step 4 bullets in the status section below, which separate the sites that do ride such a transaction from the ones whose contract call already exists and from the part blocked on the missing bucket-wide version drain.

---

### Task 10: re-point the version engine onto the contracts

**Files:**
- Modify: `crates/tinio-version/src/ports.rs` (delete `VersionRows` and `ContentPort`; keep `CurrentRows`), `crates/tinio-version/src/{lib,engine}.rs` (bounds become `M: Metadata + VersionMeta`, `C: ObjectContent<Error = storage::Error>`, `B: BlockStore`; every contract call site gains its `.map_err(Into::into)`), `Cargo.toml` (dependencies: `tinio-core`, `tinio-blob`, `async-trait`, `tokio` — **no feature, no redb, no fs**), `crates/tinio-fs/src/version_adapter.rs` (keeps `CurrentRows`: `VersionMeta::current_slot` + the F01/F04 gate), `crates/tinio-fs/src/backend/version.rs`, `crates/tinio-fs/Cargo.toml`

**Interfaces:**
- Produces: `tinio-version` depending on `tinio-core` and `tinio-blob` alone; `CurrentRows` as its one surviving port.

- [x] **Step 1: Re-point** — replace the two deleted ports' bounds with the contract bounds; the engine's own signatures and tests are unchanged.
- [x] **Step 2: Run the engine's tests** — `cargo test -p tinio-version` PASS with adapter-only edits.
- [x] **Step 3: Run the fs version paths** — `cargo test -p tinio-fs --lib version`, then `-p tinio-fs` PASS. `list_object_versions` stays fs-side (spec D44).
- [x] **Step 4: Prove the dependency cut** — `cargo tree -p tinio-version` shows **`tinio-core` and `tinio-blob`** (the block plane is the engine's storage) and no `redb`, `tinio-fs` or `tinio-store-redb`.
- [x] **Step 5: Gate and commit** — the workspace gates plus `cargo hack check --package tinio-fs --each-feature` (no crate-root gates).

---

## Plan-set ownership

- **P1 (this plan) owns**: the four contracts' method block and rows, the conformance suite's creation and every leg except the mem runs, the PostgreSQL spike gate (its schema seed and findings), the `tinio-store-redb` rename and driver relocation, the metadata/version/content implementations, the engine re-point, the pre-change fixture, and the fs migration.
- **P2** owns: the `tinio-mem` migration, its conformance run, cleanup and the doc sync.
- **P3/P4/P5** own: `tinio-store-pg`, `tinio-ops` and `tinio-pg` (their specs own their models).
- **The spec** owns every decision, the contract properties and the topology; **`2026-09-03-shared-store-table-layer-design.md`** owns the table set and the codecs (its arity table is stale against the landed 5/8/5 tuples — a correction line beside it is part of P2's doc sync).

## Notes for the executor

- **Do not start Task 3 before Task 2 is green.** The gate is the only evidence that the contracts are implementable off redb, and the spike must cover the whole D23 surface — including the calls the audit added.
- In the spike, shape problems are yours (fix the contract + its suite leg + record the finding); scope problems are the operator's.
- Never move a metadata write to the other side of the content operation it describes: publish first, row last (the one exception is fs's `Warn` policy, spec D39).
- `crates/tinio-store-redb/src/state.rs` owns `FORMAT_VERSION`; Task 4 moves the driver **around** it and never re-declares it.
- `walk` (strict) and `walk_healing` are two methods because the tree has two behaviours; do not collapse them.
- `tinio-version` must not gain a redb or fs dependency in Task 10; `CurrentRows` is the one port that stays.

---

## P1 status after execution (2026-10-07)

All ten tasks were implemented and individually reviewed. This section records what the ticks above do and do NOT claim, because the whole-branch review found that the tick state overstated the fs migration. It is the hand-off record and stands as written; the dated note at its end carries what changed afterwards.

**What the ticks do not claim.**
- Task 9 Step 4 is left UNTICKED on purpose: its title says "prove no table access remains" and what landed is a census, not the property. Re-measured on the working tree on 2026-10-07, `crates/tinio-fs/src` carries 56 `_store_redb::` lines across 18 files and 101 raw table opens — 65 `Table::open(` plus 36 `Table::open_readonly(` under the unanchored pattern, which the `StateTable::open(`-style forms also match, and 97 by the bare-name one (62 plus 35) — across 16 files for the unanchored pattern and 14 for the bare-name one, so spec section 6 item 6 is unmet. The "69 `Table::open*` sites" this bullet first carried was wrong.
- The residue is not uniformly justified by the version-domain argument. Where a site rides a transaction that also carries version rows, `ObjectMeta::put`/`put_with_parts`/`move_` cannot express it — but that is not every site. `remove`, `remove_many`, `load_entries` and `walk` in `crates/tinio-fs/src/meta.rs` open `object_meta`/`object_part` alone, write no version row, and each has a contract call that already covers it (`remove_many`, `load`, `walk`): those are a call swap this plan did not make. `set_batch_owned`, in that file, is **not** one of them: `BatchEntry` carries only `key`/`etag`/`size`/`mtime`/`identity` and no recorded elements, while the contract's `ObjectMeta::put_batch` writes the caller's `Row` elements verbatim - so a call from that site would clear the row's `tags`/`checksum`/`owner`/`acl`, and carrying them over would need a `load` first, a second transaction that breaks the single-transaction carry-over the site's `rewrite_preserving` rule depends on. That site is an interface gap (a batch-preserving method), not a call swap. One part of the residue is not a deferral by judgement at all but a missing contract method — `VersionMeta` carries no bucket-wide version drain, so `crates/tinio-fs/src/backend/mod.rs` has to open `version_rows::Table`/`version_index::Table` raw to drain a bucket, and D10 makes that method and its conformance leg part of the task that needs it.
- Task 9 Steps 3 and 5 were completed only partly by Task 9 itself and partly by Task 10; the prose note before Task 10 records the split.

**Round-2 review items that never reached this plan's text.** Items 29 and 30 of `docs/superpowers/reviews/2026-10-03-metadata-store-review-round2.md` — the composed-keep rule and its constant moving to `tinio-ops` in P4 with both fs call sites (`meta.rs`, `etag.rs`) calling that copy, and Task 9's Files gaining `crates/tinio-fs/src/{etag.rs,backend/multipart.rs}` with the invariant grep scoped to name the `database/mod.rs` boundary — are decisions the code partly followed but the Files lists above never absorbed. Apply them when this plan is next revised.

**Post-plan renames (2026-10-07, after execution).** The Files lists above are the record of what was written and stand as written. Two names changed afterwards, and those paths should be read through them: `crates/tinio-store-redb/src/impls/` is now `crates/tinio-store-redb/src/meta/`, and its `content.rs` moved out to `crates/tinio-store-redb/src/content.rs` (beside `tinio-core/src/content.rs`); the top-level table module `src/meta.rs` is now `src/object_meta.rs`, the table's own name. The live P2 plan was updated in place because it is still to be executed; T167 and T168 in `specs/001-s3-local-server/tasks.md` carry the details.

**Known next-phase items, not defects of this plan.**
- `ObjectContent::publish` answers only `()`: `materialize` returns `PublishedBody { etag, size }` built from the row it read, and the contract-level `Published` candidate that would let a staged object answer what it actually left remains a D23 amendment for P2/P4.
- A typed `put_version` conflict variant: a same-sequence conflict currently projects as `storage::Error::Io` because `storage::Error` has no conflict or precondition variant to name it.
- **Interface gaps at the Task-9 residue sites** (a later wave must decide them; they are gaps the residue list did not name, not defects of this diff): `ObjectMeta` has `put_preserving` for one key but no **batch-preserving** form, so `set_batch_owned` (`crates/tinio-fs/src/meta.rs`) cannot use `put_batch` without clearing the row's recorded elements; fs's `rewrite_preserving`/`carried` re-implements `ObjectMeta::put_preserving` because the contract cannot express the caller's **staleness gate** (the contract carries the version columns over unconditionally, while fs needs a compare-and-set gated on the F01/F04 verdict); and fs's `Store::set_tags`/`ensure_tags`/`clear_tags` return a **`TagRow { row, records_versions }`** composite read in one transaction, while the contract's `ObjectMeta::{set_tags,clear_tags}` return `bool` only, so the composite has no contract home.

**Follow-up (2026-10-07, after this section was written).** Four items this section and the note before Task 10 recorded as still open landed afterwards, and are kept here as the record rather than deleted from it. Task 9 Step 1's two named tests landed in `crates/tinio-fs/src/content.rs` — `a_completed_multipart_objects_metadata_appears_with_its_bytes` and `a_row_less_content_key_is_listed_and_statted`, each with the doc comment the brief asked for — so `shared_content_conformance_is_green` is a third test and not their weaker substitute. The fs `row_conflict` warn gained its redb equivalent (`crates/tinio-store-redb/src/meta/version.rs`, the same bucket/key/seq fields and message; `tracing` was added to the crate for it). `list_parts_page`'s corrupt-ETag resume-marker delta was pinned in redb's own tests and then closed at the contract — `list_parts_page` answers a `PartsPage` carrying the RAW read's last number, so a truncated page with no served rows still names a resume marker, pinned from fs with a red-checked fallback — and `list_uploads_page` was deepened from one shallow suite call to seven invariants (order, prefix, page boundary, exclusive-after marker, `max` past the end, `max = 0`, delimiter rollup). The pre-existing unlocked bucket row writers went the same way: `put_bucket_tags`, `delete_bucket_tags` and `delete_bucket_cors` in `crates/tinio-fs/src/backend/buckets.rs` now take the lock `put_bucket_acl` takes, each pinned red-then-green, while `put_bucket_cors` stays unlocked by the operator's 2026-10-07 ruling — the lock guards a read-modify-write lost-update window, and `BucketMeta::set_cors` refuses with `NoSuchBucket` rather than inserting first-sight, so it cannot enter one. The Tasks 9 and 10 ticks above are the pre-ruling record and stand as written.

**Gate state at hand-off.** No commits were made (agents are forbidden from writing git). `cargo clippy --workspace --all-targets -- -D warnings`, `cargo +nightly fmt --check` and rustdoc are clean; `tinio-core`, `tinio-store-redb`, `tinio-util`, `tinio-version` and `tinio-mem` are fully green. The only failures are environment-bound: six `tinio-fs` lib tests that need Windows junction/symlink privileges (error 1314), the `tinio-cli`/`tinio-sandbox-windows` AppContainer probes, and the `doctor.feature:483` sandbox scenario. Every gate requires `TEMP`/`TMP` set inside the workspace, or roughly 27 unrelated tests fail with PermissionDenied.

**PostgreSQL gate re-run (2026-10-07, after this section was written).** The operator ran the gate a third time and **all eight legs completed and passed**, exit 0 (`cargo run --manifest-path spike/pg-meta/Cargo.toml`); this is the first run whose 8/8 verdict covers the landed contract surface - the 2026-10-03 run had also completed all eight legs, but it predates the contract's growth, and the two re-runs earlier today stopped at leg 3 and then at leg 4 - so Task 2's verdict is re-established on the landed contract. Recorded in `docs/superpowers/pg-meta-spike-findings.md` §1.2.
