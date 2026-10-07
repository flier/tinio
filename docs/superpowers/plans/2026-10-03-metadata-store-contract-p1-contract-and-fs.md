# Metadata store contract, plan 1 — the contracts, the PostgreSQL gate, the redb implementation and the fs migration: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the four persistence contracts in `tinio-core` **with their method block written out here** (this task is the single definer of the signatures) plus their conformance suite, prove them against a PostgreSQL spike (the gate, which P3 later promotes), then rename `tinio-store` to `tinio-store-redb`, move the redb transaction machinery into it, implement the metadata/version/content contracts there, re-point `tinio-version`'s ports onto the contracts, and migrate `tinio-fs` onto the contracts' concrete handles — with the fs suite and the `@fs` cucumber pass green and the S3 surface unchanged.

**Architecture:** `tinio-core` owns decoded row types and async traits; a workspace-excluded spike proves the contracts from a non-redb transport before anything depends on them; `tinio-store-redb` owns the tables, the codecs, the driver and the implementations; `tinio-version` owns the version state machine and (after Task 10) depends only on the contracts; `tinio-fs` keeps its paths, files and policy and consumes the contracts through facades that keep their names.

**Tech Stack:** Rust (edition 2024), `async-trait`, `redb` (via `tinio-store-redb`), `tokio`, `tinio-blob`, `tinio-util`'s `testing` harness; for the spike: `tokio-postgres` + `postgres:18-alpine` (docker compose), excluded from the workspace and from CI.

**Spec:** `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md` — this plan implements its **§1 The contract set** (its method block is pinned here, in Task 1), **§2 Crate topology**, **§3 `tinio-fs`**, **§5 The version engine and plan 2**, **§5a The PostgreSQL spike gate** and the fs half of **§6 Testing and acceptance**.

**Depends on:** `docs/superpowers/plans/2026-10-03-s3-versioning-p3-s3-surface.md` (the whole versioning set lands first), and `docs/superpowers/plans/2026-10-03-s3-versioning-p2-versioned-store.md` as amended. Plan 2 (`…-p2-mem-and-conformance.md`) migrates `tinio-mem`.

## Global Constraints

- English only in repo artifacts; no hard-wrapped prose (`docs/compress.md`). Public items carry doc comments; the four contract traits carry a trait-level `ignore`d `rustdoc` example (spec D14). Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- `tinio-core` gains **no** `redb`, `tokio`, `tinio-store-redb` or `tinio-fs` dependency.
- **Task 2 is a hard gate** (D21): if no PostgreSQL is reachable, stop and ask.
- Per-crate `Error` with `impl From<Error> for tinio_core::storage::Error`; the fs `Error::Redb` lift and the mem `DatabaseError` name survive. **`RedbMetadata`'s associated error is `tinio_store_redb::Error`** (one impl, two consumers); each backend maps it at its facade (Task 5 Step 1). A `put_guarded` refusal projects to `storage::Error::NoSuchBucket` and `remove_if_empty`'s outcomes to `NoSuchBucket`/`BucketNotEmpty` (D22).
- redb schema: the tables keep the names and value arities the versioning set left; `FORMAT_VERSION` stays `1` and lives in `crates/tinio-store/src/state.rs` — the table crate already owns it, so Task 4 **moves the driver around it**, never re-declares it.
- Every multi-table operation is one write transaction; a contract method that mutates rows commits them together (a `put_with_parts` writes the object row and its retained parts together; `move_` moves both).
- The contract traits are used **generically**, never as `dyn` (round 40's probe: the category methods' `where Self: Metadata` clause makes `Metadata` non-dyn-compatible); methods still take slices and argument structs rather than closures or generic parameters, so the surface stays uniform.
- Ordering is ascending by UTF-8 byte sequence; the spike pins `COLLATE "C"` **and** verifies the database encoding.
- Unit tests inline (`#[cfg(test)] mod tests`); conformance legs live in `tinio-util`'s `testing` module, whose entry points are public API for consumers outside the workspace (the spike).
- **P1 changes only the metadata-access layer of `tinio-fs`** — function boundaries, branches and error variants stay as they are, **except `load_bucket`**, whose signature becomes `async` (and which keeps its `read_blocking` hop); moving the operation bodies into `tinio-ops` is P4's.
- Git: every commit step asks the operator first (`docs/git.md`); no AI/agent trailers. The spike is never a workspace member and never in CI.
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

`VersionMeta` (declared in `meta/version.rs`, never a supertrait of `Metadata` — D20) carries `bucket_state`/`set_bucket_state`, **`current_slot(bucket, key) -> Option<(version::Id, u64)>`** (D37), the version row reads/writes (`put_version`, `get_version`, `list_versions`, `expire`) and the index lookup. `ObjectContent` (in `content.rs`) carries the surface fixed by the persistence spec's D23 — **the single definer is that spec's §1**; this plan implements it and the suite tests it:

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

- [ ] **Step 1: Re-baseline** — read the landed tree (the version tables and row shapes, the `object_meta` value tuple as the versioning set left it, mem's content tables, `tinio-version`'s ports) and record the findings in this task's notes; adjust Tasks 8 and 10 in place if they differ. The contracts' shape does not change.
- [ ] **Step 2: Write the failing contract tests** — in `meta/mod.rs`: a **shape probe** that instantiates the engine's generic form with one concrete store (`Ops<Store, Store, Store, Store, Blocks, Hooks>` with `Store` implementing `Metadata`/`VersionMeta`/`ObjectContent` — the shape probe A compiled) and the ordering pin (`"data" < "data-2" < "data/"`); in `meta/object.rs`: the moved self-heal pins for **both** traversals. Do **not** write a `dyn` probe: round 40's probe proved no `dyn Metadata` form exists.
- [ ] **Step 3: Run to verify they fail** — `cargo test -p tinio-core --lib meta`.
- [ ] **Step 4: Implement the contracts** — move the decode helpers out of the table crate into `meta`, add the row types (`meta::object::PartRow` included) with their conversions, and the traits above with a trait-level `ignore`d example each.
- [ ] **Step 5: Write the conformance suite** — `crates/tinio-util/src/testing/metadata.rs`, public entry points, the legs of spec §6: byte ordering (the adversarial pair), per-bucket boundaries, numeric part order, **both** traversal rules, per-method atomicity, drain idempotence, batch input order (`load`), the `put_guarded`/`remove_if_empty` refusals and projections, `publish` visibility, `open` range semantics, `rename`/`copy`/`stat`/`is_empty`/`create_bucket`/`remove_bucket`, the part key shape, `put_with_parts`/`parts`/`move_`, and **the 8 MiB staged leg** (64 KiB chunks, full + ranged read — D43).
- [ ] **Step 6: Run to verify they pass** — `cargo test -p tinio-core --lib`, then `cargo build -p tinio-util --features testing` (the suite compiles; the spike and Tasks 5–8 supply consumers).
- [ ] **Step 7: Gate and commit** — clippy, the doc gate, `cargo fmt`. Ask the operator, then `git commit -m "feat(core): add the persistence contracts and their conformance suite"`.

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

- [ ] **Step 1: Bring up PostgreSQL 18** — `docker-compose.yml` pins `postgres:18-alpine` with a named volume and a healthcheck; document `docker compose up -d` + `psql` verification in the README. If no PostgreSQL is reachable, **stop and ask** (D21).
- [ ] **Step 2: Write the schema** — key columns `text COLLATE "C"`, part numbers bigint, one row per bucket/upload/part, the version tables, the bucket versioning state, and the content/block tables. **Pin the forms the pg design defers to this spike** (PG18–PG23): the content `kind` discriminator and the live-generation marker, the collision-proof part namespace, the `blocks` `len`/`published` columns and the publish-completeness rule, and the bounded publish transaction (batching + incompleteness marker, or a declared cap). Record each choice in the findings.
- [ ] **Step 3: Implement the contracts in a library** — `src/lib.rs` holds the `tokio-postgres` implementation of the metadata contracts, `VersionMeta` and **the whole content surface (D23)**, plus the crate's `Error` (redacting the dsn); one statement per point operation, one transaction per mutating method, one statement for `remove_if_empty`'s probe-and-delete, `COLLATE "C"` on every ordered read, **bound parameters only**. `src/main.rs` is the gate driver.
- [ ] **Step 4: Run the gate** — `src/main.rs` runs `assert_metadata_conformance`, the content legs (including the 8 MiB leg) and its own raw-SQL self-heal leg (a garbage etag inserted directly reads as absent by key and fails a strict `walk`). All green, exit `0`; the version-row walkthrough stays a human-review item (Step 6).
- [ ] **Step 5: Resolve findings** — shape versus scope (D24); shape problems are fixed in `tinio-core` plus their suite leg in the same edit.
- [ ] **Step 6: Write the version-row walkthrough** — a per-method table (method, SQL shape, which property it satisfies) in the findings.
- [ ] **Step 7: Record the verdict** — `docs/superpowers/pg-meta-spike-findings.md`: the suite output, the schema seed, each finding with its resolution, the walkthrough, the collation/encoding pin, and every pg-design form this step pinned (PG18–PG23).
- [ ] **Step 8: Gate and commit** — confirm the spike is outside the workspace (`cargo metadata --no-deps --format-version 1 | rg 'pg-meta'` finds nothing) and in no CI job; ask the operator, then commit.

---

### Task 3: rename `tinio-store` → `tinio-store-redb`

**Files:**
- Modify: root `Cargo.toml`, `crates/tinio-store/Cargo.toml` → `crates/tinio-store-redb/Cargo.toml`, `crates/tinio-fs/src/lib.rs`, `crates/tinio-mem/src/lib.rs` (`pub extern crate tinio_store_redb as _store_redb;`), every `_store::` site (~35 across 14 files), **`docs/style.md` (both the alias rule's example *and* the error rule's crate path)**, `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md` (a one-line pointer; its record is not rewritten)

**Interfaces:**
- Produces: the crate `tinio-store-redb` with today's `tinio-store` public surface; alias `_store_redb` in both backends.

- [ ] **Step 1: Move and rename** — `git mv`, rename the package, update manifests and alias sites.
- [ ] **Step 2: Verify** — `rg -n 'tinio_store\b|_store::' crates` shows only intended hits; `cargo test -p tinio-store-redb -p tinio-fs -p tinio-mem` PASS, assertion-free.
- [ ] **Step 3: Gate and commit.**

---

### Task 4: the redb driver relocates into `tinio-store-redb`, and the pre-change fixture

**Files:**
- Create: `crates/tinio-store-redb/src/driver.rs`, `open.rs`, `compact.rs`, `crates/tinio-store-redb/tests/fixtures/pre-change-meta.redb`, `crates/tinio-store-redb/tests/open_pre_change_database.rs`
- Delete: `crates/tinio-fs/src/database/{handle,open,compact}.rs` (their tests move with them); `crates/tinio-fs/src/database/mod.rs` becomes a re-export shim
- Modify: `crates/tinio-store-redb/src/lib.rs`, `Cargo.toml` (`tokio`), `crates/tinio-fs/src/lib.rs` and the call sites importing `database::{Handle, open, compact}`

**Interfaces:**
- Produces: `tinio_store_redb::Driver` (today's fs `Handle`), `WriteLockSnapshot`, `open::open(state_dir)`, `compact::compact_if_needed`; fs re-exports them; **`crates/tinio-store/src/state.rs` keeps `FORMAT_VERSION`** (the driver reads it; nothing re-declares it).

- [ ] **Step 1: Move the code and its tests** — split `open.rs` along the policy line (state-dir layout, permissions and read-only relocation stay in `tinio-fs`; the redb half moves).
- [ ] **Step 2: Run the moved tests** — `cargo test -p tinio-store-redb`, then `-p tinio-fs`.
- [ ] **Step 3: Verify the error surface** — the fs/mem error tests and the conversion pins; identical lifts, paths and text.
- [ ] **Step 4: Verify the metrics surface** — the histogram tests; re-exported, not reimplemented.
- [ ] **Step 5: Commit the pre-change fixture (D41)** — generate a `meta.redb` with the **pre-change** binary (the commit before this plan's first task), copy it to `tests/fixtures/pre-change-meta.redb`, and write `open_pre_change_database.rs` to open it and read a row through the public implementations. This is the only artifact that proves "a database written before the change still opens" — the `--test layout` target the earlier drafts cited no longer exists.
- [ ] **Step 6: Gate and commit.**

---

### Task 5: `BucketMeta` over redb + the fs bucket facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/bucket.rs`
- Modify: `crates/tinio-store-redb/src/lib.rs` (`RedbMetadata` + constructors + the error pin), `Cargo.toml` (dev-dependency `tinio-util` with `testing`), `crates/tinio-store-redb/src/contract.rs`, `crates/tinio-fs/src/bucket.rs`, `crates/tinio-fs/src/database/tables.rs` (the strict traversal moves to the new impl's `walk`; the file is named by this task)

**Interfaces:**
- Consumes: Task 1's `BucketMeta`; Task 4's `Driver`.
- Produces: `tinio_store_redb::RedbMetadata { driver: Arc<Driver> }` with `open(state_dir)`, `in_memory()`, `driver(&self) -> Arc<Driver>`, **`type Error = tinio_store_redb::Error`** — implementing `Metadata` (this task the `BucketMeta` half) — and the resolution of `database/tables.rs`'s strict walk into that impl.

- [ ] **Step 1: Pin the error and wire the leg** — `RedbMetadata::Error = tinio_store_redb::Error`; each consumer maps it at its facade (fs: `Error::Redb(e.into())`; mem: its `DatabaseError` alias). Call `assert_bucket_meta_conformance` from `contract.rs`; run it (fails: no impl).
- [ ] **Step 2: Implement `BucketMeta`** — one read or write transaction per method; `remove_if_empty` probes `object_meta` and `uploads` in the transaction that removes the row, its refusals projecting to `NoSuchBucket`/`BucketNotEmpty`.
- [ ] **Step 3: Run to verify it passes.**
- [ ] **Step 4: Migrate the fs bucket facade** — bodies delegate; `cors`/`acl`/`tags` derive from `row`; the bucket-mutation lock stays in fs's `Storage` impl around the engine call (P4's rule, and today's lock placement is unchanged). `cargo test -p tinio-fs --lib bucket` PASS.
- [ ] **Step 5: Gate and commit.**

---

### Task 6: `ObjectMeta` over redb + the fs meta facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/object.rs`
- Modify: `crates/tinio-store-redb/src/contract.rs`, `crates/tinio-fs/src/{meta.rs,scanner.rs}` (the `load_bucket` await ripple)

**Interfaces:**
- Produces: the `ObjectMeta` half of the implementation, including `put_with_parts`/`parts`/`move_` (one transaction each) and **both traversals** (`walk` strict, `walk_healing`).

- [ ] **Step 1: Wire the legs** — `assert_object_meta_conformance`; the corrupt-row traversal legs from `contract.rs` (they plant a corrupt row through the raw table handles, which is why they live here).
- [ ] **Step 2: Implement `ObjectMeta`** — one transaction per method; `walk` and `walk_healing` (the strict and gated traversals the tree already has); `put_preserving` is today's `rewrite_preserving`; `put_guarded` reads the `buckets` row in the same transaction; `move_` moves the row and its parts together.
- [ ] **Step 3: Run to verify it passes.**
- [ ] **Step 4: Migrate the fs meta facade** — the accessors delegate; `ensure_row`'s stat gate, `md5_of_file` and `composed_keep` **stay in fs** (the engine calls them through `stat`/`open` in P4; here they keep working); `load_bucket` becomes `async` **and keeps its `read_blocking` hop** (a full-bucket traversal never runs inline on a runtime worker). `cargo test -p tinio-fs --lib meta` PASS.
- [ ] **Step 5: Gate and commit.**

---

### Task 7: `UploadMeta` over redb + the fs multipart facade

**Files:**
- Create: `crates/tinio-store-redb/src/impls/upload.rs`
- Modify: `crates/tinio-store-redb/src/contract.rs` (the upload legs + the aggregating `assert_metadata_conformance`), `crates/tinio-fs/src/multipart.rs`

**Interfaces:**
- Produces: the `UploadMeta` half of the implementation; the aggregating metadata leg.

- [ ] **Step 1: Wire the legs** — run, expected FAIL.
- [ ] **Step 2: Implement `UploadMeta`** — one transaction per method; `put_part` writes the `PARTS` row and its `PART_CHECKSUMS` row after the key-matching check; `create` writes the `UPLOADS` row and its `UPLOAD_CHECKSUMS` row together; `part_records` is the completion-time join in one read transaction.
- [ ] **Step 3: Run to verify it passes** — including the full `assert_metadata_conformance`.
- [ ] **Step 4: Migrate the fs multipart facade** — record access delegates; part files, the per-upload part lock, the assembly, the atomic writer and `idle_since` stay. The part lock's placement is P4's (through `Hooks::lock_part`), so here it keeps today's shape. `cargo test -p tinio-fs --lib multipart` PASS.
- [ ] **Step 5: Gate and commit.**

---

### Task 8: `VersionMeta` and the redb `ObjectContent`

**Files:**
- Create: `crates/tinio-store-redb/src/impls/version.rs`, `crates/tinio-store-redb/src/impls/content.rs`
- Modify: `crates/tinio-core/src/meta/version.rs` (the method list, derived from `tinio-version`'s `VersionRows` port and the landed tables), `crates/tinio-util/src/testing/metadata.rs`, `crates/tinio-store-redb/src/{contract.rs,lib.rs}` (the byte-table modules gain `#[doc(hidden)]`; their paths stay while `tinio-mem` still uses them — P2 privatizes them)

**Interfaces:**
- Produces: `impl VersionMeta for RedbMetadata` (including `current_slot`); `tinio_store_redb::RedbContent` implementing **the whole D23 content surface** (object and part keys, `rename`, `copy`, `scan` with the prefix bound, `stat`, `create_bucket`, `remove_bucket`, `is_empty`); the two legs.

- [ ] **Step 1: Derive `VersionMeta`'s method list** from the port and the landed tables, including `bucket_state`/`set_bucket_state`/`current_slot`; record it.
- [ ] **Step 2: Write/extend the failing legs** — version rows (put/get, order, lookup by id, delete-marker row, drain, `current_slot`); content (`publish` visibility, ranges, an overwrite, `remove`, a part round-trip, `rename`, `copy`, `scan` order + prefix bound + limit, `stat`, the bucket calls, `is_empty` with a directory-only bucket, and the 8 MiB staged leg).
- [ ] **Step 3: Implement** — one transaction per version-row method; `RedbContent` over the byte tables, `scan` delegating to the same walk the fs scanner uses (Task 9 wires fs's impl; here the redb one is the mem-side table scan).
- [ ] **Step 4: Run to verify they pass.**
- [ ] **Step 5: Gate and commit.**

---

### Task 9: `tinio-fs` migration and wiring

**Files:**
- Create: `crates/tinio-fs/src/content.rs` — the file `ObjectContent` implementing **all eleven** D23 calls: `stage` (temp file; the parent-symlink policy check and the folder-marker case), `publish` (the atomic rename; the unix directory hardening), `open` (the range reader), `remove` (the unlink), `rename` (the file move), `copy` (`copy_file_range` where available), `scan` (**delegating to the same streaming walk the scanner uses**, with the prefix pruning and the reserved-path filters — the batch accumulator stays in fs), `stat` (the key's file metadata), `create_bucket`/`remove_bucket`/`is_empty` (the directory operations; `is_empty` counts folder-marker directories)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`FsStorage` holds the concrete handles (`Arc<RedbMetadata>`, `Arc<FsContent>`) beside the `Driver` clone; the row-write policy is `Warn` — spec D39), the three facades' wiring, `crates/tinio-fs/src/{write,write_task,listing,cleanup,scanner,sweep}.rs` for call sites that touched a table or the raw handle
- Test: a facade-level test that a completed multipart object's metadata is present exactly when its bytes are served; a cold-listing test that a content key with no row still appears with its content ETag source available (`stat`)

**Interfaces:**
- Produces: `FsStorage`'s contract handles; the file `ObjectContent`; no fs module outside the facades touching `tinio-store-redb`'s table handles.

- [ ] **Step 1: Write the failing tests** — the visibility test and the cold-listing test above.
- [ ] **Step 2: Implement `ObjectContent` for fs** — the eleven calls over the existing `AtomicWriter`, layout and walk; the scanner's feed is unchanged (it keeps `walk_files_streaming`).
- [ ] **Step 3: Wire and migrate the call sites** — the metadata-access layer only, by domain; the bucket-mutation lock keeps today's placement.
- [ ] **Step 4: Prove no table access remains** — `rg -n '_store_redb::' crates/tinio-fs/src crates/tinio-mem/src` outside the implementations (enumerate the crate's public modules; the prefix `version` misses `bucket_versioning`).
- [ ] **Step 5: Full fs suite and the `@fs` pass** — PASS with import/await edits only.
- [ ] **Step 6: Gate and commit.**

---

### Task 10: re-point the version engine onto the contracts

**Files:**
- Modify: `crates/tinio-version/src/ports.rs` (delete `VersionRows` and `ContentPort`; keep `CurrentRows`), `crates/tinio-version/src/{lib,engine}.rs` (bounds become `M: Metadata + VersionMeta`, `C: ObjectContent<Error = storage::Error>`, `B: BlockStore`; every contract call site gains its `.map_err(Into::into)`), `Cargo.toml` (dependencies: `tinio-core`, `tinio-blob`, `async-trait`, `tokio` — **no feature, no redb, no fs**), `crates/tinio-fs/src/version_adapter.rs` (keeps `CurrentRows`: `VersionMeta::current_slot` + the F01/F04 gate), `crates/tinio-fs/src/backend/version.rs`, `crates/tinio-fs/Cargo.toml`

**Interfaces:**
- Produces: `tinio-version` depending on `tinio-core` and `tinio-blob` alone; `CurrentRows` as its one surviving port.

- [ ] **Step 1: Re-point** — replace the two deleted ports' bounds with the contract bounds; the engine's own signatures and tests are unchanged.
- [ ] **Step 2: Run the engine's tests** — `cargo test -p tinio-version` PASS with adapter-only edits.
- [ ] **Step 3: Run the fs version paths** — `cargo test -p tinio-fs --lib version`, then `-p tinio-fs` PASS. `list_object_versions` stays fs-side (spec D44).
- [ ] **Step 4: Prove the dependency cut** — `cargo tree -p tinio-version` shows **`tinio-core` and `tinio-blob`** (the block plane is the engine's storage) and no `redb`, `tinio-fs` or `tinio-store-redb`.
- [ ] **Step 5: Gate and commit** — the workspace gates plus `cargo hack check --package tinio-fs --each-feature` (no crate-root gates).

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
- `crates/tinio-store/src/state.rs` owns `FORMAT_VERSION`; Task 4 moves the driver **around** it and never re-declares it.
- `walk` (strict) and `walk_healing` are two methods because the tree has two behaviours; do not collapse them.
- `tinio-version` must not gain a redb or fs dependency in Task 10; `CurrentRows` is the one port that stays.
