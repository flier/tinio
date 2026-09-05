# Shared redb Table Layer (tinio-store) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the duplicated redb table layer — schema, row types, row-decode helpers, scan/drain helpers, and the five-variant redb error core — from tinio-fs and tinio-mem into a new shared crate `tinio-store`; tinio-fs relocates code with no on-disk format change, tinio-mem re-keys every table to tinio-fs's tuple-key convention.

**Architecture:** New workspace member `crates/tinio-store` (deps: `redb` + `tinio-core` + `thiserror`) hosts: `tables.rs` (seven shared `TableDefinition`s, the `table_impl!` macro, the seven named per-table handle structs with their accessor methods, and `StoredMeta` + `validate_stored` — the row decode delegates to tinio-core's wire codecs, whose leaf names derive via parse-display), `scan.rs` (the per-prefix keep-predicate scan/drain helpers), and `error.rs` (the five-variant redb error core `tinio_store::error::Error` — thiserror `#[from]`, the repo error convention; fs/mem nest it with derive_more `#[from(forward)]` hops). tinio-fs's `database/tables.rs` shrinks to re-exports + the fs-local `STATE` table + fs-specific accessors; its `database/error.rs` nests the shared error behind the surviving fs-only variants (`Compaction`/`Io`/`UnsupportedVersion`/`CorruptMeta`). tinio-mem drops its string-key machinery (`bucket\0key`, zero-padded part numbers, `band_start`), adopts the shared tables + handles for the seven common tables, re-keys its three local content tables to tuple keys, and aliases `DatabaseError` onto the shared error.

**Tech Stack:** Rust 2024, redb 4.2 (workspace), tinio-core (contract + domain types), derive_more 2 (workspace — `#[from(forward)]` for the fs/mem nesting hops; project rule 2026-09-03: no hand-written `From` impls), parse-display (workspace — already derives the wire-name codecs in tinio-core), tokio async tests, tinio-util conformance harness, cucumber (`@fs`/`@mem` passes).

**Spec:** `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md` — approved in brainstorming + grilling 2026-09-03 (Q1–Q9). The plan argues from the spec; executors read both.

## Global Constraints

- **English only** — code, comments, docs (project rule).
- **No git writes** — never commit/push; leave changes in the tree; report at checkpoints; the user commits (project rule).
- **TDD** — failing test → implement → passing test per task. Refactor/motion tasks validate with the existing suites: suites must stay green through the task; where a suite needs edits (mem re-key, error variants), the edited tests land first and fail, then the code change makes them green.
- **Derives over hand-written `From`** (user rule, 2026-09-03): error conversions derive — never hand-written `impl From`. Where a variant's field **is** the source type, thiserror's `#[from]` does it (`tinio_store::error::Error`'s five redb variants — thiserror is the repo error convention); where a variant **nests** a convertible inner error (fs `Redb(tinio_store::error::Error)`, mem `Database(DatabaseError)`), derive_more `#[from(forward)]` emits the hops (`From<T>` for every `T` the inner converts from) [**SUPERSEDED (E0119 ruling):** `#[from(forward)]` was rejected for fs AND mem; wrappers are thiserror-only with explicit constructor hops.] and `#[from(skip)]` keeps ctor-built struct variants out of the derive. Reclassification impls — fs crate-level `From<database::Error>` (Io-unwrap match), mem `From<io::Error>`/`From<etag::Error>`/`From<ParseIntError>` (storage-ctor hops) — are semantic mappings, not forwarding boilerplate, and survive; thiserror keeps `#[error]` display + `Error`/`source`.
- **Async tests** — `#[tokio::test]` directly (project rule).
- **On-disk format is a contract** — the seven shared table names, tuple arities, and ordering semantics written to `meta.redb` must be byte-identical to today (spec Goal 4). The fs schema-guard test added in Task 3 pins the table-name set.
- **Execution gate (user-confirmed, grilling 2026-09-03 Q1=B)**: do **not** begin executing this plan until the in-flight 2026-08-31 tagging/conditions work is committed on `dev` — the two changes touch the same uncommitted files (fs/mem row layer), and separate commits keep the review/revert boundary clean. The spec's "first task reconciles" clause is **not** triggered either way: the working tree's rows already match the spec's post-tagging shapes (fs `MetaValue` 6-tuple with tags + checksum at `database/tables.rs:269`; mem `OBJECT_META` 5-tuple at `storage.rs:65`; `OBJECT_PARTS` present in both), and committing the tagging work does not change row shapes. Full `cargo test --workspace` / cucumber green may additionally depend on the tagging plan's tinio-server-layer tasks completing; per-crate gates (`cargo test -p tinio-store/-p tinio-fs/-p tinio-mem`) are self-sufficient.
- **Line references drift** — the tree is dirty mid-flight. Locate symbols by name; the cited lines are approximate anchors.
- **No version compatibility** — dev-local DBs are disposable; no migration paths (spec Non-goals).
- **mem state is ephemeral** — in-memory redb, fresh per boot; re-keying has zero migration cost.

---

### Task 1: `tinio-store` crate + shared error core; fs error rewiring

Creates the crate skeleton and the first shared module (`error.rs`), then rewires tinio-fs's `database::Error` to nest it. This is the shared crate's first consumer (spec risk row: "lands with its first consumer in the same task").

**Files:**
- Create: `crates/tinio-store/Cargo.toml`
- Create: `crates/tinio-store/src/lib.rs`
- Create: `crates/tinio-store/src/error.rs` (+ its test module)
- Modify: `crates/tinio-fs/Cargo.toml` (add `tinio-store.workspace = true`)
- Modify: `crates/tinio-fs/src/database/error.rs` (drop the five redb variants; add the nesting variant — its `From` family derives via derive_more `#[from(forward)]`, nothing hand-written)
- Test: `crates/tinio-fs/src/database/tests.rs` (adapt the redb-variant tests), `crates/tinio-fs/src/error.rs` test module (adapt `converts_into_contract_error`)

**Interfaces:**
- Produces (consumed by Tasks 2-6):
  - `pub enum tinio_store::error::Error { Open(#[from] redb::DatabaseError), Transaction(#[from] redb::TransactionError), Table(#[from] redb::TableError), Storage(#[from] redb::StorageError), Commit(#[from] redb::CommitError) }` — `#[derive(Debug, thiserror::Error)]` (thiserror's `#[from]` derives the five conversions — no hand-written impls; the repo error convention), display strings verbatim from today: `"database error: {0}"`, `"transaction error: {0}"`, `"table error: {0}"`, `"storage error: {0}"`, `"commit error: {0}"`.
  - fs `database::Error` variant `Redb(#[error(transparent)] tinio_store::error::Error)` — `#[from(forward)]` on the variant derives `From<T>` for every `T` the shared error converts from, so the five redb hops into fs `Error` derive too (the wrapper's name is `Redb`).
- Consumes: nothing from later tasks; `redb`, `thiserror`, `tinio-core` workspace deps (derive_more enters only the fs/mem nesting errors, Tasks 1 Step 6 / 5 — not tinio-store).

- [x] **Step 1: Write the failing shared-crate tests** — create the crate so tests compile and fail on the missing type:

`crates/tinio-store/Cargo.toml`:
```toml
[package]
name = "tinio-store"
version.workspace = true
edition.workspace = true
description = "Shared redb table layer for the tinio storage backends"
publish = false

[dependencies]
redb.workspace = true
thiserror.workspace = true

tinio-core.workspace = true

[lints.rust]
unsafe_code = "forbid"
```

`crates/tinio-store/src/lib.rs`:
```rust
//! Shared redb table layer for the tinio storage backends.
//!
//! tinio-fs (on-disk `meta.redb`) and tinio-mem (`InMemoryBackend`) both
//! persist the same derived-metadata rows in redb tables; the schema,
//! per-table handles, and scan/drain helpers live here so a schema
//! change lands once (row decoding delegates to tinio-core's wire
//! codecs — the parse-display-derived names). See
//! `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md`.

pub mod error;
```

`crates/tinio-store/src/error.rs` test module (write first):
```rust
#[cfg(test)]
mod tests {
    use redb::{
        CommitError::TransactionPoisoned,
        DatabaseError::DatabaseAlreadyOpen,
        StorageError::{Corrupted, ValueTooLarge},
        TableError::TableDoesNotExist,
        TransactionError::Storage as TxnStorage,
    };

    use super::*;

    #[test]
    fn every_variant_wraps_its_redb_kind() {
        let cases: [(Error, &str); 5] = [
            (Error::from(DatabaseAlreadyOpen), "database error:"),
            (
                Error::from(TxnStorage(ValueTooLarge(1))),
                "transaction error:",
            ),
            (Error::from(TableDoesNotExist("x".into())), "table error:"),
            (Error::from(Corrupted("boom".into())), "storage error:"),
            (Error::from(TransactionPoisoned), "commit error:"),
        ];
        for (err, prefix) in cases {
            assert!(err.to_string().starts_with(prefix), "{err}");
        }
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<Error>();
    }
}
```

- [x] **Step 2: Run to verify they fail**

Run: `cargo test -p tinio-store`
Expected: FAIL — `error` module has no `Error` enum (the crate is new; add the two files above first so the failure is the missing type, not a missing file).

- [x] **Step 3: Implement `error.rs`** — verbatim from fs `database/error.rs:9-28` today (drop `Compaction` and `Io`), swapping the doc header to the shared layer — thiserror with `#[from]` per variant, the repo error convention (a field **is** its source type here; derive_more enters only the fs/mem nesting hops):

```rust
//! The shared redb error core: the five redb-mapping variants that
//! tinio-fs and tinio-mem nest/alias (spec 2026-09-03, grilling Q6).
//! fs-lifecycle variants (`Compaction`/`Io`/`UnsupportedVersion`/
//! `CorruptMeta`) stay in tinio-fs; tinio-mem has no compaction, version
//! gate, or db-layer file I/O.

/// A redb failure: open, transaction, table, storage, or commit.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database open/create failed.
    #[error("database error: {0}")]
    Open(#[from] redb::DatabaseError),
    /// A transaction failed.
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Opening a table failed.
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    /// A get/insert/range failed.
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    /// Commit failed.
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
}
```

Note: `use std::io;` is not needed — omit it. The five `#[from]` conversions are what let shared-crate table methods `?` raw redb errors (Tasks 2, 4) — and what the fs/mem `#[from(forward)]` hops ride on (Task 1 Step 6, mem alias task).

- [x] **Step 4: Run to verify they pass**

Run: `cargo test -p tinio-store`
Expected: PASS.

- [x] **Step 5: Adapt the failing fs error tests first** — tinio-fs tests that construct a redb-mapped variant by its old path now fail; adapt them before the rewiring so the suite shows the red:

In `crates/tinio-fs/src/database/tests.rs` — `redb_errors_wrap_per_kind` and any sibling test constructing `Error::from(TableDoesNotExist(...))`/`TxnStorage(...)`/`ValueTooLarge(...)` (approx. L39-49) becomes wrapper-path:
```rust
    let err = database::Error::from(TableDoesNotExist("x".into()));
    assert!(matches!(err, database::Error::Redb(tinio_store::error::Error::Table(_))), "{err}");
```
(Adjust names to the local imports of that test module: it uses `super::*`, so `Error::` refers to `database::Error` — the assertion must match the new `Redb(...)` nesting.)

In `crates/tinio-fs/src/error.rs` test module — `converts_into_contract_error` (approx. L179-199) imports `database::Error::{Open, UnsupportedVersion}` (L124) and builds `let db_err: Error = Open(DatabaseAlreadyOpen).into();`. `Open` no longer exists on `database::Error`; change the import to `database::Error::{Redb, UnsupportedVersion}` and the construction to:
```rust
        let db_err: Error = Redb(DatabaseAlreadyOpen.into()).into();
```

Run: `cargo test -p tinio-fs`
Expected: FAIL — `database::Error` has no `Open` variant (and no `Redb` yet).

- [x] **Step 6: Rewire `database/error.rs`** — delete the five redb variants; the nesting `Redb` variant's `From` family derives via derive_more (`#[from(forward)]`), so no forwarding impls are written by hand:

`crates/tinio-fs/src/database/error.rs` — delete the `Open`/`Transaction`/`Table`/`Storage`/`Commit` variants (L10-28) and their doc comments; keep `Compaction`, `Io`, `UnsupportedVersion`, `CorruptMeta` and the two ctor fns. Add the `From` derive to the enum header and the `Redb` variant:

```rust
/// A redb or state-database failure.
///
/// Conversions derive via derive_more: per-field for `Io`/`Compaction`;
/// `#[from(forward)]` on `Redb` emits `From<T>` for every `T` the shared
/// error converts from — all five redb errors hop into `Error::Redb` in
/// one step. (`From` is not transitive, but the forward derive makes the
/// direct hop; nothing is hand-written.)
#[derive(Debug, thiserror::Error, derive_more::From)]
pub enum Error {
    /// Filesystem I/O around the state database.
    #[error("I/O error: {0}")]
    Io(io::Error),
    /// A compaction failed.
    #[error("compaction error: {0}")]
    Compaction(redb::CompactionError),
    /// The `STATE` table version does not match.
    #[error(
        "unsupported {} version {found} (expected {expected})",
        .path.display()
    )]
    #[from(skip)]
    UnsupportedVersion {
        /// The state file path.
        path: PathBuf,
        /// The version read from disk.
        found: u64,
        /// The supported version.
        expected: u64,
    },
    /// A stored `OBJECT_META` row failed domain validation (key or etag).
    #[error("corrupt object_meta entry for key `{key}`: {source}")]
    #[from(skip)]
    CorruptMeta {
        /// The raw key as stored.
        key: String,
        /// The domain validation failure (`InvalidKey` / etag parse).
        #[source]
        source: storage::Error,
    },
    /// A shared redb failure (the five mapping kinds of
    /// [`tinio_store::error::Error`]).
    #[error(transparent)]
    #[from(forward)]
    Redb(tinio_store::error::Error),
}
```

`#[from(skip)]` on the two struct variants stops derive_more emitting `From<(PathBuf, u64, u64)>` / `From<(String, storage::Error)>` — those rows are built by the ctor fns only. `#[source]` and the `#[error]` strings stay thiserror's (display text byte-identical; `Redb` displays transparently as the shared error, whose strings are unchanged). The derive emits `From<io::Error>` and `From<redb::CompactionError>` from the field types, so the `?` sites in `open.rs`/`handle.rs`/`compact.rs`/`tables.rs` keep compiling unchanged. Coherence: the `Redb` forward blanket does not overlap the sibling derives — neither `io::Error` nor `redb::CompactionError` converts into `tinio_store::error::Error` (if the shared core ever gained an `Io`/`Compaction` kind, that hop would need its own wrapper or a split error).

Add `tinio-store.workspace = true` to `crates/tinio-fs/Cargo.toml` `[dependencies]` (derive_more is already there, `features = ["full"]`).

The crate-level lift (`crates/tinio-fs/src/error.rs:76-86`, `DatabaseError::Io(e) => Error::Io(e), other => Error::Database(other)`) is **unchanged** — a semantic reclassification (Io-unwrap), not forwarding boilerplate; `Redb` and the fs-local variants fall into the `other` arm.

- [x] **Step 7: Run to verify they pass**

Run: `cargo test -p tinio-fs`
Expected: PASS (unit + database + layout + proptest + conformance). `cargo test --workspace` still passes if the in-flight tagging server work is complete; if not, `-p tinio-fs` is the gate for this task.

- [x] **Step 8: Checkpoint** — report; do not commit.

---

### Task 2: Shared tables + scan modules; tinio-fs relocates onto them

Moves the seven table definitions, the `table_impl!` macro, the named handle structs with their accessor methods, `StoredMeta`/`validate_stored`, and the scan/drain helpers into tinio-store; tinio-fs's `database/tables.rs` shrinks to re-exports, the fs-local `STATE` table, and fs-specific accessors. The wire codecs are **not** moved — the in-flight 2026-08-31 tagging plan already centralized them in tinio-core (`object::Tags::parse_wire_limited`, `checksum::Recorded`, with the wire-name spellings derived via parse-display); shared `validate_stored` delegates to them exactly as fs `database/tables.rs` does today. This is pure code motion — fs behavior suites are the gate and must stay green with import-only edits.

**Files:**
- Create: `crates/tinio-store/src/tables.rs` (defs + macro + handles + `StoredMeta`/`validate_stored` + tests)
- Create: `crates/tinio-store/src/scan.rs` (the drain/for_each helpers, moved from fs `database/scan.rs`)
- Modify: `crates/tinio-store/src/lib.rs` (`pub mod tables; pub mod scan;`)
- Modify: `crates/tinio-fs/src/database/tables.rs` (shrink: re-exports + `STATE` + fs-specific accessors)
- Modify: `crates/tinio-fs/src/database/mod.rs` (re-export list unchanged in names; sources adjust)
- Delete: `crates/tinio-fs/src/database/scan.rs` (and its `mod scan;` declaration in `database/mod.rs`)
- Test: new shared-crate unit tests (ensure, scan boundaries); fs suites unchanged

**Interfaces:**
- Produces (consumed by Task 5, and by fs call sites through `database::` re-exports, which keep today's names):
  - `tinio_store::tables::{BUCKETS, OBJECT_META, UPLOADS, PARTS, UPLOAD_CHECKSUMS, PART_CHECKSUMS, OBJECT_PARTS}` — `TableDefinition` consts; type aliases `BucketKey`/`BucketValue`/`MetaKey`/`MetaValue`/`UploadKey`/`UploadValue`/`PartKey`/`PartChecksumValue`/`UploadChecksumValue`/`ObjectPartKey`/`ObjectPartValue`; handle structs `BucketsTable`/`ObjectMetaTable`/`UploadsTable`/`PartsTable`/`UploadChecksumsTable`/`PartChecksumsTable`/`ObjectPartsTable` with today's methods (see Step 2's move list) — every method's error type becomes `tinio_store::error::Error`.
  - `tinio_store::tables::{StoredMeta, validate_stored}` — the row decode delegates to tinio-core's codecs (`Tags::parse_wire_limited`, `checksum::Recorded::to_wire`/`from_wire_opt`, parse-display-derived wire names) and the tag caps stay core's `object::{OBJECT_TAGS_MAX, BUCKET_TAGS_MAX}`; no codec code relocates into tinio-store.
  - `tinio_store::scan::{for_each_pair, drain_pair, drain_triple}` (signatures from fs `database/scan.rs`, error type swapped to `tinio_store::error::Error`).
  - `table_impl!` macro exported `pub` (mem's three local content tables use it in Task 5).
- Consumes: `tinio_store::error::Error` (Task 1).

- [x] **Step 1: Write the failing shared-crate tests** — the scan tests are new and exercise the moved code directly (the wire-codec round-trips are already covered in tinio-core — `checksum.rs` `recorded_wire_round_trips` and the object tags tests, the parse-display-derived names; tinio-store owns no codec to test). Write the scan tests in `scan.rs`'s test module (`tinio_store::lib.rs` needs no `_core` alias — tests import `tinio_core::…` directly); `tables.rs` gets its failing test in Step 6 (`ensure_all`'s table-name set):

```rust
// scan.rs test module
#[cfg(test)]
mod tests {
    use redb::{Database, ReadableTable, TableDefinition};

    use super::*;

    fn mem_db() -> Database {
        Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap()
    }

    #[test]
    fn drain_pair_removes_only_the_matching_prefix() {
        let table: TableDefinition<(&'static str, &'static str), &'static str> =
            TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "a"), "1").unwrap();
                t.insert(("data", "b"), "2").unwrap();
                t.insert(("other", "c"), "3").unwrap();
            }
            txn.commit().unwrap();
        }
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                drain_pair(&mut t, ("data", ""), |b, _| b == "data").unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        let keys: Vec<_> = t.iter().unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(keys, [("other", "c")]);
    }

    #[test]
    fn for_each_pair_stops_before_a_longer_prefix_key() {
        // Bucket "data" must never leak into "data-x": the scan starts
        // at the lower bound and stops at the first key failing `keep`
        // (the no-exclusive-upper-bound ruling, redb-notes pit 14).
        let table: TableDefinition<(&'static str, &'static str), u64> =
            TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "a"), 1).unwrap();
                t.insert(("data", "x"), 2).unwrap();
                t.insert(("data-x", "b"), 3).unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        let mut seen: Vec<(String, String)> = Vec::new();
        for_each_pair(&t, ("data", ""), |b, _| b == "data", |b, k, _| {
            seen.push((b.to_string(), k.to_string()));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [("data".into(), "a".into()), ("data".into(), "x".into())]);
    }
}
```

(If the moved `for_each_pair`'s `keep`/`visit` closure shapes differ from the fs inventory's `(|b, _| …)` two-arg keep / `(|b, k, v| …)` three-arg visit, adapt the test closures to the real signature — the assertions stay.)

- [x] **Step 2: Implement `scan.rs` and `tables.rs` in tinio-store**

Move verbatim from fs, with two mechanical adaptations:
1. Every `database::error::Error` (fs) return type becomes `tinio_store::error::Error`; every `Error` in method bodies resolves to the shared crate's own `error::Error` (no import change needed inside tinio-store).
2. Table-handle method signatures take **plain `&str` row elements**, exactly as the fs row keys do today — core `bucket::Name`/`object::Key` deref to `&str` at the fs call sites, so the shared methods keep fs's parameter shapes (`get(bucket: &str, key: &str)` etc.) to avoid forcing deref changes through every store. Verify each moved method against its fs call sites in Step 4 and keep the call-site text compiling unchanged.

**`scan.rs`** — move fs `database/scan.rs` wholesale (the `drain_impl` macro, `drain_pair`, `drain_triple`, `for_each_pair`; ~93 lines), keeping the keep-predicate boundary semantics and the "collect owned keys first, then remove" drain order (redb has no bulk range delete). Doc comments keep the no-exclusive-upper-bound ruling (redb-notes pit 14).

**`tables.rs`** — the seven table blocks + their method impls, moved table by table from fs `database/tables.rs`. Per-table move list (fs line anchors; method names as inventoried 2026-09-03):
- `BUCKETS` (fs L168-259): `BucketKey`/`BucketValue` aliases, const, `BucketsTable`, `table_impl!`, methods `get`/`row`/`for_each`/`put`/`put_full`/`get_or_insert`/`remove`.
- `OBJECT_META` (fs L261-440): `MetaKey`/`MetaValue`, `StoredMeta` (fields stay `pub` — fs `etag.rs` tests build literals), `validate_stored` (moves with its body untouched — it already calls core's `Tags::parse_wire_limited`/`Recorded::from_wire_opt`; only the import path changes), methods `get`/`for_bucket_gated`/`put`/`remove`/`drain_bucket` — the strict `for_bucket` **stays in fs** as the free fn `for_bucket_strict` (it constructs the fs-only `CorruptMeta`; Step 3 pins the outcome).
- `UPLOADS` (fs L444-570): `UploadKey`/`UploadValue`, methods `has_bucket`/`key_matches`/`get_matching`/`for_bucket`/`for_each`/`put`/`remove`/`drain_bucket`.
- `PARTS` (fs L576-653): value `&'static str` (etag hex), methods `get_hex`/`list_from`/`put`/`drain_bucket`/`drain_upload`.
- `UPLOAD_CHECKSUMS` (fs L665-716), `PART_CHECKSUMS` (fs L724-790), `OBJECT_PARTS` (fs L805-877): methods `get`/`put`/`remove`(+`drain_bucket`/`drain_upload`/`list`/`remove_key` per table, as today).
- No codec block moves — fs `database/tables.rs` has none today: the 2026-08-31 tagging plan moved the wire codecs into tinio-core, and `validate_stored`/the read paths already delegate to `object::Tags::parse_wire_limited` + `checksum::Recorded` (the wire-name spellings derive via parse-display in core); the tag caps are core's (`object::{OBJECT_TAGS_MAX, BUCKET_TAGS_MAX}`, re-exported through fs `database/mod.rs` today, untouched).
- `table_impl!` (fs L22-87) moves verbatim; its generated `open`/`ensure`/`open_readonly` return `tinio_store::error::Error`. Keep the `no_ensure` arm (fs `STATE` uses it — fs re-invokes the macro locally for `STATE` in Step 3; the arm must remain).

The `// --- NAME ---` section banners and doc comments move with their blocks. Public items that both backends need are `pub` in tinio-store (fs gates re-exports; mem imports directly).

- [x] **Step 3: Shrink fs `database/tables.rs`**

The file becomes: the fs-local `STATE` table (const `STATE`, `StateKey`/value `u64`, `StateTable` with its private `stored`, `compact_marker`, `ensure_version`, `set_compact_marker_value` — fs L882-943, re-invoking the shared `table_impl!` with `no_ensure`), re-exports of everything else the crate uses, and any fs-specific free fns. Re-export set (names unchanged from today's `mod.rs` surface so no fs caller edits):

```rust
pub use tinio_store::tables::{
    BUCKETS, BucketsTable, OBJECT_META, ObjectMetaTable, OBJECT_PARTS,
    ObjectPartsTable, PARTS, PART_CHECKSUMS, PartChecksumsTable, PartsTable,
    StoredMeta, UPLOADS, UPLOAD_CHECKSUMS, UploadChecksumsTable, UploadsTable,
    validate_stored,
};
pub(crate) use tinio_store::scan::{drain_pair, drain_triple, for_each_pair};
```

(The tag caps are NOT in this list — they stay core items re-exported by fs `database/mod.rs`'s existing `pub(crate) use crate::_core::object::{BUCKET_TAGS_MAX, OBJECT_TAGS_MAX}` line, which is untouched.)

(Adjust the public/`pub(crate)` split to fs `database/mod.rs`'s existing re-export list — `pub use tables::{ObjectMetaTable, StoredMeta}` stays the public pair; everything else stays `pub(crate)`. fs `database/mod.rs`'s `pub(crate) use tables::{…}` list then only needs its source module name to remain `tables`, which it does.)

**The strict `for_bucket` outcome (verified 2026-09-03)** — `rg "\.for_bucket\("` shows two distinct methods: `UploadsTable::for_bucket` (tables.rs:511, plain row decode, used by `multipart.rs:1045,1120` — moves to shared as-is, no validation to fail) and `ObjectMetaTable::for_bucket` (tables.rs:340, strict — corrupt rows **fail the walk** with `CorruptMeta`). The strict variant's only external caller is `meta::Store::walk` (meta.rs:812-831, the scanner reclamation pass + doctor's meta-orphan check), whose doc states "Corrupt rows fail the walk (`CorruptMeta`) — the reclamation and doctor callers must not skip them". It **cannot move to shared** (it constructs the fs-only `CorruptMeta`), so keep it as an fs free fn in `tables.rs` over the shared handle's `Deref` (the `table_impl!` `@deref` arm gives the handle `Deref` to the raw redb table, so the fn iterates rows directly):
```rust
/// Strict per-bucket walk: a corrupt row fails with `CorruptMeta`
/// (the scanner reclamation pass and doctor must not skip them —
/// meta.rs `Store::walk`).
pub(crate) fn for_bucket_strict(
    table: &ObjectMetaTable<'_, impl redb::ReadableTable<MetaKey, MetaValue>>,
    bucket: &bucket::Name,
    mut visit: impl FnMut(object::Key, ETag, u64, u64) -> Result<(), Error>,
) -> Result<(), Error>
```
with the body moved from fs's `ObjectMetaTable::for_bucket` (raw-key/etag parse failures construct `corrupt_meta(key, …)` as today; `validate_stored`'s self-heal is NOT used here — the strict path needs the raw row).

Delete `database/scan.rs` and its `mod scan;` line in `database/mod.rs`.

- [x] **Step 4: Run the fs suite (unchanged tests)**

Run: `cargo test -p tinio-fs`
Expected: PASS with **no fs test edits in this step** — the re-export shims keep every `database::{ObjectMetaTable, StoredMeta, …}` path alive (the caps keep flowing through `database::{BUCKET_TAGS_MAX, OBJECT_TAGS_MAX}`, core re-exports); `cargo check -p tinio-fs --benches` passes too (benches/meta.rs uses `ObjectMetaTable::open` through the same path).

- [x] **Step 5: Run the shared-crate tests**

Run: `cargo test -p tinio-store`
Expected: PASS (scan boundary + prefix-stop tests and the `ensure_all` name-set test; the `for_each_pair`/`drain_pair` types resolve against the moved code).

- [x] **Step 6: Shared `ensure` helper** — test first, then implement:

Write the failing test in the `tables.rs` test module (it asserts the on-disk name set — the same list Task 3's fs guard pins on the real `meta.redb`):
```rust
    #[test]
    fn ensure_all_creates_exactly_the_seven_tables_idempotently() {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        {
            let txn = db.begin_write().unwrap();
            ensure_all(&txn).unwrap();
            txn.commit().unwrap();
        }
        // Idempotent: a second open-time ensure on the same db is a no-op.
        {
            let txn = db.begin_write().unwrap();
            ensure_all(&txn).unwrap();
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let mut names: Vec<String> = txn
            .list_tables()
            .unwrap()
            .map(|h| h.name().to_string())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "buckets",
                "object_meta",
                "object_parts",
                "part_checksums",
                "parts",
                "upload_checksums",
                "uploads",
            ]
        );
    }
```
(If `TableDefinition` has no `ensure` on redb 4.2, call `txn.open_table(def)?` and drop the handle — `open_table` on a non-existent table creates it inside a write transaction; `list_tables` + `name()` are verified present on 4.2.)

Then add to `tables.rs` the one-transaction creation helper both backends call at open (fs `open.rs` calls it in the same write txn as `STATE`; mem calls it in Task 5):
```rust
/// Create the seven shared tables inside a write transaction
/// (idempotent). Backends create their local tables in the same
/// transaction alongside.
pub fn ensure_all(txn: &mut redb::WriteTransaction) -> Result<(), error::Error> {
    BUCKETS.ensure(txn)?;
    OBJECT_META.ensure(txn)?;
    UPLOADS.ensure(txn)?;
    PARTS.ensure(txn)?;
    UPLOAD_CHECKSUMS.ensure(txn)?;
    PART_CHECKSUMS.ensure(txn)?;
    OBJECT_PARTS.ensure(txn)?;
    Ok(())
}
```

Run: `cargo test -p tinio-store`
Expected: PASS.

- [x] **Step 7: Point fs `open.rs` at the shared ensure** — in `crates/tinio-fs/src/database/open.rs` (approx. L78-94) replace the seven per-table `ensure` calls (which now resolve to the shared handles' methods — they still work, but the shared helper is the single source):
```rust
    tinio_store::tables::ensure_all(&mut txn)?;
    state.ensure_version(&path)?;
```
Keep `StateTable::open` + `ensure_version` + `compact_marker` as today (STATE stays fs-local), and the `txn.stats()` snapshot.

- [x] **Step 8: Run the fs suite again + layout**

Run: `cargo test -p tinio-fs`
Expected: PASS — the suite includes `tests/layout.rs` (state-dir file layout + self-heal), which must pass unchanged. `cargo check --workspace` passes if the in-flight tagging server work allows; otherwise `-p tinio-fs -p tinio-store -p tinio-mem -p tinio-core -p tinio-util` is the task gate (mem is untouched by this task and keeps compiling on its own).

- [x] **Step 9: Checkpoint** — report the `for_bucket` branch taken and the final fs `tables.rs` item list; do not commit.

---

### Task 3: On-disk schema guard test + full fs verification

The spec's "byte-format guard" (spec §4): today **no** test asserts the on-disk schema (inventoried 2026-09-03 — `tests/layout.rs` only asserts file layout). Add the missing guard so the extraction's no-format-change claim is pinned by a test, then run the full fs verification set.

**Files:**
- Create/Modify: fs test asserting the table-name set of a freshly opened `meta.redb`
- Test: the new guard + full fs suite

**Interfaces:**
- Consumes: fs `database::open` (unchanged signature), `tinio_store::tables::ensure_all` (Task 2).
- Produces: the regression pin for any future schema-name change.

- [x] **Step 1: Write the failing guard test** — in `crates/tinio-fs/tests/layout.rs` (or a sibling `schema.rs` if the file's module layout prefers), using the existing test scaffold that opens a fresh `Open`:

```rust
#[test]
fn open_creates_exactly_the_schema_tables() {
    // The on-disk format contract: the seven shared tables plus the
    // fs-local STATE table, nothing else (spec 2026-09-03 Goal 4).
    let dir = tempfile::tempdir().unwrap();
    let opened = crate::open(dir.path()).unwrap();
    let txn = opened.db.begin_write().unwrap();
    let mut names: Vec<String> = txn
        .list_tables()
        .unwrap()
        .map(|h| h.name().to_string())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "buckets",
            "object_meta",
            "object_parts",
            "part_checksums",
            "parts",
            "state",
            "upload_checksums",
            "uploads",
        ]
    );
}
```

(Check the exact fs layout test scaffold (`tests/layout.rs`) for how it constructs a state dir + `Open` — mirror its constructor. `WriteTransaction::list_tables` returning `UntypedTableHandle`s with `name() -> &str` is verified present on redb 4.2.)

Run: `cargo test -p tinio-fs --test layout`
Expected: FAIL only if the scaffold needs the test added — otherwise the test passes immediately (it asserts current reality); if it fails, the extraction drifted the schema and must be fixed before proceeding.

- [x] **Step 2: Full fs verification**

Run: `cargo test -p tinio-fs` and `cargo check -p tinio-fs --benches`
Expected: PASS — unit, database, layout (file layout + the new schema guard), proptests, conformance, benches compile.

- [x] **Step 3: Checkpoint** — report; do not commit.

---

### Task 4: tinio-mem — error alias onto the shared core

Ships the `DatabaseError` alias **before** the re-key (Task 5): the re-key's shared-handle calls return `tinio_store::error::Error`, and the alias is what gives mem's `Error` its `From` conversion for those calls — `Error::Database` derives `From` via derive_more `#[from(forward)]` on the aliased field type. Grilling Q8. [**SUPERSEDED during execution (E0119 coherence ruling):** the alias uses thiserror field-level `#[from]`; raw-redb hops are explicit `Error::Database(e.into())` — no derive_more anywhere.]

**Files:**
- Modify: `crates/tinio-mem/Cargo.toml` (add `tinio-store.workspace = true` and `derive_more = { workspace = true, features = ["from"] }` [**SUPERSEDED (E0119 ruling):** `derive_more` was NOT added — mem `Error` is thiserror-only.])
- Modify: `crates/tinio-mem/src/error.rs` (alias + delete helpers + `derive_more::From` on the `Error` enum)
- Modify: `crates/tinio-mem/src/storage.rs`, `object.rs`, `multipart.rs` (the eight `database_storage` guard sites)
- Modify: `crates/tinio-mem/src/lib.rs` if the re-export path changes (it should not — `pub use crate::error::Error` and the `DatabaseError` name survive)
- Test: mem error tests adapt; the two helper-constructing tests migrate to tinio-store

**Interfaces:**
- Consumes: `tinio_store::error::Error` (Task 1).
- Produces: `DatabaseError` = alias of the shared type (consumers keep matching `DatabaseError::Open(..)` — variant paths unchanged under `tinio_store::error::`); mem `Error::Database` carries the shared enum via the alias and derives `From` with `#[from(forward)]` — the derived [**SUPERSEDED (E0119 ruling):** `derive_more` was NOT added — mem `Error` is thiserror-only.] `From<tinio_store::error::Error> for Error` **and** `From<redb::…> for Error` (through the shared error's own derived `From`s) is what the re-key needs.


- [x] **Step 1: Adapt the failing tests first** — in `crates/tinio-mem/src/error.rs` test module, delete `database_constructors_cover_every_variant` and `every_database_variant_projects_onto_contract_io` (their coverage — five variants construct + display prefix + contract-`Io` projection — now lives in the tinio-store tests from Task 1 plus `redb_errors_wrap_as_database`/`every_database_variant_wraps_and_displays`/`extras_project_onto_contract_io`, which survive unchanged on the alias: `matches!(err, Error::Database(DatabaseError::Storage(_)))` still compiles because `DatabaseError::Storage` resolves through the alias). `parse_int_error_funnels_through_storage` dies too — the re-key removes the string part keys, taking `From<ParseIntError>` with it (deleted in Step 2). Run the module tests to confirm which fail.

- [x] **Step 2: Implement the alias + deletions** in `crates/tinio-mem/src/error.rs`:
  1. Delete the local `pub enum DatabaseError` (L51-68) and its doc comment; keep the `Error` shell's `Database` variant (the alias keeps the name resolvable):
  ```rust
  /// A redb failure: the shared five-variant mapping core
  /// (tinio-store). The alias keeps the historical name.
  pub use tinio_store::error::Error as DatabaseError;
  ```
  2. Delete the five `database_*` helper fns (L220-248).
  3. Delete the five `From<redb::…> for Error` impls (L88-116) and add the `From` derive to the `Error` enum — `#[derive(Debug, thiserror::Error, derive_more::From)]`: the `Database` variant swaps thiserror's `#[from]` for derive_more's `#[from(forward)]`, and `Storage` drops its `#[from]` (the derive emits `From<storage::Error>` from the field type):
  ```rust
  /// An in-memory backend failure.
  ///
  /// Conversions derive via derive_more: `#[from(forward)]` on `Database`
  /// emits `From<T>` for every `T` the shared error converts from — the
  /// five redb hops into `Error::Database` derive; nothing is
  /// hand-written (thiserror keeps `#[error]` only).
  #[derive(Debug, thiserror::Error, derive_more::From)]
  pub enum Error {
      /// A contract-domain failure.
      #[error(transparent)]
      Storage(storage::Error),
      /// A redb database failure.
      #[error(transparent)]
      #[from(forward)]
      Database(DatabaseError),
  }
  ```
  (A raw redb `err.into()` now resolves through the forward derive: redb error → shared error → `Error::Database`.) The `From<io::Error>`/`From<etag::Error>` impls (L70-86) survive unchanged — semantic reclassifications into `Error::Storage` via the storage ctors, not forwarding boilerplate; `From<ParseIntError>` (L82-86) dies with the string part keys (delete the impl and its `parse_int_error_funnels_through_storage` test together).
  4. The `From<Error> for storage::Error` projection (L118-125) is **unchanged** (`Error::Database(e) => Io(IoError::other(e))` — `e` is now the shared type, still `Display`).

- [x] **Step 3: Replace the eight guard sites** — each `database_storage(e)` in `storage.rs:377,409`, `object.rs:465,668,720`, `multipart.rs:606,630` becomes a plain `.into()` (no constructor hop):
```rust
                Some(Err(e)) => return Err(e.into()),
```
(`e` is a raw redb error; `.into()` resolves through the `Database` variant's `#[from(forward)]` derive — redb error → shared error → `Error::Database`. Remove the now-unused `database_storage` imports.)

- [x] **Step 4: Run the suites**

Run: `cargo test -p tinio-mem` and `cargo test -p tinio-store`
Expected: PASS — mem unit + conformance; the tinio-store error tests already cover the migrated cases.

- [x] **Step 5: Checkpoint** — report; do not commit.

---

### Task 5: tinio-mem — full re-key onto the shared tables

Runs **after** the error alias (Task 4): shared-handle calls return `tinio_store::error::Error`, which converts into mem `Error` through the aliased `Database` field's derived `From` (`#[from(forward)]`) [**SUPERSEDED (E0119 ruling):** `#[from(forward)]` was rejected for fs AND mem; wrappers are thiserror-only with explicit constructor hops.]. tinio-mem's `storage.rs` table section is replaced by the shared definitions for the seven common tables plus three re-keyed local content tables; string-key machinery (`band_start`, `\0`-concatenated keys, zero-padded part numbers) is deleted; every scan/drain runs through the shared helpers; ops modules (`bucket.rs`, `object.rs`, `multipart.rs`) are updated to the tuple rows; tests are re-keyed. mem state is ephemeral — a wrong key shape fails loudly in tests, never corrupts user data. This is the largest task; the crate does not compile between Steps 2 and 5 (accepted — mirror the tagging plan's Task 2 note).

**Files:**
- Modify: `crates/tinio-mem/src/storage.rs` (tables → shared + three local; open via shared ensure; delete key builders/band_start/string scans)
- Modify: `crates/tinio-mem/src/bucket.rs`, `object.rs`, `multipart.rs` (row access → shared handles/keys)
- Test: re-keyed raw-row tests across the four modules; conformance

**Interfaces:**
- Consumes: `tinio_store::tables::*` (Task 2): the seven consts, `StoredMeta`, `validate_stored` (decode delegates to core's codecs — `Tags::parse_wire_limited`, `checksum::Recorded::to_wire`/`from_wire_opt`; caps from `tinio_core::object`), the handle structs' methods, `table_impl!` (for the three local tables), `scan::{for_each_pair, drain_pair, drain_triple}`, `error::Error`, `ensure_all`; mem's aliased `DatabaseError` (Task 4) for `?` on shared results.
- Produces: mem row layout identical to fs's convention.


**Target row layout** (spec §3 — the "after" column):

| mem table | key | value |
|---|---|---|
| `BUCKETS` (shared) | `name` | `(created_at_nanos, tags_wire)` |
| `OBJECT_META` (shared) | `(bucket, key)` | `(etag_hex, size, mtime_nanos, file_identity=0, tags_wire, checksum_wire)` |
| `UPLOADS` (shared) | `(bucket, upload_id)` | `(key, initiated_at_nanos, tags_wire)` |
| `PARTS` (shared) | `(bucket, upload_id, part_number)` | `etag_hex` |
| `UPLOAD_CHECKSUMS` (shared) | `(bucket, upload_id)` | `(algorithm_wire, checksum_type_wire)` |
| `PART_CHECKSUMS` (shared) | `(bucket, upload_id, part_number)` | `(algorithm_wire, base64_value)` |
| `OBJECT_PARTS` (shared) | `(bucket, key, part_number)` | `(size, algorithm_wire, base64_value)` |
| `objects` (local, renamed shape) | `(bucket, key)` | `&[u8]` content bytes |
| `part_data` (local, renamed from `parts`) | `(bucket, upload_id, part_number)` | `&[u8]` content bytes |
| `part_meta` (local) | `(bucket, upload_id, part_number)` | `(size, mtime_nanos)` |


- [x] **Step 1: Write the failing re-keyed tests first** — the mem raw-row/layout-assertive tests (inventoried 2026-09-03) break under re-keying; rewrite them to the new layout **before** the code change so they show red. Affected tests and their re-key:
  - `storage.rs`: `concurrent_put_and_delete_never_orphans` (scans `OBJECTS` with the `"{bucket}\0"` string prefix → scan `(bucket, "")` via `for_each_pair` with `keep |b, _| b == bucket`); `collect_part_keys_stops_at_a_non_prefix_key` (raw-inserts `part_key("u1", 1)` into `PARTS` → insert `("data", "u1", 1)` style tuple keys into `part_data`).
  - `object.rs`: `mem_garbage_meta_elements_self_heal` (raw-inserts OBJECT_META+OBJECTS at `object_key(bucket, key)` → tuple `(bucket, key)` inserts with the 6-tuple value incl. `file_identity: 0`); `mem_object_parts_lifecycle` (OBJECT_PARTS rows at `object_part_key(ok, n)` → `(bucket, key, n)` tuples).
  - `bucket.rs`: `mem_garbage_bucket_tags_self_heal` (BUCKETS value unchanged — insert `(bucket, (1u64, "team=%zz&"))` still works; no edit needed beyond imports).
  - `multipart.rs`: `corrupt_checksum_rows_self_heal` (raw-inserts UPLOAD_CHECKSUMS via `upload_key(bucket, key, id)` and PART_CHECKSUMS via `part_key(id, n)` → tuple `(bucket, id)` / `(bucket, id, n)` keys with uploads created via the API so the bucket/upload identity is real); `mem_complete_retains_object_parts` (asserts composite/tag wires — value shapes unchanged; keys now tuples).
  - `storage.rs`/`object.rs` codec-round-trip tests via the API (`mem_commit_records_the_stage_tee_checksum`, tag round-trips) — unchanged behavior; verify only.

Run: `cargo test -p tinio-mem`
Expected: FAIL — the raw-row tests compile against the old `storage.rs` item names/keys and fail once Step 2 lands; **write them so they fail after Step 2**, not before (the crate still compiles with old code until Step 2).

- [x] **Step 2: Replace the `storage.rs` table section** — in order:
  1. Delete the nine local `TableDefinition` consts, the key-builder fns (`object_key`/`part_key`/`upload_key`/`object_part_key`/`parse_part_number`), `band_start`, and the string-scan helpers (`collect_part_keys`/`remove_object_parts`/`remove_all_parts`/prefix-probe bodies). No codec copies exist to delete — mem already delegates to core's wire codecs (`Tags::parse_wire_limited`; caps from `tinio_core::object`), exactly like fs, and the shared handles keep that delegation. Keep `check_bucket`/`check_upload` semantics as thin fns over the new handles (their bodies change: point gets on `BucketsTable`/`UploadsTable` with tuple keys; `check_upload` now takes `(bucket, upload_id)` and verifies `value.key == key` — the upload row's stored key — returning `NoSuchUpload` on miss).
  2. Add the three local table definitions + typed handles via the shared macro:
  ```rust
  //! Local content tables (no fs counterpart): the in-memory bytes and
  //! per-part stat rows, keyed like the shared tables.

  type ObjectKey = (&'static str, &'static str); // (bucket, key)
  type PartDataKey = (&'static str, &'static str, u32); // (bucket, upload_id, part_number)
  type PartMetaValue = (u64, u64); // (size, mtime_nanos)
  const OBJECTS: TableDefinition<ObjectKey, &'static [u8]> = TableDefinition::new("objects");
  const PART_DATA: TableDefinition<PartDataKey, &'static [u8]> = TableDefinition::new("part_data");
  const PART_META: TableDefinition<PartDataKey, PartMetaValue> = TableDefinition::new("part_meta");
  ```
  (`table_impl!` invocations for the three, plus hand-written methods the ops need: `OBJECTS` get/put/remove; `PART_DATA` get/put/remove/`drain_upload`/`drain_bucket`; `PART_META` get/put/remove/`drain_upload`/`drain_bucket` — each drain via the shared `scan` helpers.)
  3. Replace `with_options`'s open block (currently creates all nine tables): call `tinio_store::tables::ensure_all(&mut txn)?` then create the three local tables (`OBJECTS`/`PART_DATA`/`PART_META` via `ensure`) in the same first write transaction. The `db` field stays `pub(crate)`.
  4. Re-export the shared surface the ops modules import (they keep importing from `storage.rs` today — keep that path by re-exporting):
  ```rust
  pub(crate) use tinio_store::tables::{
      BUCKETS, OBJECT_META, OBJECT_PARTS, PARTS, PART_CHECKSUMS, UPLOADS,
      UPLOAD_CHECKSUMS, validate_stored,
  };
  pub(crate) use tinio_store::scan::{drain_pair, drain_triple, for_each_pair};
  ```
  (No caps or codec names in the list — mem ops read `BUCKET_TAGS_MAX`/`OBJECT_TAGS_MAX` and the `Tags`/`checksum::Recorded` codecs from `tinio_core::object`/`checksum` directly, as they do today.)

- [x] **Step 3: Re-key `object.rs`** — every row access changes:
  - `get_object`/`head_object`: open `OBJECTS` + `OBJECT_META`; `OBJECT_META.get(bucket, key)` returns `Option<StoredMeta>` via the shared handle (6-tuple decode, `file_identity` read as 0); content via `OBJECTS.get(bucket, key)`. The meta row stays the existence gate.
  - `write_object`/commit paths: write the 6-tuple — `etag.as_str()`, size, `mtime_nanos()`, `0` for file identity, `tags.to_wire()` from core, checksum wire via core's `checksum::Recorded::to_wire` (or `""`).
  - `list_objects`: replace the `band_start` + string-prefix range scan with `for_each_pair` over `OBJECT_META` from `(bucket, "")`, `keep |b, _| b == bucket`, yielding `(key, StoredMeta)`-decoded rows (folder markers/reserved skip and pagination logic unchanged — the row key is now the raw key, no `bucket_prefix.len()` slicing).
  - `rename_object`: one write txn — remove `OBJECT_META`/`OBJECTS` rows at `(bucket, src)` (+ the key's `OBJECT_PARTS` rows), insert at `(bucket, dst)` (migrate OBJECT_PARTS rows per-key via `list`+`remove_key`+`put` or a `drain_pair`+re-insert loop over the shared `OBJECT_PARTS` handle).
  - Delete paths: remove `(bucket, key)` rows + the `OBJECT_PARTS` rows in one txn.
  - The `stage_body` tee/checksum flow: unchanged logic; the recorded checksum element writes via core's `checksum::Recorded::to_wire` (tinio-store hosts no codec code).

- [x] **Step 4: Re-key `multipart.rs` and `bucket.rs`**
  - Uploads live in shared `UPLOADS` as `(bucket, upload_id) → (key, initiated, tags)`; `create_multipart_upload` writes that shape; `upload_part`/`complete`/`abort` locate the upload by `(bucket, upload_id)` point get (`get_matching`/`key_matches` semantics on the shared handle — the stored key must equal the requested key, else `NoSuchUpload`).
  - Read paths that list uploads for a `(bucket, key)` prefix (`list_multipart_uploads`) become `UploadsTable::for_bucket` scans filtered on `value.0 == key` (fs `walk_uploads` parity).
  - Parts: `upload_part` writes in one write txn — `PART_DATA` bytes at `(bucket, upload_id, part_number)`, `PARTS` etag row at the same key, `PART_META` `(size, mtime)` row, and the `PART_CHECKSUMS` row (upsert/remove as today). `list_parts`: `PartsTable::list_from(bucket, upload_id, start, max)` + per-part `PART_META`/`PART_CHECKSUMS` joins (fs's PARTS + stat join shape); part numbers come from the tuple element, no string parsing (`parse_part_number` deleted).
  - `complete_multipart_upload`: validation reads `PARTS` (etag per listed part) + `PART_META` (size for the assembled-object accounting); the object write goes to `OBJECTS`/`OBJECT_META` `(bucket, key)` rows; `OBJECT_PARTS` retention rows keyed `(bucket, key, n)`; upload teardown removes `(bucket, upload_id)` rows + drains `PARTS`/`PART_DATA`/`PART_META`/`PART_CHECKSUMS` under the upload prefix (shared `drain_upload`-style via `scan`).
  - `abort`: same teardown; byte accounting (`total_bytes` adjust) uses the removed `PART_DATA` row sizes as today.
  - `bucket.rs`: `BUCKETS` shape unchanged — only the handle source changes (`BucketsTable` from tinio-store); `delete_bucket`'s non-empty probes on `OBJECTS`/`UPLOADS` become `for_each_pair`-style first-match probes from `(bucket, "")` with the keep boundary; `list_buckets` unchanged (single-column table).

- [x] **Step 5: Run the mem suite**

Run: `cargo test -p tinio-mem`
Expected: PASS — the re-keyed raw-row tests (Step 1) + the API-level suites + `assert_conformance` (mem runs it against `MemoryStorage`; the harness is backend-agnostic — green here is the behavioral-equivalence proof). Fix any compile fallout from Steps 3-4 the compiler lists (the workspace need not compile beyond `-p tinio-mem` here; tinio-server's mem usage compiles once its imports of `tinio_mem::{Error, DatabaseError, MemoryStorage, MemoryOptions, MemoryCleanup}` resolve — they are unchanged names).

- [x] **Step 6: Checkpoint** — report the mem table inventory after the re-key and the conformance result; do not commit.

---

---

### Task 6: Docs sync + workspace verification

Syncs the two documentation sites the spec names (style.md error rules, meta-redb-spec §5.4) and runs the full acceptance set (spec Verification 1-6).

**Files:**
- Modify: `docs/style.md` (error/schema section — the five redb kinds now live in `tinio_store::error::Error`)
- Modify: `specs/001-s3-local-server/meta-redb-spec.md` (§5.4 error-model wording — the five redb-mapping variants nest via the shared type; UnsupportedVersion/CorruptMeta unchanged)
- Verify: workspace-wide gates

**Interfaces:**
- Consumes: everything from Tasks 1-5.

- [x] **Step 1: Sync `docs/style.md`** — in the error rules line (approx. L25: "Wrap `Storage(#[from])`; redb `Database`; extras → `Io`.") add the shared-type + derive pointer, e.g.:
  "redb `Database` (the five mapping kinds live in `tinio_store::error::Error` — thiserror `#[from]`; the fs/mem conversions derive via derive_more `#[from(forward)]` on the wrapping variant, never hand-written impls; `#[from(skip)]` on ctor-built struct variants); extras → `Io`." Adjust the schema line (L21) only if the `table_impl!` wording no longer matches (it does not name a module — leave it).

- [x] **Step 2: Sync `meta-redb-spec.md` §5.4** — update the variant description: the five redb sub-variants (`Open`/`Transaction`/`Table`/`Storage`/`Commit`) are defined once in `tinio-store` (`tinio_store::error::Error`, thiserror `#[from]` per variant) and nested by fs `database::Error` (transparent `Redb` variant; the raw-redb hops derive via derive_more `#[from(forward)]`) and aliased [**SUPERSEDED (E0119 ruling):** raw-redb hops are EXPLICIT constructor calls, not derived.] by mem (`DatabaseError`, same derive); `Compaction`/`Io`/`UnsupportedVersion`/`CorruptMeta` and the crate-lift rule are unchanged.

- [x] **Step 3: Full workspace verification**

Run:
- `cargo test -p tinio-store -p tinio-fs -p tinio-mem` — PASS (Tasks 1-5 gates).
- `cargo test --workspace` — PASS when the in-flight tagging/conditions plans' server-layer tasks are complete; if not, report the failing crates as pre-existing in-flight work, not regressions from this plan.
- `cargo clippy --workspace` — clean.
- `cargo check -p tinio-fs --benches` — PASS.
- Cucumber: `cargo test -p tinio-e2e --test cucumber` with the `@fs` pass and `TINIO_E2E_BACKEND=mem` `@mem` pass — green when the in-flight work allows.
- `cargo tree -p tinio-fs` / `-p tinio-mem` — both show `tinio-store`; `cargo tree -p tinio-core` has **no** redb.

- [x] **Step 4: Checkpoint** — report the full delta (files created/moved/deleted per crate), the verification results, and the known in-flight dependencies; do not commit.

---

## Self-Review Notes

- **Spec coverage**: §1 (shared crate modules tables/scan/error/ensure — row decode delegates to core's parse-display-backed codecs, no codec relocation) → Tasks 1-2; §2 (fs pure relocation + error rewire + on-disk guarantee) → Tasks 1-3; §3 (mem full re-key incl. the table mapping, upload value shape, part split, local content renames) → Task 5; error extraction decisions (Q5-Q9) → Tasks 1 + 4; §4 testing/acceptance + Verification → Tasks 3 + 6. Non-goals honored: fs `Handle`/`open` two-phase/`compact`/`STATE` untouched; mem byte accounting/`MemoryCleanup` untouched; no crate-level error-model extraction beyond the five variants.
- **Guard-test gap closed**: the spec's "unmodified fs layout test asserting the schema" premise was checked against the tree (inventoried 2026-09-03) — no such test exists; Task 3 adds it so the no-format-change claim is pinned.
- **Error-type plumbing**: shared table-handle methods return `tinio_store::error::Error` (Task 2); fs call sites keep compiling because tinio-store's five thiserror `#[from]` conversions (Task 1) plus fs `database::Error`'s `Redb` `#[from(forward)]` derive (Task 1 Step 6) make the direct redb-error hops — `From` is not transitive, but the forward derive emits the hops, so no forwarding impls are hand-written anywhere. mem gets the same on `Error::Database` through the alias (mem alias task). Reclassification impls (fs crate-level `From<database::Error>` Io-unwrap; mem `From<io::Error>`/`From<etag::Error>` storage-ctor hops) survive because derive cannot express a variant change — they are semantic mappings, not forwarding.
- **Derive rule (user, 2026-09-03)**: error conversions derive — never hand-written `impl From`. `tinio_store::error::Error` uses thiserror `#[from]` (its variant fields ARE the redb errors; the user's ruling — derive_more would add nothing there). The fs/mem nesting wrappers — whose field is a *convertible* inner type, not the source — use derive_more `#[from(forward)]`, with `#[from(skip)]` on the ctor-built struct variants; thiserror retains `#[error]` display + `Error`/`source`. Wire codecs stay in tinio-core (parse-display-derived leaf names): earlier drafts' codec-move steps (fs `database/tables.rs` L91-156, mem `storage.rs` copies) reference code the in-flight tagging plan already relocated to core — Tasks 2/5 move nothing codec-related and tinio-store adds no parse-display usage of its own.
- **The strict `for_bucket` variant** is fs-only (constructs `CorruptMeta`); Task 2 Step 3 pins the outcome (verified 2026-09-03): `meta::Store::walk` needs it, so it survives as the fs free fn `for_bucket_strict`.
- **mem compile windows**: Task 5 leaves `-p tinio-mem` non-compiling between Steps 2-5 (accepted; the tagging plan's Task 2 set the same precedent); every other task boundary compiles green.
- **Error-alias ordering**: the alias (Task 4) lands before the re-key (Task 5) because mem ops call the shared handles (which return `tinio_store::error::Error`) — the derived `From` conversion (`#[from(forward)]`) exists only once `Error::Database` carries the shared type via the alias.
- **In-flight baseline**: the dev tree's rows already match the spec's post-tagging shapes (verified 2026-09-03); the server-layer tagging tasks may keep `cargo test --workspace`/cucumber red until that plan completes — per-crate gates are the task gates here.
