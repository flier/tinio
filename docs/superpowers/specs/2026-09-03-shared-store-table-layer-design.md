# Design: shared redb table layer (tinio-store)

**Date**: 2026-09-03
**Status**: approved — brainstorming + grilling 2026-09-03 (Q1–Q9; the redb error-conversion extraction was folded into scope at grilling Q5=B — see Decisions); factual review pass 2026-09-03 (code citations corrected against the tree; `thiserror` added to the crate's dependencies; the on-disk-format guard made a new schema-assertion test — no pre-existing test pins table names/arities)
**Scope**: a new workspace crate hosts the redb table schema, row types, wire codecs, per-prefix scan/drain helpers, and the redb error-conversion core duplicated between the tinio-fs and tinio-mem backends; tinio-fs relocates code with no on-disk format change; tinio-mem re-keys every table (shared and content) to tinio-fs's tuple-key convention and aliases its `DatabaseError` onto the shared error type. Not touched: per-backend ops semantics, the fs file layer, the fs database machinery (handle/open/compact/STATE), the per-crate crate-level error enums, mem byte-limit accounting.

## Background

tinio-fs and tinio-mem both implement the `Storage` contract over redb — fs against an on-disk `meta.redb`, mem against `redb::backends::InMemoryBackend`. Both maintain the same derived-metadata rows (bucket records, object metadata, multipart uploads, part etags/checksums, completed-object parts), and both carry byte-identical wire codecs for those rows:

- `tags_from_wire` (cap-parameterized tolerant parser — callers pass 10 for object/upload rows, 50 for bucket rows) + `percent_decode` — mem `crates/tinio-mem/src/storage.rs:100-147` vs fs `crates/tinio-fs/src/database/tables.rs:91-138`.
- checksum wire `<algorithm>:<base64>:<kind>` — mem `checksum_to_wire`/`recorded_from_wire` (`storage.rs:153-175`) vs fs `validate_stored` (`tables.rs:307-318`), self-healing semantics identical.
- Per-bucket range scans and drains — fs `database/scan.rs` (`for_each_pair`, `drain_pair`/`drain_triple`, keep-predicate boundary) vs mem's `band_start`/prefix-scan machinery (`storage.rs:39-44`, `363-417`).
- redb error-conversion helpers — mem `error.rs:51-68` (the `DatabaseError` enum) + `error.rs:220-248` (the `database_*` helper family) mirror fs `database/error.rs` (folded into scope at grilling Q5=B — see Decisions).

The cost of the duplication is not just volume: **every schema change is implemented twice.** The 2026-08-31 tagging plan's Tasks 3/4 extended the same object/upload/bucket rows and added `OBJECT_PARTS` once per backend, by hand, in lockstep. The two copies can also drift structurally — they already have: fs keys rows by tuple `(bucket, key)`, mem by flat `bucket\0key` strings with zero-padded part numbers.

## Goal

1. **One home for the table layer.** A new workspace crate (**tinio-store**) defines the seven shared redb tables (`BUCKETS`, `OBJECT_META`, `UPLOADS`, `PARTS`, `UPLOAD_CHECKSUMS`, `PART_CHECKSUMS`, `OBJECT_PARTS`), their key/value shapes, the decoded row type `StoredMeta`, the wire codecs, the per-table handle structs, and the scan/drain helpers — consumed by both backends. A future schema change lands once.
2. **One redb error core.** The five redb error-mapping variants (`Open`/`Transaction`/`Table`/`Storage`/`Commit` — names, payloads, display strings) exist once, in `tinio-store::error`; fs nests it and mem aliases it, deleting mem's mirrored `DatabaseError` definition and `database_*` helper family (grilling Q5=B).
3. **One key convention.** tinio-mem adopts tinio-fs's tuple keys for **all** of its tables, shared and content; its `band_start`/string-key machinery is deleted. mem state is ephemeral (in-memory backend, fresh per boot), so re-keying has zero migration cost.
4. **No format change for tinio-fs.** The relocation is pure: table names, tuple shapes, arity, and ordering semantics on disk are byte-identical. The proof is a **new** schema-assertion test in tinio-store pinning every table's name and key/value arity (no pre-existing test pins them — `tests/layout.rs` covers only the state-dir filesystem layout), plus the fs suites passing unchanged.
5. **Behavior is preserved, proven by the existing harness.** The `assert_conformance` suite runs both backends; cucumber runs `@fs` and `@mem` passes; both stay green.

## Non-goals

- **No shared database machinery.** The fs `Handle` closure-transaction layer (`database/handle.rs`), `open.rs` (two-phase construction, version gate), and `compact.rs` stay where they are — now the only remaining database-machinery duplication, a possible later phase.
- **No crate-level error-model extraction.** The per-crate `Error` enums, their `From<Error> for storage::Error` contract projections, and the fs-only `database::Error` variants (`Compaction`, `Io`, `UnsupportedVersion`, `CorruptMeta`) stay local; only the five-variant redb mapping core is shared (grilling Q6).
- **No ops-layer extraction.** fs's file-backed object/multipart semantics (write.rs, sweep, cleanup stages, mutation locks) and mem's in-memory byte accounting (`max_object_bytes`/`max_total_bytes`) are untouched.
- **No fs `STATE` sharing.** The version/`compact_needed` table is fs lifecycle machinery (open-time version gate, compact marker); mem has no version gate and does not gain one.
- **No behavior change in mem.** Only the row layout changes; the `Storage` contract surface and observable semantics are unchanged.
- **No format migration anywhere.** Same-version dev DBs may change shape under the fs backend only if a future plan touches fs rows — this plan does not; mem state is disposable by nature.

## Decisions (locked in brainstorming, 2026-09-03)

- **Scope: schema single-sourcing** (user pick) — table definitions, row types, wire codecs, and per-bucket scan/drain helpers move to the shared crate; each backend keeps its own ops facades.
- **Home: a new shared crate** (user pick) — tinio-core stays the contract crate (no redb dependency, so future non-redb backends such as the planned s3/webdav are not forced to pull redb); tinio-fs would drag fs-specific heavy deps (md-5, moka, rustix, strict-path) into tinio-mem. The crate depends only on `redb` + `tinio-core` + `thiserror` — thiserror backs the shared error enum's derive, as in both backends today (workspace precedent: tinio-util exists for shared code).
- **Depth: full tuple-key unification** (user pick) — mem's local content tables (`OBJECTS` bytes, part content, part meta) are re-keyed to the same tuple conventions, so every scan/drain in mem runs through the shared helpers. mem state is ephemeral; the cost is test/read-path churn only.
- **Crate name: `tinio-store`** (confirmed, grilling Q1). ("store" is the layer's own vocabulary: fs exposes `MetaStore`/`BucketStore`/`MultipartStore` over these tables.)
- **mem part metadata splits to mirror fs** (design decision) — fs's part model is "content on disk + `PARTS` etag rows + size/mtime from file stat". mem's is "content bytes + `(etag, size, mtime)` per part". Sharing `PARTS` requires mem to adopt the fs split: etag rows in the shared `PARTS` table, size/mtime in a mem-local stat row. The mem-local content table currently named `parts` is renamed (`part_data`) — a redb database cannot hold two tables with the same name, and mem state is ephemeral so the rename is free.
- **mem `OBJECT_META` gains `file_identity`** — the fs row is a 6-tuple (etag, size, mtime, file identity, tags wire, checksum wire); mem's is a 5-tuple. The shared row is the 6-tuple; mem stores 0 (fs stores 0 where file identity is unavailable, e.g. Windows). mem does not use the identity gate (its content is immutable in-DB bytes) — the field is carried for shape parity only.
- **mem `UPLOADS` adopts the fs value shape** — fs keys `(bucket, upload_id)` with the object key **in the value** `(key, initiated_at, tags_wire)`; mem keys `bucket\0key\0upload_id` with the key in the composite key. The shared shape is fs's. mem's `(bucket, key, upload_id)` validations (today point reads via `check_upload`) stay point reads on the shared `(bucket, upload_id)` key plus a stored-key comparison — the same pattern as fs `UploadsTable::get_matching` (`tables.rs:491-508`); only bucket-level listings (`list_multipart_uploads`, the delete-bucket emptiness probe) become shared-helper prefix scans.
- **`STATE` stays fs-local; mem keeps no version table.**
- **Timing baseline** (confirmed, grilling Q2=A): the current dev tree is mid-execution of the 2026-08-31 tagging/conditions plans — the shared rows below are written for the post-tagging shapes (object row 6-tuple with tags + checksum elements, upload row with tags, `OBJECT_PARTS` present). The implementation plan is sequenced to land after those plans are merged.
- **Table handles shared** (grilling Q4=A): the `table_impl!` macro **and** the seven named per-table handle structs move to tinio-store; fs keeps `STATE` and its fs-specific accessors; mem adopts the shared handles (gains typed access instead of raw `open_table` calls).
- **Error extraction folded into scope** (grilling Q5=B): the mem `error.rs:220-248` mirror of fs `database/error.rs` is extracted with the table layer — it was a Non-goal in the brainstorming draft.
- **Shared error = the five redb variants only** (grilling Q6=A): `Open`/`Transaction`/`Table`/`Storage`/`Commit`, each `#[from]`-wrapping one redb error type with today's display strings; fs's `Compaction`/`Io`/`UnsupportedVersion`/`CorruptMeta` stay fs-side (mem has no compaction, version gate, or db-layer file I/O). (fs `database::Error` today carries **six** redb `#[from]` mappings — `Compaction` wraps `redb::CompactionError`; it stays fs-local as its own direct `#[from]` variant, so fs's new explicit forwarding impls number five, and `Compaction`/`Io` keep their direct `#[from]`s.)
- **fs keeps `database::Error`** (grilling Q7=A): the module name and crate-lift rule survive — `database::Error` becomes the fs-only variants plus one transparent variant nesting `tinio_store::error::Error`, with explicit `From<redb::…>` forwarding impls replacing today's direct `#[from]` (Rust `From` is not transitive). `docs/style.md` error rules and `specs/001-s3-local-server/meta-redb-spec.md` §5.4 wording sync to the shared type.
- **mem aliases the shared type** (grilling Q8=A): `DatabaseError` becomes a `pub` alias/re-export of `tinio_store::error::Error` (the lib.rs re-export name survives); the `database_*` helper family is deleted (the seven `database_storage` guard sites move to the shared constructor); the explicit `From<redb::…> for Error` family stays, retargeted to the shared type.
- **Shared error name: `tinio_store::error::Error`** (grilling Q9=A) — namespaced `error` module, per the `storage::Error` precedent.

## Design

### §1 The shared crate: `tinio-store`

New workspace member `crates/tinio-store`; dependencies: `redb` + `tinio-core` + `thiserror` (the shared error enum derives `thiserror::Error`, as both backends' enums do today), all workspace versions; registered as a path dep in the root `[workspace.dependencies]` (the `members = ["crates/*"]` glob already covers membership); per-crate `[lints.rust]` `unsafe_code = "forbid"` like its siblings (there is no `[workspace.lints]`). Description: "Shared redb table layer for the tinio storage backends".

Modules (working layout, mirroring the fs `database/` module it is extracted from):

- **`tables.rs`** — the seven shared table definitions and their key/value type aliases:

  | Table | Key | Value |
  |---|---|---|
  | `BUCKETS` (`"buckets"`) | `name: &str` | `(created_at_nanos: u64, tags_wire: &str)` |
  | `OBJECT_META` (`"object_meta"`) | `(bucket: &str, key: &str)` | `(etag_hex: &str, size: u64, mtime_nanos: u64, file_identity: u64, tags_wire: &str, checksum_wire: &str)` |
  | `UPLOADS` (`"uploads"`) | `(bucket: &str, upload_id: &str)` | `(key: &str, initiated_at_nanos: u64, tags_wire: &str)` |
  | `PARTS` (`"parts"`) | `(bucket: &str, upload_id: &str, part_number: u32)` | `etag_hex: &str` |
  | `UPLOAD_CHECKSUMS` (`"upload_checksums"`) | `(bucket: &str, upload_id: &str)` | `(algorithm_wire: &str, checksum_type_wire: &str)` |
  | `PART_CHECKSUMS` (`"part_checksums"`) | `(bucket: &str, upload_id: &str, part_number: u32)` | `(algorithm_wire: &str, base64_value: &str)` |
  | `OBJECT_PARTS` (`"object_parts"`) | `(bucket: &str, key: &str, part_number: u32)` | `(size: u64, algorithm_wire: &str, base64_value: &str)` |

  Each definition comes with the decoded row type where one exists (`StoredMeta` for `OBJECT_META` — etag/size/mtime/file-identity/tags/checksum, validated on read, corrupt rows self-healing to `None`/empty like today) and the named per-table handle struct (`BucketsTable`, `ObjectMetaTable`, …). The `table_impl!` macro moves here and is made shared (grilling Q4=A) — it generates `Deref`/`open`/`open_readonly`/`ensure` (plus the `no_ensure` arm fs's `StateTable` keeps using); it does **not** generate the accessors. The hand-written per-table inherent impls (`get`/`put`/`remove`/`for_each`/`for_bucket`/`get_matching`, including `validate_stored`'s read validation and the deliberate asymmetry between point-get self-healing and `for_bucket`'s corrupt-row hard fail) move with the handles — they are the bulk of the relocation. Table **names and tuple arities are fixed constants** — they are the on-disk format contract, asserted by tests (see §4).

- **`codec.rs`** — the wire codecs, moved verbatim: `checksum_to_wire`/`recorded_from_wire` (`<algorithm>:<base64>:<kind>`; kind recorded at write time; garbage self-heals to `None`), the tolerant `tags_from_wire` (cap-parameterized parsing of the canonical sorted RFC-3986 form) + `percent_decode` (hand-rolled, no external crates). `to_nanos`/`from_nanos` stay where they are — imported from `tinio-core::storage` (the `time` module is private; the helpers are re-exported at `tinio_core::storage`), core remaining the codec home for the domain types. Pure string/domain transforms — no redb types in this module, unit-testable without a database.

- **`error.rs`** — the shared redb error core (grilling Q6): `pub enum Error` with the five variants `Open`/`Transaction`/`Table`/`Storage`/`Commit`, each `#[from]`-wrapping one redb error type and carrying the display strings both crates use today, plus the constructor fns that survive mem's helper deletion (`storage(err)` and friends). No fs-lifecycle variants (`Compaction`/`Io`/`UnsupportedVersion`/`CorruptMeta` stay in fs — mem has no compaction, version gate, or db-layer file I/O).

- **`scan.rs`** — the per-prefix scan/drain helpers moved from fs `database/scan.rs`: keep-predicate iteration from a lower bound (`for_each_pair`-style, stop at the first non-matching element — no exclusive upper bound, per the redb-notes pit 14 ruling), and range drain for `(bucket, ..)` / `(bucket, second, ..)` key prefixes. The helpers are generic over the **value** type (`V: redb::Value`) with the two fixed key shapes used throughout — `(&str, &str)` pairs and `(&str, &str, u32)` triples; both crates' shared **and local** tables fit these shapes (mem's `part_data`/`part_meta` are triples, `objects` a pair).

- **`ensure`** — one shared helper to create the seven shared tables inside a write transaction (idempotent), called by both backends at open alongside their local tables (fs: `STATE`; mem: its content tables). The fs open-time one-transaction-all-tables pattern (`database/open.rs`) carries over unchanged.

### §2 tinio-fs: pure relocation

- `database/tables.rs` shrinks to: re-exports of the shared definitions/handles the stores use, the fs-local `STATE` table (version + `compact_needed` marker) and its `ensure_version` logic — `StateTable` continues to use the moved `table_impl!` macro's `no_ensure` arm via import — and any fs-specific accessor methods the shared handles do not cover.
- `database/scan.rs` drains into the shared `scan.rs` (fs call sites import from tinio-store).
- `database/error.rs` rewires to the shared core (grilling Q7): `Error` keeps its name — the fs-local variants (`Compaction`, `Io`, `UnsupportedVersion`, `CorruptMeta`) plus one transparent variant nesting `tinio_store::error::Error`; five explicit `From<redb::…> for Error` forwarding impls replace today's direct `#[from]`s on the shared variants (`From` is not transitive), while `Compaction` and `Io` keep their own direct `#[from]`s. The crate-level lift in `error.rs` (`database::Error::Io` unwraps to `Error::Io`, everything else nests as `Error::Database(..)`) and the redb→contract `Io` projection are **unchanged**; `docs/style.md` error rules and `specs/001-s3-local-server/meta-redb-spec.md` §5.4 wording sync to the shared type.
- `database/{mod,open,handle,compact}.rs` and the three Store facades (`bucket.rs`, `meta.rs`, `multipart.rs`) are **unchanged in behavior**; their imports adjust.
- `Cargo.toml`: add `tinio-store`; no dependency removed (redb stays for the local machinery).
- **On-disk guarantee**: the same table names, tuple shapes, and ordering semantics are written to `meta.redb` as today. No pre-existing test pins the redb schema (`tests/layout.rs` asserts only the state-dir filesystem layout; `open_creates_database_and_all_tables` opens tables via the same constants, so a rename would pass it) — the plan therefore **adds** a schema-assertion test in tinio-store (table names + key/value arities) as the byte-format guard; the existing fs suites pass without modification.

### §3 tinio-mem: full re-key

`crates/tinio-mem/src/storage.rs`'s table section is replaced by the shared definitions for the seven common tables, plus three re-keyed local tables:

| mem table today | after |
|---|---|
| `BUCKETS` `name → (created, tags)` | shared `BUCKETS` (unchanged shape) |
| `OBJECT_META` `bucket\0key → (etag, size, mtime, tags, checksum)` | shared `OBJECT_META` (gains `file_identity`, stored 0) |
| `UPLOADS` `bucket\0key\0upload_id → (initiated, tags)` | shared `UPLOADS` `(bucket, upload_id) → (key, initiated, tags)` |
| `PARTS` `upload_id\0NNNNNNNNNN → bytes` (content) | **renamed local** `part_data` `(bucket, upload_id, part_number) → bytes` |
| `PART_META` `upload_id\0NNNNNNNNNN → (etag, size, mtime)` (same 10-digit padding as `PARTS`) | local `part_meta` `(bucket, upload_id, part_number) → (size, mtime_nanos)`; etag moves to shared `PARTS` rows |
| `PART_CHECKSUMS` | shared `PART_CHECKSUMS` (re-keyed) |
| `UPLOAD_CHECKSUMS` | shared `UPLOAD_CHECKSUMS` (re-keyed) |
| `OBJECT_PARTS` | shared `OBJECT_PARTS` (re-keyed) |
| (none) | shared `PARTS` `(bucket, upload_id, part_number) → etag_hex` |
| `OBJECTS` `bucket\0key → bytes` | local `objects` `(bucket, key) → bytes` (re-keyed; listing scans become shared-prefix scans) |

Key construction (string concat + `\0` separators + zero-padded part numbers: `storage.rs:340-386`) and `band_start`/prefix-scan machinery (`storage.rs:39-44, 363-417`) are deleted; every table is now tuple-keyed and every scan/drain goes through the shared helpers. The db open (`storage.rs:248-258`) calls the shared `ensure` plus the three local tables in the same first write transaction. Upload validations by `(bucket, key, upload_id)` (today point reads via `check_upload`, `storage.rs:438-452`) stay point reads on the shared `(bucket, upload_id)` key plus a stored-key comparison — the fs `UploadsTable::get_matching` pattern (`tables.rs:491-508`), not `walk_uploads` (which is a whole-table listing scan). Bucket-level listings (`list_multipart_uploads`, the delete-bucket emptiness probe) run as shared-helper prefix scans. Part lifecycle ops write the etag row and the content/stat rows in the same transaction (mem has no file/db window — a single write txn, strictly stronger than fs's file+row pairing).

`MemoryOptions` byte accounting, `MemoryCleanup` (no-op), the ops modules (`bucket.rs`, `object.rs`, `multipart.rs`), and the `Error` shell stay local; only their row access changes.

`error.rs` adopts the shared core (grilling Q8): `pub use tinio_store::error::Error as DatabaseError;` keeps the existing lib.rs re-export name alive (consumers keep matching `DatabaseError::Open(..)` — and no crate outside tinio-mem references the name today, so the alias is externally zero-risk); the `Error::Database` field type becomes the shared enum. The `database_*` helper family (`error.rs:220-248`) is **deleted** — the seven `database_storage` guard sites (`storage.rs:377,409`; `object.rs:465,668,720`; `multipart.rs:606,630`) call the shared constructor and convert; the explicit `From<redb::…> for Error` impls (`error.rs:88-116`) stay, retargeted to the shared type. The `Error` shell (`Storage`/`Database` variants), the contract ctor family, and the `From<Error> for storage::Error` projection (redb → `Io`) are unchanged.

### §4 Testing & acceptance

- **Shared crate unit tests**: schema-assertion pins (every table's name string and key/value arity asserted — this is the fs on-disk byte-format guard, newly added since no pre-existing test pins them); codec round-trips moved from the backends (tags wire incl. Unicode/percent-encoding, checksum wire incl. garbage self-heal) — must pass unchanged where they were copied from; error-mapping tests (each of the five variants' construction + display string, verbatim from the backends' expectations); scan boundary tests (bucket/key prefix scans with special characters, drain idempotency); `ensure` idempotence (double-open).
- **tinio-fs**: full existing suite green — unit, layout, error-conversion pins (`converts_into_contract_error`-style), proptests, conformance — with import-only edits everywhere except the error tests: a redb-mapped variant can no longer be constructed by its old path (`database::Error::Open(..)` becomes the wrapper nesting the shared variant), so the handful of tests that build one adapt to the wrapper (redb error instances are hard to fabricate, so such tests are few). The byte-format guard is the new shared-crate schema-assertion test above — fs itself needs no schema-test edits (`tests/layout.rs` covers only the state-dir filesystem layout and passes unmodified).
- **tinio-mem**: unit tests re-keyed to the new row layout; error tests adapt to the aliased `DatabaseError` (variant paths unchanged); `assert_conformance` green (the harness is backend-agnostic and already runs both — it is the behavioral equivalence proof).
- **Workspace**: `cargo test --workspace` and `cargo clippy --workspace` clean on Windows and WSL2; cucumber `@fs` and `@mem` passes green.
- **Ordering**: the plan is written to execute after the in-flight 2026-08-31 tagging/conditions plans land on `dev`; if it runs against the current tree instead, the first task reconciles the shared rows with whatever row shapes are present (the mem/fs row extensions land with those plans).

## Risks

| Risk | Mitigation |
|---|---|
| mem re-key touches every mem read path (largest churn) | the re-key lands table-by-table with the mem suite + conformance green after each step; mem state is ephemeral — a wrong key shape fails loudly in tests, never corrupts user data |
| fs on-disk format drift during relocation | names/arities are fixed constants in the shared crate; the new schema-assertion test pins them (no pre-existing test did); pure code motion, no tuple changes |
| shared-code pull into tinio-fs changes semantics subtly | fs suites (unit + proptest + conformance) run unmodified; any behavior delta fails them |
| fs error re-plumb (wrapper nesting + `From` forwarding) changes log/error text or breaks a conversion pin | display strings are copied verbatim into the shared enum; the fs lift rule (`Io` unwrap, rest nest) is unchanged; error-conversion pins and suites catch deltas |
| mem's `DatabaseError` alias shifts error text or paths consumers match on | display strings and variant paths survive the alias (same names under `tinio_store::error::`); fs/mem unit tests + doctests pin them |
| name collision on shared tables (mem `parts` content table) | renamed to `part_data` in the same task that introduces shared `PARTS`; ephemeral state makes the rename free |
| sequencing against in-flight plans | baseline assumption recorded (Decisions); first task reconciles shapes if the tree state differs |
| the shared crate is a new workspace member with no consumers yet | it lands with its first consumer (fs) in the same task; empty-crate state never compiles |

## Out of scope (recorded for a possible later phase)

- Shared `Handle`/transaction machinery, open/version-gate, compaction — fs lifecycle code, mem does not need it (the remaining database-machinery duplication).
- Crate-level error models — the per-crate `Error` enums, contract projections, and fs's `database::Error` extras (`Compaction`/`Io`/`UnsupportedVersion`/`CorruptMeta`); only the five-variant redb mapping core is shared (grilling Q6).
- fs `STATE` table and `cleanup.rs` stages.
- Content layout: fs files vs mem in-redb bytes stay as they are (the `Storage` contract abstracts them).

## Verification

1. `cargo test -p tinio-store` green (schema-assertion pins + codec + error-mapping + scan + ensure unit tests).
2. `cargo test -p tinio-fs` green with no test edits beyond imports and the noted error-test adaptations — the tinio-store schema-assertion test from step 1 is the proof the on-disk schema (names, arities) is unchanged.
3. `cargo test -p tinio-mem` green — re-keyed unit tests + conformance.
4. `cargo test --workspace` + `cargo clippy --workspace` clean (Windows and WSL2).
5. Cucumber `@fs` and `@mem` passes green.
6. `cargo tree -p tinio-fs`/`-p tinio-mem` show the new `tinio-store` dependency; tinio-core's dependency list is unchanged (no redb added).
