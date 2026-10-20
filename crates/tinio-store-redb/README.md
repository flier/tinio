# `tinio-store-redb` — the shared redb implementation for the tinio storage backends

`tinio-store-redb` is the redb implementation both storage backends (`tinio-fs`, `tinio-mem`) persist through: the shared table layer (the table definitions and their row encodings, the per-table handles, the prefix scan and drain helpers, the redb error core) and the state-database driver relocated from `tinio-fs` (the async transaction handle with its blocking-pool hop and write-lock histograms, the open-time format gate, and offline compaction). It implements `meta::BucketMeta`, `meta::ObjectMeta`, `meta::UploadMeta` and `meta::VersionMeta` — the aggregate `meta::Metadata` — and the byte-table `content::ObjectContent`; the contracts, their row types and the wire codecs the rows encode through are `tinio-core`'s. It owns no operation semantics (the S3 bodies, the refusals and the two per-backend policies are `tinio-ops`'s) and no path or lifecycle policy: which directory is the state dir, that directory's permission hardening and the fs-facing error mapping stay in `tinio-fs`, and the byte-accounting decorator stays in `tinio-mem`. One schema, one error core, one driver — a schema change lands here once.

## The state database

- **One format version, no migration.** A read-write open writes the current version onto a fresh database and refuses any stored version that is not it, naming the state file so the operator knows which database to delete; a database written in an older row format is the same remedy. Additive schema changes do not bump the version.
- **Table names and row arities are the on-disk format contract** — fixed constants, pinned by tests, never respelled per backend. The shared schema is created in one write transaction at open, and a backend creates its own tables alongside it; the open creates the state directory when missing, and making that directory private is the caller's (tinio-fs hardens it).
- **A read-only open creates and writes nothing**: the state dir, the file and the tables must already be there, and a version this build does not read is refused. A run whose database cannot be opened degrades to an empty metadata view rather than failing, and the caller reports it.
- **The three version tables and `bucket_lifecycle` may be absent; no other table may.** A read-only handle answers their absence as empty history or as "no configuration" — a root written before the version machinery or before the lifecycle row legitimately lacks them — while any other table's absence is a database this build did not write.
- **A non-writable handle refuses every write** (`Error::ReadOnly`). The read-only policy refuses mutating operations upstream, so a write reaching a read-only or empty handle is a programming error — except on a read path rewriting a row it could not hit, where the refusal is the designed not-written-back.
- **A database an earlier build wrote still opens and serves its rows.** The committed pre-change fixture pins that, and the test copies it out of the repository before opening it, so the committed bytes stay unchanged.

## Compaction

Offline only, and never from the runtime: compaction runs on a **not-yet-shared** database — the only moment a mutable reference to it exists — from the server-startup orchestration and `doctor --fix`, before a backend is constructed around it. The decision is the `compact_needed` marker the runtime or `doctor --fix` set **or** an open-time fragmentation snapshot above the floor — a marked database compacts whatever its fragmentation, while an unmarked one below the floor gains nothing from a rewrite; a compaction that reclaims nothing clears the marker anyway. The integrity check `doctor` reports opens the file exclusively, so no server may be running.

## Layout

| File | What it is |
|---|---|
| `src/lib.rs` | `RedbMetadata` — the metadata contracts over one driver — and `ensure_all`, the shared schema's creation helper |
| `src/table.rs` | the parameterized table handle over a per-table definition marker |
| `src/store.rs` | the store handle: its three arms (writable, read-only, the degraded empty view) and the transaction entry |
| `src/driver.rs` | the async transaction handle: the blocking-pool hop and the write-lock histograms the server's `tinio_storage_write_lock_*` metrics are scraped from |
| `src/open.rs` | the state-database open, the format gate, and the integrity check |
| `src/compact.rs` | offline compaction: the fragmentation snapshot, the decision, the execution |
| `src/error.rs` | the redb error core the backends nest or alias |
| `src/state.rs` | the state table: the format version and the compact marker |
| `src/scan.rs` | prefix scan and drain helpers |
| `src/meta/` | the metadata and version planes (`BucketMeta`, `ObjectMeta`, `UploadMeta`, `VersionMeta`) |
| `src/content.rs` | the byte-table `ObjectContent` |
| `src/bucket.rs`, `src/object_meta.rs`, `src/upload.rs`, `src/part.rs`, `src/object_part.rs`, `src/part_checksum.rs`, `src/upload_checksum.rs`, `src/part_stamp.rs`, `src/version.rs`, `src/version_index.rs`, `src/bucket_versioning.rs` | the shared tables: one definition marker, handle and row codec each |
| `src/objects.rs`, `src/part_data.rs`, `src/part_meta.rs` | the mem-side content tables: object bytes, part bytes, and the parts' size/mtime stat rows |
| `src/acl.rs` | the owner/ACL wire decoders, re-exported from the row codec's one home in `tinio-core` |
| `tests/` | the aggregate conformance call, the cross-table lifecycle test, and the pre-change-database compatibility fixture |

## Gates

Every leg is in-process — an in-memory database or a tempdir — so the gate needs no external service:

```console
cargo test -p tinio-store-redb
```
