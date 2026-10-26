# Design: tiered storage and ILM transitions

**Status:** final after two adversarial review rounds (correctness, consistency, performance, security, compatibility) and the decision grill.

## Background

- `[storage] type` selects one complete backend today: `fs` (default) or `pg`; `mem` is a test backend.
- Each backend owns metadata rows and object content. `ObjectContent` is the byte plane; `Storage` is the S3 contract.
- Lifecycle implements expiration only.
- A backend without tiers refuses transitions (LC9, `2026-10-18-s3-object-lifecycle-design.md`).
- This design gives the tiered backend the demote-only `Transition` T11 defines.

## Goal

One root, three content tiers: mem cache (hot), fs (warm, durable), pg (cold). Reads fill mem. A background mover demotes and promotes by heat and fs capacity; lifecycle rules demote. Object rows stay in the fs redb. Existing backends stay unchanged outside tier mode.

## Non-goals

- Versioned buckets, Object Lock, multipart part tiering.
- `NoncurrentVersionTransition`; S3 Intelligent-Tiering emulation.
- Compression, encryption or dedup at a tier.
- Multi-node tier sharing; a cold backend other than pg.
- Durable mem.
- Rule-driven promotion (promotion is heat-driven only).

## Decisions

| ID | Decision |
|----|----------|
| T1 | **The layer is one crate, `tinio-tier`.** It decorates `Arc<FsStorage>`; pg is the cold plane through `PgStore`; mem is a layer-internal cache/buffer. Dependencies: `tinio-core`, `tinio-fs` with `tier` **and `versioning`**, `tinio-pg`, `tinio-store-pg`, `tinio-util`, `redb`, `tokio`, `async-trait`, `futures`, `bytes`, `thiserror`, `tracing`, `serde`, `serde_json` (the index payloads and the quarantine record), `uuid` (the root identity and the quarantine entry id), `secrecy` (the pg dsn), `humantime-serde`, `garde`, optional `tinio-store-redb` (the in-memory content plane in tests and benches), and target-gated `rustix`/`windows-sys` for capacity. |
| T2 | **`ColdInfo` is the cold metadata authority.** For every pg-placed key, `ColdInfo` (key, size, last_modified, ETag, tags, checksum, owner, ACL, version identity, version-recording flag) answers HEAD, GET info, tags, ACL and listing. The fs row is a mirror; retained multipart part rows come from the fs row. Losing `tier.redb` loses placement; repair rebuilds placement by scanning pg keys and matching fs rows (a row with no file is cold; a row with a file is warm and the pg copy is a reported orphan; no row is unadoptable). |
| T3 | **The fs crate gains one row-only seam.** `FsStorage::tier_rows()` (feature `tier`) returns `tier::Rows` with `info`, `facts` → `tier::Facts`, `set_row(info, mtime)`, `parts` and `scan(bucket, prefix, after, limit)`. `set_row` restores every metadata field (ETag, tags, checksum, owner, ACL) while preserving the version slot, part rows and lock rows; it stats the file for the identity when one exists. `scan` is a key-ordered, bounded row scan of warm and cold rows. No content I/O and no behavior change outside tier mode. |
| T4 | **Cold content has one representation: absent.** Demotion removes the fs file and keeps the row. Promotion recreates the file through the fs content plane's stage/publish path (ownership, mode and dir-owner rules included), then restores the row with `set_row` and the cold `last_modified`. No sparse file, no mode switch, and no `unsafe` in any crate except one scoped exemption: `crates/tinio-tier/src/capacity.rs` calls Windows `GetDiskFreeSpaceExW` (T1's target-gated `windows-sys`) under `#[expect(unsafe_code)]`, because no safe wrapper exists; the exemption covers only that call's two out parameters and its NUL-terminated wide path. |
| T5 | **Cold reads never trust the fs content.** HEAD/GET info come from `ColdInfo`. The layer compares `tier_rows.facts` with the placement and stats the file path: no row → deleted (clear placement/pg, `NoSuchKey`); a file with a different ETag → a completed overwrite (fs wins); a file with the same ETag → a crash-before-removal (serve cold and let GC remove it); no file → cold. A full-length all-zero file is not special. Backups and copies therefore reconcile by ETag, not inode. |
| T6 | **Read path:** write buffer → mem cache → fs → pg. A pg hit fills mem only on a full-object GET (never a range). A cold GET verifies pg presence and length inside the open call; a missing copy or wrong length is an error naming the key. A null-version address on an unversioned bucket routes through the same path and must match the placement's version identity. |
| T7 | **Writes stream; only batch buffers.** `stage_body` delegates to the fs streaming stage (a temp file) for sync and non-batch writes; batch-eligible writes are read into a bounded mem buffer that spills to the fs stage past `max_object_bytes`. Only `commit_object` decides buffering. Buffering requires an unversioned bucket, a plain request (empty tags, default ACL, no owner, no lock), **no storage class**, and size at most `max_object_bytes`. A failed precondition or aborted request is never visible. |
| T8 | **Batch flush preserves Last-Modified and resets placement.** The flush writes through the fs commit, sets the file mtime to the buffered time, and `set_row`s the buffered `Info`; the destination's placement and pg copy are cleared. Client listings and the lifecycle pass flush; the metrics scan does not. The flush is single-flight and the buffer has a hard cap. A buffered write that keeps failing is retried three times with backoff, then quarantined under `<root>/.tinio/quarantine/` (its bytes and the metadata that recovers it) and counted on the quarantine gauge, so one poisoned entry never wedges a later flush, a listing or a shutdown; the drain continues with the next entry. A crash loses unflushed acknowledged writes; that is the documented cost. |
| T9 | **Mem and heat are bounded.** Mem is an LRU of whole objects at most `max_object_bytes`, under a capacity shared with the buffer (one budget, partitioned). Heat is recorded on every GET, including mem hits, in sharded counters with a promotion queue fed at threshold crossing; heat rows have a TTL and flush in chunks. Caches, pending entries and the key's heat are invalidated on every mutation. |
| T10 | **Classes:** `STANDARD` = fs, `STANDARD_IA` = pg. Only `GetObjectAttributes` reports the class. `STANDARD_IA` on a plain unversioned PUT demotes before answering; on a versioned or suspended bucket it is `InvalidRequest`; on a backend without tiers the field is **ignored**, today's behavior. `CopyObject` and `CreateMultipartUpload` accept the field but ignore it. |
| T11 | **Lifecycle transitions only demote and follow AWS's limits.** `Transition { days | date, storage_class }` accepts `STANDARD_IA` only; `Days ≥ 30`; a past `Date` is accepted. A transition-only rule is valid; `transitions` is skipped when empty on the wire. `NoncurrentVersionTransition` is refused. Expiration is evaluated first and wins when both are due; delete markers are never transitioned. Versioned/suspended buckets skip **transitions only** — expiration still runs. The tier gate covers the S3 surface, the management-plane admin, the CLI `put` and the seed; on a backend without tiers all four keep today's refusal. |
| T12 | **Moves and deletes reconcile; GC is not a freelancer.** A cold write path records a `copying` placement before publishing to pg, so GC skips in-flight content. After `gc_grace`, GC resolves a `copying` placement: pg content complete and the fs snapshot matching → finish the demote; otherwise discard the partial pg copy and revert to warm; `verify` reports anything it cannot decide. GC finishes tombstones, removes a file whose placement is pg and whose row matches `ColdInfo` after the grace and with no readers, and removes pg content only for a tombstone or a reported orphan — never by an automatic full diff. `--delete-orphans` is the explicit destructive action. |
| T13 | **Move order, with revalidation.** Demote: snapshot facts, copy fs→pg (placement `copying`), then under the per-key write lock re-read the facts and placement; on any change, discard the copy; otherwise set `pg` and leave the fs file for GC. Promote: snapshot the cold ETag/placement, copy pg to the fs content plane's stage, then under the lock re-check placement `pg` and the cold ETag; publish, `set_row` from `ColdInfo` with its `last_modified`, set placement fs, remove pg. Delete: tombstone under the write lock, delete fs, clear the index, remove pg; a failure rolls the tombstone back; GC completes only tombstones whose row is already absent and that are older than the grace. |
| T14 | **Serialization.** A per-key `RwLock`: reads hold the read side through the body stream; moves/deletes take the write side for the snapshot-check/finalize. A mover uses `try_write` and skips a busy key. A listing and a move's finalize hold the bucket gate's shared read side — the listing for the whole page, the finalize briefly — so a finalize never swaps a plane under a page being assembled, and the per-key write lock excludes a competing move on the same key. A versioning flip takes the gate's write side, so no listing or move finalizes across it. Moves and the lifecycle loop run with configurable concurrency, default 1. |
| T15 | **Paths stay in the layer.** The layer composes `path::bucket_path`/`key_path`, checks `is_symlink_or_reparse` on every component under the resolved `follow_symlinks`, and opens no-follow handles for mutation (a narrow Unix/Windows helper in `tinio-tier`); the fs symlink policy is reproduced and tested. `head_bucket` is checked before writing. |
| T16 | **Every byte-producing operation resets and invalidates.** Copy, rename, multipart complete and `copy_part` reset the destination to fs, remove its pg copy, and invalidate the mem/buffer entry. A cold source is promoted first and stays warm; `copy_part` is intercepted because fs's fast path opens the source file. |
| T17 | **Construction fails closed.** Index at `<root>/.tinio/tier.redb` (created with mode `0600`; the recorded format version gates the authoritative tables — placement, warm, heat and the root/format meta — while the derived `warm_key` reverse map is created and backfilled from `warm` on open, so it is not part of the version gate), marker at `<root>/.tinio/tier.mode`, both recording the root ID; the marker is created and fsynced before the index and any placement. `type = "fs"`/`"pg"` refuses a root with **either** marker or index; `tinio doctor` refuses either, even with the index missing; doctor's full repair never runs there. The index compacts at construction when its fragmentation reaches `index_compact_percent` (redb requires exclusive access), not in the background. |
| T18 | **The pg plane is owned and root-scoped.** Tier mode requires a dedicated database and takes `PgStorage::connect`'s ownership. The root UUID lives in a dedicated reserved namespace (`tinio-tier-root`): every S3 operation on that name is refused, conversion refuses a root that already holds it, and GC/repair exclude it. `--adopt` refuses a root-ID mismatch. A fresh index over non-empty pg refuses until `--adopt`. Sharing a database across roots is unsupported. |
| T19 | **The index supports bounded work.** `placement` keyed `(bucket,key)` with `ColdInfo`, state (`live`/`copying`/`deleting`), mtime; `warm` keyed `(mtime, bucket, key)` so capacity demotion finds never-read objects; `warm_key` keyed `(bucket,key)` → mtime is `warm`'s derived reverse map, so a rewrite retires the superseded candidate by point lookup, not a table scan; `heat` with a TTL; `scan_placement`/`scan_warm`/`scan_heat` are keyed range scans; placements use immediate durability. |
| T20 | **Listings merge bounded streams.** The layer scans warm fs rows and the cold index range with prefix/after/limit, merges by key with index precedence, applies delimiter grouping before cutting the page, and never repeats a key. `ListObjectVersions` includes cold keys as their null version. The metrics scan uses a non-flushing count path. |
| T21 | **Capacity uses warm candidates and survives restarts.** Once the warm volume passes the high watermark, the mover's capacity pass pops the oldest `warm` rows until the volume reaches the low watermark — demotion stops at the low watermark, never at the high one. An idle stage demotes a warm object no read has touched within `demote_idle` (a zero `demote_idle` disables it) and counts those moves apart from the capacity ones; durable heat rows are retained for `max(heat_window, demote_idle)`, so a read inside the idle threshold stays visible; heat is the promotion signal; dwell applies. After bind, the warm table is rebuilt from the fs row scan (a `warm_ready` flag gates candidate selection until the rebuild completes) and the promotion queue is restored from heat rows inside the window; reads and heat keep working during the rebuild. A full volume surfaces the OS write error; there is no invented admission policy. |
| T22 | **Configuration has one definer.** The CLI's `[storage.tier]` owns the TOML schema, defaults and validation; `tinio_tier::Config` is a runtime value, not a second schema. Durations use `humantime_serde`; the section is `skip_serializing_if` absent in generated configs. Keys: `capacity_bytes`, `max_object_bytes`, `flush_mode`, `batch_bytes`, `batch_interval`, `heat_window`, `promote_reads`, `stats_flush`, `high_water_percent`, `low_water_percent`, `demote_idle`, `dwell`, `scan_interval`, `move_concurrency`, `index_compact_percent`, `gc_grace`. The lifecycle pass's concurrency is the existing `[lifecycle] concurrency` (a `usize`, default 1, zero refused at parse; `crates/tinio-cli/src/config/lifecycle.rs`), not a second `[storage.tier]` key. No `representation`, no critical watermark. |
| T23 | **Observability has an owner.** `tinio-util::metrics` gains `Backend::Tier` and tier families (hits/misses per tier, bytes per tier, promotions/demotions, flush/move latency, orphans, quarantined writes waiting for an operator, cache evictions). Spans cover move, flush, verify, repair and downgrade and never print keys, tags, ACLs or `ColdInfo`; the no-identity rule governs spans, while the layer's events follow the fs and pg backends' log convention and may name the bucket and key an operator needs for triage. |
| T24 | **Compatibility deviations are recorded.** Class only in `GetObjectAttributes`; third-party tools reading HEAD/listing see `STANDARD`. Lifecycle transitions cannot promote and require the IA floor. Batch keeps its crash window. Downgrading requires `prepare-downgrade`. Out-of-band edits to a tier root's files are unsupported. Sharing a pg database across roots is unsupported. `storage::Error::NoTiers` and `InvalidRequest` are additive variants on a facade-re-exported exhaustive enum; the semver gate's record is `docs/semver.md`. |
| T25 | **E2E covers the product and third-party tools.** `@tier` is a backend tag excluded from the default pass; `TINIO_E2E_BACKEND=tier` is parsed; `Server::tier` uses the shared pg permit/reset and a `testing` constructor that skips ownership and the root check, and starts the mover/flush/GC and lifecycle tasks. A separate external suite drives aws-cli, rclone, mc and boto3 against a spawned tier server; all four are required in CI, which installs the clients and the boto3 venv and fails if one is missing. |
| T26 | **Existing-crate surface is bounded.** core: `Class`, `#[async_trait] Storage`, four defaults (`supports_tiers`, `storage_class`, `transition`, `metrics_scan`), `NoTiers`, `Backend::Tier`; util: `time::civil_from_days` (moved from `testing/sigv4.rs`); fs: one `tier_rows()` seam, one `ScannerOptions` field, and `max_concurrent_uploads` as an `FsOptions` construction option (plain `u32`, default `DEFAULT_MAX_CONCURRENT_UPLOADS`) — the tier constructors pass the fs options through and the CLI sets `[s3] max_concurrent_uploads` in them, so there is no post-construction mutation; tier: the root re-export of `FORMAT_VERSION` (its rustdoc example runs from outside the crate); server: attributes class, the shared PUT/`PostObject` handler honouring `x-amz-storage-class` (T10 names only PUT), lifecycle gate, threshold; server tests: the `otel_round_trip.rs` lint refactor; lifecycle: transitions; cli: config/start/metrics/commands; e2e: tests; mem: one test-only `tier_defaults.rs` pinning the defaulted trait methods; store-redb: `compact::MIN_ALLOCATED` public for a tier test; pg/store-redb: test-only literals gain `transitions: Vec::new()` for the lifecycle model's new field. |
| T27 | **Read-only tier writes nothing.** It opens the index and pg read-only, runs no tasks, performs no repair/refresh, and serves cold GET/HEAD/listing from `ColdInfo`; a missing index refuses. |
| T28 | **Upgrade/downgrade is explicit.** The marker blocks non-tier starts and doctor. `prepare-downgrade` promotes every cold object, removes index and marker, and verifies the root is fs-only. An older binary does not read the marker; the operator must run `prepare-downgrade` before rollback. |

## Architecture

```
S3 backend
    │
TierStorage (Storage)
    ├─ MemCache        whole objects <= max_object_bytes, shared budget
    ├─ WriteBuffer     staged tokens; visible only on commit
    ├─ Index           tier.redb: root ID, placement + ColdInfo, warm, heat
    ├─ Mover           warm candidates (capacity/idle), heat (promote)
    ├─ tier::Rows      fs row info/facts/set_row/parts/scan
    ├─ FsStorage       warm content, publish (ownership/mode), auth
    └─ PgStore         cold content
```

- Read: buffer → mem → fs → pg; cold info from `ColdInfo`; reconcile by row/file/ETag.
- Write: fs streaming stage; batch only for plain, unversioned, classless, small writes.
- Demote: copy, revalidate, placement pg, file left to GC.
- Promote: copy to the fs stage, revalidate, publish, `set_row`, placement fs, remove pg.
- Delete: tombstone under the lock, fs delete, index clear, pg remove; rollback on failure.

## Configuration

The keys are listed in T22. The CLI's `[storage.tier]` owns the TOML schema, defaults and validation; `tinio_tier::Config` is a runtime value with no second defaults, and `contracts/config.md` carries the contract. `[storage.pg]` is reused; `[storage] type = "tier"` requires it. Invalid combinations: missing pg, low ≥ high, `max_object_bytes > capacity_bytes`, `batch_bytes > capacity_bytes`.

## Lifecycle integration

- Model: `Transition { days | date, storage_class }`, `STANDARD_IA` only, `Days ≥ 30`, past dates accepted.
- A transition-only rule is valid; empty transitions are omitted on the wire.
- Expiration first; markers skipped; versioned/suspended buckets skip transitions only.
- Gates: S3, management plane, CLI `put`, seed.
- The runner calls `Storage::transition` with configurable concurrency.

## S3 surface

- `GetObjectAttributes` reports the class; GET/HEAD/listings unchanged.
- PUT `x-amz-storage-class`: `STANDARD`/`STANDARD_IA`; unknown → `InvalidStorageClass`; non-tier ignores the field.
- Tier + versioned/suspended + IA → `InvalidRequest`; tier + plain unversioned IA → synchronous commit then demote.
- Copy/MPU ignore the class.

## Core contract additions

- `Storage` is `#[async_trait]`.
- Defaulted: `supports_tiers`, `storage_class`, `transition` and `metrics_scan` (default `list_objects`).
- `tinio_core` gains `Class` with `STANDARD`/`STANDARD_IA` wire spellings.
- `tinio-util::metrics` gains `Backend::Tier`.

## Failure, repair, GC and downgrade

- Construction: index/marker (marker first, fsync), pg ownership, root ID, then bind; compaction at construction.
- Lazy reconciliation as T5.
- `verify`: report-only; `--deep` hashes pg content for cold objects and fs content for warm.
- Composed multipart ETags are unverified unless the part rows recompose.
- `repair`: report-only; `--adopt` (root-ID matched), `--delete-orphans` (the only destructive flag). Rebuild as T2.
- `prepare-downgrade`: promotes all, removes index/marker, verifies fs-only.
- `tinio doctor` refuses marker-or-index roots; full repair never runs there.

## Testing

- Conformance over fs + pg; crash matrix at copy, `copying`, placement, file removal, promote publish, index clear, tombstone.
- Also both cold and warm reads, null versions, cache invalidation, batch and read-only.
- Lifecycle: transition-only rules, IA floor, expiration-first, versioned expiration regression, gates.
- S3: attributes class, non-tier ignore, copy/MPU ignore.
- E2E: `@tier` backend and the in-process tasks.

## External and compatibility testing

- External: aws-cli, rclone, mc, boto3 — IA PUT, cold GET/HEAD, attributes class, cold-source copy and UploadPartCopy.
- Also multipart, lifecycle round trip, batch LIST, restart and versioned refusal; all four are required in CI.
- Benches: the `bench-ab` interleaved harness with a tier leg for warm GET, cold GET and PUT, one named op per process.
- Compatibility: feature-off, each-feature, WSL/Linux, downgrade.

## Accepted limits

- Versioned buckets, Object Lock, multipart parts and noncurrent transitions are out.
- Batch mode loses unflushed acknowledged writes; it is the mode's cost.
- Listings merge the fs row scan with the cold index; cost grows with the cold set.
- Class is reported only by `GetObjectAttributes`.
- Lifecycle rules cannot promote and require the AWS floor.
- A cold-source copy promotes first and leaves the source warm.
- `verify --deep` cannot validate composed multipart ETags without part rows.

## Accepted limits: compatibility and operations

- Downgrading requires `prepare-downgrade`; the old binary does not read the marker.
- Out-of-band edits to a tier root's files and sharing a pg database across roots are unsupported.
- A pg restore from an older backup can leave reported orphans; `--delete-orphans` is manual.
- GC's file removal is deferred by the grace and a reader count.

## Open calls recorded

- Absent-only; no sparse/stub/mode.
- `ColdInfo` authority with a full-row restore seam.
- Warm candidate table for capacity; heat promotion queue.
- AWS-tight lifecycle; non-tier class ignored.
- E2E harness runs the tier tasks; an external client suite is part of acceptance.
- Warm/heat rebuild runs after bind behind `warm_ready`; the `bench-ab` tier leg owns the A/B verdict.
- `gc_grace` is 5m; `NoTiers` and `InvalidRequest` are recorded semver exemptions (`docs/semver.md`).
