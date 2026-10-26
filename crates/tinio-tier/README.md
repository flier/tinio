# `tinio-tier` — the tiered-storage plane

`tinio-tier` serves one storage root from three content planes: an in-memory cache and write buffer (hot), the filesystem (warm and durable) and PostgreSQL (cold). `TierStorage` **decorates** an `Arc<FsStorage>` rather than replacing it — it reads the filesystem's rows through that crate's row-only `tier` seam, keeps the placement and the cold metadata in an index of its own at `<root>/.tinio/tier.redb`, and answers the `Storage` contract from whichever plane holds the key. It owns the layer's own rules and no other crate's: the contracts are `tinio-core`'s, the warm plane and its contents are `tinio-fs`'s, the cold plane's connection and root identity are `tinio-pg`'s and `tinio-store-pg`'s, and the `[storage.tier]` schema, its defaults and its validation are `tinio-cli`'s — `Config` here is the runtime value those settings convert into.

A demoted object's cold representation is **absent**: the file is gone, the row and its `ColdInfo` remain, and `ColdInfo` is the authority HEAD, GET info, tags, ACL and listings answer from. A background mover demotes and promotes by heat, capacity and idle time; a batch write is acknowledged before its bytes land and is flushed later, and a flush that never succeeds is set aside in quarantine rather than wedging the next one. Construction fails closed: the `tier.mode` marker and the index both record the root ID, and a non-tier start and `tinio doctor` refuse a root carrying either.

## Features

| Feature | Effect |
|---|---|
| `testing` | the constructors and helpers the crate's own tests and the e2e `@tier` pass build a scratch layer with |
| `bench` | the `tier-leg` bench binary and the criterion bench, over the `warm_get` / `cold_get` / `put` operations `cargo xtask bench-ab` drives |

## Layout

| File | What it is |
|---|---|
| `src/storage.rs` | `TierStorage`: the `Storage` decorator over the three planes, the marker and index paths, the tier-root refusals |
| `src/config.rs` | the runtime `Config` and its two plane views `Mem`/`Fs`, `FlushMode`, and the cross-field rules |
| `src/stage.rs` | the write stage: a body streams to the filesystem's temp file, or waits in the bounded batch buffer |
| `src/buffer.rs` | the batch buffer: acknowledged-but-unflushed entries, its cap, its single-flight flush |
| `src/cache.rs` | the in-memory object cache and the byte `Budget` it shares with the buffer |
| `src/heat.rs` | the read counters and the bounded promotion queue |
| `src/index.rs` | the index: `Placement`/`ColdInfo`/`State`, the warm candidates, the heat rows, the root identity |
| `src/mover.rs` | the move state machine: demote, promote, the delete tombstone, the garbage pass, verify and repair |
| `src/capacity.rs` | the volume-pressure probe the capacity pass reads (T21) |
| `src/listing.rs` | the bounded merge of the warm row stream and the cold index range into one page |
| `src/quarantine.rs` | the pairs a failed flush is set aside in, and their recovery metadata |
| `src/pg.rs` | the cold plane's connection, its ownership lock and the reserved root-identity namespace |
| `src/atomic.rs` | the owner-only, fsynced-and-renamed write every private file goes through |
| `src/error.rs` | the layer error and its projection onto the contract error |
| `tests/`, `benches/` | the plane tests (read/write path, batch, stage, listing, moves, mover, pg plane, conformance) and the bench |

## Design

Model, moves, the offline verbs (`verify`, `repair`, `prepare-downgrade`) and the accepted limits: [tiered storage design](../../docs/superpowers/specs/2026-10-24-tiered-storage-design.md). Keys, defaults and validation: `specs/001-s3-local-server/contracts/config.md` (`[storage.tier]`); the wire — classes, classed writes, transitions, deviations: `specs/001-s3-local-server/contracts/s3-surface.md` §Tiered storage.

## Gates

Database-free (the default gate):

```console
cargo test -p tinio-tier
```

The database-backed legs read `TINIO_PG_DSN` through `tinio-store-pg`'s harness and skip when it is unset — that harness owns the skip rule and the `TINIO_PG_REQUIRED` switch that voids it, and each leg creates its own scratch database. The dsn's export line is `tinio-store-pg`'s and is not repeated here.

Serve a tier root (the `tier` feature is on in the facade's default `full` set):

```console
cargo run -p tinio -- server ./root   # with `[storage] type = "tier"` and `[storage.pg]`
```
