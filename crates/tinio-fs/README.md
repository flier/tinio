# `tinio-fs` — the filesystem backend

`tinio-fs` is the filesystem backend: a bucket is a directory under the storage root, an object is a file at its key's path, and the private state lives in the reserved `<root>/.tinio/` directory. `FsStorage` implements `tinio-core`'s `Storage` over `tinio-ops`' operation categories, `FsCleanup` its `Cleanup`, `FsContent` its `ObjectContent`, and — under the `versioning` feature — `VersionOps`. It owns the filesystem's own rules (containment, the atomic commit, the derived-metadata stance) and no other crate's: `tinio-core` defines the contracts, `tinio-ops` the operation engine, `tinio-store-redb` the state database and its tables, `tinio-version` the version plane, `tinio-config` the `[storage.fs]` schema, `tinio-server` the protocol and the read-only refusal.

## Features

| Feature | Effect |
|---|---|
| `versioning` | compiles the version plane's fs side in: the `tinio-version` metadata ports over the state database (`version_adapter.rs`), the supersede/materialize write path, the content-addressed block store under `<state-dir>/blobs/`, the version metrics, and the sweep's retention and block passes. Off, the crate's `VersionOps` impl is empty and the trait's own defaults answer — unversioned state, the access-denied class elsewhere — as `tinio-pg`'s stripped build does. |

## Containment

Everything under a bucket directory is key space, and a key cannot leave it: traversal, absolute paths and control characters are rejected before any filesystem access (key and bucket validation is `tinio-core`'s), and the boundary proof is checked per mapping. A symlink or junction is refused by default — `follow_symlinks` opts out, and then a bucket directory is resolved to its canonical target, so the bucket *is* that target. `.tinio` is tinio's name at **any** depth: writes answer `AccessDenied`, reads `NoSuchKey`, listings skip it — which is what keeps an inner root's private state out of an outer server's data plane.

## The tree is the truth

The filesystem, not the metadata database, is authoritative (SC-006): a directory an operator creates in the root is a bucket and a file dropped into one is an object, served from the next request with no reload — Last-Modified always from the file's mtime, the ETag computed from the content when the stored one no longer describes the file. Metadata is a cache validated against the file it describes (size, mtime, and the file identity where the platform has one), so the database is **derivable**: a stale entry is recomputed, an entry whose file is gone is reclaimed, and a database that cannot be repaired is reported for deletion and rebuild rather than fixed. The mirror has one limit: a key `dir` (a file) and the folder marker `dir/` (a directory) cannot coexist, where `tinio-mem` keeps them distinct.

## Crash recovery

Two passes, different triggers, neither replacing the other, and both touch only tinio-private state — a live bucket directory and its objects are never removed. **Startup repair** (`FsCleanup`) runs after the single-instance bind and before readiness, so it does only fast, deterministic work: the whole of `tmp/` (at startup nothing under it is live, where the sweep is age-driven because it runs while requests are in flight), the in-bucket `.tinio/` staging residue, the unpublished delete-bucket tombstones, the multipart subtrees of buckets that no longer exist, and rows whose tree is gone. The **sweep** runs live and reaps by age — dead temp files, idle multipart uploads, noncurrent versions past their retention, blocks no version row references — yielding to request traffic.

## Cold listings and the scanner

A listing is correct with or without help: an entry the metadata lacks, or one that has gone stale, is recomputed synchronously during the walk — the accepted one-time cost of a tree that tools outside tinio may change. The background scanner (`Scanner`) exists to move that cost off the request path: it walks the root bucket by bucket at `[scanner]` pacing (`TINIO_SCANNER` overrides), computes the missing entries, recomputes the stale ones and reclaims the orphaned ones, so repeated listings become cheap. It is started by the launcher, not by the crate.

## Read-only deployments

A read-only instance writes nothing: its one state directory is the root's own `<root>/.tinio/`, opened read-only (a missing or stale entry is recomputed for the answer and not stored back), and the `state_dir` option is then the caller's own tool state — which the storage never holds, and which is refused when it names a path inside the root. The scanner and the sweep are not run, and of the repair stages only a dry-run pass runs, which reports and removes nothing; the two stages that would write the served tree's own `.tinio` are skipped even in a dry run, because the mode promises not to write the root, not to clean it.

## Layout

| File | What it is |
|---|---|
| `src/backend/{mod,buckets,objects,multipart,version}.rs` | the `Storage` contract over the ops categories: buckets, objects, multipart and the versioned publish |
| `src/path.rs` | key → path mapping and the containment proof |
| `src/write.rs`, `src/write_task.rs` | the atomic streaming write (temp + rename) and the metadata write-pipeline batch |
| `src/meta.rs`, `src/etag.rs` | the ETag store and the streaming compute/validate task |
| `src/database.rs` | fs policy over `tinio-store-redb`'s driver, open and offline compaction |
| `src/bucket.rs`, `src/multipart.rs`, `src/tombstone.rs` | bucket records, part files, unpublished delete-bucket trees |
| `src/listing.rs`, `src/content.rs` | the tree walk and the bytes plane (`ObjectContent`) |
| `src/scanner.rs`, `src/sweep.rs`, `src/cleanup.rs` | the background reconcile, the live aging pass, and the `Cleanup` modes (startup repair, doctor, meta orphans) |
| `src/version_adapter.rs`, `src/version/` | the version plane's fs side (the `versioning` feature) |
| `src/fsutil.rs`, `src/error.rs`, `src/metrics.rs`, `src/testing.rs` | shared filesystem helpers, the crate error, version metrics, offline construction helpers |

## Gates

```console
cargo test -p tinio-fs
cargo check -p tinio-fs
```

The crate's tests are inline, so its unit graph is the whole gate. That graph always carries `versioning` — the crate's self dev-dependency unifies it on whenever test targets build — so the feature-off arm compiles only in a dev-dep-free check, which a plain `cargo check` is (lib only; no `--all-targets`).
