# `tinio-ops` — the S3 operation bodies the backends share

`tinio-ops` writes the S3 storage semantics once, as the operation bodies behind `tinio-core`'s `Storage` categories, over the persistence contracts — the row planes (`meta::Metadata`, `meta::VersionMeta`), the content plane (`content::ObjectContent`) and the block plane (`blob::BlockStore`) through `tinio-blob`, with the version-touching bodies running on `tinio-version`'s state machine.

It is deliberately **not** a `Storage` implementation and **not** a framework: every backend keeps its own `Storage`/`VersionOps`/`Cleanup` implementations, its error enum, its `Hooks` implementation and its locks, and calls the bodies from there. The engine knows the contracts only — never a table, a path or SQL — and it carries no cargo feature: a backend without versioning refuses through its own `#[cfg]` twin, so the engine never answers for it.

## The seam and the policies

`Hooks` is the two steps the engine cannot take itself — the bucket-delete action and the part-write lock guard — and it is deliberately two methods wide; the rule for widening it is the glossary's (`../../CONTEXT.md`). Everything else the engine needs is a persistence contract. `NoHooks` serves a backend that takes neither step.

The two policies are fields of the group receivers, declared by the backend and read by the bodies, never inferred: `MetadataFailure` and `BucketRow`, each defined in the glossary (`../../CONTEXT.md`). A backend without a block plane builds no version plane and takes no body that needs one.

## A read over a read-only store

A store that refused a write because it is read-only does not fail the read: the value the write would have recorded is served instead, and the degradation is observable — one event per read — never silent. Every other failure propagates. The rule has one home in the engine, so no body grows its own reading of a store's refusal.

## What stays backend-side

Multipart **completion** and **copy** are not bodies: each backend's commit half is one state transaction of its own. `list_object_versions`, `current_version` and `ensure_rename_allowed` are the contract's backend-side methods by decision, never bodies this crate is expected to grow.

## Layout

| File | What it is |
|---|---|
| `src/ops.rs` | the `BucketOps` receiver, the version plane, the two policies and the read-that-wanted-to-write rule |
| `src/bucket.rs` | the bucket group's bodies: lifecycle, the listing page, the tag/CORS/ACL element reads and writes |
| `src/object.rs` | `ObjectOps` and the object group's bodies: the version-creating put beside the version-free lifecycle, the tag/ACL pairs, the retained part list, the read-heal |
| `src/multipart.rs` | `MultipartOps` and the multipart group's bodies: the upload lifecycle, the part pipeline, the part-write lock |
| `src/version.rs` | the version group's bodies: the versioning-state pair, the version-addressed reads and deletes, the per-version tag/ACL pairs |
| `src/hooks.rs` | the two-method `Hooks` seam, its guard, and the no-op pair |
| `src/listing.rs` | the listing path: the content scan, the page assembly and the batch enrichment |
| `src/staged.rs` | `StagedBody`: what a write streams into before it is published, beside what the commit then needs |
| `src/testutil.rs` | the in-memory reference base this crate's own tests run on |
| `benches/listing.rs` | the paginated full-walk listing benchmark |

## Gates

```console
cargo test -p tinio-ops
cargo bench --profile release -p tinio-ops --bench listing
```

The first command is the crate's whole gate — the bodies' unit tests over the in-memory base, plus the doctests a plain `cargo test` also runs — and needs no database and no environment variable. The bench's largest fixture leg is opt-in: `TINIO_OPS_BENCH_LARGE=1` adds it, and unset the run keeps the two smaller fixtures and stays short enough for the acceptance gate. How a bench is run and read is `../../docs/benches.md`'s.
