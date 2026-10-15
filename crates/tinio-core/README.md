# `tinio-core` — the Storage contract

`tinio-core` is the extension seam of tinio: the async `Storage` contract, the backend-agnostic failures every backend reports, the `Cleanup` and task-pipeline contracts, and the validated domain newtypes untrusted input must pass through before any backend is called. It also defines the four persistence contracts a backend is implemented against — the metadata planes (`meta::Metadata` and the standalone `meta::VersionMeta`), the content plane (`content::ObjectContent`) and the content-addressed block plane (`blob::BlockStore`).

The crate carries no HTTP and no filesystem. The S3 mapping layer speaks the `Storage` contract and never backend code; a backend (`tinio-fs`, `tinio-mem`, `tinio-pg`) implements these traits over its own transport and must pass the conformance harness behind `tinio-util`'s `testing` feature. What the contracts *mean* when they are run is not this crate's: the version state machine is `tinio-version`'s, the shared operation bodies are `tinio-ops`'s, and each backend's own repair semantics are its own.

## Features

| Feature | Effect |
|---|---|
| `sqlx` | the contract types' hand-written PostgreSQL mapping (`src/sql_types.rs`), compiled by `tinio-store-pg` |

## Layout

| File | What it is |
|---|---|
| `src/storage/` | the async `Storage` contract: its four categories, the backend-agnostic `Error`, listing pagination and delimiter grouping, byte ranges, streaming bodies, nanosecond timestamps |
| `src/meta/` | the persistence row contracts: `Metadata` over the bucket/object/upload categories, and `VersionMeta` — the version rows, the bucket's versioning state and the key's current slot |
| `src/content.rs` | the `ObjectContent` contract: object and part bytes, their stage/publish visibility point, and the bucket lifecycle of the content plane |
| `src/blob.rs` | the `BlockStore` contract: immutable, content-addressed blocks, visible only once published under their own hash |
| `src/cleanup.rs` | the `Cleanup` contract: startup repair, doctor diagnostics and scanner reclamation behind one seam |
| `src/pipeline.rs` | the generic task-pipeline contract (`Task`, `Runner`, `Completion`) |
| `src/object.rs`, `src/bucket.rs`, `src/etag.rs` | the validated newtypes and the rules untrusted input passes through |
| `src/version.rs` | the version domain: opaque `Id`, the three bucket states, and the shapes the version operations exchange |
| `src/acl.rs`, `src/cors.rs`, `src/checksum.rs`, `src/multipart.rs` | the domain types and wires the contracts exchange: grants and owners, CORS configuration, checksum algorithm and type, upload and part state |
| `src/mode.rs`, `src/severity.rs`, `src/percent.rs` | the served plane's write policy, the diagnostic severity levels, and the shared percent wire codecs |

## Two families of contract

The server-facing `Storage` contract is what the S3 mapping layer speaks; the four persistence contracts (`Metadata`, `VersionMeta`, `ObjectContent`, `BlockStore`) are what a backend is implemented against. A backend's own error type must convert into the contract `Error`, which is the vocabulary the mapping layer and the conformance harness read. Bodies cross the seam as a `Send` stream of bytes chunks, so neither side ever buffers a whole object, and every returned future is `Send` for the hosting layers. The associated error and the category-method bounds make the contracts dyn-incompatible by design, so a backend is generic over them rather than boxed.

## Validated input

A bucket name, an object key and an ETag are newtypes that carry their own validation; there is no second constructor and no unchecked path into the contracts. Untrusted input goes through the checked constructors first, so a backend receives values that already hold their documented rules — the contracts state the precondition rather than re-checking it per method. The rules themselves, and their property suite, are `tests/validation.rs`'s subject.

## Gates

```console
cargo test -p tinio-core
cargo check -p tinio-core --features sqlx
```

The first command is the whole crate: the unit tests, the key and bucket-name property suite in `tests/validation.rs`, and the doctests. It needs no database and reads no environment variable. The second compiles the PostgreSQL mapping arm alone, which is what a graph containing `tinio-store-pg` turns on.
