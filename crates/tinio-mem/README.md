# `tinio-mem` — the in-memory reference backend

`tinio-mem` is the in-memory reference implementation of the `tinio-core` `Storage` contract: `MemoryStorage` over a redb database held in memory, with the operation bodies taken from the shared operation engine (`tinio-ops`), the version plane under the `versioning` feature, and a `Cleanup` pass with nothing to repair. It exists so every other backend has something to be measured against — the shared conformance suite that each `Storage` implementation must pass runs here, the e2e suite's mem pass runs the backend-neutral scenarios against a served instance, and the operation engine's own in-memory doubles are shaped like mem's planes. It is a reference, **not** a production backend: nothing is durable, nothing outlives the instance, no `[storage] type` selects it, and a server wired to it serves with no storage directory at all.

## Features

| Feature | Effect |
|---|---|
| `versioning` | the version plane: `VersionOps` over mem's own rows and its content-addressed blob table. Off (the default), `MemoryStorage` answers the trait's own defaults — unversioned state, and an access-denied class for every other version operation. |

## Layout

| File | What it is |
|---|---|
| `src/storage.rs` | `MemoryStorage`, `MemoryOptions`, the database layout and its shared handles, the `Storage` impl, and the hooks the engine's bucket receiver runs under |
| `src/bucket.rs`, `src/object.rs`, `src/multipart.rs` | the three operation groups: the engine's bodies, plus the steps only this backend can take |
| `src/version.rs` | the version plane: the state machine, the blob table, `VersionOps` (`versioning`) |
| `src/counting.rs` | `CountingContent`: the byte-accounting decorator, where the resource limits are enforced and nowhere else |
| `src/cleanup.rs` | `MemoryCleanup`, the pass that reports one lint line because there is no durable state |
| `src/error.rs` | `Error`, the contract failures and the redb failures it wraps |
| `src/testutil.rs` | the crate's own test fixtures (`#[cfg(test)]`) |

## The version plane

The current version is the row, and a superseded version's whole content moves into a content-addressed blob table — whole-blob granularity, no chunking and no block files, because the block engine is an fs-side concern and the conformance suite asserts semantics, never layout. Every operation runs in one write transaction, so the crash windows the fs supersede protocol guards do not exist here, and the shared tables mem reads are the same ones `tinio-fs` opens.

## What the engine leaves to mem

The bodies the operation engine writes are contract calls, and the steps mem keeps are the ones that shape cannot carry: a version-creating write must read the outgoing bytes and land the history row in **one** transaction, a version-creating commit must write the staged bytes raw where the engine's staged value cannot hand them back, a copy's destination policy (the object-size cap, the folder-marker key, a version-creating destination) is mem's own read-then-write, and the bucket teardown must stay one transaction with its emptiness probes re-run inside it. Outside the engine sit the byte-plane accounting and the part-stat rows, which no contract carries.

## Resource limits

Both limits are optional and default to **unlimited**; an operator wiring this backend sets them. The accounting covers every byte mem holds — parts, and the superseded version blobs a versioned bucket's history keeps — and it is a soft limit: concurrent writers may briefly overshoot, but a refused write publishes nothing and an inner failure gives its reservation back. The limits are the `[storage.mem]` section's keys; how a run reaches them is the wiring's business, not this crate's.

## Gates

The crate's own dev-dependency on itself turns `versioning` on for every test graph, so the one command runs the versioned legs too. The feature-off arm is therefore reachable only by a check that builds no dev-dependencies — the CI `cargo hack check --package tinio-mem --each-feature` leg.

```console
cargo test -p tinio-mem
```
