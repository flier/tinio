# `tinio-version` — the shared version state machine

`tinio-version` is the one version state machine every backend runs: supersede, materialize and expire, and the version rows they record. It is written against the persistence contracts (`meta::Metadata`, `meta::VersionMeta`), the content contract (`content::ObjectContent`) and `tinio-blob`'s engine over the backend's block store, so a backend that implements the contracts inherits the crash-safety order and the supersede race unchanged.

It knows nothing of paths, tables, files or SQL, and it opens no transaction of its own — each backend executes the engine's answer with its own writes and I/O. The contracts themselves are `tinio-core`'s, the block plane is `tinio-blob`'s, and the compile-time versioning boundary belongs to the backends and the server: this crate carries no cargo feature and is always compiled.

## The ports that stay

Two ports survive beside the contracts, each because no contract counterpart can replace it. `CurrentRows` exists because the metadata contract's point read deliberately does not gate, while the engine must only ever see the backend's **gated** current — the staleness rule a read applies. `MarkerWrite` exists because a version-creating delete leaves a recorded slot with **no** version row, a state no contract write expresses. The block plane needs no port at all: the engine holds `Store` directly.

## One decision, two backends

The write path's decision is a pure function, not a port: `plan_write` answers whether to supersede, which identity and sequence the incoming version takes, whether the one-null-row convergence applies and whether the result is a delete marker, from the bucket's state, the key's gated current slot and the key's sequence view. It reads no table, opens no file, mints no identity and awaits nothing. Both backends execute that one answer, so their write policies cannot drift.

## What the engine owns

The bucket's versioning state, the key's gated current slot, the supersede that makes a version noncurrent, the materialize that records the published one, the expire of a named version, and the live set the backend's sweep is planned against. Nothing else: the rows it records are the persistence contracts', the bytes it moves are the content plane's and the block engine's, and the identifier a version is called by on the wire is `tinio-core`'s version domain.

## Layout

| File | What it is |
|---|---|
| `src/engine.rs` | `Versions`: the state machine over the contracts — state, current, supersede, materialize, expire, and the live set the backend's sweep takes |
| `src/plan.rs` | `plan_write` and the decision types it answers with (`Intent`, `Plan`, `Seqs`, `Identity`) |
| `src/ports.rs` | the two ports that stay (`CurrentRows`, `MarkerWrite`) and the `Current` slot read |
| `src/error.rs` | the engine's error and its projection onto the contract error |
| `src/engine/tests.rs`, `src/testutil.rs` | the engine's own tests and the in-crate contract double they run against — rows in a map, content in a tempdir, an in-memory block store |

The port double is test-only, so nothing it does reaches a production graph.

## Gates

```console
cargo test -p tinio-version
```

One command: the engine's unit tests over the in-crate double, plus the doctests. No database, no environment variable, and no feature to select.
