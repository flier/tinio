# `tinio-blob` — the content-addressed block engine

`tinio-blob` stores a blob once, addressed by the SHA-256 of its content, reads it back as a stream, and reclaims the blocks no live reference points at. It implements the block-store contract (`tinio_core::blob::BlockStore`) — `LocalBlocks`, over the local filesystem — and owns the policy above it: hashing, deduplication, blob references and reclamation.

Nothing here knows what S3, a bucket or a version is: which blob belongs to which object, and which blobs are live at a moment, are the caller's — the reachability input a sweep is handed is built outside this crate.

## Features

| Feature | Effect |
|---|---|
| `testing` | publishes the block-store conformance suite (`src/testing.rs`) for a target to share |

## Layout

| File | What it is |
|---|---|
| `src/engine.rs` | `Store`: hashing, dedup, blob references and read-back, generic over any `BlockStore` |
| `src/local.rs` | `LocalBlocks`: the filesystem `BlockStore`, its layout and its staged writes |
| `src/sweep.rs` | `LiveSet`, `SweepCandidates` and `SweepReport`: planning a sweep and applying it |
| `src/error.rs` | the engine's error and its projection onto the contract error |
| `src/testing.rs` | the conformance suite behind `testing` |
| `tests/conformance.rs` | the template a future backend copies: point it at another `BlockStore` and the same rules are asserted |

## The local layout

Blocks live at `<root>/<aa>/<hex>`, `<aa>` being the address's first two hex characters, so no directory grows with the store. Staged blocks live under `<root>/tmp/<uuid>`: `tmp` is reserved — the fan-out is hex, so it can never collide — and neither the listing nor the walk descends into it. A **dropped** stage removes its own temp; what is left for a caller's cleanup to reap is the residue of a process that died mid-stage.

## Its place in the family

`LocalBlocks` is the filesystem implementation `tinio-fs` runs, one store per bucket's own subtree; a backend with its own transport implements the contract instead and takes the same engine unchanged. The callers that hold the engine (`tinio-fs`, `tinio-version`, `tinio-ops`, and `tinio-pg`'s cleanup pass) are the only ones that know what a blob belongs to.

## Gates

```console
cargo test -p tinio-blob
```

One command: the engine and local-store unit tests, the doctests, and the conformance target. The conformance target needs the `testing` feature, which this crate's own dev-dependency on itself turns on for a test build — so the plain command is the whole gate. No database and no environment variable.
