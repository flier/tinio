# `tinio-lifecycle` — the S3 object lifecycle pass

`tinio-lifecycle` is the background sweep behind the lifecycle surface: one stateless pass over a bucket's stored configuration, applying its three locally meaningful actions — current-version expiration, noncurrent-version expiration and incomplete-upload abort — through `tinio-core`'s storage contract.

It is deliberately **not** a `Storage` implementation and **not** a cleanup pass over a backend's private state: it depends on `tinio-core` and `tinio-util`'s shared pacing seam, keeps no cursor, and re-derives what is due from the listings every cycle. Each action is the API's own deletion, so the engine owns no version-aware removal of its own.

## Layout

| File | What it is |
|---|---|
| `src/lib.rs` | the crate seam: the `_core` alias and the public exports |
| `src/clock.rs` | the `Clock` seam and its wall-clock / pinned implementations |
| `src/runner.rs` | the pass: options, the sweep, the seed, the summary and the three actions |
| `tests/engine.rs` | the pass over `tinio-mem`: what each action does and refuses |

## Gates

```console
cargo test -p tinio-lifecycle
cargo clippy -p tinio-lifecycle --all-targets -- -D warnings
```
