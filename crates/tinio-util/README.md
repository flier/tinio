# `tinio-util` — the shared runtime utilities the backends and the server build on

`tinio-util` holds the one copy of each small runtime helper the storage backends and the server both need: the evicting per-key lock map, the filesystem primitives every write path is written on, and the pacing, containment, process, secret and switch definers beside them. Nothing in it names a backend — a module may use `tinio-core`, `tokio`, `std` and the crate's other modules, never `tinio-fs` — and what it carries is the *rule*, with the policy left to its callers. It owns no storage contract (`tinio-core` does), no benchmark protocol (`tinio-bench`, `docs/benches.md`) and no harness rule (`docs/tests.md`).

## Features

| Feature | Effect |
|---|---|
| `testing` | Publishes the conformance harness (`assert_conformance`, the persistence contracts' own suite in `testing::metadata`, and the test devices the crates' legs share — the raw HTTP/1.1 client, the SigV4 signer, the in-memory block store). A production build leaves it off: no shipped binary compiles the harness, and a crate that wants it enables it in its `[dev-dependencies]`. |
| `bench` | Publishes the benches' pinned data root and the flush control every reading carries — the one copy every bench target in the workspace runs on, read by `tinio-bench`. |
| `render` | Publishes the one Handlebars renderer — strict mode, no escaping — behind the status report, the systemd unit and the Seatbelt profile. |

## The lock map's guarantee

`lockmap::Map` hands out one `tokio` mutex per key — the lock a caller actually awaits across `.await` points — and this is all a caller may assume: while its `Guard` for a key is held, no other holder of **that key** runs, and a different key is a different lock that never contends.

Eviction is the map's stance and not the caller's business: a slot goes the moment its last handle drops, so the table stays bounded by the number of keys held *at once* rather than by the keys ever seen. A caller may therefore not assume the lock remembers a key across a gap — a key that has been let go is a fresh mutex on the next `lock`, and there is nothing to drain at the end.

## Layout

| File | What it is |
|---|---|
| `src/lockmap.rs` | the evicting per-key lock map |
| `src/fsutil.rs` | the atomic commit protocol, the ranged body stream, the staged-temp drop discipline and the directory/file helpers the backends are written on |
| `src/job.rs`, `src/job_object.rs` | the guard that makes a test-spawned process die with the test process, and the Win32 job-object sequence the guard and the sandbox launcher both stand on |
| `src/testing/mod.rs`, `src/testing/metadata.rs` | the conformance harness and the persistence contracts' suite (the `testing` feature) |
| `src/testing/http.rs`, `src/testing/sigv4.rs`, `src/testing/blocks.rs` | the shared test devices: the raw HTTP/1.1 client, the SigV4 signer, the in-memory block store |
| `src/bench.rs` | the benches' pinned data root and flush control (the `bench` feature) |
| `src/render.rs` | the one Handlebars renderer (the `render` feature) |
| `src/pacing.rs`, `src/path.rs`, `src/process.rs` | the shutdown-cuttable sleep, the path-containment rule, and kill-and-reap with its bounded exit poll |
| `src/secret.rs`, `src/switch.rs` | the zeroizing secret string and the tolerant switch grammar |

## Gates

The default gate — the crate as a production build compiles it:

```console
cargo test -p tinio-util
```

The features are what that build leaves out, and each is a graph of its own — the `testing` harness compiles only with its feature on, and a default build must reach neither it nor `bench`:

```console
cargo check -p tinio-util --features testing
```
