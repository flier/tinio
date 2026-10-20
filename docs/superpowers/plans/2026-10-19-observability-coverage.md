# Observability Coverage Plan — tracing and Prometheus across every crate

> **For agentic workers:** execute tasks in order. Tick each box in the round that does the work and add one Evidence line. Never tick a step the same round it was written without running it. Use `superpowers:subagent-driven-development` or `superpowers:executing-plans`.

**Goal:** Every operation carrying data or a security/correctness decision is visible in `tracing` at its criticality; every condition an operator must page on has one bounded-cardinality Prometheus family.

**Architecture:** Three layers, one direction. The request layer (`tinio-server`, `tinio-api`) opens the outermost span and owns wire-level families. The engine layer (`tinio-ops`, `tinio-lifecycle`, `tinio-select`, `tinio-core`) owns semantic spans. The backend layer (`tinio-fs`, `tinio-mem`, `tinio-store-*`, `tinio-blob`, `tinio-version`, `tinio-pg`) owns storage spans. A span is opened by the layer that owns the unit, never re-opened above or below. A family two crates can observe is declared once in `tinio-util`.

**Tech stack:** Rust edition 2024; `tracing` 0.1; `tracing-subscriber` 0.3; `tracing-opentelemetry` 0.34 behind `otel`; `prometheus` 0.14; `s3s` 0.17; `axum` 0.8; `async-trait`.

**Fact owners (do not create a second home):**

- Logging / access log / OTLP decisions → `specs/001-s3-local-server/research.md` §9.
- Family names, types, labels, units → `specs/001-s3-local-server/data-model.md` §Metrics.
- Write-lock plumbing → `specs/001-s3-local-server/pipeline-spec.md` §4.
- Criticality tiers, span rules, field allow-list, gates → this plan, Conventions.
- Prose → `docs/compress.md`; style → `docs/style.md`.

**Audit record:** measurements and the review findings live in `docs/superpowers/reviews/2026-10-20-observability-coverage-audit.md`.

---

## Global Constraints

### Execution

- **Order.** Tasks run in order. No two tasks edit one file; a task that must edit another's file states why.
- **Green tree.** Every task ends compiling with its named suites passing. No span or family lands without a test.
- **Contract first.** Task 1 amends the metrics section before any family lands; later tasks only implement listed names.
- **No hidden families.** A family is registered in a default build of its declaring crate; a new `cfg` around a metrics module is a defect.
- **Loop.** Each task runs `clippy -D warnings`, its tests and `cargo +nightly fmt --check` before reporting.
- **No commits.** Agents edit and report; the operator reviews and commits.

### Safety & scope

- **Dependency budget.** `tracing` in auth/select/version; `prometheus` in util, pg, version, lifecycle, select, auth, ops, sandbox and cli.
- Style-mandated `parse-display`/`derive_more` edges are allowed where a derive replaces a hand impl; mem and blob declare no family.
- **Secrets never enter a span:** bodies, credentials, `Authorization`, presigned queries, tokens, DSNs, config text.
- **Paths:** no host path outside the served root; `skip_all` is the default.
- **Cardinality:** labels carry no bucket, key, id, access key, path or hash; span fields may carry bounded identities.
- **Step events are contract.** The `model`/`exposures`/`seccomp`/`sandbox-init`/`exit` steps stay events, never span fields.
- **Docs.** English, compressed, one definer.

---

## Conventions (binding)

### 1. Criticality tiers → span level

| Tier | Meaning | Level | Volume budget |
|---|---|---|---|
| **T0** | Carries data or a security/correctness decision. | `info` | ≤ 1 per request / cycle / process start-stop |
| **T1** | User-visible sub-operation inside a T0 unit. | `debug` | ≤ a handful per request |
| **T2** | Loop iteration and mechanical detail. | `trace` | unbounded, off by default |

Levels are the priority mechanism: T0 survives the default `info` filter, T1 appears under `debug`, T2 under `trace`. An existing `debug` event never satisfies a T0 gap.

### 2. Span rules

- **Default is `#[instrument]` on the function**; `async_trait` methods are instrumentable; `.instrument()` is banned.
- **Manual spans only for:** a closure/callback; a `Drop` impl; a loop body that is the unit; a thread/fork boundary; a generated method.
- **Targets:** `tinio::http`, `tinio::s3`, `tinio::api`, `tinio::storage`, `tinio::pipeline`, `tinio::sandbox`, `tinio::lifecycle`, `tinio::auth`, `tinio::cli`.
- **Fields:** `bucket`, `key` (bounded), `op`, `method`, `path` (request layer), `request_id` (request layer), `version`, `bytes`, `status`, `kind`, `count`, `outcome`, `model`, `euid` (sandbox).
- **Errors:** `field::Empty` plus `err`/`error.kind` in the error arm; never `#[instrument(err)]` or `ret` on a payload.
- **Visibility:** one close line per T0 span; skipped only for `tinio::http` and `tinio::api`, whose duration is already logged.
- **Phases:** a fmt bootstrap before parse; the full stack swaps in `confine`; the OTLP exporter never crosses the fork.

### 3. Metric rules

- **Name:** `tinio_<layer>_<metric>[_total|_seconds|_bytes]`; the layer comes from the target list. No exceptions.
- **Type:** counter for events (`_total`), histogram for durations (`_seconds`), gauge for a current value. Units live in the name.
- **Labels:** bounded enums only; a new label value is a code change with a test.
- **Ownership:** a single-observer family is declared in its crate; a shared family once in `tinio-util` via `record_*`.
- **Registration:** `LazyLock` + `register_*!`; one module-level `expect` per metrics module.
- **Tests:** every family task adds its name to the owning crate's gate and writes values through the shared test lock.
- **Renames:** breaking; no alias, nothing grandfathered.

### 4. Accepted families

The 24 new families this plan accepts are owned by `specs/001-s3-local-server/data-model.md` §Metrics, which also keeps the pre-existing families (48 rows in all). This plan implements that table; it does not restate it.

### 5. Deferred (considered, not in this plan)

| Family | Why deferred |
|---|---|
| `tinio_storage_block_publish_total{result}` | dedup ratio is capacity planning, not incident response; spans carry the outcome |
| `tinio_storage_pool_acquire_duration_seconds` | the pool gauge plus the request span already bound a stall |
| `tinio_http_connections_total{transport,outcome}` | connection failures are one warn plus the request family; revisit after transport spans |
| `tinio_cli_shutdowns_total{trigger,outcome}` | the plane serving `/metrics` is the thing being drained; the last increment may never be scraped |
| `tinio_sandbox_acl_grant_seconds` | one launch-phase histogram; the confinement histogram covers the budget |
| `tinio_sandbox_stale_profiles_total{result}` | Windows-only residue; a span on the sweep is enough until a leak is reported |
| `tinio_storage_schema_version{backend}` | schema drift is a startup refusal with a span; a gauge adds no page |
| `tinio_sandbox_host_can_confine` | `can_confine` is a startup fact; the confinement counter carries the refusal |

---

## Task waves

| Wave | Tasks | Scope |
|---|---|---|
| 0 — gate | 1, 1a, 2, 3, 3a | spec contract, renames, logging pipeline, family gate, shared metrics module |
| 1 — request spine | 4–9, 8b | admission span, 42 S3 ops, pipeline, auth, management plane, `/metrics`, OTLP |
| 2 — storage spine | 10–15 | redb, pg, blob, version, fs, mem |
| 3 — engine | 16–18 | ops/core, lifecycle, select |
| 4 — sandbox and CLI | 19–20 | five sandbox crates, CLI lifecycle |
| 5 — contract | 21 | span contract, family gate, full matrix |

---

### Task 1: The family contract in the spec

#### Files
- Modify: `specs/001-s3-local-server/data-model.md` (§Metrics)
- Modify: `specs/001-s3-local-server/research.md` §9 (one pointer, no restatement)

#### Steps
- [x] Add the five missing families the code already declares, under the Task 1a names.
- [x] Add the 24 new families, grouped by owner, with type, labels, unit and one-clause rationale.
- [x] Record the Task 1a renames once; state that nothing is grandfathered.
- [x] Note the `tinio_http_*` ownership move and the versioning families staying in `tinio-fs`.
- [x] Verify every registered name appears exactly once; no family in two sections.

#### Evidence
Fenced commands and results:

```text
rg -F 'tinio_' specs/001-s3-local-server/data-model.md
Every registered name appears once; the duplicate gate rows were removed.
```

### Task 1a: Breaking renames and the HTTP family move

#### Files
- Modify: `specs/001-s3-local-server/data-model.md`
- Modify: `crates/tinio-server/src/metrics.rs`, `crates/tinio-server/src/lib.rs`
- Modify: `crates/tinio-cli/src/commands/start/reader.rs`, `crates/tinio-store-redb/src/{driver.rs,metrics.rs}`
- Modify: `crates/tinio-api/src/metrics.rs`, `crates/tinio-api/src/router.rs`
- Test: both crates' test modules

#### Steps
- [x] Rename gauges ending in `_total` into plain gauges.
- [x] Rename the write-lock histograms into `tinio_storage_write_lock_*`; `total` meant hold time.
- [x] Move `tinio_http_*` to `tinio-api`; emit from one management middleware including 401 and `/metrics`.
- [x] Record the new spellings once in the spec; no other document repeats them.
- [x] Collapse the per-family registration attributes into one module-level `expect`.
- [x] Verify the three gauges have no `_total`, the lock histograms use the new names, and the gate count is one.

#### Evidence
```text
cargo test -p tinio-server --lib registers_all_families --all-features
1 passed; the exact family gate holds with every feature.
```

### Task 2: Two-phase logging — bootstrap, reload, close-only layer

#### Files
- Modify: `crates/tinio-cli/src/main.rs`, `crates/tinio-server/src/log.rs`
- Modify: `crates/tinio-cli/src/commands/start/run.rs`, `crates/tinio-cli/src/sandbox/confine.rs`
- Modify: `crates/tinio-sandbox-linux/src/seccomp.rs`
- Test: the server log module, the CLI start module, e2e `CONFINED_STEPS`

#### Steps
- [x] Bootstrap a fmt-to-stderr subscriber through a reload handle before parse; no file, no OTLP, no thread.
- [x] Swap only the operational layers inside the confinement; the OTLP exporter starts at the reload.
- [x] Add a close-only layer for T0 spans; skip `tinio::http` and `tinio::api`.
- [x] Wrap the verbosity seed in `EnvFilter` so `RUST_LOG` can raise one target.
- [x] Delete `announce_in_place`; keep the `step` events and the confine unit test in place.
- [x] Verify parse errors and confinement warnings reach stderr; no event appears twice; daemon logs across the fork.

#### Evidence
```text
NO_COLOR=1 target\debug\tinio.exe start --help
2026-10-20T05:37:34.131324Z  INFO span closed span="adopt" elapsed_ms=0.0373
The bootstrap and close-only layer are live before the parser.
```

### Task 3: The family gate

#### Files
- Modify: `crates/tinio-server/src/metrics.rs` (`SPEC_FAMILIES` + `registers_all_families`)
- Test: the gate module

#### Steps
- [x] Make the gate a data-driven `SPEC_FAMILIES` list, grouped by declaring crate.
- [x] Pin the names registered today, read once out of the default registry.
- [x] Assert exactness in both directions: a missing or undeclared family fails.
- [x] Verify the gate with default and all features; no `AlreadyReg`.

#### Evidence
```text
cargo test -p tinio-server --lib registers_all_families --all-features
1 passed; unregistered and undeclared names both fail the exact set.
```

### Task 3a: `tinio-util`'s metrics module

#### Files
- Create: `crates/tinio-util/src/metrics.rs`; add the module in `crates/tinio-util/src/lib.rs`
- Modify: `crates/tinio-util/Cargo.toml` (add `prometheus`)
- Test: the module

#### Steps
- [x] Declare the five cross-backend families once on the default registry.
- [x] Expose bounded label enums and `record_*` entry points.
- [x] Point fs, mem, pg and blob at the module; they declare no family.
- [x] Keep the module dumb: no `tracing`, no `tinio-core` type, no `tokio`.
- [x] Verify each backend build exposes only its own label values and each name has one declaration.

#### Evidence
```text
cargo test -p tinio-util --features testing --lib
The five families register once and render their recorded values.
```

### Task 4: The request admission span

#### Files
- Modify: `crates/tinio-server/src/data.rs`
- Test: the module's tests

#### Steps
- [x] Open the root `tinio.http.request` span at `info` with `method`, query-stripped `path`, `request_id`.
- [x] Attach the access event to that span; no second duration observation.
- [x] Keep the hot path allocation-free; bodies and the access gate stay unspanned.
- [x] Verify one span per request for 200 and 404; no span for the management path.

#### Evidence
```text
cargo test -p tinio-server --test observability_contract
2 passed; the request root, operation and storage spans nest correctly.
```

### Task 5: The S3 operation spans

#### Files
- Modify: `crates/tinio-server/src/metrics.rs` (generated dispatch)
- Modify: `crates/tinio-server/src/backend/{objects,buckets,multipart,listing,select}.rs` (T0 handlers)
- Test: the module and backend tests

#### Steps
- [x] Open one `tinio.s3.operation` span per operation inside the generated dispatch, recording the wire name as `op`.
- [x] Backfill `bucket`/`key` from the T0 handlers through the identity recorder; no second handler span.
- [x] Keep the per-operation naming honest: one span per request subtree; no `upload_id` on a metric.
- [x] Verify one span per operation table row, with `op` equal to the wire name and identity on PutObject.

#### Evidence
```text
cargo test -p tinio-server --test observability_contract the_request_spine
1 passed; exactly one operation span, `op = PutObject`, bucket and key recorded.
```

### Task 6: Pipeline spans and health families

#### Files
- Modify: `crates/tinio-server/src/pipeline.rs`, `crates/tinio-server/src/metrics.rs`
- Test: both modules

#### Steps
- [x] Instrument `enqueue` and `run_one` at `debug` with the pipeline label.
- [x] Add task-duration, task-outcome and enqueue-wait families at the audit sites.
- [x] Publish the `remove` lane; delete the busy-workers gauge; correct the spec entry.
- [x] Count refused enqueues under `outcome = "refused"`.
- [x] Verify saturation moves the wait histogram, failures and panics their outcomes, and three lanes publish.

#### Evidence
```text
cargo test -p tinio-server --lib shutdown_completes
1 passed; a refused enqueue increments `outcome="refused"`.
```

### Task 7: Auth spans and families

#### Files
- Modify: `crates/tinio-auth/Cargo.toml`, `crates/tinio-auth/src/{auth.rs,access.rs}`, new `crates/tinio-auth/src/metrics.rs`
- Modify: `crates/tinio-server/src/data.rs` (`StaticAuth` arm)
- Test: the auth modules

#### Steps
- [x] Instrument the credential lookup and the access decision at `info`; record bounded `decision` and `reason`.
- [x] Add the credential-lookup and authorization families; a denial reason stays a span field.
- [x] Instrument the bare-build `StaticAuth` lookup, matching the shared path's span.
- [x] Verify an unknown key increments `unknown` and a denial increments `denied`; no credential enters a line.

#### Evidence
```text
cargo test -p tinio-server --test observability_contract an_unknown_access_key
1 passed; the `tinio::auth` lookup span records `result="unknown"` and no access key.
```

### Task 8: Management-plane spans

#### Files
- Modify: `crates/tinio-api/src/{router.rs,server.rs,state.rs}`
- Test: the crate's modules

#### Steps
- [x] Open one `tinio.api.request` span at `info` with `method`, query-stripped `path`, `status`.
- [x] Span the state write and shutdown trigger at `debug`.
- [x] Leave the `axum` tower layers unspanned; the middleware is the unit.
- [x] Verify one span per management request and no close line for `/metrics`.

#### Evidence
```text
cargo test -p tinio-api --test spans
1 passed; request, state-write and shutdown spans have the expected targets and levels.
```

### Task 8b: `/metrics` without the injected source

#### Files
- Modify: `crates/tinio-api/src/{metrics.rs,router.rs,transport.rs}`
- Modify: `crates/tinio-cli/src/commands/start/reader.rs`, `crates/tinio-cli/src/commands/start/run.rs`
- Test: both crates

#### Steps
- [x] Move the pipeline gauges and write-lock refresh onto the reader's tick.
- [x] Delete `MetricsSource`, `Gauges` and the router parameter; render is gather plus encode.
- [x] Keep the reader single-flight and off the serving path.
- [x] Verify no injection remains, two scrapes show moved gauges, and no request refreshes them.

#### Evidence
```text
rg -F 'MetricsSource' crates/tinio-api
No matches; /metrics is a gather of the default registry.
```

### Task 9: OTLP retention and shutdown

#### Files
- Modify: `crates/tinio-server/src/log.rs`
- Modify: `crates/tinio-cli/src/commands/start/{run.rs,serve.rs}`
- Test: a span round trip behind `otel`

#### Steps
- [x] Return the `SdkTracerProvider` with the layer; dropping it on return is the bug.
- [x] Retain it at the composition root and shut it down on both stop paths.
- [x] Add an in-memory exporter round trip driven by a real request, with the parent chain asserted.
- [x] Verify the exported span carries `op` and that a default build has no test exporter.

#### Evidence
```text
cargo test -p tinio-server --features otel --test otel_round_trip
1 passed; the exported request and operation spans share an intact parent chain.
```

---

### Task 10: redb transaction and commit spans

#### Files
- Modify: `crates/tinio-store-redb/src/driver.rs`
- Modify: `crates/tinio-store-redb/src/meta/{object,version,upload,bucket}.rs`
- Modify: `crates/tinio-store-redb/src/content.rs`
- Test: the crate's modules

#### Steps
- [x] Span the transaction choke point at `trace`, with the operation as a recorded field.
- [x] Span commits at `debug`; `info` only where the contract calls the commit T0.
- [x] Warn once when a read-only run serves an empty view.
- [x] Never record `ret` or `err`; record `error.kind` only.
- [x] Verify one span per contract commit, one per transaction, and no path in a span.

#### Evidence
```text
cargo test -p tinio-store-redb --lib
151 passed; spans carry kinds, never paths.
```

### Task 11: pg store and backend

#### Files
- Modify: `crates/tinio-store-pg/Cargo.toml`, `crates/tinio-store-pg/src/metrics.rs`
- Modify: `crates/tinio-store-pg/src/{pool.rs,driver.rs,store.rs}`
- Modify: `crates/tinio-pg/src/{cleanup.rs,backend.rs,version.rs}`
- Test: both crates

#### Steps
- [x] Span the transaction boundary at `trace`; contract impls own the semantic span.
- [x] Span the version plane's slot moves and marker writes at `debug`.
- [x] Span ownership and teardown decisions at `info`; put-write open/close included.
- [x] Add the pool-connections gauge.
- [x] Record the cross-backend families through `tinio-util`.
- [x] Verify the slot chain, the pool gauge and a clean sqlx cache.

#### Evidence
```text
cargo check -p tinio-store-pg -p tinio-pg --all-targets
Both crates compile with the new spans; the pg suites run only with `TINIO_PG_DSN`, unset on this host.
```

### Task 12: Blob block plane and sweep spans

#### Files
- Modify: `crates/tinio-blob/Cargo.toml`, `crates/tinio-blob/src/{engine.rs,local.rs,sweep.rs}`
- Modify: `crates/tinio-fs/src/{sweep.rs,cleanup.rs}` (call sites only)
- Test: `tinio-blob`

#### Steps
- [x] Span put at `debug` with `bytes`; publish at `debug` with `result`; open at `trace`.
- [x] Span the block sweep at `info`; it is the unit and fs/pg both call it.
- [x] Record the pass and its failures through `tinio-util`; blob declares no family.
- [x] Verify an empty sweep still records one duration and one span.

#### Evidence
```text
cargo test -p tinio-blob --test spans
5 passed; put, publish, open and both sweep spans match the contract.
```

### Task 13: Version engine

#### Files
- Modify: `crates/tinio-version/Cargo.toml`, new `crates/tinio-version/src/metrics.rs`, `crates/tinio-version/src/engine.rs`
- Test: the crate's modules

#### Steps
- [x] Instrument supersede and materialize at `info` with bounded identity; current/set-state at `debug`.
- [x] Record the retry signal as an event plus the family's `result`.
- [x] Verify a race produces one span with `result="retried"` and one counter increment.

#### Evidence
```text
cargo test -p tinio-version --lib engine::tests
19 passed; the supersede span records the retried outcome.
```

### Task 14: Filesystem backend

#### Files
- Modify: `crates/tinio-fs/src/backend/{mod.rs,objects.rs,buckets.rs,version.rs}`
- Modify: `crates/tinio-fs/src/{write.rs,database.rs,meta.rs,multipart.rs,content.rs}`
- Modify: `crates/tinio-fs/src/{sweep.rs,scanner.rs,cleanup.rs,listing.rs,version_adapter.rs}`
- Modify: `crates/tinio-fs/src/metrics.rs`
- Test: the crate's modules and `assert_conformance`

#### Steps
- [x] Span open/refuse decisions at `info`, including read-only degradation and symlink refusals.
- [x] Span the durability path at `info`: stage, fsync, publish, commit.
- [x] Span read/delete/copy/rename and the T1 internals at `debug`.
- [x] Turn the sweep and scanner cycles into spans; the pass summary is the close event.
- [x] Record write failures, pass duration/failures and corrupt rows through `tinio-util`.
- [x] Verify a put's span chain, one read-only flag, a refusal span and the sweep close line.

#### Evidence
```text
cargo test --workspace --no-fail-fast
The fs backend, conformance and span legs pass in the workspace run; the chain matches the contract.
```

### Task 15: Memory backend

#### Files
- Modify: `crates/tinio-mem/src/{object.rs,multipart.rs,storage.rs,counting.rs,cleanup.rs,version.rs,bucket.rs}`
- Test: the crate's modules and `assert_conformance`

#### Steps
- [x] Span the counting wrapper's put/get/delete at `debug`; the cap check is the T0 decision at `info`.
- [x] Record the cap refusal through `tinio-util`; the anomaly warn stays.
- [x] Verify the conformance suite, one cap counter and no key interpolated into a metric.

#### Evidence
```text
cargo test -p tinio-mem --lib
103 passed; the conformance suite and the cap refusal counter hold.
```

---

### Task 16: Engine spans and read-heal counter

#### Files
- Modify: `crates/tinio-ops/src/{object.rs,version.rs,bucket.rs,multipart.rs,listing.rs}`, `crates/tinio-ops/src/metrics.rs`
- Modify: `crates/tinio-core/src/{content.rs,blob.rs,pipeline.rs,acl.rs,cors.rs}`
- Test: `tinio-ops` modules

#### Steps
- [x] Span the T0 engine bodies at `info` with bounded identity.
- [x] Parent the T1 internals at `debug`.
- [x] Add the read-heal counter keyed by a bounded row-kind enum.
- [x] Keep core to children only; a trait declaration never carries a span.
- [x] Verify the ops → version → blob → block chain and a single heal increment.

#### Evidence
```text
cargo test --workspace --no-fail-fast
The ops engine suites pass; heal kinds are enums and the counter moves once per heal.
```

### Task 17: Lifecycle pass

#### Files
- Modify: `crates/tinio-lifecycle/Cargo.toml`, new `crates/tinio-lifecycle/src/metrics.rs`, `crates/tinio-lifecycle/src/runner.rs`
- Test: the crate's modules

#### Steps
- [x] Span the pass and the run loop at `info`; the loop is the unit and needs a manual span.
- [x] Span the actions at `debug`; per-object decision helpers stay unspanned.
- [x] Add the pass-duration and action-outcome families.
- [x] Move the pass summary onto the span, so the close line prints it instead of a second event.
- [x] Verify per-action children, the close line and a failing action's `error` outcome.

#### Evidence
```text
cargo test -p tinio-lifecycle --lib
8 passed; the pass span carries the summary fields and its action children.
```

### Task 18: Select stream

#### Files
- Modify: `crates/tinio-select/Cargo.toml`, `crates/tinio-select/src/metrics.rs`
- Modify: `crates/tinio-select/src/{events.rs,sql.rs,record.rs,csv.rs,json/mod.rs,parquet.rs}`
- Test: the crate's modules

#### Steps
- [x] Span the stream at `info` with `format`; select never sees bucket or key.
- [x] Span parse and framing at `debug`; never record SQL text.
- [x] Add scanned-bytes and rows-returned families keyed by framing.
- [x] Verify one stream span and the two counters over a three-format fixture.

#### Evidence
```text
cargo test -p tinio-select
The select suites pass; the stream and framing spans carry no SQL text.
`record_stream` is public so the server's exact family gate can touch the two families.
```

---

### Task 19: Sandbox confinement

#### Files
- Modify: `crates/tinio-sandbox/Cargo.toml`, `crates/tinio-sandbox/src/{serve.rs,error.rs,metrics.rs}`
- Modify: `crates/tinio-sandbox-core/Cargo.toml`
- Modify: `crates/tinio-sandbox-core/src/launcher/{supervise.rs,adopt.rs,metrics.rs}`
- Modify: `crates/tinio-sandbox-windows/src/{backend/mod.rs,capability.rs,sweep.rs}`
- Modify: `crates/tinio-sandbox/{linux,macos}` entry sites; `crates/tinio-sandbox-linux/src/{enter.rs,metrics.rs}`
- Test: the sandbox crates' modules

#### Steps
- [x] Span the common `Spec::apply` entry at `info`; platform confine children at `debug`.
- [x] Span readiness, wait and attribution at `info`; the relay thread's span is manual.
- [x] Span the platform launches at their tier; the CLI keeps the `step` events.
- [x] Fix the path leak on launch failure: record a basename or a closed kind.
- [x] Add confinement, model, readiness and child-exit families.
- [x] Verify the facade → platform → core chain, a refusal outcome and no path or token in fields.

#### Evidence
```text
cargo test -p tinio-sandbox --test spans
1 passed; the refused confinement opens one `tinio::sandbox` span with `outcome="refused"`.

cargo test -p tinio-sandbox-core --test spans
1 passed; the launcher's readiness and exit spans close.
```

### Task 20: CLI lifecycle

#### Files
- Modify: `crates/tinio-cli/src/{run.rs,commands/start/run.rs,commands/start/serve.rs}`
- Modify: `crates/tinio-cli/src/{commands/stop.rs,commands/start/daemon.rs,commands/start/signal.rs}`
- Modify: `crates/tinio-cli/src/commands/start/{service/mod.rs,serving.rs,state.rs}`
- Modify: `crates/tinio-cli/src/commands/doctor.rs`, `crates/tinio-cli/Cargo.toml`, `crates/tinio-cli/src/metrics.rs`
- Test: the CLI modules and the e2e harness

#### Steps
- [x] Span the process lifecycle at `info`/`debug`; record explicit events before `process::exit`.
- [x] Span confinement from the CLI side at `debug`; keep the `step` events as events.
- [x] Span the background passes and repair at `info`; the state write stays Task 8's span.
- [x] Add the startup histogram keyed by phase; one-shot verbs get no family.
- [x] Verify one close line per phase, a complete stop trace and the e2e step assertions.

#### Evidence
```text
cargo test -p tinio-cli --test spans
1 passed; start, stop, service and cli::run open their spans on the refusal arms.
```

---

### Task 21: The span contract test and phase close

#### Files
- Create: `crates/tinio-server/tests/observability_contract.rs`
- Create: `crates/tinio-server/tests/otel_round_trip.rs`
- Modify: `crates/tinio-e2e/Cargo.toml` (dev-dependency if needed)
- Test: the new tests and the full matrix

#### Steps
- [x] Drive real operations and assert spans per layer: put, get, auth refusal, lifecycle pass, sandbox launch, CLI start.
- [x] Assert the family set in both directions on the default registry; the five cross-backend names appear once.
- [x] Run the matrix: workspace tests, clippy with warnings denied, formatting, and the pg legs when a DSN is set.
- [x] Verify every layer has at least one contract or span test and record the commands below.

#### Evidence
```text
cargo test -p tinio-server --test observability_contract
2 passed: the request spine and the auth refusal.

cargo test -p tinio-server --features otel --test otel_round_trip
1 passed: the exported parent chain.

cargo test -p tinio-server --lib registers_all_families --all-features
1 passed: the exact family set.

cargo test --workspace --no-fail-fast
3586 passed; 0 failed.

cargo clippy --workspace --all-targets -- -D warnings
No warning.

cargo +nightly fmt --check
Clean.

cargo test -p tinio-e2e --test cucumber
18 features, 303 scenarios, 2539 steps; 0 failed.

cargo test -p tinio-server --test sandbox_e2e_windows --features sandbox
3 passed.

WSL Ubuntu-26.04: cargo check/clippy/test -p tinio-fs --locked
412 passed; clippy clean.

TINIO_PG_DSN=... cargo xtask sqlx check
Cache up to date (166 entries).

TINIO_PG_DSN=... cargo test -p tinio-store-pg -p tinio-pg -- --test-threads=4
274 passed; 0 failed.

cargo hack check --package tinio --each-feature --no-dev-deps --profile ci
20 configurations, all green.

cargo audit
No vulnerabilities; rustls 0.23.46; the only warning is the ignored unmaintained daemonize.

cargo deny check
advisories ok, bans ok, licenses ok, sources ok.

cargo semver-checks check-release --baseline-rev HEAD -p tinio
202 checks pass, 58 skip; no semver update required.
```

---

## Review Focus

1. **A name declared twice.** The same `tinio_*` family in two crates panics on the second registration; `tinio-util` is the only home for the cross-backend names, and the HTTP move deletes the server statics in the same change.
