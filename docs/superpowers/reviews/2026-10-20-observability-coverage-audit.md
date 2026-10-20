# Observability coverage — audit and review record

Phase record for `docs/superpowers/plans/2026-10-19-observability-coverage.md`. The plan carries intent and tasks; this file carries the audit's measurements and findings. Volatile readings stay here, never in the plan.

## Measured state (audit pass, six clusters, 24 crates)

| Cluster | Crates | src files | events | spans | families declared | T0 ops | T0 gaps |
|---|---|---|---|---|---|---|---|
| A request | tinio-server, tinio-api | 44 | 33 | 0 | 18 | 20 | 20 |
| B storage-fs | tinio-fs, tinio-mem | 41 | 44 | 0 | 5 | 26 | 26 |
| C stores | tinio-store-redb, tinio-store-pg, tinio-pg, tinio-blob, tinio-version | 84 | 23 | 0 | 0 | 29 | 29 |
| D cli-support | tinio-cli, tinio-auth, tinio-util, tinio, xtask | 90 | 39 | 0 | 0 | 21 | 21 |
| E sandbox | tinio-sandbox, -core, -linux, -macos, -windows | 63 | 112 | 11 | 2 | 41 | 26 |
| F engine | tinio-core, tinio-ops, tinio-lifecycle, tinio-select | 80 | 13 | 0 | 0 | 12 | 12 |
| **Total** | **24 + xtask** | **406** | **264** | **11** | **25** | **149** | **134** |

Event counts are production sites only: macros are imported then called unqualified, so a `tracing::`-prefixed search undercounts. The 11 spans are all in `tinio-sandbox-linux` (`enter.rs`, `seccomp.rs`); no other crate had a span or a `span!` macro. `tinio-e2e` and `tinio-bench` carry no shipped surface and own no span or family.

## Headline findings at audit time

1. The request spine had no spans; the per-operation span `research.md` §9 promises did not exist.
2. The trace started inside `sandbox::confine`, after parse, config, resolution and the daemon fork.
3. Spans would not print: the operational `fmt` layers set no span events.
4. Store observability was inverted: the CLI polled `FsStorage::write_lock_stats`; pg published zeros.
5. Family ownership was inverted: `tinio_http_*` was emitted by the data plane, not by the crate rendering `/metrics`.
6. OTLP exported nothing: `otel_layer` dropped its `SdkTracerProvider` on return.

## Review round against the landed change

Two-axis review of the uncommitted change set against `HEAD ef8de250`.

- Standards: 7 hard violations, 5 judgement calls. The hard set: plan prose over the compression limits; the plan's family table duplicating `data-model.md`; copied span harnesses; lost `build_subscriber` docs; missing rustdoc examples; `Default` instead of `SmartDefault`; an inline `use`; unsanctioned reason prefixes.
- Spec: family #1 absent; the family gate subset-only; the `remove` lane and `refused` outcome missing; `StaticAuth` uninstrumented; pg put-write spans absent; the OTLP round trip standalone-only; every Evidence section empty.

## Verification

See the plan's Evidence section for the commands and results that closed the review round.

## Second review round — fixes

- **Management span leaks the request.** `api_request` used `skip(next)` only, so the whole `Request` (headers, token) was recorded at INFO. It now uses `skip_all` with explicit fields; a contract test asserts no `request` field and no token value.
- **Close line lost the fields.** `SpanCloseLayer` now records span fields in its extension and prints them on close; it runs only for INFO+ spans. The filesystem durability sub-steps were demoted to `debug`, so a request keeps one T0 close line.
- **Missing writers.** pg records `tinio_storage_read_only`; Put/Copy/Complete and Delete paths write the object counters.
- **Targets and levels.** `run_one` targets `tinio::pipeline`; sandbox-linux spans target `tinio::sandbox`, run at `debug`, and no longer use `ret`.
- **Family gates.** Every metrics module asserts its names via `testing::metrics::assert_registered`; the fs versioning families are no longer `cfg`-hidden.
- **OTLP.** The exporter has TLS (`tls-ring`, `tls-webpki-roots`); a non-loopback plaintext endpoint is refused; the lost-export log strips URL userinfo.
- **`/metrics`.** `[api] metrics_requires_token` can require the control token; default keeps the scraper-friendly open form.
- **Queue wait.** `enqueue_wait` is observed at dequeue from the enqueue timestamp, not at the blocking send.
- **Labels.** `status_label!` maps common HTTP statuses to static strings and falls back to `itoa`; label enums expose `as_str` (or `as_label`) instead of `to_string`.
- **Data clumps.** `http_request` takes one `RequestTrace` instead of four identity arguments; `WriteLockStats` no longer carries unrendered max fields.
- **Tests and docs.** The reader cadence/stop test is restored; the pipeline-spec, tasks, feature comment, and pg design path are corrected.
