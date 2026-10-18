# `tinio` — the facade crate

`tinio` is the facade: the only public API surface of the project, and the crate the semver checks and the rustdoc-example gate are held against. It carries the `tinio` binary and curates the re-exports an embedder needs — the storage contract from `tinio-core`, the configuration type from `tinio-cli`'s composition root (`tinio_cli::config`), the S3 layer from `tinio-server`, the management plane from `tinio-api`, and the CLI entry from `tinio-cli`. It owns no behaviour of its own: what the binary does is `tinio-cli`'s, what gets served is `tinio-server`'s, and this crate's job is the seam and the default feature set.

## The binary

`tinio`. Its `main` is one call into `tinio-cli`, so the verb surface, the flags and the exit codes are that crate's — see its README rather than this one.

## Features

| Feature | Effect |
|---|---|
| `full` (default) | every capability below, except `otel` |
| `api`, `openapi`, `tls` | the management plane, its OpenAPI document, and the plane's HTTPS transport |
| `pg`, `mem` | the PostgreSQL and in-memory backends become selectable |
| `sandbox` | process confinement (`docs/sandbox.md`) |
| `versioning` | the version plane |
| `select` | S3 Select, over the `tinio-select` engine |
| `acl`, `cors`, `copy`, `multipart`, `list-v1`, `list-v2` | the S3 surface capabilities the `[s3]` configuration toggles |
| `otel` | the OpenTelemetry exporter |

## Layout

| File | What it is |
|---|---|
| `src/lib.rs` | the re-exports: the storage contract, `Config`, and one public error type per implementation crate |
| `src/error.rs` | those error types, so no crate leaks another crate's |
| `src/main.rs` | the binary entry |
| `tests/` | the process-level legs and the feature-graph seams |

## Gates

```console
cargo test -p tinio
```

These legs spawn the real binary, so cargo's build of it is part of the gate; `tests/lifecycle.rs` is Windows-only. The cucumber scenarios that drive the CLI as an operator would are `tinio-e2e`'s.
