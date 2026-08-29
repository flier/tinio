# 001-s3-local-server — Documentation Map

Feature: S3-Compatible Local Storage Server (tinio). This directory holds the complete feature documentation set: requirements, plan, contracts, designs, validation scenarios, and the task list.

**Implementation status (2026-08-29)**: Phase 1 (workspace setup), Phase 2 (foundational — `tinio-core`, `tinio-config`, `tinio-mem`, error types, metrics registry), and US1 (`tinio-fs` + `tinio-server` S3 mapping) are complete. The pipeline refactor is done (ETag-compute IO pipeline + batched meta writes, list/scanner wired, write-lock histogram, `[pipeline.*]` + `[storage.fs]` `meta_batch` config), and the data-path hardening program is done (writes off async threads, streaming scanner walk, GET zero-copy, syscall convergence, object durability D1). See [pipeline-spec.md](pipeline-spec.md) Part 2 for the per-task status.

## Documents

| Document | Role | Read when |
|----------|------|-----------|
| [spec.md](spec.md) | Feature requirements: FR-001..FR-025, success criteria (SC-001..SC-008), edge cases | Starting anywhere — the normative source |
| [plan.md](plan.md) | Implementation plan: workspace, crate structure, constitution check, testing strategy | Making architecture decisions |
| [research.md](research.md) | Phase-0 decisions, alternatives, and dependency justifications (constitution Principle I) | Asking *why* a crate or design was chosen |
| [data-model.md](data-model.md) | Entities and on-disk state layout (`meta.redb`, `multipart/`, `tmp/`) | Implementing tinio-fs state |
| [contracts/](contracts/) | Exact schemas: [config.md](contracts/config.md), [cli.md](contracts/cli.md), [management-api.md](contracts/management-api.md), [s3-surface.md](contracts/s3-surface.md), [minio-compat.md](contracts/minio-compat.md) | Implementing a user-facing surface |
| [failure-handling.md](failure-handling.md) | Abnormal-condition taxonomy, crash recovery, reclamation division of labor | Implementing cleanup/repair (T070, T073/T074) |
| [scanner.md](scanner.md) | Background ETag scanner design: pacing, meta reclamation, lifecycle | Implementing the scanner (T045) |
| [fs-backend.md](fs-backend.md) | tinio-fs backend design: path mapping, atomic writes, meta store, listing, multipart, sweep, cleanup, scanner walk | Implementing tinio-fs |
| [meta-redb-spec.md](meta-redb-spec.md) | Metadata store migration to redb | Implementing `database::Handle` / `meta::Store` |
| [pipeline-spec.md](pipeline-spec.md) | Dual pipeline: ETag IO + batched DB writes; `Task`/`Runner`/`Completion` contract | Implementing tinio-core/tinio-fs/tinio-server pipelines |
| [quickstart.md](quickstart.md) | Runnable end-to-end validation scenarios | Verifying the build |
| [tasks.md](tasks.md) | Task list (T001–T103), phases, dependencies, parallel opportunities | Executing the work |

## Suggested reading order

1. [spec.md](spec.md) — requirements and user stories
2. [plan.md](plan.md) — architecture and structure
3. [data-model.md](data-model.md) + [contracts/](contracts/) — state layout and exact schemas
4. [failure-handling.md](failure-handling.md) + [scanner.md](scanner.md) + [fs-backend.md](fs-backend.md) + [pipeline-spec.md](pipeline-spec.md) — design details
5. [tasks.md](tasks.md) — execute

## Implementation conventions

Engineering rules live outside this feature directory:

| Document | Role |
|----------|------|
| [docs/cargo.md](../../docs/cargo.md) | Workspace dependency pins, feature layout, crate `publish` policy |
| [docs/style.md](../../docs/style.md) | Rust style: errors, validation (`bucket::name` / `object::key`), modules, `ETag` |

## Layering

- **Contract layer**: `tinio-core` traits (`Storage`, `Cleanup`, `pipeline::{Task, Runner}`) and domain newtypes (`bucket`, `object`, `etag`, `multipart`) — backend-agnostic.
- **Reference backend**: `tinio-mem` (`MemoryStorage`) — behavioral reference and no-directory CLI mode; must pass the conformance harness.
- **Design layer**: [failure-handling.md](failure-handling.md) (abnormal conditions), [scanner.md](scanner.md) (scanner), [pipeline-spec.md](pipeline-spec.md) (dual pipeline) — backend-agnostic designs.
- **Backend layer**: [fs-backend.md](fs-backend.md) — tinio-fs implementation details; future backends (tinio-s3, tinio-webdav) document their own behavior in their own backend documents.
