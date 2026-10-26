# Cargo Workspace

## Rules

- Manifests: no comments (`docs/` owns rationale); pin once at workspace; members path-only, inherit version/edition.
- Version/edition: `major` or `major.minor`, the latter preferred.
- Publish only the facade; optional/target deps at the crate that needs them; a dev-dep keeps its own feature pair.
- Groups: workspace pins, deps, dev-deps, target — external then in-tree; alpha within group; no blank when a group is one.
- Patched dep: vendor it, keep the upstream version, header names the change; root names patch + exclude (sandbox design).
- Workspace is the lookup. In-memory default does not select that backend (cli contract). Conformance: `docs/tests.md`.

## Unsafe & lints

- `unsafe_code` `forbid` by default; `deny` + enumerated allow only where raw platform API forces it.
- Sandbox backends go `deny` as each lands (design decision 14); stub stays `forbid`.
- Clippy `absolute_paths` warn (`docs/style.md`).

## Features

- Workspace pins do not enable features; enable at the crate that uses them.
- Facade's feature list decides the shipped binary; a dep pin forcing a feature on survives `--no-default-features`.
- Transitive set spelled once at the shipping crate; consumers forward the name, never the set.
- Carrier is optional normal deps (each-feature pruning stays valid); the seam is a runtime wiring test, not a compile cfg.
- `tier` wires the backend arm and its `pg`/`versioning` prerequisites; `tinio-tier` is a non-optional `tinio-cli` dependency.

## sqlx cache

- The sqlx cache task owns the PostgreSQL statement cache: `update` regenerates, `check` reports drift.
- Both modes need a live, fully migrated database: `--dsn`, else `$DATABASE_URL`, else `$TINIO_PG_DSN`.
- `.sqlx/` at the workspace root is a build input: a statement edit without an `update` fails the database-free build.
- A failed run leaves `.sqlx/` untouched.
- `--cache-dir` (default `.sqlx/`) / `--scratch-dir` (default `target/tmp/sqlx-cache`) point at a copy or move the scratch.
- `check` exits 1 listing every missing/extra/changed entry; `docs/style.md` §SQL owns when to regenerate.
