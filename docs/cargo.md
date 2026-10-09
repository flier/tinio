# Cargo Workspace

## Rules

- Manifests: no comments (`docs/` owns rationale). Pin once at workspace; members path-only, inherit version/edition (`major.minor` only); publish only the facade; optional/target deps at the crate.
- Groups: workspace pins, deps, dev-deps, target — external then in-tree; alpha within group; no extra blank when a group is one.
- Patched dep: vendor it, keep the upstream version, header names the change; root names patch + exclude (sandbox design).
- `unsafe_code` `forbid` by default; `deny` + enumerated allow only where raw platform API forces it. Sandbox backends go `deny` as each lands (design decision 14); stub stays `forbid`. Clippy `absolute_paths` warn (`docs/style.md`).
- Workspace is the lookup. In-memory default does not select that backend (cli contract). Conformance: `docs/tests.md`.

## Features

- Workspace pins do not enable features (`default-features = false` there only to stop inherit / re-enable). Enable at the crate that uses them.
- Facade's feature list, never a dependency pin, decides the shipped binary — a dep pin that forces a feature on would survive `--no-default-features`.
- Transitive set spelled once at the crate that ships the operations; consumers forward the name, never the set (spec 2026-10-03). Carrier is optional normal deps so each-feature pruning stays valid. Seam is a runtime wiring test, not a compile cfg.


## sqlx cache

- `cargo xtask sqlx` owns the PostgreSQL statement cache: `cargo xtask sqlx update` regenerates it, `cargo xtask sqlx check` reports drift without writing. Both need a live, fully migrated database — the driver describes each statement against that schema — from `--dsn`, else `$DATABASE_URL`, else `$TINIO_PG_DSN`; the fixture is `docs/tests.md`'s. The mode is the subcommand.
- The committed cache is the workspace-root `.sqlx/`, a build input: a statement edit without an `update` fails the database-free build. The command drives `cargo check -p tinio-store-pg --all-targets` with `SQLX_OFFLINE=false` and a scratch `SQLX_OFFLINE_DIR`, never `cargo sqlx`. Cargo does not track that variable, so a build it already considers fresh reruns no sqlx macro and leaves the scratch directory empty: the command notices, retries after `cargo clean -p tinio-store-pg`, and refuses an empty generated set against a non-empty cache instead of writing it -- a run that cannot generate leaves `.sqlx/` untouched. `--cache-dir` (default `.sqlx/`) and `--scratch-dir` (default `target/tmp/sqlx-cache`) check a copy or move the scratch off the cache.
- `cargo xtask sqlx check` exits 1 and lists every missing, extra or changed entry; a generation failure leaves the cache untouched. `docs/style.md` §SQL owns when to regenerate.
