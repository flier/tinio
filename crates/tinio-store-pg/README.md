# `tinio-store-pg` — the PostgreSQL implementation of the tinio persistence contracts

`tinio-store-pg` implements the four persistence contracts — the metadata and version planes (`meta::Metadata`, `meta::VersionMeta`), the content plane (`content::ObjectContent`), and the content-addressed block plane (`blob::BlockStore`) — over PostgreSQL, with numbered schema migrations, a pooled TLS-capable driver session, and an error type that projects into `tinio_core::storage::Error`. It is not a backend: the servable `Storage`/`VersionOps`/`Cleanup` over it, and the `doctor` rows a pg deployment is diagnosed by, are `tinio-pg`'s. It carries no `pg` feature of its own (PG15) — a configuration that does not serve PostgreSQL does not depend on this crate at all, and `pg` is that consumer's optional dependency on it — and it owns neither the `[storage.pg]` section's resolution (the selector, the `TINIO_PG_DSN` override and every refusal are `tinio-config`'s) nor the cleanup policy (which scans run, and when, is `tinio-pg`'s).

## Features

| Feature | Effect |
|---|---|
| `testing` | Publishes the scratch-database harness (`test_support::TestDb`) for other crates' tests. Off by default and absent from every production graph; the database-backed tests are ordinary `#[cfg(test)]` legs that skip when `TINIO_PG_DSN` is unset, so the default build and `cargo test --workspace` stay database-free (PG8/PG15). |

## Layout

| File | What it is |
|---|---|
| `sql/**` | every multi-line SQL statement, one per file (`sql/<group>/<name>.sql`), named by a `query_file*!` literal, a plain-literal `include_str!`, or `sqlx::migrate!` for `sql/migrations/`; a one-line statement may be spelled inline at the call (docs/style.md §SQL owns the rule) |
| `src/migrations.rs`, `sql/migrations/*.sql` | the numbered schema migrations: the schema's only source of truth (PG11) |
| `src/pool.rs` | the driver pool, the dsn, and the TLS policy (PG9/PG10/PG26) |
| `src/store.rs` | `PgStore`, its access mode, the schema gate, and the ownership guard (PG11–PG13) |
| `src/meta/` | the metadata and version planes |
| `src/content.rs` | the 1 MiB `bytea` content plane (PG2/PG18/PG20/PG23/PG24) |
| `src/blocks.rs` | `PgBlocks`: the 8 MiB `bytea` block plane (PG3/PG14/PG19) |
| `src/cleanup.rs` | the row facts behind `tinio-pg`'s cleanup scans (PG7) |
| `src/diagnostics.rs` | the read-only probes `tinio-pg`'s doctor renders |
| `src/teardown.rs` | the bucket teardown and the per-upload part lock |
| `src/error.rs` | the crate error and every redaction rule (PG5) |
| `src/tests/db.rs` | the `testing` harness: one scratch database per leg, the dsn read, and the skip/required switch |
| `docker-compose.yml` | the pinned PostgreSQL 18 the DB-backed tests run against |

## Bring up PostgreSQL 18

```console
docker compose -f crates/tinio-store-pg/docker-compose.yml up -d
```

The compose file pins host port **55432**, the named volume, and a `pg_isready` healthcheck. An older spike container (`tinio-pg-meta-spike`) may still hold port 55432; stop it before bringing this one up.

### Verify

```console
docker exec tinio-store-pg psql -U tinio -d tinio -tAc 'select version()'
docker exec tinio-store-pg psql -U tinio -d tinio -tAc 'show server_encoding'
```

The expected `server_encoding` is `UTF8`: the store refuses to start otherwise, because `COLLATE "C"` orders the **server encoding's** bytes while the contracts order by the key's UTF-8 sequence (PG4). The database's own default collation is not `C`, which is why every key column is declared `text COLLATE "C"` and every ordered read repeats the collation.

## The dsn

The crate compiles in **no** dsn: the DB-backed tests read `TINIO_PG_DSN`, and the value lives only in `docker-compose.yml` and here, so the credential scan (PG25) never meets a literal in the source. A connection error renders the dsn redacted — never the password (PG5).

TLS follows PG10: a dsn with **no** `sslmode` is treated as `require` (rustls over the system trust store, chain and hostname verified), and plaintext is reached only through an explicit `sslmode=disable`. **Every other spelling — `prefer`, `allow`, `verify-ca`, `verify-full`, an unknown value, or the key written in a case the driver's own parse ignores — is refused at connect** with a message naming the two modes that are served: sqlx maps its `Prefer` onto a connection that accepts any certificate, so the mode could never mean what it said. `PGSSLMODE` can no longer weaken a dsn's mode, and the session is named by the dsn or by this crate, never by `PGAPPNAME`. The compose database above serves no TLS, so name it explicitly:

```console
$env:TINIO_PG_DSN = 'postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'   # PowerShell
export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'  # POSIX sh
```

## Migrations

`sql/migrations/` is the schema's only source of truth, embedded at compile time by `sqlx::migrate!` (each version parsed out of its `{version}_{description}.sql` name, so no Rust file lists them). `PgStore::connect` reads the applied version from `schema_version` and **migrates the database forward** before it serves: each step runs in its own transaction under an advisory lock held on a dedicated out-of-pool connection (PG11), with a bounded wait, so a second instance refuses rather than blocking behind a migration that may never finish. The applied version's **checksum** is recorded beside it and is part of the gate too: a database whose recorded checksum disagrees with the embedded migration was built from a schema this binary does not carry, and serving it would let a fresh database and an existing one diverge silently. A read-only instance never migrates and refuses a database whose version it does not match.

A database **newer** than the binary is refused, and the remedy is the pg one: upgrade the binary, or `pg_dump` the database and restore it into one this binary created. **Never drop and rebuild** — these tables hold the only copy of the bytes. The spike's `RESET_SQL` (`DROP TABLE` on every connect) was a gate affordance and is deliberately not carried over.

The spike's `schema.sql` seed is not carried over either: `sql/migrations/` is the schema, and a second copy beside the README would be a file nothing applies and nothing checks.

## The statements

Every statement is a `sql/<group>/<name>.sql` file beside the module that owns it — `sql/meta/`, `sql/content/`, `sql/blocks/`, `sql/store/`, `sql/cleanup/`, `sql/diagnostics/`, `sql/tests/` — named at its call site by `query_file!`, `query_file_as!` or `query_file_scalar!`, or loaded by `include_str!` for the DDL and templated statements no macro can check; a migration is embedded by `sqlx::migrate!`'s directory literal. Which statement earns a file and which sits inline is `docs/style.md` §SQL's rule.

The macros check every statement against the schema at compile time. With no `DATABASE_URL` — and in CI, which has no database — they read the committed `.sqlx` cache at the workspace root, so a statement missing from the cache fails the build. Regenerate it after editing a statement or the schema, against a fully migrated database (`docs/cargo.md` §sqlx cache owns the command, its dsn sources and its scratch-directory rule); the ordinary build stays database-free (PG26):

```console
cargo xtask sqlx update
```

The files are formatted by sqruff (`.sqruff`: dialect `postgres`, 80 columns): run `sqruff fix crates/tinio-store-pg/sql` after editing one, and the CI `sql` job lints them. Every statement parses in that dialect, so no file is exempt.

## Gates

Database-free (the default gate):

```console
cargo test -p tinio-store-pg
cargo test --workspace
```

The driver session compiles unconditionally, so a plain build is the whole crate:

```console
cargo build -p tinio-store-pg
```

The database-backed gate, with `TINIO_PG_DSN` set (see **The dsn**):

```console
cargo test -p tinio-store-pg -- --test-threads=4 --nocapture
```

Each DB-backed test creates its own scratch database from the base dsn, and every leg opens a **4-connection pool** — which is what the `--test-threads=4` above pins for a plain `cargo test`; `cargo nextest run -p tinio-store-pg` takes the same bound from `.config/nextest.toml`'s `pg-db` test group. Why the bound exists, and the `TINIO_PG_REQUIRED` switch that voids an unset-dsn skip, are `docs/tests.md`'s.
