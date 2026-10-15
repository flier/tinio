# `tinio-pg` — the servable PostgreSQL backend over the shared ops engine

`tinio-pg` is the servable PostgreSQL backend: `PgStorage` implements `Storage` and `VersionOps` (under the `versioning` feature), with `PgCleanup` beside it implementing `Cleanup`, over the shared operation engine (`tinio-ops`) and `tinio-store-pg`'s four persistence contracts. It consumes a `Config` (the dsn and the pool size) plus the access mode the CLI resolved; the configuration schema is not its own — `[storage] type = "pg"`, `[storage.pg]`, the `TINIO_PG_DSN` override, the synthetic root the tool state lives under (PG5/PG13) and every refusal are `tinio-config`'s and the CLI's. The persistence facts — the schema and its migrations, the dsn and its TLS policy, the schema gate and the ownership guard — are `tinio-store-pg`'s; what this crate owns is the backend itself: the read-only entry guard (PG6), the constructor's ownership lock (PG12), the `doctor` rows a pg deployment is diagnosed by (PG11), and the two steps the engine's `Hooks` seam leaves to a backend — an empty bucket-delete hook, because a bucket here *is* its rows, and the real part-write lock.

This crate **is** the pg backend, so it carries no `pg` switch of its own: `tinio-store-pg` is a plain dependency and every module that implements the contracts over it is compiled in every build. A build that does not want pg does not reference this crate — the `pg` switch belongs to the consumers (`tinio-cli`, `tinio`), which is PG15's principle one level up.

## Features

| Feature | Effect |
|---|---|
| `versioning` | Adds the version plane (`tinio-version`). Without it `PgStorage` stays on the trait's own defaults for the version surface — `State::Unversioned` and an access-denied class for every other version operation — exactly as `tinio-fs`'s stripped build does. |
| `testing` | Forwards `tinio-store-pg`'s dev-only scratch-database harness; the `@pg` pass turns it on and a production graph leaves it off. |

`Config` and `Error` are feature-independent: a consumer that resolved `[storage.pg]` builds the one in every graph, and the other is what a resolution failure and a construction refusal are rendered with.

## Read-only deployments (FR-023, PG6)

Read-only mode is **three layers, not one**, and a read-only deployment must supply all three. Each holds where the one above it cannot:

1. **The entry guard** (`src/readonly.rs`) — every mutating operation of `Storage` and `VersionOps` answers the contract's `AccessDenied` **before any SQL**. This layer is the crate's and is in place for any instance constructed with `Access::ReadOnly`.
2. **The session** — a read-only instance's connections are opened with `default_transaction_read_only = on` as a **startup option** (`Pool::open_read_only`), so the server holds the setting before the first statement and refuses a write the guard somehow let through (PG26).
3. **The role** — **the deployment requirement**. The role a read-only instance connects as must carry **no `INSERT`/`UPDATE`/`DELETE` grants** on the schema. This is the only layer a *helper* that bypasses the entry guard cannot get around: a call made straight at `tinio-store-pg`'s contracts (never through `PgStorage`) never meets layer 1, so the database's own privilege check is what refuses it.

### Provisioning the read-only role

Create a login role that owns nothing and grant it **only** `SELECT`:

```console
CREATE ROLE tinio_ro LOGIN PASSWORD '<generated-here>';
GRANT USAGE ON SCHEMA public TO tinio_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tinio_ro;
```

Never grant `INSERT`, `UPDATE` or `DELETE` to it, and never make it the owner of a table (an owner can grant itself whatever it lacks). Point the read-only instance's dsn at that role.

A read-only instance never migrates and refuses a schema it does not match, so **a writable instance must have migrated the database first** (PG11). A read-only instance takes no ownership lock and coexists with the writer (PG12).

## Layout

| File | What it is |
|---|---|
| `src/readonly.rs` | the read-only entry guard (PG6, layer 1) |
| `src/storage.rs` | `PgStorage` and its `Storage` categories over the ops engine |
| `src/version.rs` | the version plane and `VersionOps` (the `versioning` feature) |
| `src/complete.rs` | multipart completion: the commit half the engine leaves to the backend |
| `src/cleanup.rs` | the `Cleanup` pass (PG7) |
| `src/doctor.rs` | the `doctor` rows for a pg deployment (PG11) |
| `src/hooks.rs` | the engine's `Hooks` seam: the empty bucket-delete hook and the real part-write lock |
| `src/config.rs` | the runtime configuration the constructor consumes: the dsn and the pool size |
| `src/error.rs` | the crate error and its total projection onto the contract's |
| `tests/` | the DB-backed legs — conformance, cleanup, delete-bucket, large-body, part-lock — each skipping without `TINIO_PG_DSN` |

## Gates

Database-free (the default gate):

```console
cargo test -p tinio-pg
```

The database-backed legs read `TINIO_PG_DSN` through `tinio-store-pg`'s harness and skip when it is unset, so `cargo test --workspace` stays database-free (PG8) — that harness owns the dsn's skip rule and the `TINIO_PG_REQUIRED` switch that voids it. Each leg creates its own scratch database.

Export a dsn first: the dsn itself, its TLS policy and the export line belong to `tinio-store-pg` and are not repeated here (`../tinio-store-pg/README.md`).

```console
cargo test -p tinio-pg --features versioning
```
