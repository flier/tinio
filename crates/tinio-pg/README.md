# `tinio-pg` — the PostgreSQL backend

`tinio-pg` is the servable PostgreSQL backend: `PgStorage` implements `Storage`, `VersionOps` (under the `versioning` feature) and, elsewhere, `Cleanup`, over the shared operation engine (`tinio-ops`) and `tinio-store-pg`'s four persistence contracts. It carries the `[storage] type = "pg"` / `[storage.pg]` configuration, the synthetic root the tool state lives under (PG5/PG13), the read-only mode (PG6), the writable-ownership guard (PG12) and the `doctor` checks (PG11).

This crate **is** the pg backend, so it carries no `pg` switch of its own: `tinio-store-pg` is a plain dependency and every module that implements the contracts over it is compiled in every build. A build that does not want pg does not reference this crate — the `pg` switch belongs to the consumers (`tinio-cli`, `tinio`), which is PG15's principle one level up. The crate's own `versioning` feature adds the version plane (`tinio-version`): without it `PgStorage` answers the trait's own defaults for the version surface — `State::Unversioned` and an access-denied class for every other version operation — exactly as `tinio-fs`'s stripped build does.

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
| `src/cleanup.rs` | the `Cleanup` pass |
| `src/doctor.rs` | the `doctor` rows for a pg deployment (PG11) |
| `src/config.rs`, `src/error.rs` | the `[storage.pg]` configuration and the crate error |

## Gates

Database-free (the default gate):

```console
cargo test -p tinio-pg
```

The database-backed legs (`tests/conformance.rs`) read `TINIO_PG_DSN` and skip when it is unset, so `cargo test --workspace` stays database-free (PG8). Each leg creates its own scratch database.

```console
$env:TINIO_PG_DSN = 'postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'   # PowerShell
cargo test -p tinio-pg --features versioning
```
