# `spike/pg-meta` — the PostgreSQL gate for the persistence contracts (P1 Task 2)

This directory is the D21/D25 spike: it implements `tinio-core`'s metadata contracts (`BucketMeta`/`ObjectMeta`/`UploadMeta`), `VersionMeta` and the whole D23 `ObjectContent` surface over **tokio-postgres**, and runs the conformance suite in `tinio_util::testing::metadata` against a real `postgres:18-alpine`.

**P3 promotes this directory into `crates/tinio-store-pg`** (D25/D28): the schema here is the seed the promoted crate derives its migrations from, and this README's dsn/port pin is what P3's own compose file carries forward.

It is **outside the workspace** (`spike` is in the root `Cargo.toml`'s `exclude`). CI carries a **compile-only** check of this crate (`cargo check --manifest-path spike/pg-meta/Cargo.toml`) so a contract addition can no longer break it silently; the full gate below needs a live PostgreSQL and stays the operator's manual run, never CI's. Its gate is one command.

**The spike resets its own relations on every connect.** `PgStore::connect` runs `RESET_SQL` (a `DROP TABLE` of all 15 relations) before applying `schema.sql`, because the file is `CREATE TABLE IF NOT EXISTS` and a database left by an earlier revision would otherwise keep an older column set - a stale column type would silently survive, which is exactly what the `integer` -> `bigint` correction had to avoid. The blast radius is the whole spike schema on every connection. It is a gate affordance, not a migration: **P3 must replace it with migrations and must not carry `RESET_SQL` over.**

## 1. Bring up PostgreSQL 18

```console
docker compose -f spike/pg-meta/docker-compose.yml up -d --pull never
```

The image `postgres:18-alpine` is expected in the local image store; `--pull never` states that the registry is unreachable for this host (the default `missing` pull policy does not fetch it either). The compose file pins the port, the named volume and a `pg_isready` healthcheck.

### Verify

```console
docker exec tinio-pg-meta-spike psql -U tinio -d tinio -tAc 'select version()'
docker exec tinio-pg-meta-spike psql -U tinio -d tinio -tAc 'show server_encoding'
docker exec tinio-pg-meta-spike psql -U tinio -d tinio -tAc 'select datcollate from pg_database where datname = current_database()'
```

Expected (verified on the host this spike was written on):

```text
PostgreSQL 18.4 on x86_64-pc-linux-musl, ...
UTF8
en_US.utf8
```

`server_encoding` is UTF-8 (the store refuses to start otherwise, PG4) and the **database default collation is `en_US.utf8`, not `C`** — which is exactly why every ordered read in `src/lib.rs` spells `COLLATE "C"` and every key column is declared `text COLLATE "C"`. On this musl image the `en_US.utf8` default happens to order bytes the same way `"C"` does (musl ships no locale data); the gate proves the pin is load-bearing anyway by ordering the same four values under an ICU collation, which reorders them (`"C"` gives `0,B,_,a`, `en-US-x-icu` gives `_,0,a,B`).

## 2. The dsn

The spike compiles in **no** dsn: it reads `TINIO_PG_DSN`, and the value lives only in `docker-compose.yml` (and here). `pg_meta::dsn_from_env` errors out when the variable is unset, naming the variable and pointing back here — there is no hardcoded fallback, so the credential scan (pg-design PG25) never meets a literal in the source.

The value `docker-compose.yml` pins (host port 55432):

```text
postgres://tinio:tinio@127.0.0.1:55432/tinio
```

Set it before running the gate:

```console
$env:TINIO_PG_DSN = 'postgres://tinio:tinio@127.0.0.1:55432/tinio'   # PowerShell
export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio'  # POSIX sh
```

Connection errors render the dsn redacted (`PgStore::redact_dsn`), never the password — an env-derived dsn exactly as a literal one.

## 3. The gate

With `TINIO_PG_DSN` set (section 2):

```console
$env:TINIO_PG_DSN = 'postgres://tinio:tinio@127.0.0.1:55432/tinio'; cargo run --manifest-path spike/pg-meta/Cargo.toml   # PowerShell
TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio' cargo run --manifest-path spike/pg-meta/Cargo.toml          # POSIX sh
```

Exit `0` is the gate (spec §5a). It runs eight legs: the encoding/collation pin, schema presence, `assert_metadata_conformance`, `assert_version_conformance`, `assert_content_conformance`, the driver-level 8 MiB staged write (64 KiB writes, full read plus a ranged read, the publish plan reported), the raw-SQL self-heal leg (a garbage etag inserted directly reads as absent by key, fails a strict `walk` and is reported as `None` and healed by `walk_healing`), and the dsn-redaction leg. A failure exits `1` and names the leg.

The library carries the store and needs no test support; only the driver runs the shared suite (the `conformance` feature, on by default, enables `tinio-util/testing`). That split keeps a store-only check available:

```console
cargo check --manifest-path spike/pg-meta/Cargo.toml --lib --no-default-features
```

While this is the gate, `cargo test -p tinio-core --lib` and the rest of the workspace are untouched: the spike is not a workspace member and adds no crate to `cargo metadata`.

## 4. Layout

| File | What it is |
|---|---|
| `src/lib.rs` | the contract implementations over tokio-postgres, the crate `Error` (redacting the dsn), the `TINIO_PG_DSN` read (`dsn_from_env`), and two gate-only raw helpers |
| `src/main.rs` | the gate driver |
| `schema.sql` | the schema, applied at connect; the seed for `tinio-store-pg` (P3) |
| `docker-compose.yml` | the pinned PostgreSQL 18 |
