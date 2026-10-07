# `tinio-store-pg` (P3): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote P1's PostgreSQL spike into a real workspace crate `tinio-store-pg` that implements all four persistence contracts — including the `VersionMeta` the spike never implemented and `PgBlocks` — with schema versioning, an error type projecting into `storage::Error`, and the conformance suite green from its own tests against `postgres:18-alpine`.

**Architecture:** The spike's SQL shapes and its verified schema move into a library crate behind a `pg` feature; the store owns one connection per transaction from a small pool, writes content as 1 MiB `bytea` chunks keyed by the spec's `(bucket, kind, key, seq)` shape with a live-generation marker the object row names (PG2, PG18, PG20, §3), pins `COLLATE "C"` on every key column and ordered read, and gates itself on a `schema_version` table the way the redb store gates on `FORMAT_VERSION`.

**Tech Stack:** Rust (edition 2024), `async-trait`, `sqlx` (the driver — PG26), `tokio`, `tinio-core` (the contracts), `tinio-blob` (block sweep), `postgres:18-alpine` via docker compose.

**Spec:** `docs/superpowers/specs/2026-10-03-pg-design.md` — this plan implements its **§3 Schema and migrations**, **§4 Connection, pool, TLS, ownership and access modes**, **§5 Content storage**, the store half of **§1–§2** (adopted from the spike's findings) and **PG1–PG25**.

**Depends on:** `docs/superpowers/plans/2026-10-03-metadata-store-contract-p1-contract-and-fs.md` (the contracts, the conformance suite, the spike and its findings) and `2026-10-03-metadata-store-contract-p2-mem-and-conformance.md` (the second consumer's legs). The version tables and the `VersionMeta` walkthrough exist only after the versioning set has landed.

## Global Constraints

- English only in repo artifacts; no hard-wrapped prose (`docs/compress.md`). Public items carry doc comments; the crate carries a runnable example on its constructor. Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- The crate is a workspace member behind a `pg` feature: `cargo test --workspace` must build and pass **without** a database; DB-backed tests run only when the `pg-tests` feature is on (PG8/PG15). The crate carries no `versioning` feature (PG16).
- Per-crate `Error` with `impl From<Error> for tinio_core::storage::Error`; the contract calls' `storage::Error` is `#[from]`-absorbed, like the other backends.
- Every ordered read pins `COLLATE "C"` (PG4); every key column is `text COLLATE "C"`.
- One transaction per mutating contract method; no transaction spans two contract calls; a multi-row operation is never half applied; a publish's boundary is bounded (PG22).
- Concurrency is the database's: row locks (`SELECT … FOR UPDATE`, `INSERT … ON CONFLICT`) and a single `UPDATE … RETURNING` for a read-modify-write; a transaction is never held across a non-database `await` (§3).
- One statement per point operation; no credential ever reaches a log line or a `Debug` impl (PG5).
- Git: every commit step asks the operator first (`docs/git.md`); no AI/agent trailers.
- Gates: `cargo test -p tinio-store-pg` (the statement layer's invariants, no database), `cargo test -p tinio-store-pg --features pg,pg-tests` (with PostgreSQL up), `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt`, the doc gate, `cargo hack check --package tinio-store-pg --each-feature`.

## Review Focus

1. **A listing that orders by the server's locale.** Every `ORDER BY` must repeat `COLLATE "C"`; the suite's adversarial key pair (`data` vs `data-2` vs a NUL-bearing key) is the pin, and a database created with a different `LC_COLLATE` must not change the order.
2. **A half-applied multi-row method.** A drain, a batch put, the completion join and `remove_if_empty` must be one transaction; a failure mid-way leaves nothing.
3. **A row without its bytes.** Publish writes the chunks and then the row; a reader that finds a row must find its chunks, and a failure between them leaves an orphan chunk that `Cleanup` reclaims (PG7).
4. **A schema newer than the binary.** The store must refuse it with **the pg remedy** — the required binary/schema version and a dump/restore path, never "delete and rebuild" (these tables hold the only copy of the bytes) — and migrate an older database forward. The audit found the earlier draft copying the redb remedy's destructive wording.
5. **A credential in a log or an error.** The dsn is secret-typed and redacted in `Debug`/`Display`; connection errors name the host and database, never the password.

---

### Task 1: re-baseline, adopt the findings, and stand up the crate

**Files:**
- Create: `crates/tinio-store-pg/Cargo.toml`, `crates/tinio-store-pg/src/lib.rs`
- Modify: root `Cargo.toml` (member glob already covers `crates/*`; remove `spike` from `[workspace] exclude`), `docs/superpowers/specs/2026-10-03-pg-design.md` (its §1–§2 are filled from the findings, marked as records)
- Move: `spike/pg-meta/{schema.sql,docker-compose.yml,README.md}` → `crates/tinio-store-pg/{schema.sql,docker-compose.yml,README.md}`; `spike/` is removed once empty

**Interfaces:**
- Consumes: P1's findings (`docs/superpowers/pg-meta-spike-findings.md`), the contracts, the conformance suite.
- Produces: the crate skeleton with a `pg` feature, the schema and compose files in their new home, and §1–§2 of the pg design document.

- [ ] **Step 1: Re-baseline** — read the landed version tables, the contracts as amended by the spike's findings, and the spike's own code; record in this task's notes every difference between the spike's assumptions and the tree.
- [ ] **Step 2: Adopt the findings** — copy `docs/superpowers/pg-meta-spike-findings.md` into the pg design document's §1–§2 verbatim, marked as records (the record rule: they are corrected by live prose beside them, never rewritten).
- [ ] **Step 3: Stand up the crate** — `git mv spike/pg-meta crates/tinio-store-pg`: the spike's `src/lib.rs` becomes the crate root (its layout is already a library, per P1's Task 2), `src/main.rs` moves to `src/bin/spike_gate.rs` so the P1 gate stays reproducible, and `Cargo.toml` declares `default = []` with **`pg = ["dep:sqlx"]`** and **`pg-tests = ["pg"]`** (PG15/PG26: `pg` gates dependencies and code, `pg-tests` the DB-backed tests, and the sqlx feature set lives in the crate's manifest; `pg` joins `crates/tinio`'s `full` set — this feature has several referents, which is why the removed `tinio-blob` crate-root gate is *not* the precedent; the crate-root gate pattern those crates used was deleted by operator ruling), the spike's store type is renamed `PgStore` (the name the later tasks use), plus module docs and the constructor's doc example.
- [ ] **Step 4: Verify the workspace stays database-free** — `cargo test --workspace` (feature off) PASS, `cargo hack check --package tinio-store-pg --each-feature` PASS.
- [ ] **Step 5: Commit** — ask the operator, then commit.

---

### Task 2: the store's surface, its error type and its pool

**Files:**
- Create: `crates/tinio-store-pg/src/{error.rs,pool.rs,store.rs}`
- Modify: `crates/tinio-store-pg/src/lib.rs`
- Test: the `#[cfg(test)] mod tests` in `store.rs` (connection and schema-version tests, behind `pg-tests`)

**Interfaces:**
- Consumes: `tinio_core::storage::Error`, `tinio_core::{blob::BlockStore, content::ObjectContent, meta::*}`.
- Produces: `tinio_store_pg::Error` (`#[from]`-absorbing `storage::Error`, redacting the dsn), the **driver's pool** (PG9/PG10/PG26: `sqlx::PgPool` sized `[storage.pg] pool`, connection timeout, health check, TLS through the driver's rustls connector over the system trust store, **an omitted `sslmode` treated as `require`** — which maps to verify-full, chain and hostname — the negotiated mode is recorded and logged, and read by `doctor` (PG10/PG26), `tinio_store_pg::{PgStore, Access}`, `PgStore::connect(dsn, pool, access)` (PG11: `Access::ReadWrite` migrates forward-only under a dedicated out-of-pool advisory lock, `Access::ReadOnly` only verifies and refuses a mismatch), **`PgStore::inspect(dsn, pool, access)`** (a non-migrating path that reads `schema_version` — what `doctor` uses), `PgStore::migrate(&self)`, and `PgStore::hold_ownership(&self) -> OwnershipGuard` (PG12: a dedicated out-of-pool connection, a named lock key, **a periodic re-verify that terminates the process on loss** (PG26: the driver exposes no keepalive knob, so the re-verify is also what keeps the connection's TCP path warm)) and the non-secret accessors `doctor` reads (`access`, `database`, `ssl_mode`, `pool`). `connect` also **verifies the database encoding is UTF-8** and refuses otherwise (PG4).

- [ ] **Step 1: Write the failing tests** — connect to the compose database and assert the pool hands out a usable connection; `Access::ReadOnly` on a database behind the binary's version must fail with an error naming the remedy (Review Focus 4) while `Access::ReadWrite` migrates it; a `sslmode=require` dsn with no TLS path must fail naming the mode, never downgrade (PG10); `hold_ownership` succeeds once and fails for the second holder with a message naming the owner (PG12); a connection failure's `Display` must not contain the dsn's password (Review Focus 5).
- [ ] **Step 2: Run to verify they fail** — `cargo test -p tinio-store-pg --features pg,pg-tests` (with `docker compose up -d`). Expected: `PgStore` does not exist.
- [ ] **Step 3: Implement** — the error enum; the pool with the TLS connector (PG26: `PgPoolOptions` over `PgConnectOptions` with the rustls native-roots feature); the `Access` mode (PG6: `ReadWrite` migrates and holds the ownership lock, `ReadOnly` verifies, refuses a mismatch and takes no lock); and `hold_ownership`'s dedicated connection (outside the pool, so the guard costs the pool nothing).
- [ ] **Step 4: Port the spike's four faces onto the store** — `impl BucketMeta`/`ObjectMeta`/`UploadMeta`/`ObjectContent` with the spike's statements adapted to the real surface (decoded rows, `storage::Error`, one transaction per method, `COLLATE "C"` everywhere), plus the content methods the spike covered (`stage`/`publish`/`open`/`remove`) and the three it did not (`rename` as one key-and-chunk `UPDATE`, `scan` as the ordered `SELECT`, and the part key shape over the same chunk table — spec D23's amendment).
- [ ] **Step 5: Run the conformance suite from the crate's own tests** — `assert_metadata_conformance` and `assert_object_content_conformance` (feature-gated). Expected: PASS; the suite's per-method atomicity legs are the pin for Review Focus 2. A failure is a shape finding for the contract (spec D24's rule, applied by P3's executor).
- [ ] **Step 6: Gate and commit.**

---

### Task 3: `VersionMeta`

**Files:**
- Create: `crates/tinio-store-pg/src/version.rs`
- Modify: `crates/tinio-core/src/meta/version.rs` only where the walkthrough and the real implementation disagree, with its suite leg in the same edit; `crates/tinio-util/src/testing/metadata.rs` if a leg turns out to be missing
- Test: a `version.rs` test module (behind `pg-tests`)

**Interfaces:**
- Consumes: the derived `VersionMeta` method list (P1's Task 8), the landed version tables, the engine's `VersionRows` shape.
- Produces: `impl VersionMeta for PgStore`; `assert_version_meta_conformance` green from PgStore's tests.

- [ ] **Step 1: Turn the walkthrough into tests** — the per-method table in the pg design document §2 becomes a test per group: version rows (put/get, sequence-descending list, lookup by id, `None` for an unknown key, the delete-marker row, drain), and the bucket's versioning state (`bucket_state`/`set_bucket_state`).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** — one transaction per method over `version_rows`/`version_index`/`bucket_versioning` equivalents, `COLLATE "C"` on the ordered list.
- [ ] **Step 4: Run to verify they pass**, including `assert_version_meta_conformance`.
- [ ] **Step 5: Gate and commit.**

---

### Task 4: `PgBlocks`, the chunk legs and the migration test

**Files:**
- Create: `crates/tinio-store-pg/src/blocks.rs`, `crates/tinio-store-pg/src/content.rs`, `crates/tinio-store-pg/src/migrations/` (numbered SQL files; v1 is the initial schema)
- Modify: `crates/tinio-store-pg/src/{lib.rs,store.rs}`
- Test: `blocks.rs`'s and `content.rs`'s test modules plus the migration test in `store.rs` (all behind `pg-tests`)

**Interfaces:**
- Consumes: `tinio_core::blob::{BlockStore, Published, StagedBlock, Entry}`, `tinio_blob::Store`, `tinio_core::content::{ObjectContent, ContentKey, ContentEntry, StagedObject}`.
- Produces: `impl BlockStore for PgBlocks` (PG3); the content implementation's **generation model** (PG18), its **row-backed `scan`** (PG23) and its `STORAGE EXTERNAL` chunk columns (PG24); the crate's migration runner and its version test.

- [ ] **Step 1: Write the failing tests** — `PgBlocks`' contract legs (stage/publish/contains/open with a range/remove/list, with `Entry.len` and `Entry.published` asserted), **a blob larger than the block chunk size and an interrupted publish** (PG19: several `blocks` rows, a ranged read touching only what it needs, a >1 GB blob storable, and a half-written block reading as *not present* until a re-publish fills its gaps), **the content generation leg** (PG18: write 3 MiB, overwrite with 1 MiB, then range-read across the old boundary — only the new bytes are ever served), **the residue leg** (PG23: a chunk with no object row is never listed and a bucket holding only residue is `is_empty`), the suite's 8 MiB content leg through `ObjectContent`, and a migration test that starts from a v0 fixture and reaches the current version through `PgStore::migrate` (each step one transaction, the advisory lock on its dedicated connection taken and released, an older database migrated forward and a newer one refused with **the pg remedy**); the migration test creates its scratch database twice, once under the composed C locale and once under a non-C locale the image provides, and runs the suite's adversarial key pair in both (PG4: the order must not move, or the collation belonged to the cluster and not to the column — Review Focus 1).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** — the block rows are **chunks of 8 MiB** keyed `(hash, seq)` with `len`/`published` on the block's first row and `ON CONFLICT DO UPDATE` on every chunk (PG14/PG19); the content relation holds **1 MiB** chunks (PG2) keyed `(bucket, kind, key, seq)` with `STORAGE EXTERNAL`, a **live generation** the object row names, a collision-proof part namespace and a `kind = 'object'` predicate in `scan` (PG18/PG20/PG24); staging is keyed, so a staged chunk is written under the target key from the start (§5, D38); the multipart completion's content half is the stated strategy — a part-list indirection, or a copy under a new generation the row's update makes live — so a partially assembled object is invisible (PG21); a publish's transaction boundary is bounded per the spike's pinned form (PG22), and **`scan` is row-backed** (PG23).
- [ ] **Step 4: Run to verify they pass** — plus `tinio_blob`'s sweep over `PgBlocks` (`LiveSet` reachability) and the bounded-publish measurement the spike recorded.
- [ ] **Step 5: Gate and commit** — `cargo test -p tinio-store-pg --features pg,pg-tests`, `cargo test --workspace`, the doc gate.

---

### Task 5: close-out

**Files:**
- Modify: `crates/tinio-store-pg/README.md` (the local gate command, the compose file, the schema-version policy), `docs/superpowers/specs/2026-10-03-pg-design.md` (Status line → landed, with what the spike's findings changed), `CONTEXT.md` if a new term earned a line

**Interfaces:**
- Produces: the crate's closed state: four contracts green from its own tests, a database-free workspace build, and the local gate documented. The CI PostgreSQL job is P5's (its consumer arrives there).

- [ ] **Step 1: The gates** — `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, the doc gate, `cargo hack check --package tinio-store-pg --each-feature`, and (with PostgreSQL up) `cargo test -p tinio-store-pg --features pg,pg-tests`.
- [ ] **Step 2: The invariants** — `rg -n 'COLLATE "C"' crates/tinio-store-pg/src` covers every ordered read (spot-check each `ORDER BY`); the credential claim is scoped so it can hold alongside the DB tests: `rg -n 'postgres://[^"]*:[^"]*@' crates --glob '!**/tests/**' --glob '!**/docker-compose.yml'` returns nothing (tests read `TINIO_PG_DSN`, and the one literal lives in the compose file, PG25); the schema-version refusal is exercised with its **pg** remedy; the encoding check is exercised; no SQL is built by formatting (`rg -n 'format!\("SELECT|format!\("INSERT|format!\("UPDATE' crates/tinio-store-pg/src` returns nothing — values are bound parameters).
- [ ] **Step 3: Doc sync and commit.**

## Plan-set ownership

- **P3 (this plan) owns**: the promoted crate, its schema and migrations, its pool and error type, `VersionMeta`, `PgBlocks`, the chunked content implementation, and the local gate.
- **P1/P2 own**: the contracts, the conformance suite, the spike and its findings, and the fs/mem migrations.
- **P4 owns**: the shared ops engine (`tinio-ops`) and its spec.
- **P5 owns**: `tinio-pg`, the `[storage.pg]` configuration with the `backend` selector, read-only with the writable-ownership lock, `Cleanup`, doctor checks, the `@pg` pass and the CI PostgreSQL job.
- **`docs/superpowers/specs/2026-10-03-pg-design.md` owns**: the schema, connection, content and backend-surface models.

## Notes for the executor

- The spike is the seed, not the spec: where its statement and the pg design document disagree, the document wins and the difference is a finding recorded in §2.
- Never let an ordered read rely on the database's default collation: `COLLATE "C"` on the column and in the `ORDER BY` (Review Focus 1).
- A shape problem is yours to fix (contract + suite leg + finding); a scope problem stops the work for a ruling — the same rule P1's gate runs under (spec D24).
- Keep the crate database-free for `cargo test --workspace`: every DB-backed test sits behind the non-default `pg-tests` feature (PG15).
