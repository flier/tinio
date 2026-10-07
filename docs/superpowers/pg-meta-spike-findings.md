# The PostgreSQL spike's findings (P1 Task 2, D21)

**Date**: 2026-10-03 · **Status**: record. The pg design adopts its section 1-2 from this file (P3 copies this record in), so the forms pinned here are binding on `crates/tinio-store-pg`. · **Gate**: `cargo run --manifest-path spike/pg-meta/Cargo.toml` exits 0. · **Spike**: `spike/pg-meta/`, outside the workspace; **from the operator's 2026-10-07 ruling a compile-only CI job checks it** (`pg-meta-check`: `cargo check --manifest-path spike/pg-meta/Cargo.toml`), which supersedes D25's "from CI" (note under D25 in the design).

> **WARNING - the standing hazard is still live; the verdict it once disclaimed is now re-established.** As of the run recorded in 1.2 (2026-10-07), the gate went **8/8 legs green, exit 0** and that verdict covers the landed contract surface: the 2026-10-03 run had also completed all eight legs, but it predates the contract's growth and its verdict was not reproducible from the committed source, so this is the first run whose 8/8 covers the landed contract (the two re-runs before it stopped at leg 3 and then at leg 4). The earlier warning, which said this document recorded a green run against a *superseded* contract rather than a green verdict for the landed one, is kept below as the record of what it disclaimed.
>
> **The standing facts this warning exists to keep are unchanged.** The spike lives **outside the workspace** (`spike` is in the root `Cargo.toml`'s `exclude`), so `cargo check --workspace` cannot see the crate at all, and **the same defect class got through twice**: a new required trait item is a compile error that no workspace gate observes. `cargo check --manifest-path spike/pg-meta/Cargo.toml` is the check no workspace gate can perform; **from the operator's 2026-10-07 ruling a compile-only CI job now runs exactly that check** (`pg-meta-check` in `.github/workflows/ci.yml`), which supersedes the earlier "in no CI job" - the spike stays outside the workspace and outside every gate that runs the suite or the full gate.
>
> **The first round**: `impl UploadMeta for PgStore` was missing the required trait item `UploadMeta::list_parts_page` (`crates/tinio-core/src/meta/upload.rs`) - a required method with no default body - so `spike/pg-meta/src/lib.rs` failed to build.
>
> **The second round**: the contract grew **three more** required methods, again with no default bodies - `BucketMeta::set_tags_or_create` and `BucketMeta::set_grants_or_create` (`crates/tinio-core/src/meta/bucket.rs`) and `UploadMeta::drain_upload_checked` (`crates/tinio-core/src/meta/upload.rs`). They are the atomic first-sight element writes and the abort path's identity-checked drain that the later review required once the fs side was found to have degraded these writes from one transaction to two or three. All three are implemented now (section 5), but the crate did not compile at all between the contract's change and that implementation.
>
> Nothing caught either round because the spike lives **outside the workspace** (`spike` is in the root `Cargo.toml`'s `exclude`) and **out of CI**, so `cargo check --workspace` cannot see the crate at all. That is the same blindness letting the same defect class through twice: a new required trait item is a compile error that no gate in this repository observes. (2026-10-07: the operator subsequently ruled a compile-only CI job in - `pg-meta-check`, `cargo check --manifest-path spike/pg-meta/Cargo.toml` - which closes this gap; "out of CI" and "no gate in this repository observes" were true of these two rounds, before that ruling.)
>
> **What the earlier warning disclaimed (kept as the record).** The gate verdict in this document was superseded and was NOT reproducible from the committed source - the crate failed to build as committed, twice over, and the 8/8 verdict then in 1.2 could not be re-established either time.
>
> **The verdict must be re-established.** Bring the database up, then run the gate:
>
> ```console
> docker compose -f spike/pg-meta/docker-compose.yml up -d --pull never
> cargo run --manifest-path spike/pg-meta/Cargo.toml
> ```
>
> **That has now happened (2026-10-07).** The output in 1.2 is the fresh run: the crate compiles, all eight legs are green, and the verdict - unlike the superseded 2026-10-03 one - covers the landed contract surface.
>
> **The 2026-10-07 re-runs before the green one (kept as the record).** With the crate compiling again, the operator ran the gate: **legs 1-3 passed** (leg 3, `assert_metadata_conformance`, after the uploads-page fix) and **leg 4 (`assert_version_conformance`) panicked** at `crates/tinio-util/src/testing/metadata.rs:465` -- "a row naming different content at the same sequence is refused". The cause is F18 (the spike's `put_version` enforced none of the contract's three rules), fixed in `spike/pg-meta/src/lib.rs`. The two re-runs stopped there - at leg 3 and then at leg 4 - until the third run recorded in 1.2, which is the first whose 8/8 covers the landed contract surface.

This document records what a real PostgreSQL 18 found that the contracts and the conformance suite could not: the pinned schema forms, the findings with their resolutions, the per-method SQL walkthrough, the encoding/collation pin, and the two suite defects the gate itself caught.

## 1. The gate

One command, one verdict:

```console
cargo run --manifest-path spike/pg-meta/Cargo.toml
```

It runs eight legs: the connect + encoding/collation pin, schema presence, the three conformance suites (metadata, version, content), the driver's own 8 MiB staged write (64 KiB writes, full read, ranged read, publish plan), the raw-SQL self-heal plant, and the dsn-redaction leg. The version-row walkthrough (section 5) is Step 6's human-review item; it is backed by the full run recorded in 1.2 (2026-10-07, 8/8 legs green).

### 1.1 The pinned server

```text
image            postgres:18-alpine (docker compose, pinned port 55432, named volume, pg_isready healthcheck)
server_version   18.4
server_encoding  UTF8
datcollate       en_US.utf8
dsn              read from TINIO_PG_DSN (its sole source; no literal in the crate) - docker-compose.yml pins postgres://tinio:tinio@127.0.0.1:55432/tinio
```

The registry is unreachable from this host, so the compose file is brought up with `--pull never` (the default `missing` policy does not fetch either).

```console
docker compose -f spike/pg-meta/docker-compose.yml up -d --pull never
```

### 1.2 The gate's output

**2026-10-07 (current).** The third run of this gate, and the first whose 8/8 verdict covers the landed contract surface. The 2026-10-03 record below also completed all eight legs, but it predates the contract's growth (`list_parts_page`, `set_tags_or_create`, `set_grants_or_create`, `drain_upload_checked`) and its verdict was not reproducible from the committed source; the two re-runs earlier on 2026-10-07 stopped at leg 3 and then at leg 4. The operator's console output, verbatim:

```console
pg-meta gate (P1 Task 2, D21)
[leg 01/8] connect + encoding/collation pin
  server_version=18.4 server_encoding=UTF8 datcollate=en_US.utf8
  datcollate order ["data", "data-2", "data/", "data/leaf"]
  COLLATE "C" ["0", "B", "_", "a"] vs COLLATE "en-US-x-icu" ["_", "0", "a", "B"] on the same values
[leg 02/8] schema presence
  15 tables: blocks, bucket_versioning, buckets, content, content_heads, current_slots, object_meta, object_parts, part_checksums, parts, schema_version, upload_checksums, uploads, version_index, version_rows
  3 part-number columns are bigint
[leg 03/8] assert_metadata_conformance
  metadata suite green
[leg 04/8] assert_version_conformance
  version suite green
[leg 05/8] assert_content_conformance
  content suite green
[leg 06/8] 8 MiB staged write (64 KiB chunks) + full and ranged read
  8388608 bytes staged in 128 x 64 KiB writes; publish plan: 8 chunk rows in 2 statements (store chunk 1024 KiB, batch 4 MiB)
  full read 8388608 bytes; ranged read 1000003..=3500000 -> 2499998 bytes; stat len 8388608 (identity 0, mtime None)
[leg 07/8] raw-SQL self-heal plant
  garbage etag -> get None, walk Err, walk_healing (bad -> None), walk after the heal: 1 row
[leg 08/8] dsn redaction
  redact_dsn -> <dsn>; failed connect -> postgres connect: error connecting to server
gate: 8/8 legs green -- exit 0
```

**Dated note (2026-10-07, after the run above).** That run was made with the dsn supplied on the command line. The operator then ruled the hardcoded credential out of the spike's source: the crate reads `TINIO_PG_DSN` and, when it is unset, errors with a message naming the variable. The gate was **re-run and is 8/8 green with the dsn taken from the environment**:

```console
$env:TINIO_PG_DSN = 'postgres://tinio:tinio@127.0.0.1:55432/tinio'; cargo run --manifest-path spike/pg-meta/Cargo.toml   # PowerShell
TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio' cargo run --manifest-path spike/pg-meta/Cargo.toml          # POSIX sh
```

The unset path was exercised as well: with the variable absent the gate prints that message and exits 1, leaking no credential (observed - the operator's own mistyped invocation produced it).

**Superseded: the earlier record (this document's original 2026-10-03 run).** The output below is the record this section carried before the 2026-10-07 run above. It predates the contract's growth (`list_parts_page`, `set_tags_or_create`, `set_grants_or_create`, `drain_upload_checked`) and was not reproducible from the committed source in the two rounds before the 2026-10-07 run; it is kept as what the gate printed then.

```console
$ cargo run --manifest-path spike/pg-meta/Cargo.toml
pg-meta gate (P1 Task 2, D21)
[leg 01/8] connect + encoding/collation pin
  server_version=18.4 server_encoding=UTF8 datcollate=en_US.utf8
  datcollate order ["data", "data-2", "data/", "data/leaf"]
  COLLATE "C" ["0", "B", "_", "a"] vs COLLATE "en-US-x-icu" ["_", "0", "a", "B"] on the same values
[leg 02/8] schema presence
  15 tables: blocks, bucket_versioning, buckets, content, content_heads, current_slots, object_meta, object_parts, part_checksums, parts, schema_version, upload_checksums, uploads, version_index, version_rows
  3 part-number columns are bigint
[leg 03/8] assert_metadata_conformance
  metadata suite green
[leg 04/8] assert_version_conformance
  version suite green
[leg 05/8] assert_content_conformance
  content suite green
[leg 06/8] 8 MiB staged write (64 KiB chunks) + full and ranged read
  8388608 bytes staged in 128 x 64 KiB writes; publish plan: 8 chunk rows in 2 statements (store chunk 1024 KiB, batch 4 MiB)
  full read 8388608 bytes; ranged read 1000003..=3500000 -> 2499998 bytes; stat len 8388608 (identity 0, mtime None)
[leg 07/8] raw-SQL self-heal plant
  garbage etag -> get None, walk Err, walk_healing (bad -> None), walk after the heal: 1 row
[leg 08/8] dsn redaction
  redact_dsn -> <dsn>; failed connect -> postgres connect: error connecting to server
gate: 8/8 legs green -- exit 0
```

## 2. The schema seed

`spike/pg-meta/schema.sql` is applied at connect (P3 derives its migrations from it). Fifteen relations:

| Table | Key | Carries |
|---|---|---|
| `buckets` | `name` | creation time, tags wire, owner wire, ACL wire, CORS wire |
| `object_meta` | `(bucket, key)` | etag, size, mtime, identity, tags, checksum, owner, ACL |
| `object_parts` | `(bucket, key, number)` | size, algorithm, base64 value (the retained part list) |
| `uploads` | `(bucket, upload_id)` | key, initiated-at, tags, owner, ACL |
| `parts` | `(bucket, upload_id, number)` | etag |
| `part_checksums` | `(bucket, upload_id, number)` | algorithm, value |
| `upload_checksums` | `(bucket, upload_id)` | algorithm, checksum kind |
| `bucket_versioning` | `bucket` | the bucket's versioning state |
| `version_rows` | `(bucket, key, seq)` | id wire, blob, size, etag, created, became-noncurrent, kind, tags, checksum, owner, ACL |
| `version_index` | `(bucket, key, id)` | the id-to-seq lookup (the point index read) |
| `current_slots` | `(bucket, key)` | the key's current version identity (nullable) and sequence (finding F3) |
| `content_heads` | `(bucket, kind, ckey)` | the generation counter, the live generation and its length (PG18) |
| `content` | `(bucket, kind, ckey, seq, chunk_no)` | a 1 MiB chunk, `STORAGE EXTERNAL` (PG18, PG20, PG24) |
| `blocks` | `(hash, seq)` | an 8 MiB block chunk plus `len` and `published` (PG14, PG19; P3's `PgBlocks` uses it) |
| `schema_version` | `id` | the applied schema version, its timestamp and note |

Every column an ordered read compares is `text COLLATE "C"`, and every ordered statement repeats the collation; discriminator and wire-only columns (`version_index.id`, `content.kind`, the tags/checksum/owner/acl wires) carry no collation because no ordered read touches them. The connect-time UTF-8 check is what makes `"C"` mean the contract's byte order (PG4, section 6).

Part numbers are `bigint` in all three part relations (the brief's pin; the store binds them as `i64`), and the gate's schema leg asserts that the three `number` columns' `data_type` is `bigint` rather than asserting the name alone.

`PgStore::connect` runs `RESET_SQL` before applying `schema.sql`: **it drops all 15 relations on every connect** (`DROP TABLE ... CASCADE`), so each connection starts from an empty schema - a second connect in the same process empties what the first one created. The blast radius is the point: the file is `CREATE TABLE IF NOT EXISTS`, so without the reset a database left by an earlier revision would silently keep an older column set and the gate would pass against yesterday's DDL - exactly the failure the `integer`->`bigint` correction would otherwise have hit.

**Spike-only, and gone at promotion**: the README promotes this directory into `crates/tinio-store-pg`, where the schema becomes migrations. `connect` must then migrate and never drop, and `RESET_SQL` must not be carried over.

The schema leg asserts **presence, not absence**: it fails when one of the 15 relations is missing, but it only prints the table list, so a stale *extra* table would be reported rather than refused. With `RESET_SQL` in place that cannot arise in the gate; P3's migration check should assert the absence of foreign relations if it wants the stricter property.

## 3. The forms the pg design defers to this spike (PG18-PG23)

### PG18 - the content discriminator and the live generation

**Pinned**: `content_heads(bucket, kind, ckey, next_seq, live_seq, live_len)` beside `content(bucket, kind, ckey, seq, chunk_no, chunk)`. `kind` is a `CHECK (kind IN ('object','part'))` discriminator in the primary key of both relations. A publish allocates a fresh generation (`INSERT ... ON CONFLICT DO UPDATE SET next_seq = content_heads.next_seq + 1 RETURNING next_seq`), writes that generation's chunks, then makes it live in one `UPDATE` guarded by `AND next_seq = $4`. A reader resolves `live_seq`, never `next_seq`, so an interrupted publish is invisible and a shrinking overwrite cannot serve residue.

**Finding F1**: the live-generation marker cannot live in `object_meta` the way the pg design's section 3 table sketches it ("the object's row records which generation is live"). The content contract is independent of the metadata plane: `assert_content_conformance` publishes bytes with no object row at all, and `scan`/`stat`/`open` must still answer. The marker therefore lives in the content relation (`content_heads`), and PG18's *requirement* holds exactly.

### PG19 - the block plane's `len`/`published` and publish completeness

**Pinned**: `blocks(hash text COLLATE "C", seq integer, chunk bytea, len bigint, published bigint)` with `chunk` `STORAGE EXTERNAL`. Every chunk row carries the block's declared `len` and the publish's comparable `published` stamp (the caller's clock), and a publish writes with `ON CONFLICT DO UPDATE` so a repeat fills gaps instead of answering "exists". A block is served only when the full chunk count for its declared `len` is present, so "some rows, not all" reads as absent.

**Finding F10 (scope)**: the spike pins the *form* but does not implement `BlockStore` - the D23 surface this task implements carries no block call, and `PgBlocks` is P3's (PG3, PG14). The columns are the seed; the list/grace-window behaviour is P3's to prove.

### PG20 - the collision-proof part namespace

**Pinned**: parts share the content relation with `kind = 'part'` and their own key space: `ckey = "p:" || hex(upload_id) || ":" || zero-padded-4-digit number` (the code's `format!("p:{hex}:{number:04}")`; no read depends on the padding) for parts, and `ckey = "o:" || object_key` for objects. The `kind` discriminator is in the primary key, so no part number can collide with an object key and no object key can collide with a part. `scan` filters `kind = 'object'` *and* `starts_with(ckey, 'o:' || prefix)`, so a part is never enumerated, listed or opened as an object.

### PG21 - the multipart completion's assembly

**Deferred to P3 - not pinned here, and not ambiguous.** No D23 call reaches completion, and review item 39 puts both the form and its leg on P3 (D43 keeps this spike to the D23 surface); pinning a form would mean inventing behaviour against a document the findings already supersede for P3.

What the spike *does* pin is the mechanism a completion must use: assemble under a **new generation** and flip the head, never rewrite a live generation in place. Both halves are implemented and green - `publish` inserts chunks under `next_seq + 1` and flips with one guarded `UPDATE`, and `copy` copies a generation's chunks and flips its own - so a partial assembly is invisible by construction. The two candidate shapes stay open and unranked for P3: (a) copy the retained parts' chunks into a fresh generation, or (b) a part-list indirection read at `open`/`stat` time.

### PG22 - the bounded publish transaction

**Pinned**: chunks commit in batches of `PUBLISH_BATCH_CHUNKS = 4` (4 MiB), one `INSERT ... SELECT ... FROM unnest($5::int[], $6::bytea[])` statement per batch, under the incompleteness marker (`live_seq` does not move until the final `UPDATE`). The statement count for a body of `n` chunks is `ceil(n / 4) + 2` (one allocate, the batches, one flip) - bounded and independent of the body size, and the body is never one unbounded transaction.

**Declared and reported**: the gate prints the plan for its 8 MiB leg - 8 chunk rows in 2 batch statements - and that leg really stages, publishes and byte-compares the chunks; the *plan* itself is a shared computation (the driver prints what `publish` computes), so this line is a declared form, not an independent measurement. A 100 GiB body would be 102 400 chunks in 25 600 batches, 25 602 statements in all (`ceil(n/4) + 2`), each a bounded transaction; the `>100 MiB` streaming leg is P5's (D43).

### PG23 - `scan` is row-backed

**Pinned**: `scan` enumerates `content_heads` rows with `live_seq IS NOT NULL` and `kind = 'object'`, ordered by `ckey COLLATE "C"` with `starts_with` as the prefix bound and `$3::text IS NULL OR ckey > $3` as the exclusive cursor. A chunk row without a live head is residue, not an object; a bucket holding only residue is empty. The "row" here is the **content head**, not `object_meta`: the content suite publishes without metadata rows, and the content plane must answer for what it published. PG23's requirement (residue is not listed) holds; its suggested source ("a join, or the metadata plane") is not available to the content contract alone.

### PG24 - `STORAGE EXTERNAL`

**Pinned**: `ALTER TABLE content ALTER COLUMN chunk SET STORAGE EXTERNAL` and the same for `blocks.chunk`. The gate's ranged read over an 8 MiB object (1 000 003..=3 500 000) reads only the chunks that window touches.

## 4. The findings

Findings F1-F3 are shape findings: the spike resolved them in its own schema (D24's "shape problems are the executor's" applies to the contracts; these three are the spike's schema, and two of them are corrections to the pg design's section 3 wording for P3). F4-F8 are implementation findings. F9 is the environment pin. F10-F12 are scope statements (F11 is an explicit deferral to P3, not an open question). F13-F15 are defects the gate itself found in the spike's SQL, its paging and its error projection. F14 is a suite defect. F16 is the contract gap the review's I1 exposed (a same-key move). F17 is a deferred performance item: the uploads listing path materializes the whole bucket in both implementations, recorded for the P3 promotion rather than fixed here. F18 is the `put_version` rule set the version leg's fourth failure exposed: the same-sequence refusal, the stored-stamp authority and the one-null-version displacement are all the shared contract's (`crates/tinio-core/src/meta/version.rs`), all enforced by the redb implementation, and all missing from the spike's unconditional upsert. F19 is the three further rule-level divergences the rule-versus-implementation audit found **after** F18 -- the `ensure_tags` identical-set no-write, the `advance_slot` monotonic guard and the `expire`/`expire_many` index-entry rule -- each a shared-contract rule the redb implementation enforces and the spike did not, and none exercised by the shared suite. Section 4.1 lists the four suite fixtures the gate caught; 4.2 lists the review round's remaining items and their dispositions.

| # | Finding | Kind | Resolution |
|---|---|---|---|
| F1 | The live content generation has no home in `object_meta`: the content contract must serve bytes published with no metadata row. | shape (schema) | The marker lives in `content_heads.live_seq`. PG18's requirement holds; the pg design's section 3 table row moves the marker to the content relation. |
| F2 | A part chunk must be addressable without ever colliding with an object key, and without being enumerable. | shape (schema) | `ckey = "p:" + hex(upload_id) + ":" + number`, `kind` in the primary key, `scan` filters `kind='object'` and the `"o:"` prefix. |
| F3 | The key's current slot cannot ride `object_meta`: the contract's object row carries no version columns (D37), and the suite advances a slot for a key with no object row. | shape (schema) | Its own relation `current_slots(bucket, key, version_id, seq)`; `current_slot` reads the slot and LEFT JOINs the version row for the delete-marker flag. |
| F4 | `remove_if_empty` cannot be a probe read followed by a delete: two statements let a concurrent writer land between them, and the suite itself called it twice expecting `Ok` on both. | shape/suite | One statement: a data-modifying CTE deletes only when the bucket row exists and no object/upload row does, and returns both `present` and `removed`, which the method projects to `NoSuchBucket` / `NotEmpty`. The suite's second call was corrected to `BucketMeta::remove` (its own line-1379 assertion already required `NoSuchBucket` for a missing row). |
| F5 | The publish boundary must be bounded and an interrupted publish must be invisible. | implementation (PG22) | Batches of 4 chunks under the live-generation marker; the flip is one guarded `UPDATE`. |
| F6 | `scan` must not enumerate residue (chunk rows with no head), and the content plane has no metadata plane to join. | implementation (PG23) | Row-backed over live `content_heads`. |
| F7 | `put_guarded` must refuse when the bucket row is absent, atomically with the upsert and the retained-part clear. | implementation | One data-modifying CTE: `guarded` (insert-select-with-exists + open conflict) and `cleared` (delete parts when `guarded` produced a row), then `SELECT EXISTS (SELECT 1 FROM guarded)`; false means `NoSuchBucket`. |
| F8 | Two suite fixtures cannot pass against any store: `OwnerId::new("conf-owner")` is domain-invalid (only 64 lowercase hex digits are admitted), and `set_cors(&bucket, &cors::Config::default())` followed by `decoded.cors.is_some()` is unrepresentable (the contract's own `decode_cors` maps a rule-less configuration to `None`). | suite defects (found by the gate) | Reported to Task 1; the owner fixture needs a canonical id and the CORS leg needs a configuration with at least one rule. |
| F9 | On this musl image the database's `en_US.utf8` collation orders bytes exactly as `"C"` does (musl ships no locale data), so a collation test that only compares the two would be vacuous. | environment | The gate orders the same four values under `COLLATE "C"` (`0,B,_,a`) and under an ICU collation (`_,0,a,B`), proving the pin is load-bearing; every ordered statement repeats `COLLATE "C"` regardless of the server default. |
| F10 | `BlockStore`'s `len`/`published` behaviour is not exercised by this spike (D43 puts the block legs in P3/P5). | scope | The schema pins the columns and the completeness rule; `PgBlocks` is P3's. |
| F11 | PG21's multipart completion is not implemented and is explicitly **deferred to P3** (review item 39 owns the form and the leg; no D23 call reaches completion). | scope (deferred, stated plainly) | The generation mechanism the completion must use is pinned and green (`publish`'s batch-then-flip, `copy`'s chunk copy); P3 owns the completion form, its leg and the choice between the two shapes. |
| F12 | The `>100 MiB` streaming leg is P5's (D43); the spike's own load is 8 MiB. | scope | The gate reports the publish plan for 8 MiB (8 chunks, 2 batch statements) so the batching arithmetic is visible. |
| F14 | The 8 MiB leg requests `ByteRange::Inclusive(start, start + 4096)` - 4097 bytes under `range.rs`'s documented inclusive `A..=B` - and compares it to the 4096-byte slice `expected[start..start + 4096]`, so any conforming store fails on length. | suite defect (found by the gate; the gate measured 4097 vs 4096 bytes) | Reported to Task 1: either the request becomes `Inclusive(start, start + 4095)` or the expectation becomes `expected[start..start + 4097]`. |
| F13 | A statement whose bound parameter list is longer than its placeholder list fails: PostgreSQL cannot infer the type of the unused placeholder (`42P18 could not determine data type of parameter $2`). | implementation | Every statement binds exactly its own placeholders; `move_`'s destination deletes were rebound to `$1, $2`. |
| F15 | `list_uploads_page` returned a continuation marker for a page that was complete, and `Error::Database` rendered a server error as the bare "db error". | implementation | The marker is `None` unless the page is truncated; `Error::Database` carries the server's message, SQLSTATE, detail, constraint and table (still userinfo-redacted). |
| F16 | `move_` destroys the object on a same-key move: the destination delete removes the very row and its retained parts, the `UPDATE` then matches nothing, and the call still reports success. The contract is silent on self-move (review I1, lead ruling R14). | implementation + contract gap | Guarded: `if from == to { return Ok(()) }`, mirroring the content plane's `rename`/`copy`. **Contract sentence implied** (routed to Task 8's re-baseline): "A move whose source and destination keys are equal is a no-op: it neither deletes nor rewrites the row or its retained parts." |
| F17 | The uploads listing path materializes the **whole bucket** before paginating: the spike by a `SELECT` with no `LIMIT`, and `crates/tinio-store-redb` by reading every `uploads` row of the bucket -- both then hand the materialized set to the shared `group_and_paginate_unordered` engine. | implementation (perf; **deferred to P3**) | Recorded, not fixed. Bounding it needs a cursor-style change in the shared pager (`crates/tinio-core/src/storage/listing.rs`), and changing only one of the two implementations would make them diverge at the delimiter-rollup boundary (a rolled-up group is only known once its whole key range has been seen, so a bound applied before the rollup would split a group across pages). The promotion into `crates/tinio-store-pg` (D25/D28) owns the bounded shape for both. |
| F18 | `put_version` was an unconditional upsert (`INSERT ... ON CONFLICT (bucket, key, seq) DO UPDATE`), so it violated **three** rules the shared `VersionMeta::put_version` doc pins (`crates/tinio-core/src/meta/version.rs`): it overwrote another version's committed row at the same sequence; it took `EXCLUDED.became_noncurrent` over the stored stamp; and it left a second `null`-id row unaddressable. The version leg's fourth failure caught the first at `crates/tinio-util/src/testing/metadata.rs:465` ("a row naming different content at the same sequence is refused"); the null-displacement assertion immediately after it (`metadata.rs` ~490-514) would have caught the third. | implementation (contract rule; found by the gate, fixed) | One transaction, still, now with a `SELECT id, blob, size, kind, became_noncurrent FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = $3 FOR UPDATE` at its head (the `put_preserving` read-lock idiom). A stored row whose id, kind, blob or size differs from the incoming row is refused with `Error::VersionConflict { bucket, key, seq }` -- the crate's typed-refusal shape, projecting into `storage::Error` the way its other refusals do -- and the transaction rolls back on the `tx` drop, so the refused row is left untouched. An equal row is the crash-window adoption path: it is rewritten with the **stored** `became_noncurrent` carried over, and a sequence with no row stores the caller's. When the committed identity is the null version, a data-modifying CTE then deletes any other `null`-id row of the key together with the index entry that pointed at it, in the same statement. The row and its `version_index` entry still commit together. |
| F19 | Three rule-level divergences remained **after** F18 -- each a **documented rule** the shared contract carries, the redb implementation enforces, and the spike did not implement: (1) `ObjectMeta::ensure_tags` issued an **unconditional** `UPDATE`, so an **identical** tag set was rewritten, where the contract says replacing an identical set happens **without a write** and redb's `write_if(changed)` commits nothing; (2) `VersionMeta::advance_slot` upserted the slot with **no monotonic guard**, so a **lower** sequence could move a slot **backwards**, where the contract refuses a lower sequence (an equal one is the crash window's own re-run) and the redb implementation gained that guard this week; (3) `VersionMeta::expire`/`expire_many` deleted the `version_index` entry **by identity alone**, so a stale entry belonging to a **newer** sequence could be dropped, where the contract and redb's `remove_pointing_at` remove it **only when it still points at the sequence being expired**. | implementation (contract rules; found by the rule-versus-implementation **audit**, not by a failure) | All three fixed in `spike/pg-meta/src/lib.rs`. (1) `ensure_tags`'s `UPDATE` gained `AND tags IS DISTINCT FROM $3` (the same no-write arm the bucket twins `set_tags_or_create`/`set_grants_or_create` carry), so an identical set rewrites no row and commits nothing; the answer stays `Ok(())`. (2) `advance_slot` reads `SELECT seq FROM current_slots WHERE bucket = $1 AND key = $2 FOR UPDATE` inside its transaction and refuses a lower sequence with the new typed `Error::SlotRewind { bucket, key, seq }` (the `tx` drop leaves the slot untouched); an equal sequence still advances, and the delete-marker bit stays derived by `current_slot`'s LEFT JOIN, never carried by the caller. (3) both expire paths delete the index entry with `AND seq = $4` bound to the sequence being expired -- `expire` binds its own `$3`, `expire_many` now `RETURNING id, seq` and binds each row's returned sequence. The shared suite exercises none of the three (`crates/tinio-util/src/testing/metadata.rs`): its slot only advances forward (1, 2, 3, 9), it never re-commits an identical tag set (`ensure_tags` is called once), and its expiry never leaves a newer row sharing the expired identity's wire -- so the gate's verdict was green or red independently of them, and the audit, not a run, is what found them. The third is a **defensive** fix, not a live bug: the audit judged the old behaviour safe *today* only because the now-enforced one-null-version rule (F18) leaves the index naming one row per identity wire. |

### 4.1 The four suite defects the gate found

All four are evidence for the spike's value: none was visible to Task 1's unit-level review, because none is a compile error and the mem/fs suite runs do not reach every fixture.

1. **`remove_if_empty` called twice** (`crates/tinio-util/src/testing/metadata.rs`): the success leg removed the bucket, then called `remove_if_empty` a second time and unwrapped the result - but the leg's own assertion (and the trait doc) says a missing row is `NoSuchBucket`. Corrected to `BucketMeta::remove` for the second call.
2. **The owner fixture** (`metadata.rs` line ~88): `OwnerId::new("conf-owner").expect("a valid owner id")`. `OwnerId::new` admits exactly 64 lowercase hex digits (or the anonymous canonical id), so the value panics and every ACL leg aborts. This is a *contract-vs-suite* mismatch the gate found on its first run; Task 1 owns the fixture.
3. **The CORS fixture expects the unrepresentable** (`metadata.rs` ~714-736, reached once the owner fixture was fixed): the leg records `cors::Config::default()` and then requires `row().cors.is_some()`. A default configuration has no rules, its wire is the empty string, and `bucket_row::decode_cors` (`crates/tinio-core/src/meta/bucket.rs`:155) maps "empty, corrupt or a configuration with no rules" to `None`. **No store can satisfy this assertion** - it needs a rule-bearing config. Task 1's fixer replaced it with a one-rule config and a field-for-field round-trip assertion, which is stronger than the original `is_some`.
4. **The 8 MiB window is off by one** (`metadata.rs` ~651-663): the leg asks for `ByteRange::Inclusive(start, start + 4096)`, which `ByteRange::resolve` (`crates/tinio-core/src/storage/range.rs`:26, "the inclusive range A..=B") answers with 4097 bytes, then compares that against the 4096-byte slice `expected[start..start + 4096]`. The gate measured the two sides (4097 vs 4096 bytes, the extra trailing byte being the object's 69639th). This is the fourth fixture the spike's gate caught and Task 1's unit-level review could not: the earlier `Inclusive(1, 3)` leg passes only because its expectation is likewise 3 bytes.

All four were corrected by Task 1's fixer; the gate then ran **8/8 legs green, exit 0** (the output in 1.2). The store needed no change for any of them: three were fixtures that no backend could satisfy and one was my own suite leg's second `remove_if_empty` call. Task 1's round-3 contract edits - the restored delete-marker assertion and the now-explicit no-row rule for `current_slot` - also pass against this store unchanged, which is why the final run is green on the settled suite. **Superseded (corrected 2026-10-07)**: that first run predated the contract's `list_parts_page`, `BucketMeta::set_tags_or_create`, `BucketMeta::set_grants_or_create` and `UploadMeta::drain_upload_checked` additions and covered none of them (see the warning at the top), and the crate then did not compile as committed. The 2026-10-07 run recorded in 1.2 re-established the verdict - all eight legs passed, this section's four corrections among them.

### 4.2 The review round's other items, and what happened to them

| Item | Disposition |
|---|---|
| I1 `move_` self-move destroys the object | Fixed (F16): a same-key move returns before any statement runs. |
| I2 the delete-marker slot assertion was dropped | Restored by Task 1's round-3 edit (metadata.rs:292-300); the settled suite passes unchanged, including the new dangling-slot leg. |
| M1 `put_version` could leave a stale index entry | Fixed: the same transaction deletes any `version_index` row for `(bucket, key, seq)` whose id differs from the new one, so the row and its id index cannot disagree. |
| M2 `remove_bucket` was check-then-delete | Fixed: one statement - `WITH empty AS (SELECT NOT EXISTS (... live_seq IS NOT NULL ...))`, both deletes gated on `(SELECT ok FROM empty)`, the outer `SELECT ok` carrying the refusal. No window between the check and the delete. |
| M3 `move_`'s source probe is a plain read | Not changed: one `Mutex<Client>` serializes every method, so nothing can land between the probe and the writes. P3's pooled store must make the whole move one transaction with the row locked - recorded here for P3. |
| M4 `open` could panic on a chunk gap | Fixed: the rows must arrive as a contiguous `first..=last` run and the slice is bounds-checked; a gap is `Error::Corrupt { bucket, key }`, never an index panic. |
| M5 the publish-plan line is a shared computation | PG22's wording now says "declared and reported", not "measured". |
| M6 the suite's window does not cross a store chunk | True of that suite leg (both ends fall in the first 1 MiB chunk). The spike's own leg 6 reads `1 000 003..=3 500 000`, which crosses two 1 MiB boundaries. |
| M7 the shape-probe comment in `crates/tinio-core/src/meta/mod.rs` still overstates | Not mine to edit (tinio-core is fenced by the Lead); reported to the Lead. |
| M8 residue after a failed run | Disclosed in the report; the spike's own legs clean up on success and tolerate residue on the next run. |

## 5. The version-row walkthrough (Step 6)

Method, the SQL shape it compiles to, and the property it satisfies. Every statement binds parameters only; every ordered read repeats `COLLATE "C"`.

| Method | SQL shape | Property |
|---|---|---|
| `BucketMeta::created_at` | one `SELECT created FROM buckets WHERE name = $1` | one statement per point read |
| `BucketMeta::load_many` | one `SELECT name, created ... WHERE name = ANY($1)` | one statement per listing page, order preserved by the caller's input |
| `BucketMeta::load_all` | one `SELECT ... ORDER BY name COLLATE "C"` | byte-ordered traversal, no per-row read |
| `BucketMeta::row` | one `SELECT created, tags, owner, acl, cors`; decode with `Row::from_raw` | a corrupt element self-heals to empty/default, the row is served |
| `BucketMeta::owners` | one `SELECT name, owner ... WHERE name = ANY($1)` | strict `decode_owner` (a domain-invalid owner self-heals to `None`) |
| `BucketMeta::record` | one transaction: upsert `created`, clear the recorded elements | record-only write, one transaction |
| `BucketMeta::create` | one transaction: `INSERT ... ON CONFLICT DO NOTHING`, 0 rows means `AlreadyExists` | refusal is atomic with the insert (no read-then-write race) |
| `BucketMeta::record_if_absent` | one transaction: `INSERT ... DO NOTHING` then `SELECT created` | returns the effective creation time either way |
| `BucketMeta::record_full` | one transaction: `INSERT ... ON CONFLICT DO UPDATE` | full row upsert |
| `BucketMeta::set_tags` / `clear_tags` | one `UPDATE ... WHERE name = $1` | `rows == 0` is "missing row", one statement |
| `BucketMeta::set_tags_or_create` | one transaction: `INSERT INTO buckets (name, created, tags, owner, acl, cors) VALUES ($1, $2, $3, '', '', '') ON CONFLICT (name) DO UPDATE SET tags = EXCLUDED.tags WHERE buckets.tags IS DISTINCT FROM EXCLUDED.tags` - the insert arm seeds `created = $2` with the tag wire and every other element empty | the create-or-update twin of `set_tags` in ONE transaction: a first-sight row never reads without its tags, an existing row's creation time, owner, ACL and CORS ride untouched, and an identical wire writes nothing (the redb table's documented no-write arm) |
| `BucketMeta::set_acl` | one `UPDATE ... SET owner = $2, acl = $3` | owner wire + grants wire in one write |
| `BucketMeta::set_grants_or_create` | one transaction: `INSERT INTO buckets (name, created, tags, owner, acl, cors) VALUES ($1, $2, '', '', $3, '') ON CONFLICT (name) DO UPDATE SET acl = EXCLUDED.acl WHERE buckets.acl IS DISTINCT FROM EXCLUDED.acl` - the insert arm seeds `created = $2` with the grants wire and every other element empty | the create-or-update twin of `set_acl` for a grants-only caller: only the ACL column is in the conflict arm's `SET`, so the recorded owner is carried over with no read to race and is preserved with the creation time, tags and CORS; a first-sight row carries the grants and no owner |
| `BucketMeta::set_cors` / `clear_cors` | one `UPDATE ... SET cors = $2` | same presence answer |
| `BucketMeta::remove` | one transaction: `DELETE FROM buckets WHERE name = $1` | idempotent, one transaction |
| `BucketMeta::remove_if_empty` | ONE statement: a data-modifying CTE (delete-when-empty) returning `present` and `removed` | the probe and the delete cannot be raced (F4) |
| `ObjectMeta::get` | one `SELECT ... WHERE bucket = $1 AND key = $2` | point read; a domain-invalid etag reads `None` (self-heal) |
| `ObjectMeta::put` | one transaction: upsert the row then `DELETE FROM object_parts` | an overwrite clears the retained parts (review item 5) |
| `ObjectMeta::put_guarded` | ONE data-modifying CTE: insert-select-where-bucket-exists, conflicts updated, parts cleared when the insert produced a row | the `NoSuchBucket` refusal is atomic with the write (F7) |
| `ObjectMeta::put_with_parts` | one transaction: upsert, clear parts, one `INSERT ... FROM unnest($3::int[], $4::bigint[], $5::text[], $6::text[])` | the row and its whole part list commit together, bounded statement count |
| `ObjectMeta::parts` | one `SELECT ... ORDER BY number` | numeric part order (property: ascending part number) |
| `ObjectMeta::put_preserving` | one transaction: `SELECT ... FOR UPDATE` then `UPDATE` of the derived columns (or an `INSERT` when absent) | content-derived elements refreshed, recorded elements carried over, no lost update |
| `ObjectMeta::move_` | a same-key move returns at once (F16); otherwise one transaction: exists-check, delete destination row and parts, `UPDATE object_meta SET key`, `UPDATE object_parts SET key` | the row and its part list move together; a missing source is a no-op; a same-key move is a no-op |
| `ObjectMeta::set_tags` / `clear_tags` | one `UPDATE ... SET tags = $3` | presence answer (`false` = missing row); one statement |
| `ObjectMeta::ensure_tags` | one `UPDATE ... SET tags = $3 WHERE bucket = $1 AND key = $2 AND tags IS DISTINCT FROM $3` | presence answer only (`Ok(())`); an **identical** set rewrites no row and commits nothing (redb's `write_if(changed)` no-write arm, and the shared contract's rule); creates no row for a missing key |
| `ObjectMeta::set_acl` | one `UPDATE ... SET owner = $3, acl = $4` | same |
| `ObjectMeta::put_batch` | one transaction, one upsert + one part-clear per input row | all-or-nothing, input order |
| `ObjectMeta::remove` | one transaction: delete row and parts | one transaction per mutating method |
| `ObjectMeta::remove_many` | one transaction: `DELETE ... WHERE key = ANY($2)` twice (row, parts) | idempotent, one transaction |
| `ObjectMeta::load` | one `SELECT ... WHERE key = ANY($2)` | one call per listing page, input order restored |
| `ObjectMeta::walk` | one `SELECT ... ORDER BY key COLLATE "C"`; a `None` decode raises `Corrupt` | strict traversal fails on a corrupt row |
| `ObjectMeta::walk_healing` | the same read; a `None` decode deletes the row (a failed delete is logged) and yields `None` | the traversal never fails; a corrupt key is reported and healed |
| `ObjectMeta::drain_bucket` | one transaction: delete every row and every part row of the bucket | idempotent |
| `UploadMeta::create` | one transaction: upsert `uploads`, replace `upload_checksums` | the row and its checksum cannot disagree |
| `UploadMeta::get` | one `SELECT` with `LEFT JOIN upload_checksums`; the stored key is compared before decoding | a foreign upload reads `None`; a corrupt row self-heals to `None` |
| `UploadMeta::put_part` | one transaction: key check, upsert `parts`, replace `part_checksums` | one transaction per part write; a key mismatch writes nothing |
| `UploadMeta::list_parts` | one `SELECT ... LEFT JOIN part_checksums ... WHERE number >= $3 ORDER BY number` | ascending part order, from a lower bound |
| `UploadMeta::list_parts_page` | one `SELECT p.number, p.etag, c.algorithm, c.value ... LEFT JOIN part_checksums ... WHERE bucket = $1 AND upload_id = $2 AND number >= $3 ORDER BY p.number LIMIT $4` (`$4 = max + 1`, the one-lookahead); the truncation flag and the raw last number are taken from the raw rows before the row join | one page from a raw `u32` lower bound at O(page) rows, plus a resume marker for a page the self-heal emptied; a domain-invalid stored ETag is dropped and a domain-invalid checksum self-heals to `None`; `max == 0` answers an empty page and `false` |
| `UploadMeta::part_records` | one key check, then one `SELECT ... WHERE number = ANY($3)` | the completion join: one record per requested number, input order, missing numbers `None` |
| `UploadMeta::list_uploads` | one `SELECT` (left join) `ORDER BY key COLLATE "C", upload_id COLLATE "C"` | byte order |
| `UploadMeta::list_uploads_page` | one `SELECT ... starts_with(key, $2)` (prefix-bound only -- no marker predicate and no `ORDER BY`); the rows feed the shared `group_and_paginate_unordered` engine | the exclusive cursor, the delimiter roll-up and the `max_uploads` cap are the shared engine's (the one home, shared with the mem and fs backends), not the statement's; the whole bucket is materialized per page (F17) |
| `UploadMeta::has_uploads` | one `SELECT EXISTS (SELECT 1 FROM uploads WHERE bucket = $1)` | the empty-bucket probe is one statement |
| `UploadMeta::walk_uploads` | one `SELECT ... ORDER BY bucket, key, upload_id (all COLLATE "C")` | the orphan sweep's cross-bucket feed |
| `UploadMeta::drain_upload` / `drain_bucket` | one transaction: parts, part checksums, upload checksums, then the rows | idempotent |
| `UploadMeta::drain_upload_checked` | ONE statement: a data-modifying CTE - `matched` (`SELECT 1 FROM uploads WHERE bucket = $1 AND upload_id = $2 AND key = $3`), then the `parts`, `part_checksums`, `upload_checksums` and `uploads` deletes each gated `AND EXISTS (SELECT 1 FROM matched)`, and the outer `SELECT EXISTS (SELECT 1 FROM matched)` carrying the refusal | the abort path's identity check and drain share one snapshot and one statement, so a racing complete cannot slip between them; an upload with no row, or one recorded under another key, drains nothing and is `NoSuchUpload`, never an empty success (D22) |
| `VersionMeta::bucket_state` | one `SELECT state FROM bucket_versioning`; no row or an unknown wire is `Unversioned` | the state is the bucket row's fact |
| `VersionMeta::set_bucket_state` | one `INSERT ... ON CONFLICT DO UPDATE` | idempotent state write |
| `VersionMeta::current_slot` | one `SELECT s.version_id, r.kind FROM current_slots s LEFT JOIN version_rows r ON r.seq = s.seq` | the raw slot fact: `None` absent, null version, marker, or a version id (F3) |
| `VersionMeta::advance_slot` | one transaction: `SELECT seq FROM current_slots WHERE bucket = $1 AND key = $2 FOR UPDATE`, refuse a **lower** sequence (`Error::SlotRewind`), then upsert `current_slots(bucket, key, version_id, seq)` | the only writer of a key's slot; a slot never moves backwards (an equal sequence is the crash-window re-run and advances) -- the shared contract's rule; the delete-marker bit is resolved by `current_slot`'s LEFT JOIN, never carried by the caller |
| `VersionMeta::put_version` | one transaction: `SELECT id, blob, size, kind, became_noncurrent ... WHERE bucket, key, seq FOR UPDATE`, then refuse a stored row naming a different version (`Error::VersionConflict`), carry the stored stamp over for an equal row (the caller's stamp for a free sequence), displace any other null-id row with its index entry (a data-modifying CTE) when the identity is the null version, then upsert `version_rows`, delete any `version_index` row for the same `(bucket, key, seq)` carrying another id, upsert `version_index` | the row and its id index cannot disagree (M1); a row another version occupies is refused, never overwritten; a stored stamp is never un-stamped by its own rewrite; a key holds at most one null-id row (F18) |
| `VersionMeta::get_version` | one `SELECT ... WHERE bucket, key, seq` | one statement; a corrupt row reads `None` |
| `VersionMeta::latest` | one `SELECT ... WHERE bucket = $1 AND key = $2 ORDER BY seq DESC LIMIT 1` | the newest row by sequence; a corrupt newest row reads absent, never an older version |
| `VersionMeta::bucket_rows` | one `SELECT ... WHERE bucket = $1 ORDER BY seq`, decoded per row | the bucket-wide feed in sequence order, per-row self-heal (a corrupt row is skipped, not failed) |
| `VersionMeta::list_versions` | one `SELECT ... ORDER BY seq DESC` | newest first (sequence order, not time order) |
| `VersionMeta::expire` | one transaction: `DELETE FROM version_rows ... RETURNING id` then `DELETE FROM version_index WHERE bucket = $1 AND key = $2 AND id = $3 AND seq = $4` (the expiring sequence) | idempotent, one transaction; the index entry goes only when it still points at this sequence -- the shared contract's **its index entry** rule (`VersionMeta::expire`), redb's `remove_pointing_at` |
| `VersionMeta::expire_many` | one transaction: `DELETE FROM version_rows ... WHERE seq = ANY($3) RETURNING id, seq`, then for each returned row `DELETE FROM version_index WHERE bucket = $1 AND key = $2 AND id = $3 AND seq = $4` (the row's returned sequence); an empty input is a no-op | idempotent batch expiry in one transaction (the sweep's crash point stays one per sweep, never one per version); each index entry goes only when it still points at the sequence being expired -- the shared contract's rule, redb's `remove_pointing_at` |
| `VersionMeta::version_seq_of` | one `SELECT seq FROM version_index WHERE bucket, key, id = $3` (`wire_of(id)`) | the point index read; the null version's wire is `"null"` |
| `ObjectContent::stage` | no statement: the staged write buffers in memory (D38's keyed staging) | staging is the implementation's own |
| `ObjectContent::publish` | `INSERT ... ON CONFLICT ... RETURNING next_seq`, then `ceil(n/4)` batch inserts, then `UPDATE content_heads SET live_seq = $4, live_len = $5 WHERE ... AND next_seq = $4` | the visibility point: one flip, invisible until then, bounded statement count (PG18/PG22) |
| `ObjectContent::open` | one head read, then one `SELECT chunk_no, chunk ... WHERE chunk_no BETWEEN $5 AND $6 ORDER BY chunk_no`; the rows must form a contiguous `first..=last` run, then the first and last chunks are sliced | a ranged read touches only its chunks (PG2/PG24); a missing key is an error, never an empty stream; a gap in the run is `Error::Corrupt`, never an index panic (M4) |
| `ObjectContent::remove` | one transaction: delete the chunks and the head | idempotent |
| `ObjectContent::rename` | one transaction: source head check, clear the destination, `UPDATE content SET ckey`, `UPDATE content_heads SET ckey` | the bytes and the head move together; a missing source is `NoSuchKey` |
| `ObjectContent::copy` | one transaction: read the source head, clear the destination, allocate a generation, `INSERT ... SELECT ... FROM content`, flip | same-bucket object-to-object copy under a fresh generation; the source generation is never shared |
| `ObjectContent::scan` | one `SELECT ckey, live_len FROM content_heads WHERE kind = 'object' AND live_seq IS NOT NULL AND starts_with(ckey, $2) AND ($3 IS NULL OR ckey > $3) ORDER BY ckey COLLATE "C" LIMIT $4` | byte-ordered page, object keys only, residue excluded (PG20/PG23) |
| `ObjectContent::stat` | one `SELECT live_len FROM content_heads WHERE ... live_seq IS NOT NULL` | the cheap probe: no rehash, no metadata dependency |
| `ObjectContent::create_bucket` | no statement (a content namespace with no directory) | the metadata plane's bucket row is `BucketMeta::create` |
| `ObjectContent::remove_bucket` | ONE statement: `WITH empty AS (SELECT NOT EXISTS (... live_seq IS NOT NULL ...))`, `del_heads`/`del_chunks` gated on it, the outer `SELECT ok` carrying the refusal (M2) | the emptiness decision and both deletes share one snapshot; a non-empty bucket is `NotEmpty`, and no writer can land between a check and the delete |
| `ObjectContent::is_empty` | one `SELECT NOT EXISTS (SELECT 1 FROM content_heads WHERE bucket = $1 AND live_seq IS NOT NULL)` | the content-side emptiness authority |

## 6. The collation and encoding pin

- **Encoding**: the connect path reads `current_setting('server_encoding')` and refuses anything but UTF-8 (`Error::Encoding`). `"C"` orders the bytes of the *server* encoding, so without this check the contract's UTF-8 byte order would be a coincidence (PG4).
- **Collation**: every column an ordered read compares is declared `text COLLATE "C"` and every ordered statement repeats the collation in its `ORDER BY`; discriminator and wire-only columns carry none and need none. The gate proves the pin is load-bearing against an ICU collation (F9): `"C"` orders `0,B,_,a`, `en-US-x-icu` orders `_,0,a,B`.
- **Server default**: `datcollate = en_US.utf8` on this image. It happens to agree with `"C"` because musl carries no `en_US.utf8` locale data; on a glibc or ICU-defaulted server it would not, which is why the schema does not rely on it either way.
- **UTF-8 storage**: the encoding check runs *before* the schema is applied, so a non-UTF-8 database is refused rather than migrated.

## 7. What the spike does not prove

- No `BlockStore` (F10), no multipart completion (F11), no `>100 MiB` leg (F12) - all named P3/P5 steps (D43).
- No pool, TLS, migrations, ownership lock, read-only role or cleanup: the spike is one serialized connection and a `batch_execute` of the schema (PG9-PG12, PG26 are P3's).
- No error-taxonomy parity with `storage::Error` beyond the projection the contract asks for: the spike's `Error` is deliberately small.
- Nothing outstanding blocks the gate: the four suite fixtures in 4.1 were corrected, the round-3 contract edits (the restored delete-marker assertion and the explicit no-row rule for `current_slot`) pass against this store unchanged, and the final run is 8/8 legs green. **The verdict was superseded and is now re-established (corrected 2026-10-07)**: the first run predated the contract's `list_parts_page` addition and the three later additions (`set_tags_or_create`, `set_grants_or_create`, `drain_upload_checked`), and the crate did not compile as committed (twice - see the warning at the top); the 2026-10-07 re-runs then reached leg 4 and failed there (F18, now fixed), and the third run - recorded in 1.2 - completed all eight legs green, so the verdict now covers the landed contract surface.
