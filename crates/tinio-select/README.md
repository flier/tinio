# `tinio-select` — the streaming SQL SELECT engine for S3 objects

`tinio-select` is the S3 Select engine: given a validated query plan, one request's input and output framing, and a blocking reader over the stored object's bytes, it yields that request's event sequence (`Records` / `Progress` / `Stats` / `Cont` / `End`) as a synchronous iterator. It implements the covered S3 Select contract — the SQL subset below, the CSV/JSON serialization vocabulary (input and output) plus Parquet input, the compression codecs, the ScanRange window and the record-boundary event framing — and it is deliberately ignorant of everything around it: the request-level validation, the mapping onto the protocol's events and errors, and the async bridge are `tinio-server`'s (`backend/select.rs`), while the object's bytes reach the caller through `tinio-core`'s read path. This crate depends on neither of those two.

The pipeline is synchronous and pull-driven — no tokio, no futures — so the caller must run it on a blocking thread, never a worker. Nothing here reads storage or resolves a request: a ScanRange is consumed already resolved, a Parquet object already buffered by the caller, and the size arithmetic that rejects an out-of-range window is the server's.

## Features

| Feature | Effect |
|---|---|
| `parquet` | Compiles the Parquet input reader and its dependencies (`parquet`, `arrow`, `bytes`, `time`). Off by default, so a deployment that serves no Parquet object carries no arrow at all; `tinio-server`'s own `select-parquet` feature forwards this one. |

## Supported SQL

`SELECT list FROM S3Object [AS alias] [WHERE expr] [LIMIT n]`, and nothing beyond it: the list is `*`, field references, and expressions built from comparisons, logical and arithmetic operators, `LIKE [ESCAPE]`, `IN`, `BETWEEN`, `IS NULL` and `IS [NOT] MISSING`. `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` are the only functions, and they run without `GROUP BY` — a query naming any other function is refused rather than approximated. An unquoted identifier folds case-insensitively, a `"double-quoted"` one is case-sensitive, and a case-insensitive match spanning several fields is an error, never a guess.

JSON input additionally accepts the traversal form `FROM S3Object[*].segments` with `.name`, `['name']`, `[index]`, `.*` and `[*]` steps, where a step that matched nothing yields a MISSING row — a state distinct from a present `null`. Numbers ride a 28-digit decimal spine, parsed lazily: `SELECT *` passes a field's text through untouched, and a value the spine cannot hold is an error rather than a silently rounded number.

## Input and output

| Item | What it serves |
|---|---|
| CSV input | `FileHeaderInfo` USE/IGNORE/NONE, single-byte field/record delimiters, quote and escape characters, `Comments`. Positional `_1.._N` columns always; header names when `USE`. `AllowQuotedRecordDelimiter` is carried but not enforced — the reader cannot tell a record delimiter inside quotes apart, so both values read permissively (the one documented deviation). |
| JSON input | `Type` LINES or DOCUMENT. A DOCUMENT is never buffered whole: each root value is one record, so an array of any length streams while a single root over the cap does not. |
| Parquet input | The `parquet` feature only, from an object the caller buffered; column pruning to the names the projection and predicate reference. |
| Output | CSV or JSON, one record at a time, values only (no header line). Never Parquet. |

Compression is GZIP or BZIP2, decoded as a **multi-member** stream read to EOF — what the `gzip`/`bzip2` command line emits — while `NONE` and an unset field are the same absence. Any other codec is refused at the parse, never silently read as uncompressed.

## Limits

Both sides of the stream are bounded, and every bound answers an error rather than a truncated or split result: an input record past 1 MB aborts the stream, and a serialized row past 1 MB errors instead of being split — a `Records` event packs whole records at record boundaries and never carries a partial one. Parquet input, buffered whole, is refused past `max_parquet_bytes` (256 MiB by default — the same ceiling `tinio-server` checks request-level). The SQL text is capped at 256 KiB (AWS's expression limit) and evaluation is charged against a per-request work budget, so an adversarial pattern costs bounded work instead of pinning the engine.

## Refusals

| Refused | Answers |
|---|---|
| `JOIN`, `UNION`, a subquery, `GROUP BY`, `HAVING`, `DISTINCT`, `ORDER BY`, `OFFSET`, `LIMIT BY`, `FETCH`, `TOP`, `WITH`, `QUALIFY`, `WINDOW`, `LATERAL VIEW`, `PREWHERE`, `CONNECT BY`, `CLUSTER BY`, `DISTRIBUTE BY`, `SORT BY` | `Unsupported` |
| a qualified wildcard (`s.*`), an alias column list, table-function arguments, a `JSON_TABLE` factor, `PARTITION`, `VERSION`, `WITH ORDINALITY`, `TABLESAMPLE`, index hints, `SETTINGS`, `FORMAT`, `PIPE`, `EXCLUDE`, a SELECT modifier, `INTO`, a FROM-first `SELECT` | `Unsupported` |
| every function outside the aggregate set, and every expression shape with no evaluator — `CAST`/`TRY_CAST`/`SAFE_CAST`, `CASE`, `ILIKE`, `IS DISTINCT FROM`, the scalar string and date functions (`SUBSTRING`, `TRIM`, `EXTRACT`, `CEIL`, `FLOOR`, `POSITION`) | `Unsupported` |
| a malformed query operand (an `ESCAPE` that is not one character, a literal outside the numeric model), a `LIMIT` that is not a positive integer | `Parse` |
| a case-insensitive name matching several fields | `Ambiguous` |
| a projected or filtered name no header carries, with `FileHeaderInfo=USE` | `MissingHeader` |
| a malformed record, a JSON root past 1 MB, a JSON document that will not parse | `Format`, `Json` |
| a serialized row past 1 MB, a Parquet nested value under CSV output | `TooLarge`, `NestedCsv` |
| a Parquet object past the memory bound | `ParquetTooLarge` |
| a numeric value past the 28-digit spine, a `LIKE` pair past its cell cap, an evaluation budget exhausted | `Value` |
| an I/O failure on the object stream | `Io` |
| an invariant the pipeline believed unreachable | `Invariant` |

`Parse` and `Unsupported` are the request-level verdicts, which `tinio-server` renders before it streams anything; every other row arrives as an item inside the 200 event stream (that mapping is the server's). `Ambiguous` and `MissingHeader` are decided from the data being read rather than from the statement — a case-folded name that spans several fields, or a referenced name the input's own naming does not carry — which is why they cannot be request-level. `Invariant` keeps its wire text generic on purpose: the broken invariant's name is for the log, never for the response body.

## Layout

| File | What it is |
|---|---|
| `src/sql.rs` | parse + validate into the plan, and the request-level verdicts |
| `src/dialect.rs`, `src/grammar/object_path.pest`, `src/path.rs` | the sqlparser dialect's two hooks (the custom `S3Object` FROM factor and the `IS [NOT] MISSING` syntax), the object-path grammar, and the segments a traversal walks |
| `src/engine.rs` | plan evaluation: WHERE, projection, aggregates, LIMIT |
| `src/row.rs` | the value/field/record shapes, the name index and its ambiguity rules, the numeric spine |
| `src/record.rs` | the `RecordReader` contract, the compression decoders, the scan window |
| `src/csv.rs` | the CSV reader, its options and its capped read-ahead |
| `src/json/**` | the JSON readers: LINES and DOCUMENT, the path matcher, the streaming kernel's contract and its error vocabulary |
| `src/parquet.rs` | the Parquet reader and its bounded decode (feature `parquet`) |
| `src/output.rs` | the CSV and JSON serializers |
| `src/events.rs` | `SelectConfig`, `SelectEvent` and `select_iter`: the pipeline pull, the record-boundary event packing, the byte counters |
| `src/budget.rs` | the per-request evaluation budget |
| `src/error.rs` | the crate error and its request-level / in-stream split |
| `benches/select_scan.rs` | the criterion scan bench (CSV full vs filtered, the JSON LINES/DOCUMENT pair) |

## Gates

```console
cargo test -p tinio-select
```

Parquet is a distinct build, and so is the test set it adds:

```console
cargo test -p tinio-select --features parquet
```

The large-DOCUMENT case reads `tests/fixtures/large/document.json.gz`, a git-LFS object, so that checkout must have run `git lfs pull`. Two `#[ignore]`d tests regenerate the committed fixtures (the Parquet file, that document) — pass `-- --ignored` deliberately, never as part of a gate.

```console
cargo bench -p tinio-select
```
