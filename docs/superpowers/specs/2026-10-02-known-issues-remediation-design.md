# Design: known-issues remediation (audit run-2)

**Date**: 2026-10-02
**Status**: approved — design confirmed 2026-10-02 after five grilling rounds. One implementation plan. Not yet planned.
**Scope**: `tinio-auth` (new `source` module, `access.rs`), `tinio-server` (`backend/objects.rs`, `identity.rs`, `metrics.rs`), `tinio-cli` (`commands/start/serve.rs`), `tinio-select` (`engine.rs`, `row.rs`, `csv.rs`), `tinio-sandbox-linux` (`enter.rs`, `tests/jail.rs`). No config-schema change, no wire-format change, no new FR/SC ID; one deliberate startup-behaviour change (P2).

## Goal

1. One parse authority for `x-amz-copy-source` / `x-amz-rename-source`: the ACL authorizes the key the handler acts on (P1, P6).
2. An `[auth]`-less config never serves the built-in constant off loopback (P2).
3. Attacker-controlled Select evaluation cost and metric-label cardinality are bounded (P3, P4).
4. The Linux jail binds the validated object, not a re-resolved name (P5).

## Non-goals

- No change to `root_auth_pair`'s returned value.
- No generation/persist work on the existing-config-file path (the absent-file path already generates CSPRNG credentials).
- No Select SQL/serialization semantics change; no new S3 error code (the budget uses the existing `Error::Value` → `S3QueryError` path).
- No Windows/macOS launcher change; P5 is Linux-in-place.
- No access-point/outpost ARN support (rejected as today).
- No `--anonymous` flag revival.

## Shared symbols (one definer each)

| Symbol | Definer | Consumers |
|---|---|---|
| `source::copy_source`, `source::rename_source` | `crates/tinio-auth/src/source.rs` (new) | `AclAccess`, `op_copy_object`, `op_upload_part_copy`, `op_rename_object` |
| `identity::uses_builtin_fallback` | `crates/tinio-server/src/identity.rs` | `serve.rs` startup decision |
| `EVAL_CELL_CAP`, the per-request spent counter, the per-stream name index, the per-row JSON index | `crates/tinio-select/src/engine.rs` / `row.rs` | Select evaluation |
| `metrics::method_label` | `crates/tinio-server/src/metrics.rs` | `record_http_request` |
| `ExposureFd` | `crates/tinio-sandbox-linux/src/enter.rs` plan | `apply_bind`, teardown |

Loopback is `std::net::IpAddr::is_loopback()`, already the predicate in `serve.rs`'s `warn_if_non_loopback(address: SocketAddr)`.

## Evidence base

Full traces, reproductions and severity live in audit run-2 at `C:\Users\Admin\security-audit-skill\tinio\run-2\`. This spec owns the fix decisions; the audit owns the reproductions; the plan may quote an audit fingerprint, code comments do not.

| ID | Fingerprint | Root-cause anchor |
|---|---|---|
| P1 | `tinio.acl.copy-source-versionid-source-confusion-read-bypass` | `tinio-auth/src/access.rs:641` `parse_source` vs the s3s `CopySource` dto |
| P6 | `tinio.auth.access.rs.parse_source:tinio-server.backend.objects.rs.rename_source_key:RenameObject-source-gate-resource-mismatch` | `access.rs:641` vs `backend/objects.rs:1304` `rename_source_key` |
| P2 | `tinio.default-root-credential-minioadmin-fallback-when-auth-section-absent` | `tinio-server/src/identity.rs:41` |
| P3 | `tinio.select.engine.named-column-lookup-quadratic-scan` | `tinio-select/src/engine.rs:732,746`; per-call cap precedent `:1451,1498` |
| P4 | `tinio-server/metrics/http-method-label-unbounded-cardinality` | `tinio-server/src/metrics.rs:326` |
| P5 | `tinio-sandbox-linux/enter/exposure-path-revalidation` | `tinio-sandbox-linux/src/enter.rs:296,957` |

## Decisions (locked in grilling, 2026-10-02)

- **P1/P6 — s3s is the parse authority; the shared code lives in `tinio-auth`.** `tinio-auth` already depends on `s3s`, and `tinio-server` already depends on `tinio-auth`; a hand-mirrored parser in `tinio-core` would be a second authority. Accepted forms: `Bucket` only; `?versionId=` stripped and ignored; ARN forms rejected.
- **P1 — malformed source is an ACL denial (403), not a pre-auth shape gate.** No second pre-auth parse point; the op's own 400 is unchanged on the authorized path.
- **P2 — enforcement is runtime, in `serve.rs`;** the config layer does not learn credential semantics. No install-time check in `tinio service install` (that arm does not read the config file).
- **P3 — fix the mechanism, then budget the residual.** Per-stream name index for CSV/Parquet; a per-request budget charged only for possibly-superlinear work (the entries a JSON lookup scans, LIKE cells, and the headerless name walk).
- **P4 — whitelist six served verbs; everything else collapses to `OTHER`.**
- **P5 — descriptor-bound bind** (`open_tree` clone + `move_mount`, with a `/proc/self/fd` fallback), with the ancestor-symlink policy retained.
- **One plan**, no new FR/SC, no `CONTEXT.md` term, audit fingerprint only in spec/plan.

## Design

### §1 Source key (`tinio-auth/src/source.rs`) — P1, P6

`copy_source(raw) -> Option<(bucket::Name, object::Key)>` calls `s3s::dto::CopySource::parse(raw)`; on `CopySource::Bucket { bucket, key, .. }` it validates `bucket::name` / `object::key`; every ARN variant (and every malformed value) yields `None`. `version_id` is dropped (`versionId` is stripped by s3s and tinio has no versioning).

`rename_source(raw) -> Option<object::Key>` strips at most one leading `/` and validates the remainder with `object::key`; a malformed key yields `None`. The s3s dto keeps `rename_source` a raw `String`, so this is the one function both the ACL and `op_rename_object` call.

The handler's `copy_source` extraction (from the already-parsed dto) stays; it and the ACL now derive from the same authority. The local `rename_source_key` (`objects.rs:1304`) is deleted. Both call sites in `AclAccess` (`access.rs:212`, `:239`) fail closed with `AccessDenied` on `None`; `op_upload_part_copy` uses `copy_source` too.

### §2 Startup credential policy — P2

`identity::uses_builtin_fallback(config) -> bool` is `config.auth.is_none()`.

**Superseded 2026-10-03 (uncommitted change): the refusal is no longer raised in `serve.rs`.** The policy is decided once by `Resolution::credential_policy()` (`CredentialPolicy`, predicate `identity::uses_builtin_fallback`) and raised in `crates/tinio-cli/src/commands/start/run.rs` with the other static refusals — before the state directories, the daemon fork and the confinement — while `serve.rs` renders only the loopback warning (`CredentialPolicy::warning()`, still one `eprintln!` on stderr through `warn`). Kept as the record of the placement this section was written with: "In `serve.rs`, after `resolution.address()?` and before the bind: a non-loopback address (including the wildcard `0.0.0.0`) plus the fallback refuses to start with a config error naming `[auth]`, `TINIO_ACCESS_KEY`, `TINIO_SECRET_KEY`; a loopback address plus the fallback logs one `eprintln!` warning (same channel as `warn_if_non_loopback`). An explicit `[auth]` pair — from the file or the environment fold — is the escape. The existing non-loopback warning is unchanged for configured credentials."

### §3 Select evaluation bound — P3

Resolution: `Columns::new` is constructed per record (`csv.rs:246`), while the CSV/Parquet name vector is `Rc`-shared per stream (`names_header: Rc<Vec<String>>`). The `name → index` map is therefore built once per stream where the header is materialized and carried by the record's columns through an `Rc`; `csv_field` becomes a lookup. JSON keys differ per row (`serde_json::Map`) and `json_lookup` is called on nested maps too, so no single per-row index is correct; JSON keeps its existing search order and is bounded by the budget below, which charges the entries each lookup scans (`RowCtx`, `engine.rs:112`, carries the counter).

Budget: `EVAL_CELL_CAP = 10_000_000`, mirroring `LIKE_CELL_CAP` (`engine.rs:1451`, kept as the per-call guard). `Engine` is constructed once per request (`events.rs`), so the spent counter is `Engine` state (a `Cell<u64>`) charged only for possibly-superlinear work — the entries each `json_lookup` scans, `tokens × (text + 1)` per LIKE, and the headerless `is_positional_names` walk over the record's names. The O(1) indexed CSV/Parquet lookup and the per-record projection list are **not** charged, so an ordinary wide `SELECT` streams instead of aborting near the cap; the quadratic the finding names is removed by the index, not by the budget. **Superseded 2026-10-03 (uncommitted change): `project()` no longer checks anything per record.** This sentence read "`project()` checks the running total once per record and returns `Error::Value` on breach"; the zero-charge re-check (`charge(spent, 0)`) and its dedicated test are deleted, and nothing is left unenforced because every `charge` crossing `EVAL_CELL_CAP` already returns `Err` (`Error::Value`). The cap, the counter and the one message now live in `crates/tinio-select/src/budget.rs` (`Budget`, shared into every evaluation path as `RowCtx.budget`) rather than in `engine.rs`/`RowCtx` fields. The error surfaces where the LIKE cap and the engine-panic path already do: an in-stream `S3QueryError` inside the 200 response (`map_stream_error`), so a request can end with a partial result plus that error item.

### §4 HTTP method label — P4

`metrics::method_label(method) -> &'static str` maps `GET/PUT/POST/DELETE/HEAD/OPTIONS` to themselves and everything else to `OTHER`; `record_http_request` uses it for both the counter and the duration histogram. `record_s3_operation` labels are static codegen names and are unchanged. The existing metrics unit tests that label with `"FAM"` / `"INC"` move to a whitelisted verb or `OTHER`.

### §5 Linux jail exposure bind — P5

`validate_exposures` keeps `refuse_symlink_exposure` (the ancestor-component policy) and its pre-unshare position, and returns `Result<()>`; a separate `open_exposures` opens each exposure as a descriptor **after** `enter_mount_namespace()` — a mount is bindable only from the namespace that owns it, so a descriptor opened before the `unshare` would be refused with `EINVAL` (measured) — and holds the `Vec<ExposureFd>` for the run. The symlink policy rides on that open as well as on the startup walk: one `openat2` resolution with `RESOLVE_NO_SYMLINKS`, or on a kernel below 5.6 an `O_PATH | O_NOFOLLOW` open whose components are re-checked immediately after it. `apply_bind` binds the descriptor: `open_tree(fd, "", OPEN_TREE_CLONE | AT_EMPTY_PATH | AT_RECURSIVE)` clones the subtree, and `move_mount(clone, "", AT_FDCWD, target, MOVE_MOUNT_F_EMPTY_PATH)` lands it — `move_mount` requires a *mount* descriptor, which is why the clone is what it receives. On `ENOSYS` (kernel < 5.2) or when `TINIO_SANDBOX_FORCE_PROC_FALLBACK` is set, it binds the descriptor's `/proc/self/fd/<n>` link with `MS_BIND | MS_REC` instead. **The descriptor closes the substitution that tries to slip a link in after the policy has run, not every swap**: a rename *before* the open can still change which directory is exposed, and the `O_PATH` fallback carries its own open-to-re-check window, in which a link swapped in and restored is resolved by the open and invisible to the walk. Descriptors are closed after all binds and on every failure path. Read-only remount and the nested-writable-mount check are unchanged.

The force-the-fallback env var is read unconditionally, matching the existing `TINIO_SANDBOX_TEST_*` hooks; it selects between two descriptor-bound paths, so it grants no authority.

## Testing & acceptance

Placement follows `docs/tests.md`.

- P1/P6: inline unit tests for both functions; negative regressions in `crates/tinio-e2e/tests/features/conditions.feature` (bucket-WRITE-only rename with a bucket-prefixed header is denied; a `?versionId=` copy cannot read a private object).
- P2: `crates/tinio/tests/` — `[version = 1]` config + non-loopback refuses, loopback starts and warns, explicit `[auth]` non-loopback starts.
- P3: inline unit tests for the index and the budget; the growth cases extend `crates/tinio-select/benches/select_scan.rs`.
- P4: inline unit test in `metrics.rs` (two unknown methods → one `OTHER`; known verbs unchanged).
- P5: `crates/tinio-sandbox-linux/tests/jail.rs` gains a forced-fallback leg asserting the same confinement invariants as the `move_mount` leg.
- Gates: `cargo check --profile ci --workspace --all-targets`, `cargo clippy --profile ci --workspace --all-targets -- -D warnings`, and the touched crates' tests. Acceptance: each audit reproduction for P1/P2/P6 and the Select growth harness show the negative result gone and the positive control unchanged.

## Risks

- **P2 residual**: a loopback client still shares the public constant. Documented in `README.md` and the `[auth]` schema doc; the non-loopback refusal is the boundary.
- **P3 budget**: a request that exceeds the budget ends with a partial stream plus an `S3QueryError` item (same shape as the LIKE cap). Very large legitimate projections are refused rather than served slowly.
- **P4**: existing metrics tests must change with the label (a consequence, not a defect).
- **P5**: `/proc` must be mounted for the fallback path (`docs/sandbox.md` records it); the fallback is exercised in CI only via the forced hook, never on a real old kernel.

## Plan decomposition

One plan: `docs/superpowers/plans/2026-10-02-known-issues-remediation.md`, covering P1/P6, P2, P3, P4 and P5 in that order, each with its own tests.

## Verification

Per `docs/superpowers/README.md`: each symbol above has one definer; no plan restates another document's fact; the audit's records are referenced, not copied; the audit-run `REMEDIATION-PLAN.md` is superseded by this spec.
