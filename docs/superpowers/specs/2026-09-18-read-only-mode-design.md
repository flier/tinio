# Design: read-only mode (FR-023) + the shared S3 operation table

**Date**: 2026-09-18
**Status**: draft — awaiting review
**Scope**: `tinio-core` (`AccessMode`, `CleanupOptions`), `tinio-config` (state-dir resolution, `TINIO_READ_ONLY`), `tinio-fs` (read-only startup gating), `tinio-server` (the shared operation table, the `MetricS3` refactor, the capability migration, `S3ReadonlyBackend`, `PlaneConfig`), the `serve` example, tests, bookkeeping. Not touched: the CLI (T066/T067), the management plane, `doctor`, credential generation, config auto-create and discovery.
**Spec**: `specs/001-s3-local-server/spec.md` FR-023 — the authoritative contract; this document is its implementation design.
**Tasks**: T058 (end-to-end test), T076 (state relocation), T077 (write enforcement).

## Background

FR-023 is fully specified and entirely unimplemented. What exists today:

- `[server] read_only` parses (`crates/tinio-config/src/schema/server.rs:30`) and is read by nobody.
- `FsOptions.state_dir: Option<PathBuf>` (`crates/tinio-fs/src/backend/mod.rs:116`, doc comment at 112-114) is already the override seam, and its doc comment names the read-only relocation. `Some(dir)` is taken verbatim — no derivation, no mode bits, no containment proof (decision 17).
- `StorageError::AccessDenied` + `access_denied()` exist (`crates/tinio-core/src/storage/error.rs:114-117`), documented for FR-023, currently used only for reserved `.tinio` segments. `map_backend_error` (`crates/tinio-server/src/backend/errors.rs:7`) already maps it to an S3 `AccessDenied`.
- The `[s3]` capability gate (`require_cap`, `crates/tinio-server/src/backend/mod.rs:335`) is the established shape for a cross-cutting refusal, but it answers `NotImplemented`.

What is missing or hostile to FR-023:

- Nothing derives `~/.tinio/roots/<hash>/`; `dirs` is not a dependency anywhere; `sha1` was dropped from the workspace in T100 (`sha2` and `hex` remain).
- `tombstone::dir` computes `<root>/.tinio/deleting` from the **root** (`crates/tinio-fs/src/tombstone.rs:29`), and it must stay there: the unpublish is a `fs::rename` of the live bucket directory onto the tombstone, and a rename cannot cross volumes (`EXDEV`) — a relocated state dir may sit on another volume. The module doc and `buckets.rs::delete_bucket_tombstone_stays_on_the_data_volume` pin this deliberately. The tombstone is therefore **not** part of the relocatable state; read-only mode only has to keep the two background stages that touch it away from the root.
- Startup writes reach into the root: `serve.rs:153` `create_dir_all(root)`, `FsCleanup::repair(Startup)` (bucket staging residue, tombstone enqueue), the scanner's root-side tombstone purge.
- No CLI: `crates/tinio-cli/src/commands/mod.rs` is a doc comment and `tinio-cli/src/lib.rs:52` returns `ExitCode::SUCCESS`. The only entry point is `crates/tinio-server/examples/serve.rs`, which hand-parses `--config`/`--port`/`--address`. Consequently `--read-only` (T067) and `TINIO_READ_ONLY` have no home yet, and config precedence (`sources.rs`'s documented `flags > env > .env > file`) is unimplemented.

And the fact that motivates the refactor in §4: **the set of S3 operations tinio implements is maintained by hand in two places today** — `backend/s3.rs` (`impl S3 for S3Backend`, 36 methods) and `metrics.rs` (`impl S3 for MetricS3`, 36 methods, verified to be the same 36 with no duplicates). The read-only decorator would make it three.

## Goal

Deliver FR-023's substance through the entry point that exists: a read-only server that refuses every mutating S3 operation with `AccessDenied`, never writes to the storage root — so it can serve a genuinely read-only filesystem — and keeps all tool state under `~/.tinio/roots/<hash>/`. Along the way, collapse the hand-maintained operation inventory into one machine-checked table and move the `[s3]` capability refusals onto it, so an operation's facts — dto pair, mutability, feature gate, capability toggle, post-success side effect — are stated exactly once.

## Non-goals

- The CLI surface (T066/T067). The `--read-only` flag lands with clap; this design puts the resolution logic where that flag will call it.
- The management plane, `doctor`, credential generation (T059–T075), and config auto-create and discovery.
- **Reading a pre-existing `<root>/.tinio/.tinio.toml`.** Nothing in the tree reads or writes a root-side config today (no auto-create, no walk-up discovery — `serve` takes an explicit `--config <path>`), so there is nothing to read-but-not-write. The rule becomes live when T066/T067 land; §2's resolver is what decides where an auto-created config would go.
- **The rest of FR-023's state list** — the state file, control socket, logs, and the `.env` load from the state dir. Those are tinio-api surfaces that do not exist in this tree either (T059–T075), so they land with the CLI. Both deferrals are FR-023 MUSTs, so they are moved out of T058/T076 into a new task explicitly rather than ticked as done (§11) — the one place where this design knowingly delivers less than the requirement text, recorded instead of smoothed over.
- Any change to normal-mode behavior, the ACL/authz plane, or the S3 wire semantics of reads. The `MetricS3` refactor in §5 is behavior-preserving, including every metric label.

## Decisions (locked 2026-09-18)

1. **Read-only enforcement lives in a decorator, not in the handlers.** A new `S3ReadonlyBackend<T>` wraps the inner `S3` impl; `S3Backend` and the handler modules stay untouched *by this part of the change* (the separate capability migration of decision 9 does edit the handlers, to remove gates that move into `CapabilityS3`). The mode is a deployment concern layered over a pure protocol mapping, and the operation set becomes one explicit, machine-checked table.
2. **The `delegate` crate cannot be used here.** Measured, not assumed: `delegate!` inside an `#[async_trait]` impl fails with `E0195` (*lifetime parameters or bounds on method do not match the trait declaration*). `#[async_trait]` expands first, passes the opaque `delegate!{…}` item through untouched, and the delegate-generated native `async fn`s never receive the lifetime rewrite, so they no longer match the trait's `Pin<Box<dyn Future + Send + 'async_trait>>` signatures. `tinio-select/src/dialect.rs:429` delegates a **sync** trait (`sqlparser::Dialect`), which is why the crate works there. The replacement is a local `macro_rules!` that expands to the **whole** `#[async_trait] impl` — the macro runs first, so `async_trait` transforms the generated methods. Verified against a standalone crate.
3. **`~/.tinio/roots/<hash>/`, with `dirs` and SHA-256.** The hash is the first 16 hex characters of `sha2::Sha256` over the canonical root path — a deliberate deviation from the spec's `sha1` wording (§11). It is a directory name with no security meaning, and `sha1` was removed from the workspace in T100 and is not worth re-adding for it.
4. **Relocation applies to read-only mode only.** Normal mode keeps `FsOptions.state_dir = None`, so `<root>/.tinio` still resolves through `state_dir()`'s `prove_contained` (which refuses a symlinked `.tinio`). An explicit state dir bypasses that proof by construction, so it must not become the default path.
5. **The `[s3]` capability toggles are moot in read-only mode.** The chain is `MetricS3 → S3ReadonlyBackend → CapabilityS3 → S3Backend`, so a read-only verdict precedes the capability one: an operation that is both disabled and read-only answers `AccessDenied`, matching the config contract's "`[s3]` write-related toggles are moot (writes are rejected regardless)".
6. **Read-only mode does not disable the scanner.** Per spec it keeps running, with meta writes landing in the home state dir. Root-touching repairs are skipped instead (§3).
7. **One operation table, three decorators.** `backend/ops.rs` holds the single inventory of the operations tinio implements; `MetricS3`, `S3ReadonlyBackend`, and `CapabilityS3` are all generated from it, and an `include_str!` cross-check pins it against `backend/s3.rs`. Adding an operation becomes one table row plus one dispatch method, instead of one row per decorator.
8. **The two behavior-preserving refactors land first, in order.** The `MetricS3` refactor is gated on the existing metric test and the `metrics.feature` scenarios staying green with no edits; the capability migration is gated the same way on the existing off-mode scenarios (`@cors-off`, `@tagging-off`, `@acl-off`, `@minimal-caps`), which must pass unmodified apart from the six normalized multipart messages (decision 9).
9. **The `[s3]` capability refusals move onto the table, and the multipart message is normalized.** The 26 `require_cap` / `require_multipart` call sites are replaced by `#[gated(<field>[: "<feature>"] …)]` rows and a `CapabilityS3` decorator (§7). Because the clause carries capability *field names* rather than an expression (§4), the denial subject is the operation's derived wire name — so the six multipart operations change from `"multipart is disabled"` to `"<Operation> is disabled"`, joining the form every other gate already uses. That string is pinned by no test and by no spec text today, so the normalization is a recorded wire-visible change (§11), not a silent one.
10. **`#[cfg]` folds into `#[gated]`.** The compile-time strip and the runtime refusal are one fact at two layers, and the strip is a *function* of the capability: mechanically verified across all 36 operations and the 19 `#[cfg]` attributes of `backend/s3.rs`, `cfg ⊆ gated` holds and the feature set is exactly `multipart`/`cors`/`acl`/`select` (same names as their capability) plus `copy`/`list-v1`/`list-v2` (naming `copy_object`/`list_objects_v1`/`list_objects_v2`), while `tagging` and `delete_objects` have none. The row states the feature once — `#[gated(copy_object: "copy")]` — and the consumers derive the attribute, so the two can no longer disagree. **The literal cannot be omitted.** `#[cfg]` takes a literal, `macro_rules!` cannot make one from an ident, and the only workaround — a lookup macro that matches the ident and emits the attribute — breaks `#[async_trait]` with the same `E0195` that ruled out `delegate!` (decision 2): the muncher is a macro invocation *inside* the impl, and `#[async_trait]` passes opaque items through untouched. Measured, not assumed. The same finding is why the capability names are **not** renamed to match the features: with the literal mandatory, `copy_object: "copy"` carries information at the point of use, whereas `copy: "copy"` would be a tautology on 19 rows.


11. **The `status` label becomes the real HTTP status.** `record`'s Err arm maps `err.status_code().unwrap_or(500)` — byte-for-byte the status s3s puts on the wire (`s3s/src/ops/mod.rs:136-140`) — instead of today's class mapping (`InternalError`→500, `NotImplemented`→501, everything else→400). The Ok arm stays `200`: s3s's generated ops discard `S3Response.status` and re-serialize from the output, so a ranged `GetObject` is 206 on the wire and unreachable at this layer — one known gap, documented on `record`. Exactly one existing assertion changes (`metrics.rs:790`, `400`→`404`); no spec text, cucumber feature or recorded dashboard contract pins the old class values.
12. **Read-only mode answers `AccessDenied` for a write to a missing bucket too** — a recorded, mode-specific deviation. AWS and MinIO both check authorization before resource existence (AWS: *"it first verifies that the requester has the necessary permissions"*; MinIO's `cmd/auth-handler.go` never probes the bucket and returns `ErrAccessDenied` for a denied caller whether or not it exists), so a server that denies every write answers 403 in both cases. It is also what falls out for free: the decorator sits inside the ACL plane, which passes a genuinely missing bucket through. The server's own rule — 404 `NoSuchBucket` to every principal for a missing bucket (`s3-surface.md`, the B3 pass-through) — is a read-write-mode rule; the read-only section records the exception.
13. **`FsOptions` gains `access: AccessMode`, and construction rejects read-only with no state dir.** `FsOptions { access: ReadOnly, state_dir: None }` is an error at `FsStorage::new`: the default resolves to `<root>/.tinio` and `database::open` would create it, writing the root silently. This turns the one place that can break "the root is never written" into a startup failure. An explicit in-root state dir is refused too (decision 17a) — the earlier draft argued that an in-root path plus read-only is a deliberate caller act that a genuinely read-only root would reject loudly, but the loud case is the one where the root cannot be written anyway; the quiet case, a writable root, is exactly the mistake worth failing on. The mode reaches the fs layer from the same value the caller hands the plane (`PlaneConfig::access`); no runtime cross-check compares the two, because every construction site is ours (the `serve` example, the e2e harness, the future CLI).
14. **The chown-privilege probe is skipped in read-only mode.** `serve` probes chown privileges at startup whenever a `local_uid` mapping is configured; the probe creates `<root>/.tinio-chown-probe-<nonce>/probe`, chowns it and deletes it (`crates/tinio-server/src/identity.rs:139-165`) — a write into the storage root the original design missed. Read-only mode has nothing to chown, and on a genuinely read-only root the probe is a startup failure.

15. **The root's own `.tinio/` metadata is not read, and that consequence is documented rather than left undefined.** Tags, ACLs, CORS configuration and upload records live *only* in the redb state DB (there are no in-root sidecars), so a root previously served read-write keeps them in `<root>/.tinio/meta.redb` where read-only mode will not look. The spec already licenses this — *"other contents of the root's `.tinio/` are ignored in read-only mode"* — so the behavior is not a deviation; what was missing is that it be **stated and pinned**. A `?tagging` / `?acl` / `?cors` read therefore answers empty while objects still serve from disk, and the ACL plane is fail-closed: the owner identity still passes via the owner bypass, a non-owner identity loses its grants. Recorded in `contracts/s3-surface.md`'s read-only section and asserted by a leg of `tests/read_only.rs` (§10). An operator who needs the state somewhere specific gets `TINIO_STATE_DIR` (decision 16).

16. **`TINIO_STATE_DIR` overrides the derived state dir.** The read-only deployments that motivate the mode — service accounts, containers, immutable images, mounted media — are exactly the ones with no writable `$HOME`, where `dirs::home_dir()` is `None` and `default_home_state_dir` would fail startup outright. The variable is used **verbatim** (not part of the `flags > env > .env > file` chain, because it selects the state dir rather than a config value); an empty value is an error, never the current directory; its parents are not created mode 0700 and are not symlink-checked, because the caller chose the location — the backend still creates the leaf and sets it 0700. It is an addition to FR-023's state-dir rule, recorded in `contracts/config.md` (§11).

17. **Two hardening clauses ride with the guard.** (a) A read-only storage refuses an explicit state dir **inside** the served root: the promise is that the root is never written, and a writable root would never complain, so the mistake must fail at construction. The check is lexical (`std::path::absolute` on the state dir against the canonical root) because the state dir need not exist yet; a symlinked state dir can still resolve into the root — residual recorded in the Risks. (b) The derived `<home>/.tinio` and `<home>/.tinio/roots` are refused if they are symlinks, matching normal mode's `prove_contained`, which already refuses a symlinked `<root>/.tinio`; `TINIO_STATE_DIR` is the escape hatch for a deliberate layout. Both are cheap, and their absence would leave the mode's central promise resting on the caller's good behavior.

## Design

### §1 `tinio-core`: the mode type

New `crates/tinio-core/src/mode.rs`:

```rust
/// The write policy of a served plane (FR-023).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

impl AccessMode {
    pub fn is_read_only(self) -> bool { matches!(self, Self::ReadOnly) }
}

impl From<bool> for AccessMode { /* the `read_only` config key → mode */ }
```

`tinio-core` hosts it because both `tinio-fs` (`CleanupOptions.access`, `FsOptions.access`) and `tinio-server` (`PlaneConfig`, the three decorators) need it, and it is the only crate they share.

`CleanupOptions` (`crates/tinio-core/src/cleanup.rs`) gains `access: AccessMode`, defaulting to `ReadWrite` — the one flag that tells `FsCleanup` which repair stages are legal.

### §2 `tinio-config`: state-dir resolution and the environment

New `crates/tinio-config/src/state.rs`:

```rust
/// `~/.tinio/roots/<sha256(canonical root)16>/` (FR-023).
pub fn home_state_dir(root: &Path, home: &Path) -> Result<PathBuf, Error>;

/// [`home_state_dir`] over `dirs::home_dir()`.
pub fn default_home_state_dir(root: &Path) -> Result<PathBuf, Error>;

/// The `TINIO_STATE_DIR` override, used verbatim (decision 16).
pub const STATE_DIR_VAR: &str;
pub fn env_state_dir() -> Result<Option<PathBuf>, Error>;
pub fn parse_state_dir(raw: &str) -> Result<PathBuf, Error>;
```

- `home` is a parameter rather than a hidden global: tests must never write the real `$HOME`.
- The root is canonicalized first (spec: a root's identity is its canonical path), so a missing root fails with a clear error instead of silently minting a new identity.
- The parents (`<home>/.tinio`, `<home>/.tinio/roots`) are mode 0700 on unix — otherwise they inherit the umask and other users can enumerate roots. The mode is **set unconditionally**, not only on creation: a `<home>/.tinio` an earlier version created 0755 would otherwise stay enumerable forever. That is the same shape `database/open.rs:67-74` already uses on the leaf (create, then `set_permissions`).
- Both parents are refused if they are **symlinks** (decision 17b) — normal mode already refuses a symlinked `<root>/.tinio` through `prove_contained`, and the state dir is where the mode's promise lives.
- `default_home_state_dir` fails with `Error::Missing("home directory")` when `dirs::home_dir()` is `None`. That failure is why `TINIO_STATE_DIR` exists (decision 16): no writable home is a normal read-only deployment, not an error to be worked around by the operator editing their environment.
- The override is deliberately outside the derived path's rules: taken verbatim (trimmed), empty is an error, no 0700 parents, no symlink check. The leaf is still created 0700 by the backend.

`sources.rs` gains a `TINIO_READ_ONLY` overlay applied to a parsed `Config`: `1`/`true` → read-only, `0`/`false` → read-write, any other value → error, unset → the config file's value stands. That parse is deliberately **stricter** than `TINIO_SCANNER`'s, whose handler (`tinio-fs`'s `scanner_enabled`) ignores an unrecognised value and falls back to the `[scanner]` section gate: a typo in `TINIO_READ_ONLY` must not silently serve a writable root. It lives here rather than in `serve.rs` so the CLI can reuse or replace it. `TINIO_STATE_DIR` is *not* an overlay — it selects the state dir, and the parse lives beside the resolver in `state.rs` (decision 16).

`dirs`, `sha2`, and `hex` are added to the workspace and to `tinio-config` (the latter two are already workspace entries used by `tinio-core`; `dirs` is new).

### §3 `tinio-fs`: the seam, the tombstone, and startup writes

- **Seam**: `FsOptions.state_dir` is unchanged. A read-only caller passes `Some(state)`; every consumer already reads the resolved dir (`database/open.rs`, `write.rs`'s tmp area, `multipart.rs`, `sweep.rs`, `cleanup.rs`).
- **Tombstone: no change.** `tombstone::dir` stays root-based (`<root>/.tinio/deleting/`) — see Background. The write path needs nothing: read-only mode refuses `delete_bucket` long before the unpublish. The `delete_bucket_tombstone_stays_on_the_data_volume` test keeps passing unmodified.
- **Construction guard** (decisions 13 and 17a): `FsOptions` gains `access: AccessMode`, and `FsStorage::new` rejects `ReadOnly` with `state_dir: None` (in `resolve`, before `canonicalize`, so the failure does not depend on the root existing) **and** rejects `ReadOnly` with a state dir inside the root (in `resolve`, after `canonicalize`, as a lexical containment test — the state dir need not exist yet, and a symlinked one can still escape; residual in the Risks).
- **Chown probe** (decision 14): read-only mode skips `identity::ensure_chown_privilege`, whose probe writes `<root>/.tinio-chown-probe-<nonce>/probe`.
- **Scanner plumbing**: read-only reaches the scanner through a new `ScannerOptions.access` field — the struct is already `Scanner::new`'s input, so no constructor signature changes.
- **Startup writes**: `FsCleanup` gains an `access: AccessMode` field (mirroring `dry_run`, which is the only `CleanupOptions` field it stores today) and skips its two **root-writing** stages under `ReadOnly`: `repair_delete_tombstones` (which enqueues removals under `<root>/.tinio/deleting/`) and `repair_bucket_staging` (in-bucket `.tinio` residue). The state-dir stages — `repair_tmp`, `repair_multipart_orphans`, `repair_orphan_upload_dirs` — still run, as do the read-probe stages (`repair_buckets`). The scanner skips its own tombstone-leftover purge the same way. Nothing needs repairing in read-only mode in any case: writes are refused long before they can create residue, and a genuinely read-only root cannot be repaired at all. A root previously served read-write may legitimately still hold an unpublished tombstone or staging residue — private state, deliberately left alone rather than repaired.

### §4 `tinio-server/src/backend/ops.rs`: the operation table

The single inventory of the operations tinio implements. A row states a fact about an operation — its s3s dto pair, whether it mutates, the capability that refuses it together with the cargo feature that strips it, and any post-success side effect — and five consumers read those facts: the three decorators of §5–§7 (`metric_s3_impl` in `metrics.rs`, `readonly_impl` in `readonly.rs`, `capability_impl` in `capability.rs`), this module's own `op_names` (the `include_str!` cross-check against `backend/s3.rs`, the read/write group pins, and the `read_ops`/`write_ops` lists the read-only tests read), and `readonly.rs`'s test-only `readonly_probe`. Every operation appears **exactly once**, in exactly one group: a duplicated row is a compile error in every generated `impl` (two methods of one name) and a failing table test, and `list_parts` / `list_multipart_uploads` are reads, so they live in the read group where the read-only decorator forwards them.

```rust
/// Every S3 operation tinio implements, grouped by whether it mutates.
/// Consumers: `metric_s3_impl` (§5), `readonly_impl` (§6),
/// `capability_impl` (§7), this module's `op_names` (the `include_str!`
/// cross-check against `backend/s3.rs`, §10), and `readonly.rs`'s
/// test-only `readonly_probe`.
macro_rules! tinio_s3_ops {
    ($consumer:ident) => {
        $consumer! {
            read {
                fn head_bucket (HeadBucketInput) -> HeadBucketOutput;
                fn list_buckets (ListBucketsInput) -> ListBucketsOutput;
                #[gated(cors: "cors")]
                fn get_bucket_cors (GetBucketCorsInput) -> GetBucketCorsOutput;
                // …
            }
            write {
                fn put_object (PutObjectInput) -> PutObjectOutput;
                #[gated(multipart: "multipart", copy_object: "copy")]
                fn upload_part_copy (UploadPartCopyInput) -> UploadPartCopyOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(multipart_in_progress_inc())]
                fn create_multipart_upload (CreateMultipartUploadInput)
                    -> CreateMultipartUploadOutput;
                // …
            }
        }
    };
}
```

Three properties make this the right shape:

- **The wire name is derived, not declared.** s3s's codegen names each dto pair after the operation, so the metrics label is the input type's name minus the `Input` suffix — `&stringify!($input)[..stringify!($input).len() - "Input".len()]`. Verified against all 36: `CreateMultipartUploadInput` → `CreateMultipartUpload`, `PutObjectAclInput` → `PutObjectAcl`, `ListObjectsV2Input` → `ListObjectsV2`. This keeps the row grammar to what it carries today and makes the label impossible to drift from the operation.
- **`read` / `write` is the one consumer-specific fact**, and it is a fact about the operation, not about a decorator: the read-only decorator branches on it, the metrics decorator ignores it and treats both groups alike.
- **`#[gated(<field>[: "<feature>"] …)]` states both the `[s3]` capability that refuses the operation and the cargo feature that strips it** (FR-021). The two are one fact at two layers — compile-time strip versus runtime refusal — and the second is a function of the first: the compile-time features are exactly `multipart`, `cors`, `acl`, `select` (same names as their capability) plus `copy`/`list-v1`/`list-v2` (naming the `copy_object`/`list_objects_v1`/`list_objects_v2` capabilities), while `tagging` and `delete_objects` have no feature at all and appear without one. 26 of the 36 operations carry a clause; §7 is its consumer.
- **The comma list is a conjunction** — `upload_part_copy` is the only one, gated on `multipart` *and* `copy_object`, and therefore stripped only when both features are off. The clause carries field names of `Capabilities`, not an expression, because `self` cannot cross the macro's hygiene boundary (below); the consumer builds `self.caps.<field>` itself.
- **`#[on_ok(<expr>)]` is the escape hatch for the three operations that are not uniform** — `create_multipart_upload` increments the in-progress-multipart gauge on success, `complete_multipart_upload` and `abort_multipart_upload` decrement it. Only `MetricS3` uses the clause; the read-only decorator accepts and drops it. Everything else in the operation set is uniform `record` + forward.

The clause is an **attribute**, not a trailing clause on the signature, and each form was compiled rather than reasoned about:

- **A trailing clause cannot be a bare identifier.** `$out:ty` may only be followed by `{`, `[`, `=>`, `,`, `>`, `=`, `:`, `;`, `|`, `as`, or `where`; `fn f (In) -> Out on_ok g();` is rejected with *"not allowed after `ty` fragments"*. A trailing clause therefore needs a bracketing workaround, which is why the clause moved in front of `fn` — as `#[on_ok(...)]` there is no follow-set constraint at all, and the signature line stays clean.
- **Every consumer's matcher must spell the clauses out, in order.** The row matcher is `$(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident;`. The shape is forced from several directions, each established by compiling the alternative:
  - **The catch-all `$(#[$m:meta])*` cannot coexist with a named clause.** `#[gated(...)]` and `#[on_ok(...)]` are themselves valid `meta` productions, so the parser cannot decide whether to consume them as the catch-all or as the clause and refuses: *"local ambiguity when calling macro … built-in NTs meta ('wm') or 1 other option"*. The matcher must name its clauses explicitly.
  - **A derived attribute repeats one nesting level deeper than its source.** The row's clause is optional, so emitting `#[cfg(...)]` per entry inside it needs `$( $( $(#[cfg(feature = $f)])? )+ )?` — three levels, mirroring `$(#[gated($($cap:ident $(: $feature:literal)?),+)])?`. One level short the expansion fails with *"this must repeat at least once"* (a row without the clause makes the group match zero times). The same depth applies to the runtime conjunction, `$( $( && self.caps.$cap )+ )?`.
  - **A derived `#[cfg]` needs a literal, and `macro_rules!` cannot produce one from an ident.** That is why the feature is written in the row rather than looked up from a capability→feature table: the mapping cannot live in the macro. Multiple `#[cfg]` attributes conjoin, so a conjunction needs no `all(...)`.
  - **Attribute order is significant.** `#[gated]` must precede `#[on_ok]`; reordering yields a loud but misleading diagnostic (the parser's note points at the clause, not at the real problem). Unlike real Rust, where attribute order is irrelevant, the rule here is: **`#[gated]` first, then `#[on_ok]`.**
  - **A clause cannot carry an expression over `self`.** `self` is hygienic in `macro_rules!` — an identifier written in the table cannot see the generated method's `self` (*"an identifier with the same name is defined here, but is not accessible due to macro hygiene"*). That is what forces `#[gated]` to carry field names (resolved by type, so they cross the hygiene boundary) and what makes a comma list the only encoding for a conjunction. `#[on_ok(...)]` is unaffected: it references a path (an item), not a local.
- **The dto pair is captured as `ident`, not `ty`.** A `ty` fragment cannot be used as a path segment (`dto::$in` fails on the invisible delimiter), and `stringify!` of a multi-token path yields `"dto :: HeadBucketInput"` — which would break the derived wire name. Capturing `$in:ident` / `$out:ident` lets each consumer write `dto::$in` *and* derive the label from `stringify!($in)`. Every s3s dto name is a single identifier.

Each consumer must spell out every clause in order, since `macro_rules!` has no partial matching: adding a clause to the table means editing the matcher of all five consumers (the three decorators, `op_names`' name dump, and `readonly_probe`). The three decorators are `pub`, matching the existing `MetricS3`, so integration tests and the e2e harness can name them; `wire_name!` is defined beside `MetricS3` in `metrics.rs` and re-exported `pub(crate)` — one copy of the label derivation, not three.

The cross-check reads the table **two** ways, and the split is load-bearing: `table_features` pushes unconditionally, so it carries the table's facts (name + stripping features) and can be compared with `s3.rs`'s raw text — which `include_str!` never filters by feature — while `table_ops` carries the derived `#[cfg]` and is what *this* build compiles, so it is checked as a subset. Comparing the `#[cfg]`-filtered list against the file would only ever hold in an all-features build (measured on the real table: 17 of its 36 rows compile under `--no-default-features`, 23 with `multipart` alone, 36 with `--all-features`). The row order is free — reads first, then writes, not `s3.rs`'s order — because the comparison sorts both sides. The helpers' `#[allow]`s are equally load-bearing: clippy **does** lint local `macro_rules!` output, and the repo gates on `-D warnings` (`clippy::vec_init_then_push` fires on the `Vec::new()` + `push` shape, `unused_mut` fires on the rows without `#[gated]`); `clippy::absolute_paths` is expansion-suppressed and does not.

The full table — 36 operations, 20 writes and 16 reads:

| Operation | Kind | `#[gated]` | Note |
|---|---|---|---|
| `create_bucket` | write | — |  |
| `delete_bucket` | write | — |  |
| `head_bucket` | read | — |  |
| `list_buckets` | read | — |  |
| `get_bucket_location` | read | — |  |
| `get_bucket_tagging` | read | `tagging` |  |
| `put_bucket_tagging` | write | `tagging` |  |
| `delete_bucket_tagging` | write | `tagging` |  |
| `get_bucket_cors` | read | `cors: "cors"` |  |
| `put_bucket_cors` | write | `cors: "cors"` |  |
| `delete_bucket_cors` | write | `cors: "cors"` |  |
| `get_bucket_acl` | read | `acl: "acl"` |  |
| `put_bucket_acl` | write | `acl: "acl"` |  |
| `get_object_acl` | read | `acl: "acl"` |  |
| `put_object_acl` | write | `acl: "acl"` |  |
| `put_object` | write | — |  |
| `get_object` | read | — |  |
| `head_object` | read | — |  |
| `get_object_attributes` | read | — |  |
| `delete_object` | write | — |  |
| `delete_objects` | write | `delete_objects` |  |
| `get_object_tagging` | read | `tagging` |  |
| `put_object_tagging` | write | `tagging` |  |
| `delete_object_tagging` | write | `tagging` |  |
| `copy_object` | write | `copy_object: "copy"` |  |
| `rename_object` | write | `copy_object: "copy"` | s3s extension, not AWS |
| `list_objects` | read | `list_objects_v1: "list-v1"` |  |
| `list_objects_v2` | read | `list_objects_v2: "list-v2"` |  |
| `create_multipart_upload` | write | `multipart: "multipart"` | `on_ok` +1 in-progress |
| `upload_part` | write | `multipart: "multipart"` |  |
| `upload_part_copy` | write | `multipart: "multipart"`, `copy_object: "copy"` | the one conjunction |
| `complete_multipart_upload` | write | `multipart: "multipart"` | `on_ok` −1 in-progress |
| `abort_multipart_upload` | write | `multipart: "multipart"` | `on_ok` −1 in-progress |
| `list_parts` | read | `multipart: "multipart"` |  |
| `list_multipart_uploads` | read | `multipart: "multipart"` |  |
| `select_object_content` | read | `select: "select"` |  |

26 of the 36 carry a `#[gated]`; the ten that do not are the core always-on operations (`create_bucket`, `delete_bucket`, the five bucket/object reads with no toggle, `put_object`, `get_object`, `head_object`, `get_object_attributes`, `delete_object`).

The write set is wider than FR-023's parenthetical ("bucket create/delete, object put/delete/copy, all multipart ops"): the rule is *all* mutating operations, so the tagging, ACL, and CORS writes are included. `contracts/s3-surface.md` gets the fixed list (§10).

### §5 `tinio-server`: the `MetricS3` refactor

`metrics.rs`'s hand-written `impl S3 for MetricS3<T>` (36 methods) is replaced by the table:

```rust
macro_rules! metric_s3_impl {
    (
        read  { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
        write { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
    ) => { /* one impl block, uniform bodies + the optional on_ok clause */ };
}

tinio_s3_ops!(metric_s3_impl);
```

Each generated body is the current uniform shape — `self.record(<derived label>, self.inner.<op>(req)).await` — with the `on_ok` clause emitted only where the row has one, `$( if out.is_ok() { <expr> } )?` (the optional group, so the 33 rows without the clause get no empty `if`), and the `#[cfg]` attributes derived from the `#[gated]` entries so the compiled operation set is unchanged. The operations, labels, side effects and compile-time gates are all preserved.

One deliberate change rides along, in the shared `record` helper rather than in the generated bodies (decision 11): the `status` label becomes the **real HTTP status**. `s3s::S3Error::status_code()` exists and is populated for every code the server raises except `Custom`/`InvalidAddressingHeader`/`MissingAttachment`, and the status s3s puts on the wire is exactly `err.status_code().unwrap_or(500)` (`s3s/src/ops/mod.rs:136-140`) — so the Err arm reproduces the wire status byte-for-byte. The Ok arm stays `200`: s3s's generated ops discard `S3Response.status` and re-serialize from the output, so a ranged `GetObject` is 206 on the wire and cannot be seen at this layer; that single known gap is documented on `record`.

The existing test `metric_s3_records_every_delegated_operation` is the safety net and must stay green **with no edits**. Its hand-written 36-call list can then be driven by the same table, since the table is no longer the only check on the operation set (§9's cross-check is).

### §6 `tinio-server`: `S3ReadonlyBackend`

New `crates/tinio-server/src/readonly.rs`, a peer of `metrics.rs` — both are decorators over the `S3` chain.

```rust
pub struct S3ReadonlyBackend<T> { inner: T }

impl<T> S3ReadonlyBackend<T> {
    pub fn new(inner: T) -> Self;
}

macro_rules! readonly_impl {
    (
        read  { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
        write { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
    ) => { /* one impl block: reads forward, writes deny */ };
}

tinio_s3_ops!(readonly_impl);
```

Reads forward `self.inner.<op>(req).await`. Writes return `s3_error!(AccessDenied, "Access Denied")` **without touching `inner`**. The `on_ok` clauses are matched and dropped; the `#[gated]` entries are matched and dropped too — a read-only refusal is unconditional, the capability verdict belongs to `CapabilityS3` one layer in. The grouped matching, the `fn`-prefixed entries, the optional clauses and the derived `#[cfg]` were each verified against a standalone crate.

A gated-out entry leaves no method, so the trait default (`NotImplemented`, 501) answers — correct for a build where that group is stripped, and unchanged by read-only mode.

The denial message is the same literal as `tinio-auth`'s `denied()` (AWS's default), so a read-only refusal is indistinguishable on the wire from an ACL denial. That is consistent with `contracts/s3-surface.md`'s existing 403-ambiguity note, and it leaks nothing about the mode.

`post_object` (the browser form upload) needs no entry: s3s's trait default maps `PostObjectInput` to `PutObjectInput` and calls `self.put_object(…)`, where `self` is the decorator, so it lands on the rejection.

### §7 `tinio-server`: the capability decorator

New `crates/tinio-server/src/capability.rs`, the table's third consumer:

```rust
pub struct CapabilityS3<T> {
    inner: T,
    caps: Capabilities,
}

macro_rules! capability_impl {
    (
        read  { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
        write { $( $(#[gated($($g:ident $(: $f:literal)?),+)])? $(#[on_ok($h:expr)])? fn $m:ident ( $in:ident ) -> $out:ident; )* }
    ) => { /* one impl block: gated rows refuse, everything else forwards */ };
}

tinio_s3_ops!(capability_impl);
```

A gated row's body is:

```rust
$( $( $(#[cfg(feature = $f)])? )+ )?
async fn $m(&self, req: S3Request<dto::$in>) -> S3Result<S3Response<dto::$out>> {
    if !(true $( $( && self.caps.$g )+ )?) {
        let name = wire_name!($in);
        return Err(s3_error!(NotImplemented, "{name} is disabled"));
    }
    self.inner.$m(req).await
}
```

The optional group wrapping a nested repetition is the idiom that collapses an absent clause to `true`; the `#[on_ok(...)]` clauses are matched and dropped. Verified against a standalone crate, conjunction included.

This decorator is **unconditional** — it rides the chain in every mode, because it now carries the FR-021 gates that used to live in the handlers:

- 26 call sites lose their gate: `listing.rs:118/147`, `select.rs:258`, `cors.rs:34/56/71`, `objects.rs:745/870/894/912/932/961/1017/1197`, `buckets.rs:281/304/320/340/370`, `multipart.rs:220/352/441/551/879/924/1016`.
- `S3Backend::require_cap` (`backend/mod.rs:335`) and `require_multipart` (`multipart.rs:206`) are deleted. Neither has a unit test of its own; what the deletion breaks is the eight **off-mode behaviour tests**, which assert the refusal by driving a bare `S3Backend` (`cors.rs:762`, `buckets.rs:1299/1660`, `multipart.rs:1375`, `objects.rs:2124/2878/3675`, `select.rs:924`). They move onto the decorator through one new `testutil.rs` helper, with their assertions unchanged — the code, the CORS trio's message and `upload_part_copy`'s either-toggle-off conjunction are all preserved by construction. This is a cost decision 9 did not price, now recorded.
- Every gate was the handler's first statement, so removing it changes no other ordering: the decorator's verdict lands before any handler-side validation, exactly where the gate ran.
- **Authorization still precedes the gate, in both modes.** Per-request ACL checks are not handler code: they are s3s's `S3Access` layer (`AclAccess`, `crates/tinio-auth/src/access.rs:71`), which `ops::prepare` invokes at `ops/mod.rs:1002` before `op.call` dispatches to the `S3` impl. `CapabilityS3` therefore sits below the ACL plane exactly as the handlers did, and the migration does not change what an unauthorized caller sees (403 either way) or leak capability state to it. Same for read-only mode: its `AccessDenied` is also decided below authorization, which is what makes decision 12's missing-bucket answer fall out — the ACL plane passes a missing bucket through (B3), and the decorator then refuses.
- **One wire-visible change**: the six multipart operations answer `"<Operation> is disabled"` (decision 9).

### §8 `tinio-server`: `PlaneConfig` plumbing

`data.rs` gains:

```rust
pub struct PlaneConfig {
    pub caps: Capabilities,
    pub access: AccessMode,
}

impl From<Capabilities> for PlaneConfig { /* ReadWrite */ }
impl PlaneConfig { pub fn read_only(caps: Capabilities) -> Self; }
```

The four `DataPlane` constructors take `plane: impl Into<PlaneConfig>` in place of `caps: Capabilities`. `From<Capabilities>` keeps all 11 existing construction sites compiling unchanged (5 in `data.rs`'s own tests, 2 in the `serve` example, 2 in the e2e harness, plus the two constructor chains); a read-only caller writes `DataPlane::new_with_acl(storage, PlaneConfig::read_only(caps), identity)`.

The chain order is `MetricS3 → S3ReadonlyBackend → CapabilityS3 → S3Backend`, built in the constructors where `MetricS3::new(S3Backend::new_shared(…))` stands today:

```rust
// read-write
let backend = MetricS3::new(CapabilityS3::new(S3Backend::new_shared(storage, caps), caps));
// read-only
let backend = MetricS3::new(S3ReadonlyBackend::new(CapabilityS3::new(
    S3Backend::new_shared(storage, caps),
    caps,
)));
```

The two arms have different types, so the branch lives in each constructor — beside the `#[cfg(feature = "cors")]` branch the constructors already fork on. `MetricS3` is outermost so a refusal, read-only or capability, is still recorded in `tinio_s3_operations_total{op,status}`; `S3ReadonlyBackend` is outside `CapabilityS3` so a read-only write answers `AccessDenied` rather than `NotImplemented` (decision 5). The `acl` plane's `with_identity` is applied to `S3Backend` before the decorators wrap it.

### §9 The `serve` example

- A new `--read-only` flag, plus `TINIO_READ_ONLY` applied through `tinio_config::sources`.
- Precedence (`flags > environment > .env > config file`), applied lowest-first: the parsed config file, then the `TINIO_READ_ONLY` overlay, then the flag. `.env` joins when the CLI brings `dotenvy` wiring.
- When read-only: skip `create_dir_all(&root)` (the root must already exist; canonicalization supplies the error otherwise), set `FsOptions.state_dir = Some(state)`, set `CleanupOptions.access = ReadOnly`, and build the plane through `PlaneConfig::read_only`.
- The state dir comes from `state::env_state_dir()` when `TINIO_STATE_DIR` is set, otherwise from `state::default_home_state_dir(root)` (decision 16).
- The resolved state dir is logged at startup, naming `TINIO_STATE_DIR` as the source when it came from the override — it now lives outside the root, and operators need to find it.

### §10 Testing

- **`backend/ops.rs` cross-check**: `include_str!("s3.rs")` (the same directory) supplies `backend/s3.rs`'s `async fn` names **and their `#[cfg]` feature sets**; the test asserts both equal the table's — the names from the rows, the features derived from the `#[gated]` entries. Because the features are now `literal` fragments rather than opaque `meta`s, the compile-time half is machine-checked too, which is what lets decision 10 drop the separate `#[cfg]` clause.
- **`MetricS3`**: the existing `metric_s3_records_every_delegated_operation` (36 calls against a `FakeS3` in `NotImplemented` mode) stays green with no edits — the refactor's gate. The `metrics.feature` cucumber scenarios likewise.
- **`S3ReadonlyBackend`**: table-driven — every read reaches the inner backend (and is not 501), every write answers `AccessDenied` **and the inner backend is never called**; `post_object` answers `AccessDenied`.
- **`CapabilityS3`**: the table-driven unit test drives the six `#[gated(tagging)]` rows — the ones whose capability carries no cargo feature, so they run in every build — asserting the refusal with `NotImplemented "<WireName> is disabled"` and the forward path with the toggle on; the remaining gated rows' refusals are covered by the eight migrated handler tests (§7) and the off-mode cucumber scenarios (`@cors-off`, `@tagging-off`, `@acl-off`, `@minimal-caps`), with all 26 gated rows' names and cargo features pinned independently by `ops.rs`'s `include_str!` cross-check and the grouping by its `WRITE_OPS` pin. The conjunction row (`upload_part_copy`) refuses when *either* toggle is off; ungated rows are unaffected. The migration's gate is two halves: the **cucumber** off-mode scenarios (`@cors-off`, `@tagging-off`, `@acl-off`, `@minimal-caps`) pass with no edits, because they drive a real plane that now carries the decorator; the **eight backend-level off-mode tests** (§7) are migrated onto `CapabilityS3` through the new `testutil.rs` helper, with `cors.rs`'s `contains("is disabled")` assertion kept verbatim. Running `cargo test -p tinio-server` last, not first, is what catches any ninth test doing the same thing.
- **`tinio-config`**: hash stability and the derived layout, 0700 parents on unix, the symlinked-parent refusal (`#[cfg(unix)]`), and both parse matrices (`TINIO_READ_ONLY`, `TINIO_STATE_DIR` — including that an empty override is an error, never the current directory). Both env wrappers cannot be unit-tested by mutating the process environment — `std::env::set_var` is `unsafe` under edition 2024 and every crate forbids `unsafe_code` — so each splits into a pure parse (`parse_read_only(&str)`, `parse_state_dir(&str)`) that the tests drive, and the `env::var` lookup stays a two-line wrapper.
- **`tinio-fs`**: `backend/tests.rs::read_only_state_relocation_keeps_root_clean` already exists and keeps its explicitly-passed state dir; it asserts relocation (the root holds only `data`, `meta.redb` lives in the state dir, `<root>/.tinio` does not exist), and it is **not** extended — it stays the positive control for "relocation alone is enough to keep the root clean". The read-only-specific assertions are new tests: the cleanup gating (root-writing stages skipped, state-dir stages still running), the scanner's tombstone-purge gate with its `ReadWrite` positive control, and the guard's two clauses (`ReadOnly` + `None`; `ReadOnly` + an in-root state dir). `tinio-fs` does not depend on `tinio-config` and must not: the resolver is config policy, and reaching it here would drag `dirs` into the storage contract (the alternatives — sinking the resolver into `tinio-core`, or a `tinio-config` dev-dependency — were both rejected).
- **The root-never-written criterion** (§10): on unix the authoritative proof is a genuinely read-only root (`chmod 0500`) — every write would fail with `EACCES`, so a clean start plus successful reads *is* the proof. Windows has no equivalent, so that leg asserts the weaker portable property (the root's entries are unchanged across a start) and the asymmetry is stated rather than papered over.
- **`crates/tinio-server/tests/read_only.rs`** (T058): reads behave identically; a representative write from each mutating class answers `AccessDenied` (all 20 write rows are covered by the decorator's own table-driven test, which also proves the inner backend is never reached); a write to a missing bucket answers 403, not 404 (decision 12); state lands under the injected home; the root's own `meta.redb` is not read, so a tag written through a read-write run reads back empty (decision 15 — the in-root meta view is empty by design, pinned rather than left undefined); on unix, a genuinely read-only root (`chmod 0500`) still starts cleanly and serves reads.
- **Cucumber**: a `read_only.feature` scenario set with a `Server::read_only(…)` harness constructor; FR-023 leaves the `traceability.rs` allow-list. Every scenario carries `@fs` (the constructor builds an `FsStorage`), so they run in the default fs leg and are filtered out of the mem leg by its `not @fs` tag expression — the feature file says so, and the tag is per scenario because the hook reads `scenario.tags` and never sees feature-level tags. Fixtures are planted on disk by a new `given` step: the mode refuses the writes (`create bucket`, `upload`) that would otherwise build them, and reads stop at what a planted root can express (buckets, objects) — tagging, ACL and multipart read state lives in the meta DB and stays with the Rust suites, where decision 15's empty-view leg pins what a fresh home actually answers.

### §11 Bookkeeping

- **Spec deviation**: the spec says `sha1(canonical root)16`; the implementation uses SHA-256's first 16 hex characters. `specs/001-s3-local-server/{spec.md,data-model.md,contracts/config.md,research.md}` get the wording updated.
- `tasks.md`: T077 is delivered in full and is checked; T058 and T076 each carry a clause this design does not implement (the root-side config read; the `.env` load from the state dir; tinio-api's state/socket/logs relocation), so those clauses move into **one new task** appended to US2 — they land with the CLI (T066/T067) — and the delivered parts are checked. A tick means done; the split keeps it honest rather than relying on a note. The note on the ticked entries records that the CLI flag (T067) reuses `tinio_config::state::{home_state_dir, env_state_dir}`.
- **Multipart denial messages normalized** (decision 9): the six multipart operations answer `"<Operation> is disabled"` instead of `"multipart is disabled"`. `contracts/s3-surface.md`'s FR-021 paragraph and `contracts/config.md`'s `[s3]` bullet get the note that the subject is always the operation name; `tasks.md`'s T050/T133 notes follow. No test pins the old strings, and no client matches on them — the S3 error *code* (`NotImplemented`) is unchanged.
- **Metrics** (decision 11): `metrics.rs`'s module doc, `record`'s own doc and `metric_s3_records_status_classes`'s `400` assertion follow the real-status mapping; `data-model.md` and `research.md` already describe the label as *"op × status incl. error codes"*, which the change matches rather than contradicts.
- **Read-only missing-bucket deviation** (decision 12): `contracts/s3-surface.md`'s read-only section records that a write to a missing bucket answers `AccessDenied`, cites the AWS/MinIO authorization-first ordering, and states that the B3 every-principal-404 rule does not apply in this mode.
- **`TINIO_STATE_DIR`** (decision 16): `contracts/config.md`'s read-only section and its environment-variable list gain the override, with the verbatim semantics, the empty-is-an-error rule, and the fact that it is not part of the precedence chain. It is an addition to FR-023's state-dir rule, recorded as such — not a silent extension.
- **Hardening** (decision 17): `contracts/config.md` notes that a read-only storage refuses an in-root state dir and that the derived home parents are refused as symlinks.
- **The in-root meta view is empty** (decision 15): `contracts/s3-surface.md`'s read-only section spells out that a root previously served read-write has its tags, ACLs, CORS and upload records in `<root>/.tinio/meta.redb`, which read-only mode does not read — those reads answer empty while objects still serve, and the fail-closed ACL plane keeps the owner (via the owner bypass) and drops non-owner grants. `CONTEXT.md` carries the same sentence.
- `CONTEXT.md`: a "Read-only mode" glossary entry alongside "Capability toggle", including the state-dir override and the empty-meta consequence.
- `contracts/s3-surface.md`: the read-only section gains the fixed operation list, and the 403-ambiguity note gains read-only mode as a fourth source.

## Standing costs of the capability migration (decision 9)

Folding the `[s3]` gates into the table was considered, priced, and **adopted** (§7). Three costs came with it; the first two are paid in this design, the third is a standing caveat:

- **The denial messages had to be normalized.** The CORS trio's `"{name} is disabled"` is pinned per operation by `cors.feature:252/258/262`, but the six multipart operations answered `"multipart is disabled"` — the group name, pinned by nothing (`multipart.rs:210`). Because the `#[gated]` clause carries field names rather than an expression (§4), the subject is necessarily the derived wire name, so the six strings change (§11). Recorded, not silent.
- **One gate is a conjunction.** `upload_part_copy` needs `multipart && copy_object` (`multipart.rs:441`); the comma list is the encoding, and the hygiene constraint rules out any other.
- **Standing caveat: only half of a capability is a refusal.** The toggles keep per-request data-path effects that cannot move — measured `self.caps.*` occurrences outside the 26 gates: `caps.checksum` 15, `caps.tagging` 6, `caps.acl` 5 (the off-mode plane at `mod.rs:520` plus the per-op ACL data reads), and the CORS preflight route (`data.rs:174`); the pagination knobs (`allow_zero_page_size`, `max_keys`, `max_buckets`) were never gates at all. An earlier draft of this section said 18; the count is re-derived here rather than estimated. After this migration, "what does `[s3] tagging = false` do?" is answered in two places: the refusal in the table, the off-mode data behavior in the handler. That is the price of centralizing the refusals, and it is worth paying now that five consumers share the table — but a reader asking that question must be told to look in both places.

## Risks

1. **A new operation added to `S3Backend` but not to the table** answers 501 in read-only mode only, and the default CI matrix never runs read-only. Mitigation: §10's `include_str!` cross-check fails the moment the operation lands in `backend/s3.rs`, and it now compares the compile-time feature sets as well as the names — the gap that used to be "mirrored by review".
2. **The table's grammar is the union of its consumers' needs.** A new per-operation fact widens the row grammar for all five consumers, each of which must spell the clause out in its matcher and accept-and-drop it if it does not read it — `macro_rules!` has no partial matching, and the catch-all `$(#[$m:meta])*` form is unavailable once named clauses exist (§4's ambiguity finding). The clause set is deliberately restricted to facts about the operation — dto pair, mutability, capability toggle (carrying the cargo feature), post-success side effect — rather than decorator-specific hooks, and it stands at two clauses.
3. **The capability migration touches 26 call sites across six handler modules.** It is mechanical but wide, and unlike the `MetricS3` refactor its safety net is the off-mode cucumber scenarios rather than one exhaustive unit test. Sequenced after the metrics refactor (decision 8) so the two refactors are reviewable separately, and its only intended behavior change is the six normalized messages.
4. **The `MetricS3` refactor rewrites working, tested code.** Sequenced first (decision 8) so its verification is independent of the read-only work, and gated on the existing test and cucumber scenarios passing unmodified.
5. **Root identity is its canonical path** — renaming or re-linking a root yields a new home state dir. Spec-documented, restated because it surprises operators.
6. **Windows has no unix mode bits**, so the home state dir relies on ACL inheritance. The spec already accepts the asymmetry ("flag-only on Windows").
7. **`PlaneConfig` via `impl Into<…>`** is a new pattern in this crate. The alternative — an explicit parameter on four constructors — costs 15 call-site edits and reads more literally.
8. **Metrics**: a read-only instance reports `AccessDenied` at whatever rate clients attempt writes. Expected, but it will look like an incident on a dashboard that has no read-only dimension.
9. **A genuinely read-only root** must already exist, and `--config` must point at a readable file, since no config is auto-created in this scope.
10. **The Ok arm books `200` while the wire may say otherwise** — a ranged `GetObject` is 206 on the wire, and s3s discards `S3Response.status` before `MetricS3` can see it. Documented on `record`; the label stays an accurate 2xx/4xx/5xx indicator, which is what a dashboard keys on.
11. **Read-only mode answers `403` where the read-write server answers `404`** for a write to a missing bucket (decision 12). Recorded in `contracts/s3-surface.md`; a client that distinguishes `NoSuchBucket` from `AccessDenied` sees a different code in this mode.
12. **The extra decorator costs a little on every request, in every mode.** `#[async_trait]` boxes per layer, so the chain goes from two boxed futures per operation (`MetricS3 → S3Backend`) to three in read-write mode (`MetricS3 → CapabilityS3 → S3Backend`) and four in read-only (`… → S3ReadonlyBackend → CapabilityS3 → …`). Two more allocations and two more dynamic calls is nothing against local I/O — the only workload this server has — but it is a real change to the hot path of a refactor whose brief was "behavior-preserving", and the chain is now deep enough that the next decorator should be a deliberate decision rather than a reflex.
13. **The state dir may be a different filesystem from the root.** That is the point (a local home beats fsyncing into a slow or read-only volume), but if the home itself is on NFS/overlay, redb's file locking and `fsync` semantics are weaker than the local case — untested territory, and a documented limitation rather than a supported configuration.
14. **The in-root containment check is lexical** (decision 17a): it compares `std::path::absolute(dir)` with the canonical root, so a state dir that is *itself* a symlink pointing into the root still escapes it. The comparison must also be made between like path shapes: `canonicalize` answers a Windows verbatim path (`\\?\C:\…`) while `absolute` answers the plain form, so comparing only the verbatim shape is a silent no-op on Windows — measured while implementing Task 9, which is why the check compares both. On Windows it is also case-insensitive, because NTFS is: a differently-cased in-root path would otherwise evade the guard (measured) — the relaxation is Rust's Unicode lowercasing applied per component, which is an approximation of NTFS's own equivalence and is not applied on unix, where the filesystems are case-sensitive. Resolving that would require canonicalizing a path that need not exist yet. The derived path has no such hole (its parents are refused as symlinks), and `TINIO_STATE_DIR`'s lack of checking is documented (decision 16).
15. **`TINIO_STATE_DIR` is used verbatim** — no 0700 parents, no symlink check, no containment proof beyond 17a. Deliberate: the caller chose the location. An operator who points it at a shared or enumerable directory has made that choice, and the `.env`-style footgun is the reason the variable is documented in `contracts/config.md` rather than left to the code.
