# CONTEXT.md

Domain glossary for the tinio workspace. English only. This file is a glossary — it records the meaning of terms, not implementation details.

## BDD test suite (cucumber)

- **Feature file** — a Gherkin `.feature` document; the executable form of a contract in `specs/001-s3-local-server`.
- **Scenario** — one behavior example in a feature file, composed of Given/When/Then steps; the atomic test unit.
- **Scenario outline / Examples** — a parameterized scenario template with data rows.
- **Step / step definition** — one Given/When/Then line in a feature and the Rust function that executes it.
- **World** — the per-scenario shared state: the server under test and the last response.
- **Spec scenario** — a scenario that carries a spec ID (`@FR-xxx` / `@SC-xxx` / `@Txxx`) and documents a contract behavior.
- **Spec traceability tags** — the `@FR-xxx`/`@SC-xxx`/`@Txxx` tags linking a scenario to the spec; feature-level (inherited by tag filters, invisible to the hooks).
- **Scenario-level tags** — read by the `#[before]` hook via `config_from_tags`, shared by the in-process and `@external` spawns.
- **Backend scenario tags** — `@fs`/`@mem`/`@pg`/`@tier`; the hook maps the tag to the backend under test.
- **Other scenario tags** — `@nested-root`, `@checksum-on`, `@minimal-caps`, `@cold-listing`, `@max-buckets-3`.
- **External-client tags** — `@interop`/`@boto3`/`@mc`; `@aws`/`@rclone` mark the driven client (not read by the hook).
- **Backend-neutral scenario** — no backend tag; runs on the default backend and in every backend pass.
- **Backend-tagged scenario** — carries `@fs`, `@mem`, `@pg` or `@tier`; runs only on that backend.
- **Tag precedence** — an explicit backend tag wins over `TINIO_E2E_BACKEND`, which wins over the fs default.
- **Backend pass** — a full run of the backend-neutral suite on one backend.
- **fs pass** — the default run, with no environment variable set.
- **mem pass** — `TINIO_E2E_BACKEND=mem`, excluding `@fs`-only scenarios.
- **pg pass** — `TINIO_E2E_BACKEND=pg`, serialized, in the PostgreSQL job.
- **tier pass** — `TINIO_E2E_BACKEND=tier` with `--features tier`, serialized, excluding `@pg` and the external clients.
- **@external umbrella** — the union of `@interop` ∪ `@boto3` ∪ `@mc`; scenarios that need external client binaries and are excluded from default runs.
- **fail_on_skipped** — the runner's always-on mode: undefined or otherwise skipped steps fail the run, so a feature that drifts out of sync with its step definitions never passes silently.
- **Traceability check** — `cargo test -p tinio-e2e --test traceability`: cross-references the spec corpus and the feature tags both ways (with the documented `NOT_COVERED_BY_CUCUMBER` allow-list).
- **Parity** — a migrated test's feature scenarios cover the old test 1:1; the old file is deleted only after parity holds.
- **Deletion rhythm** — port → run old and new side by side → verify 1:1 coverage → delete the old file in-tree.

## Testing concepts

- **Conformance harness** — the `tinio-util` `assert_conformance` suite, Rust-only; proves every `Storage` implementation meets the storage contract. Cucumber scenarios never repeat its assertions.
- **Migration rule** — a test migrates iff it carries a spec semantic AND its behavior is observable through a product surface (the S3 API, the CLI/process surface, or the product's own tracing/metrics).
- **Interop** — scenarios that drive external clients (aws-cli, rclone, boto3, mc) against a spawned server; `@interop` is CI-gated, `@boto3`/`@mc` are manual.

## Storage & server

- **Served root** — the directory a server instance serves; the fs backend keeps its state and buckets inside it, and never writes outside it.
- **Synthetic root** — the pg run's stand-in for a served root: `~/.local/state/tinio/pg/<hash of the dsn's credential-free identity>/` (identity = `scheme://host:port/database`; credentials and query dropped, so a password rotation does not move it), or `TINIO_STATE_DIR` when set, so config discovery, the state dir and the CLI surface work unchanged though a pg run serves no directory; the family leaves `.tinio` alone so a pg start cannot make `$HOME` a discoverable root; a positional `DIR` under `[storage] type = "pg"` is a usage error.
- **State dir** — the fs backend's metadata database and temp areas: `.tinio/` inside the served root in normal mode; in read-only mode it stays the root's own `<root>/.tinio/`, opened read-only (its `meta.redb` is the storage's metadata source), while only the run's **tool state** relocates to the home state dir (see Read-only mode).
- **Scanner** — the fs backend's background sweeper (temp cleanup, interrupted-upload cleanup); "cold listing" exercises listing before the scanner has caught up.
- **Capability toggle** — a server configuration switch that enables/disables an S3 operation or feature (multipart, copy, list v1/v2, delete-objects, checksum validation).
- **Read-only mode** — the served plane's write policy (FR-023): every mutating S3 operation answers `AccessDenied` and the storage root is never written, so a genuinely read-only filesystem can be served. **Tool state** — state file, control socket, logs, auto-created config — relocates from `<root>/.tinio/` to `~/.tinio/roots/<hash>/` (`TINIO_STATE_DIR` overrides the location), while the storage's metadata source stays the root's own `<root>/.tinio/meta.redb`, opened read-only: metadata written by an earlier read-write run — tags, ACLs, CORS, upload records and the version history — is served, and an open that fails (no database, a corrupt one, a format this build refuses, or a read-write server holding the same root) degrades the run to serving current versions with an empty metadata view.

## Tiered storage

- **Tier** — one content plane of `[storage] type = "tier"`: the mem cache (hot), the filesystem (warm, durable) or PostgreSQL (cold); `tinio-tier` layers them over the existing planes.
- **Absent** — a demoted object's cold representation: the filesystem file is gone, the row and `ColdInfo` remain; no sparse file, no mode switch.
- **ColdInfo** — the cold metadata authority for a pg-placed object (key, size, last-modified, ETag, tags, checksum, owner, ACL, version identity, version-recording flag); HEAD, GET info, tags, ACL and listings answer from it, and the fs row is its mirror.
- **TierRows** — the fs crate's row-only seam, shipped as `tinio_fs::tier::Rows` (`info`, `facts` → `tier::Facts`, `set_row`, `parts`, bounded `scan`) behind `FsStorage::tier_rows()`; no content I/O, no change outside tier mode.
- **Placement** — one key's index entry: tier (`fs`/`pg`), state (`live`/`copying`/`deleting`), the `ColdInfo` when cold, and an mtime that is state-start for a transient state, completion for a settled pg placement, or the object's mtime when warm.
- **Warm candidate** — a warm row in the index's `warm` table, mtime-ordered, that the capacity pass may demote down to the low watermark; oldest-mtime candidates lead.
- **Idle demotion** — the mover stage demoting a warm object unread for `demote_idle`, oldest first, apart from capacity moves; zero `demote_idle` disables it. Durable heat rows are retained for `max(heat_window, demote_idle)` so a read inside the idle threshold stays visible.
- **Quarantine** — a buffered write whose flush keeps failing, set aside under the tier state dir with the metadata that recovers it, so one poisoned entry never wedges a later flush, a listing or a shutdown; verify reports an entry and the destructive repair clears it.
- **Copying** — the placement state recorded before a demotion publishes to pg, so GC skips in-flight content.
- **Tier marker** — `<root>/.tinio/tier.mode`, recording the root ID; it marks the root as tier-owned and blocks a non-tier start and `tinio doctor`.
- **Prepare-downgrade** — `tinio tier prepare-downgrade`: promote every cold object, remove the index and marker, verify the root is fs-only; required before an older binary serves the root. It has no `--adopt`: a lost index is bound with `tinio tier repair --adopt` first, then the downgrade is retried.

## Persistence contracts

- **Metadata contract** — the four `tinio-core` traits every backend implements over its own transport: `Metadata` (the row error, aggregating `BucketMeta`/`ObjectMeta`/`UploadMeta`), `VersionMeta` (version rows, the bucket's versioning state, the key's current slot) and `ObjectContent` (object and part bytes); `BlockStore` is the block plane beside them. Used generically, never as `dyn`.
- **Backend handle** — the shared implementation a consumer holds concretely and delegates to (`RedbMetadata`, `RedbContent`); the consumer maps the contract error at its facade and never redeclares a row.
- **Visibility point** — `ObjectContent::publish` returns only once the bytes are visible; the metadata row that describes them is written after it. A staged write is invisible by construction.
- **Byte accounting decorator** — the `ObjectContent` wrapper that owns a backend's byte limits and total (`CountingContent`), so no operation body counts bytes itself.

## The ops engine

- **Ops engine** — the `tinio-ops` layer: the S3 operation bodies the backends share, written once over the persistence contracts and called from each backend's own `Storage`/`VersionOps` implementations. It is not a `Storage` implementation and not a framework — a backend keeps its error enum, its `Hooks`, its locks and its `Cleanup`.
- **Group receiver** — the per-group value an operation body hangs off (`BucketOps`, `ObjectOps`, `MultipartOps`, with the version plane beside them for the bodies that touch versions); it carries only the handles its group uses, and `mem` builds no version plane at all.
- **Hooks seam** — the two-method trait a backend implements for the steps only it can take: the bucket-delete action and the per-upload part lock. A third method needs a named consumer, a spec amendment and the operator's ruling.
- **The two policies** — `MetadataFailure` (what a metadata-row write that fails after the content is published means) and `BucketRow` (whether object rows require a bucket row); declared per backend and read by the bodies, never inferred.
- **Read-heal** — the engine's rule that a read or listing whose row is absent or stale against the content recomputes the row from the content and persists it, so a file the scanner has not recorded is still served.

## CLI and management plane

- **Management plane** — the control surface beside the S3 data plane (FR-018): one local channel (a unix socket or a Windows named pipe) or one configured TCP HTTP(S) listener, carrying `/status`, `/stop`, `/metrics` and `/openapi.json` under the token rule its bind address selects.
- **State file** — the plane's record of a running server, published in the state dir at readiness and removed by a graceful stop: version, PID, endpoint, token, root and the control channel's name. Clients read it instead of re-deriving any of those, which is what keeps `status`'s answer the server's rather than the client's reconstruction.
- **Root discovery** — how a command finds the served root: the positional `DIR` when one is given, else a walk up from the current directory to the nearest ancestor holding `.tinio/`. A root that cannot be found is a usage error naming the remedy, never an in-memory server.
- **Sandbox channel** — each of the three ways confinement is asked for: the `--sandbox` flag, the `TINIO_SANDBOX` variable and the `[server] sandbox` key, resolved through one precedence chain, so any one of them is enough and an explicit `false` at a higher level beats a `true` below it. A request the build or the platform cannot honour is refused — a silent unconfined serve is the failure the request exists to prevent.
