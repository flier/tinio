# `tinio-e2e` — the BDD acceptance suite

`tinio-e2e` is the workspace's end-to-end acceptance suite: cucumber scenarios driven over the wire against the in-process tinio server, and — for the `@external` scenarios — against a spawned `tinio` binary steered by third-party S3 clients. Its feature files are the executable form of `specs/001-s3-local-server/contracts/s3-surface.md`, which owns the S3 surface; every scenario carries spec traceability tags that a dedicated target cross-checks against the spec corpus, both directions. The canonical run commands and the full gate matrix live in `docs/tests.md`; this README owns the crate's layout, its tag taxonomy and its environment variables.

## Layout

| File | What it is |
|---|---|
| `tests/cucumber.rs` | The single cucumber binary (`harness = false`): installs the default tag filter and `fail_on_skipped()`. |
| `tests/features/` | The `.feature` files, one per domain — the spec's executable form; `interop/` holds the external-client journeys. |
| `tests/steps/` | The `World`, the `#[before]`/`#[after]` hooks, the domain step modules (`acl`, `buckets`, `cli`, `clients`, `common`, `conditions`, `doctor`, `errors`, `lifecycle`, `listing`, `multipart`, `objects`, `reserved_paths`, `select`, `versioning`) and `tag_filter.rs`, which composes the default filter. |
| `tests/traceability.rs` | The spec↔tag cross-check — a plain-harness target with no cucumber involvement. |
| `scripts/wsl-interop.sh` | One-command `@interop` run inside WSL2 (Linux-side aws-cli/rclone). |

`cucumber` is `harness = false`, so cargo does not run it through libtest and the binary owns its argv, while `traceability` is a plain libtest target that rejects cucumber's flags. **Every run that passes cucumber args must scope with `--test cucumber`** — unscoped args reach `traceability` and fail it with "Unrecognized option".

Step phrases are registered by the `given`/`when`/`then` attributes in `tests/steps/`; regex steps are unanchored, and a client command line is tokenized shell-style (a double-quoted segment stays one word) with `{work}` expanding to the scenario's scratch workdir and `{captured}` to the last captured client output. The features are the step reference: `tests/features/` shows the phrases in use and `tests/steps/` defines them.

## Tags

The vocabulary the table leans on — scenario-level vs feature-level tags, the `#[before]` mapping, the `@external` umbrella — is defined in `../../CONTEXT.md`; each row records what its tag does to a scenario.

| Tag | Effect |
|---|---|
| `@FR-xxx` `@SC-xxx` `@Txxx` | Spec traceability; cross-checked by the `traceability` target. |
| `@external` | Umbrella over the external-client tags below (the definition is `../../CONTEXT.md`'s); the default filter excludes every scenario carrying it unless `TINIO_E2E_EXTERNAL` is set. |
| `@interop` | External: aws cli v2 + rclone against a spawned `tinio`; CI-gated. |
| `@boto3` | External: boto3 SDK; best-effort tier (FR-025). |
| `@mc` | External: MinIO `mc`; best-effort tier (FR-025). |
| `@aws` `@rclone` | Which client a given `@interop` scenario drives. |
| `@fs` `@mem` `@pg` | Explicit backend, winning over `TINIO_E2E_BACKEND`; the traversal / nested-root / out-of-band scenarios ride `@fs`, while the mem and pg passes select by the env var. A `@pg` scenario needs a live PostgreSQL (`TINIO_PG_DSN`) and the `--features pg` build. |
| `@nested-root` | fs: served root = tempdir/`root` (traversal-proof scenarios). |
| `@checksum-on` | `caps.checksum = true` (checksum-validation scenarios). |
| `@minimal-caps` | Multipart / copy-object / list v1 / list v2 / delete-objects / tagging / cors / acl all off — the unsupported-op error scenarios. |
| `@tagging-off` | `caps.tagging = false` — the six `?tagging` ops answer `NotImplemented`. |
| `@cors` | The CORS scenario set (`cors.feature`, where it is also the feature-level tag); a selector only — no hook reads it, the capability toggle is `@cors-off`. |
| `@cors-off` | `caps.cors = false` — the three `?cors` ops answer `NotImplemented`, and the preflight route is not registered. |
| `@lifecycle` | The lifecycle corpus (`lifecycle.feature`, `lifecycle_cli.feature`, where it is also the feature-level tag): the `?lifecycle` trio and the pass (FR-042). A selector and a traceability tag — no exclusion axis names it. |
| `@read-only` | `PlaneConfig::read_only` — every mutating operation answers `AccessDenied` (FR-023); an `@external` scenario spells the same mode on its spawned server (`--read-only`). |
| `@acl` | The identity-wired ACL plane (`DataPlane::new_with_acl`, the config fixture's identity): the four ACL ops and the enforced authorization pipeline; the hook spawns this plane instead of the plain one. |
| `@acl-off` | `caps.acl = false` on the legacy no-identity plane — the four ACL ops answer `NotImplemented`; `config_from_tags` reads it, and it is not `@acl`'s branch. |
| `@versioning-off` | `caps.versioning = false` — the three `?versioning` ops answer `NotImplemented`, and a version-addressed request is refused outright rather than answered from the current version (the fail-open guard, FR-039). |
| `@cold-listing` | fs scanner interval 100 ms (cold-listing scenarios). |
| `@parquet` | Parquet input; needs `--features parquet` (the reader lives behind `tinio-server/select-parquet`) — excluded by the default filter when the feature is off. |
| `@max-buckets-3` | `caps.max_buckets = 3` (ListBuckets pagination scenarios). |
| `@cli` | The scenarios that spawn the real `tinio` binary and observe it from outside (`cli_lifecycle.feature`, `doctor.feature`, plus `confinement.feature`'s sandbox channels): the hook spawns nothing for them — no in-process server, no wire client. `{root}`/`{state}` in a step's command line expand to the scenario's scratch root and relocated state dir. |
| `@linux` `@macos` `@windows` `@unix` | Platform-bound: the scenario runs where the host's target matches at least one of its platform tags (`@unix` is both unix platforms), and the default filter excludes it where none matches — its step definitions exist only where it may run, and an undefined step is a failure under `fail_on_skipped()`. |
| `@confinement` | Host-bound: the scenario's server runs a real confinement. Both families carry it — `confinement.feature`'s `@cli` scenarios and the external clients' confined legs, where the tag becomes the CLI's `--sandbox` (plus `--read-only` when the scenario also carries `@read-only`). The default filter selects it on macOS and excludes it elsewhere; a forced run is serial. |

## The default filter

With no `--tags`, the runner composes `CUCUMBER_FILTER_TAGS` (`tests/steps/tag_filter.rs`) from five independent axes: `@parquet` is excluded unless the binary was built `--features parquet`; a scenario carrying platform tags is excluded where the host's target matches none of them; the `@external` umbrella is excluded unless `TINIO_E2E_EXTERNAL` is set (any value, `0` included — the axis tests presence, not the value); `@confinement` follows `TINIO_E2E_CONFINEMENT`; and `@pg` is always excluded, whatever the host — the `@pg` pass is its own explicit-`--tags` run against a live database (PG17).

**An explicit `--tags` replaces the default filter rather than adding to it**, so a tagged run re-states every exclusion it wants. The platform axis needs care in a hand-written list: the runner's clause keeps a scenario carrying *any* matching platform tag, which a list of `not <tag>` clauses cannot express — so the backend-neutral lists below exclude every platform tag instead.

## Runs

```console
cargo build -p tinio                             # the @cli scenarios spawn this binary
cargo test -p tinio-e2e --test cucumber -- --tags '@cli and not @unix and not @windows and not @linux and not @macos and not @confinement'
cargo test -p tinio-e2e --test cucumber -- --tags '@interop and not @confinement' --retry 1
cargo test -p tinio-e2e --test cucumber -- --tags '@boto3 and not @confinement'
cargo test -p tinio-e2e --test cucumber -- --tags '@mc and not @confinement'
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'
TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036 and not @unix and not @windows and not @linux and not @macos' --concurrency 1
TINIO_E2E_CONFINEMENT=1 TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and (@interop or @boto3 or @mc)' --concurrency 1
TINIO_PG_DSN='postgres://…/tinio?sslmode=disable' cargo test -p tinio-e2e --features pg --test cucumber -- --tags 'not @fs and not @unix and not @windows and not @linux and not @macos and not @parquet and not @interop and not @boto3 and not @mc and not @confinement' --concurrency 1
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @unix and not @windows and not @linux and not @macos and not @parquet and not @interop and not @boto3 and not @mc and not @confinement and not @pg'
cargo test -p tinio-e2e --test traceability
```

- **The `@pg` pass is serial** (`--concurrency 1`): every scenario truncates the one shared database before it runs, so nothing in the harness enforces it and a hand-run without it produces confusing cross-scenario resets.
- **The `@confinement` family is opt-in off macOS and serial when it runs.** Force it in with `TINIO_E2E_CONFINEMENT=1` only on a host that can actually confine (WSL2 with an unprivileged userns, or a Windows host with a console and a writable volume) — no CI leg does, and its real gate is the platform script on a host that owes the precondition (`docs/tests.md`, "Platform-gated verification"). One confined run per scenario pays a real confinement grant, so `--concurrency 1` is the spelling to use. The external clients have a confined arm too: the `interop/*.feature` legs tagged `@confinement` run the same client journeys through a spawned `--sandbox` server (and `--read-only --sandbox` for the `@read-only` ones), asserting each client's own refusal shape, and need the family's opt-ins plus `TINIO_E2E_EXTERNAL` set.
- **The `@cli` scenarios spawn the real binary**, so it must be built before the run — `assert_cmd::cargo::cargo_bin` resolves `<profile>/tinio` beside the test binary and builds nothing.
- **Missing clients fail in the hook** with a hint naming the tool and the way out: `@interop` needs `aws` **and** `rclone` on PATH — both, even for a scenario that drives only one of them — `@mc` needs `mc`, and `@boto3` needs the venv python (`TINIO_BOTO3_PYTHON`).
- `--retry 1` retries failed scenarios once (the CI `@interop` run uses it against external-client flakes); a filter that matches nothing runs 0 scenarios and exits 0; tag expressions are cucumber's `and`/`or`/`not` syntax with parentheses.

## Environment

| Var | Effect |
|---|---|
| `TINIO_E2E_BACKEND` | `mem`/`pg` run the backend-neutral scenarios on that backend (CI's mem and pg passes); explicit `@fs`/`@mem`/`@pg` scenario tags win over it; default `fs`. |
| `TINIO_E2E_EXTERNAL` | Set it to any value — presence is what counts, `0` included — to disable the default `@external` exclusion (the `@parquet` exclusion is unaffected — it follows the cargo feature; an explicit `--tags` still wins). |
| `TINIO_E2E_CONFINEMENT` | Three-state switch for the `@confinement` gate: unset is the platform default (kept on macOS, excluded elsewhere), `1`/`true` forces the family in on every target and `0`/`false` forces it out on every target, macOS included; any other value stops the run rather than defaulting. Force it in only on a host that can confine, and note that an explicit `--tags` still wins (the variable is not read at all then). |
| `TINIO_E2E_REPORT` | Path of the Cucumber-JSON report written alongside the pretty stdout (CI uploads it for the PR test report); a bare filename lands in `crates/tinio-e2e/`, cargo's cwd for a test binary. |
| `TINIO_BOTO3_PYTHON` | Override for the boto3 venv python. Default: `<target-dir>/tinio-e2e-venv/bin/python3` (Linux) / `Scripts\python.exe` (Windows). |

`fail_on_skipped` is always on (`../../CONTEXT.md`) — so a hand-picked `--tags` must select only scenarios whose steps exist in this build and on this host; anything else fails the run rather than skipping quietly.

## Client tiers (FR-025)

| Tier | Clients | Gating |
|---|---|---|
| Mandated | aws cli v2, rclone | CI-gated (the interop stage) |
| Best-effort | boto3, mc | targeted / manual — never CI; promoting them into CI needs an FR-025 amendment |

### Scenarios → tools

Which client covers which basic scenario — the FR-025 coverage claim at a glance (T036). `✓` marks a scenario that drives that client, with the client's own spelling of the operation where it has one.

| Scenario | aws cli v2 | rclone | boto3 | mc |
|---|---|---|---|---|
| Create bucket | ✓ | ✓ (mkdir) | ✓ | ✓ (mb) |
| Delete bucket (empty) | ✓ (rb) | ✓ (purge) | — | ✓ (rb) |
| Upload object | ✓ (cp) | ✓ (copy) | ✓ | ✓ (cp) |
| Download (byte-identical) | ✓ | ✓ | ✓ | ✓ |
| List full / prefix / delimiter | ✓ | ✓ (lsf) | ✓ | ✓ (ls) |
| Zero-byte object | ✓ | ✓ | ✓ | ✓ |
| Multipart (> 8 MiB, composed ETag) | ✓ | ✓ | ✓ (upload_file) | ✓ |
| Server-side copy | ✓ | ✓ | — | — |
| Delete object (idempotent) | ✓ | ✓ (delete) | ✓ | ✓ (rm) |
| Bucket-not-empty on delete | ✓ | — | — | ✓ (rb --force) |
| Auth (SigV4) | ✓ (wrong-credential refusal) | ✓ (credentialed remote; no refusal leg) | — | ✓ (alias) |
| Cold listing w/ and w/o scanner | ✓ (`advanced.feature`) | — | — | — |
| Ephemeral port (`--port 0`) | ✓ (`journey.feature`) | — | — | — |
| Version history (overwrite, `?versions`, marker, restore, purge) | ✓ (`journey.feature`) | — | ✓ (`boto3_journey.py`) | — |

### Known deviations (v1)

- `x-amz-checksum-*` headers are accepted and ignored (v1 has no checksum verification).
- `x-amz-meta-*` user metadata is accepted and dropped (not stored, not returned).
- Content-Type is inferred from the extension at serve time, not stored.
- ETags: `"<md5>"` for single uploads, `"<md5>-N"` for multipart (the AWS composed form).
- Trailing-slash keys (`key/`) are not stored as objects: the fs backend maps them to directories under the bucket root (put answers with an empty-body ETag, head 404s).
- `head-object` on a missing key answers with the raw `404` code, not AWS's `NoSuchKey`.
- SigV2 is disabled by default (aws cli v2 and rclone always use SigV4).

### Unsupported clients (v1, per FR-025)

Clients that require virtual-hosted-style addressing or features outside the v1 surface (s3cmd in some configurations, WinSCP, CloudBerry) are unsupported — path-style addressing is the only mode.

## Gates

```console
cargo test -p tinio-e2e
```
