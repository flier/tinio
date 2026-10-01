# tinio-e2e

BDD acceptance suite: cucumber scenarios over the in-process tinio server, and — for the
`@external` scenarios — over a spawned `tinio` binary driven by third-party clients. The
feature files are the executable form of `specs/001-s3-local-server/contracts/s3-surface.md`;
every scenario carries spec traceability tags (`@FR-xxx` / `@SC-xxx` / `@Txxx`) that a
dedicated test target cross-checks against the spec corpus (both directions).

## Layout

| Path | Role |
|---|---|
| `tests/cucumber.rs` | The single cucumber test binary (`[[test]] cucumber`, `harness = false`): sets the default tag filter, picks the writers, `fail_on_skipped()`. |
| `tests/features/` | The `.feature` files (specs, one per domain; `interop/` holds the external-client features). |
| `tests/steps/` | The `World` + domain-organized step modules (`buckets`, `cli`, `clients`, `common`, `conditions`, `errors`, `listing`, `multipart`, `objects`, `reserved_paths`) and the `#[before]`/`#[after]` hooks (the `metrics`/`tagging` scenarios ride the generic `common` request steps; `cli` spawns the real binary). |
| `tests/traceability.rs` | The spec↔tag cross-check — a plain-harness test target, no cucumber involvement. |
| `scripts/wsl-interop.sh` | One-command `@interop` run inside WSL2 (Linux-side aws-cli/rclone). |

## harness = false, and the `--test cucumber` scoping rule

`cucumber` is `harness = false`: cargo does not run it through libtest, the binary owns its
argv. The second target, `traceability`, is a plain-harness libtest binary. **Every run that
passes cucumber args must scope with `--test cucumber`** — the args would otherwise reach the
`traceability` target, which rejects `--tags`/`--retry` with "Unrecognized option". The
traceability check runs as:

```
cargo test -p tinio-e2e --test traceability
```

## Tag taxonomy

Scenario-level tags are read by the `#[before]` hook (`config_from_tags` — one mapping shared
by the in-process server and the `@external` spawn); feature-level tags are inherited by tag
*filters* but are invisible to the hooks.

| Tag | Level | Effect |
|---|---|---|
| `@FR-xxx` `@SC-xxx` `@Txxx` | feature, some scenario | Spec traceability; cross-checked by the `traceability` target. |
| `@interop` | scenario | External: aws cli v2 + rclone against a spawned `tinio`; CI-gated. |
| `@boto3` | scenario | External: boto3 SDK scenarios; manual tier (FR-025). |
| `@mc` | scenario | External: MinIO mc scenarios; manual tier (FR-025). |
| `@aws` `@rclone` | scenario | The client a specific `@interop` scenario drives. |
| `@fs` | scenario | Explicit fs backend (wins over `TINIO_E2E_BACKEND`); traversal / nested-root / out-of-band scenarios. |
| `@mem` | scenario | Explicit mem backend (wins over `TINIO_E2E_BACKEND`); currently no feature carries it — the CI mem pass uses the env var. |
| `@nested-root` | scenario | fs: served root = tempdir/`root` (traversal-proof scenarios). |
| `@checksum-on` | scenario | `caps.checksum = true` (checksum-validation scenarios). |
| `@minimal-caps` | scenario | Multipart / copy-object / list v1 / list v2 / delete-objects / tagging / cors all off (unsupported-op error scenarios). |
| `@tagging-off` | scenario | `caps.tagging = false` (the six `?tagging` ops answer `NotImplemented`). |
| `@cors` | scenario | The CORS scenario set (`cors.feature`, where it is also the feature-level tag); a selector only — no hook reads it, and the capability toggle is `@cors-off`. |
| `@cors-off` | scenario | `caps.cors = false` (the three `?cors` ops answer `NotImplemented`; the preflight route is not registered). |
| `@read-only` | scenario | `PlaneConfig::read_only` — every mutating operation answers `AccessDenied` (FR-023). An `@external` scenario spells the same mode on its spawned server (`--read-only`; `steps/clients.rs`). |
| `@acl` | scenario | The identity-wired ACL plane (`DataPlane::new_with_acl`, the config fixture's identity): the four ACL ops and the enforced authorization pipeline; read by the `#[before]` hook, which spawns this plane instead of the plain one. |
| `@acl-off` | scenario | `caps.acl = false` on the legacy no-identity plane — the four ACL ops answer `NotImplemented`; read by `config_from_tags`, and not `@acl`'s branch. |
| `@cold-listing` | scenario | fs scanner interval 100 ms (cold-listing scenarios). |
| `@parquet` | scenario | Parquet input; needs `--features parquet` (the reader lives behind `tinio-server/select-parquet`) — excluded by the default filter when the feature is off. |
| `@max-buckets-3` | scenario | `caps.max_buckets = 3` (ListBuckets pagination scenarios). |
| `@cli` | scenario | The CLI scenarios (`cli_lifecycle.feature`, `doctor.feature`, plus `confinement.feature`'s sandbox channels): they spawn the real `tinio` binary and observe it from outside, so the `#[before]` hook spawns nothing for them — no in-process server, no wire client. The binary must be built first (`cargo build -p tinio`); `{root}`/`{state}` in a step's command line expand to the scenario's scratch root and relocated state dir. |
| `@linux` `@macos` `@windows` `@unix` | scenario | Platform-bound: a scenario runs where the host's target matches at least one of its platform tags (`@linux`/`@macos`/`@windows` are one platform each, `@unix` is both unix platforms), and the default filter excludes it where none matches — the step definitions exist only on the platform it may run on, and an undefined step is a failure under `fail_on_skipped()`. They live in `confinement.feature` (plus `cli_lifecycle.feature`'s `@unix` notify leg). |
| `@confinement` | scenario | Host-bound: the scenario's server runs a real confinement. Two families carry it: `confinement.feature`'s `@cli` scenarios, and the external clients' confined legs in `interop/*.feature` (where `steps/clients.rs`'s `Confinement` turns the tag into the CLI's `--sandbox`, plus `--read-only` when the scenario also carries `@read-only`). The runner's default filter is what selects them (`tests/steps/tag_filter.rs` composes it and `tests/cucumber.rs` installs it as `CUCUMBER_FILTER_TAGS` — on macOS by default, excluded elsewhere, with `TINIO_E2E_CONFINEMENT`'s explicit states overriding the platform default on any target; the external legs also need `TINIO_E2E_EXTERNAL=1`), and CI's interop `--tags` excludes them explicitly. Their real gate is the platform script on a host that owes the precondition (`docs/tests.md`, "Platform-gated verification"). |

The **`@external` umbrella** is `@interop` ∪ `@boto3` ∪ `@mc`.

## Running

```
cargo test -p tinio-e2e                          # in-process suite only (default)
cargo test -p tinio-e2e --test cucumber -- --tags '@cli and not @unix and not @windows and not @linux and not @macos and not @confinement'  # the platform-neutral CLI process scenarios
cargo test -p tinio-e2e --test cucumber -- --tags '@interop and not @confinement' --retry 1  # the unconfined tier; the confined legs are opt-in (below)
cargo test -p tinio-e2e --test cucumber -- --tags '@boto3 and not @confinement'  # the unconfined tier, as above
cargo test -p tinio-e2e --test cucumber -- --tags '@mc and not @confinement'
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'  # Parquet input
cargo test -p tinio-e2e --test traceability      # spec↔tag cross-check
```

- **The default run excludes what this build cannot serve**: `cargo test -p tinio-e2e` without `--tags` gets a runner-composed `CUCUMBER_FILTER_TAGS` (`tests/steps/tag_filter.rs` owns the composition) from four independent axes — `@parquet` is excluded unless the binary was built `--features parquet`; a scenario carrying platform tags is excluded where the host's target matches none of them, so `@linux`/`@macos`/`@windows` are each bound to their own platform and `@unix` to both unix platforms (those step definitions exist only where they may run); the `@external` umbrella is excluded unless `TINIO_E2E_EXTERNAL=1` is set; and `@confinement` follows `TINIO_E2E_CONFINEMENT`'s three states — unset is the platform default (kept on macOS, excluded elsewhere), `1`/`true` keeps the family on every target and `0`/`false` excludes it on every target, macOS included; any other value stops the run rather than defaulting.
- **An explicit `--tags` replaces the default filter** — a tagged run must re-state all four exclusions. The platform axis needs care in a hand-written list: the runner's clause keeps a scenario carrying any matching platform tag, which a list of `not <tag>` clauses cannot express, and one shared list cannot be the clause of three different runners. CI's mem pass therefore excludes every platform tag: `--tags 'not @fs and not @unix and not @windows and not @linux and not @macos and not @parquet and not @interop and not @boto3 and not @mc and not @confinement'` with `TINIO_E2E_BACKEND=mem` (it runs on the ubuntu, windows and macos runners).
- **The `@confinement` family is opt-in off macOS, and serial when it runs.** Force it in (`TINIO_E2E_CONFINEMENT=1`) only on a host that can actually confine (WSL2 with an unprivileged userns, or a Windows host with a console and a writable volume) — no CI leg does: ubuntu's AppArmor default refuses the userns, a hosted Windows runner owes no ACE-grant budget, and either would make the leg permanently red, which certifies nothing. macOS needs no opt-in (the launcher only binds and spawns there), and CI's macOS `e2e-port` row is the variable's one CI use — the `0` state, forcing the family out of the parallel default pass so the serial `--tags` step is its only run there. `--concurrency 1` is the spelling to use: the family timed out twice at the default concurrency while the harness still had its 30 s bound, and one launcher run's grant alone is tens of seconds (the six timings, and what they do not prove, are in `docs/tests.md`, "Platform-gated verification (Windows)"). `TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036 and not @unix and not @windows and not @linux and not @macos' --concurrency 1` — the list is the platform-neutral one, because a recipe that names no host cannot spell the runner's platform clause (which keeps a scenario carrying any matching platform tag); the host-specific spelling of the axis is the platform gates, which both scripts carry (`crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`, `crates/tinio-sandbox-windows/scripts/verify-windows.ps1`).
- **The external clients have a confined arm too** (G19–G22): the `interop/*.feature` legs tagged `@confinement` run the same client journeys through a spawned `--sandbox` server — and for the `@read-only` ones through `--read-only --sandbox`, asserting each client's own refusal shape. They need the same opt-ins as the rest of the family *plus* the external one: `TINIO_E2E_CONFINEMENT=1 TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and (@interop or @boto3 or @mc)' --concurrency 1` (the client binaries on PATH and the boto3 venv, as ever — a missing one fails in the hook with its hint). CI never runs them: the interop job's `--tags` excludes `@confinement` explicitly. One confined run per scenario is what makes them slower than the rest of the external tier: on Windows each one pays the launcher's ACE grant.
- **The `@cli` scenarios spawn the real binary**, so it has to be built before the run
  (`cargo build -p tinio`) — `assert_cmd::cargo::cargo_bin` resolves `<profile>/tinio` beside the
  test binary and builds nothing. Both CI e2e jobs build it first.
- `--retry 1` retries failed scenarios once (the CI `@interop` run uses it to mitigate
  external-client flakes).
- A filter that matches nothing (`--tags @nonexistent`) runs 0 scenarios and exits 0.
- Tag expressions are the cucumber tag-expression syntax: `and`/`or`/`not`, parentheses.

**Missing-client panics.** The `#[before]` hook presence-checks the external tools; a scenario
whose client is missing fails in the hook with a hint naming the tool and the way out:
`@interop` needs `aws` and `rclone` on PATH ("run them in WSL2 or filter them out"), `@mc`
needs `mc` on PATH, `@boto3` needs the venv python (see `TINIO_BOTO3_PYTHON`). The `@interop`
check is per scenario tag and demands BOTH `aws` AND `rclone` on PATH — an `@interop` scenario
that only drives `aws` (e.g. the invalid-credentials and ephemeral-port edge legs) still fails
the presence check when `rclone` is missing.

## Environment variables

| Var | Effect |
|---|---|
| `TINIO_E2E_BACKEND` | `mem` runs the backend-neutral scenarios on the mem backend (CI mem pass). Explicit `@fs`/`@mem` scenario tags win over it; default `fs`. |
| `TINIO_E2E_EXTERNAL` | Set to `1` to disable the default `@external` exclusion (the `@parquet` exclusion is unaffected — it follows the cargo feature; an explicit `--tags` still wins). |
| `TINIO_E2E_CONFINEMENT` | Three-state switch for the `@confinement` gate: unset is the platform default (kept on macOS, excluded elsewhere), `1`/`true` forces the family in on every target and `0`/`false` forces it out on every target, macOS included (CI's macOS parallel pass). Only a host that can build the confinement should force it in (see the tag table); any other value stops the run, never defaults. An explicit `--tags` still wins (and the variable is not read at all then). |
| `TINIO_E2E_REPORT` | Path of the Cucumber-JSON report written alongside the pretty stdout (CI uploads it for the PR test report). A bare filename lands in `crates/tinio-e2e/` — cargo runs test binaries with the package root as cwd. |
| `TINIO_BOTO3_PYTHON` | Override for the boto3 venv python. Default: `<target-dir>/tinio-e2e-venv/bin/python3` (Linux) / `Scripts\python.exe` (Windows). |

`fail_on_skipped()` is always on: undefined or otherwise skipped steps fail the run — a
feature that drifts out of sync with its step definitions never passes silently.

## Step vocabulary

Steps are defined per domain in `tests/steps/` and registered by the `given`/`when`/`then`
attribute macros. Regex steps are unanchored; client command lines are tokenized shell-style
(double-quoted segments stay one word) with `{work}` expanding to the scenario's scratch
workdir and `{captured}` to the last captured client output. Two phrases differ from the
migration plan's vocabulary table — the implemented ones are:

- `the listing shows {int} key(s)` (not "keys")
- `the scratch file "{a}" equals the scratch file "{b}"` (not "the file … equals the uploaded
  bytes" — upload source and download target are both scratch files named in the scenario)

The features themselves are the step reference: read `tests/features/` to see the phrases in
use, and `tests/steps/` for their definitions.

## FR-025 tiering matrix

Folded in from the deleted `e2e/interop/README.md` (the bash interop scripts it documented
were replaced by the cucumber `@interop`/`@boto3`/`@mc` features).

### Tiers

| Tier | Clients | Gating |
|---|---|---|
| Mandated | aws cli v2, rclone | CI-gated (`.github/workflows/ci.yml` interop stage) |
| Best-effort | boto3, mc | Targeted/manual — NOT CI-gated (FR-025); promoting them into CI requires an FR-025 amendment |

### Scenarios → tools

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
| Auth (SigV4) | US3 (auth.sh) | US3 | — | ✓ (alias) |
| Cold listing w/ and w/o scanner | ✓ (interop/advanced.feature) | — | — | — |
| Ephemeral port (`--port 0`) | ✓ (interop/journey.feature) | — | — | — |

### Known deviations (documented, v1)

- `x-amz-checksum-*` headers are accepted and ignored (v1 has no checksum verification).
- `x-amz-meta-*` user metadata is accepted and dropped (not stored, not returned).
- Content-Type is inferred from the extension at serve time (`mime_guess`), not stored.
- ETags: single uploads `"<md5>"`, multipart `"<md5>-N"` (AWS composed form).
- Trailing-slash keys (`key/`) are not stored as objects: the fs backend maps them to
  directories under the bucket root (put answers with an empty-body ETag, head 404s).
- `head-object` on a missing key answers with the raw `404` code, not AWS's `NoSuchKey`.
- SigV2 is disabled by default (aws cli v2 and rclone always use SigV4).

### Unsupported clients (v1, per FR-025)

Clients that require virtual-hosted-style addressing or features outside the v1 surface (e.g.
s3cmd in some configurations, WinSCP, CloudBerry) are not supported — path-style addressing is
the only mode.

## WSL2 (local-dev convenience, not a CI substitute)

The `@interop` scenarios need Linux-side aws-cli/rclone — the same tools CI uses. One command
from inside WSL2 at the repo checkout (`/mnt/e/GitHub/tinio`):

```
bash crates/tinio-e2e/scripts/wsl-interop.sh
```

The script checks `aws` and `rclone`, points `CARGO_TARGET_DIR` at `$HOME/tinio-target` when run from `/mnt/*` (ext4 build artifacts — the Windows build stays untouched), and runs `cargo test -p tinio-e2e --test cucumber -- --tags '@interop and not @confinement' --retry 1 "$@"` with the Linux cargo toolchain — the same filter the CI interop job uses, so the script stays the unconfined mirror of CI. Extra args (e.g. `--tags 'not @interop and @rclone'`) are forwarded; a forwarded `--tags` replaces the script's own filter (last one wins), which is how the confined legs are run deliberately (`--tags '@confinement and (@interop or @boto3 or @mc)'`).
