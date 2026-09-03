# tinio-e2e

BDD acceptance suite: cucumber scenarios over the in-process tinio server, and — for the
`@external` scenarios — over a spawned `serve` binary driven by third-party clients. The
feature files are the executable form of `specs/001-s3-local-server/contracts/s3-surface.md`;
every scenario carries spec traceability tags (`@FR-xxx` / `@SC-xxx` / `@Txxx`) that a
dedicated test target cross-checks against the spec corpus (both directions).

## Layout

| Path | Role |
|---|---|
| `tests/cucumber.rs` | The single cucumber test binary (`[[test]] cucumber`, `harness = false`): sets the default tag filter, picks the writers, `fail_on_skipped()`. |
| `tests/features/` | The `.feature` files (specs, one per domain; `interop/` holds the external-client features). |
| `tests/steps/` | The `World` + domain-organized step modules (`buckets`, `clients`, `common`, `conditions`, `errors`, `listing`, `multipart`, `objects`, `reserved_paths`) and the `#[before]`/`#[after]` hooks (the `metrics`/`tagging` scenarios ride the generic `common` request steps). |
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
| `@interop` | scenario | External: aws cli v2 + rclone against a spawned `serve`; CI-gated. |
| `@boto3` | scenario | External: boto3 SDK scenarios; manual tier (FR-025). |
| `@mc` | scenario | External: MinIO mc scenarios; manual tier (FR-025). |
| `@aws` `@rclone` | scenario | The client a specific `@interop` scenario drives. |
| `@fs` | scenario | Explicit fs backend (wins over `TINIO_E2E_BACKEND`); traversal / nested-root / out-of-band scenarios. |
| `@mem` | scenario | Explicit mem backend (wins over `TINIO_E2E_BACKEND`); currently no feature carries it — the CI mem pass uses the env var. |
| `@nested-root` | scenario | fs: served root = tempdir/`root` (traversal-proof scenarios). |
| `@checksum-on` | scenario | `caps.checksum = true` (checksum-validation scenarios). |
| `@minimal-caps` | scenario | Multipart / copy-object / list v1 / list v2 / delete-objects / tagging all off (unsupported-op error scenarios). |
| `@tagging-off` | scenario | `caps.tagging = false` (the six `?tagging` ops answer `NotImplemented`). |
| `@cold-listing` | scenario | fs scanner interval 100 ms (cold-listing scenarios). |
| `@max-buckets-3` | scenario | `caps.max_buckets = 3` (ListBuckets pagination scenarios). |

The **`@external` umbrella** is `@interop` ∪ `@boto3` ∪ `@mc`.

## Running

```
cargo test -p tinio-e2e                          # in-process suite only (default)
cargo test -p tinio-e2e --test cucumber -- --tags '@interop' --retry 1
cargo test -p tinio-e2e --test cucumber -- --tags '@boto3'
cargo test -p tinio-e2e --test cucumber -- --tags '@mc'
cargo test -p tinio-e2e --test traceability      # spec↔tag cross-check
```

- **The default run excludes `@external`**: `cargo test -p tinio-e2e` without `--tags` runs
  only the in-process scenarios. The runner sets `CUCUMBER_FILTER_TAGS=not @interop and not
  @boto3 and not @mc` unless an explicit `--tags` appears on the argv or
  `TINIO_E2E_EXTERNAL=1` is set.
- **An explicit `--tags` replaces the default filter** — a tagged run must re-state the
  `@external` exclusion. CI's mem pass does: `--tags 'not @fs and not @interop and not
  @boto3 and not @mc'` with `TINIO_E2E_BACKEND=mem`.
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
| `TINIO_E2E_EXTERNAL` | Set to `1` to disable the default `@external` exclusion (all scenarios run; an explicit `--tags` still wins). |
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

The script checks `aws` and `rclone`, points `CARGO_TARGET_DIR` at `$HOME/tinio-target` when
run from `/mnt/*` (ext4 build artifacts — the Windows build stays untouched), and runs
`cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1 "$@"` with the Linux
cargo toolchain. Extra args (e.g. `--tags 'not @interop and @rclone'`) are forwarded; a
forwarded `--tags` replaces the script's own filter (last one wins).
