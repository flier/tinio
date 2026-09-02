# Design: BDD cucumber test migration (cucumber-rs)

**Date**: 2026-08-31
**Status**: draft — pending user review
**Scope**: workspace-wide test migration — new `crates/tinio-e2e` cucumber suite (cucumber-rs 0.23.0) absorbing all black-box integration tests, the CI-gated bash interop scripts, and the API-observable business-behavior unit tests; CI rework (quality + interop jobs, PR test reports); spec traceability (`specs/001-s3-local-server`); docs updates. Not touched: proptest files, pure-mechanics unit tests, benches, the `tinio-util` conformance harness.

## Goal

Migrate tinio's test cases to BDD-style cucumber so that:

1. **One cucumber suite** (`crates/tinio-e2e`) holds all black-box S3-surface scenarios — the current `tinio-server/tests/*.rs` integration files (data_plane, error_codes, coverage_gaps, reserved_paths) and the `#[ignore]` external-client files (journey, advanced, boto3, mc, edge).
2. **The bash interop scripts merge into cucumber.** `e2e/interop/journey.sh` and `advanced.sh` (CI-gated), plus `boto3.sh`/`mc.sh` (manual), are rewritten as tagged cucumber features driven from Rust steps; after 3-OS CI parity the bash scripts and the `e2e/` directory are deleted.
3. **Feature files are the executable spec.** Features derive from `specs/001-s3-local-server/contracts/s3-surface.md`; every scenario carries its `@FR-xxx` / `@SC-xxx` / `@T0xx` tags; a CI traceability check fails on any spec ID without a feature tag and any feature tag without a spec ID.
4. **CI reports to the PR.** Every cucumber run emits a JSON report; `dorny/test-reporter` posts a pass/fail summary comment to the PR; the JSON artifact is uploaded as a fallback.
5. **API-observable business-behavior unit tests** (multipart semantics, listing, conditions) migrate into the same suite as Gherkin `Examples`-parameterized scenarios; pure-mechanics unit tests stay in Rust.

## Non-goals

- **No migration of pure-mechanics unit tests** (≈600 of ~670): path encoding, checksum math, redb/DB layout, lockmap, schema validation, error formatting, scanner/cleanup internal state machines. Gherkin does not express these better than Rust.
- **No migration of proptest files**: `tinio-core/tests/validation.rs`, `tinio-fs/tests/proptest_meta.rs`, `tinio-fs/tests/proptest_multipart.rs`. Property-based testing is a different paradigm; cucumber scenarios use representative fixed values instead.
- **No direct-Storage cucumber steps.** Cucumber scenarios are black-box HTTP against the S3 API only. The `tinio-util` conformance harness (`assert_conformance`) stays as-is in Rust — it serves benches and `tinio doctor`, and its helpers become the step layer's assertion primitives.
- **No removal of `tinio-fs/tests/layout.rs`** — it inspects on-disk state-dir structure (`meta.redb`, `tmp/`, `multipart/`), an implementation detail requiring direct fs access.
- **No changes to tinio-config / tinio-api / tinio-cli tests** (pure schema/error-formatting logic).
- **No CI matrix reduction.** ubuntu/windows/macos stay; WSL2 is a local-dev convenience, not a CI substitute.
- **No cucumber in library crates.** The cucumber binary lives only in `tinio-e2e`; library crates gain no cucumber dependency.

## Decisions (locked in brainstorming, 2026-08-31)

- **Dedicated e2e crate, single cucumber binary.** `crates/tinio-e2e` with one `[[test]] name = "cucumber"` target (`harness = false`). Rationale: cucumber-rs registers steps per-binary; a single binary is the only layout where step definitions, the `World`, and the conformance helpers share one registry. Per-crate targets would duplicate steps (each compiled per binary) and leave interop tests homeless.
- **HTTP-surface-only level.** Scenarios express behavior through the S3 API (`PUT/GET/List/CompleteMultipartUpload…`). Backend selection is a tag (`@fs`/`@mem`), not a direct `Storage` call.
- **Strong spec binding.** Features are derived from `contracts/s3-surface.md`; spec changes must update features in the same PR; a CI traceability check enforces it.
- **Tag umbrella `@external`** = `@interop` ∪ `@boto3` ∪ `@mc`. CI quality job runs `~@external`; interop job runs `@interop`.
- **Bash merge with deletion.** `e2e/interop/*.sh` + `lib.sh` are deleted once the cucumber `@interop` scenarios pass on 3 OS CI; the FR-025 tiering matrix folds into the tinio-e2e README; the `e2e/` directory retires.
- **cucumber 0.23.0 pinned** (latest stable, 2026-04-23; tokio executor; `--format json`, `--tags`, `--retry` available).
- **English-only feature files and steps** (project language rule).
- **Zero commits during the migration** (project git rule — the user commits). All phases land in the working tree; the user reviews and commits manually after everything is complete. Intermediate verification is local (Windows native + WSL2); 3-OS CI proof happens after the user pushes. A post-push failure is fixed as a follow-up — restore from git is zero-cost while nothing is committed.

## Architecture — `crates/tinio-e2e`

```
crates/tinio-e2e/
├── Cargo.toml
│     dev-deps: cucumber = { version = "0.23", features = ["output-json"] },
│               tokio (macros/rt-multi-thread/time),
│               tinio-fs, tinio-mem, tinio-server, tinio-util (testing), assert_cmd, tempfile
│     [[test]] name = "cucumber", harness = false
├── tests/cucumber.rs            # World::cucumber().init_tracing().run_and_exit("tests/features")
├── tests/features/              # all .feature files (English)
│   ├── buckets.feature          # @SC-001 @FR-xxx
│   ├── objects.feature          # @T025 CRUD / byte-identical / Range / conditionals
│   ├── multipart.feature        # @T032 composed ETag / part validation / cleanup
│   ├── listing.feature          # prefix / delimiter / pagination @SC-001
│   ├── error_codes.feature      # @SC-004
│   ├── conditions.feature       # 304/412 / copy-source-range
│   ├── reserved_paths.feature   # @FR-020
│   ├── tagging.feature          # GetObjectTagging / DeleteObjects quiet mode
│   ├── metrics.feature          # /metrics via middleware
│   └── interop/
│       ├── journey.feature      # @interop @aws @rclone
│       └── advanced.feature     # @interop multipart>8MiB / copy / cold listing
└── tests/steps/                 # domain-organized step modules (cucumber-recommended;
    ├── mod.rs                   #   avoids feature-coupled steps)
    ├── buckets.rs  objects.rs  multipart.rs  listing.rs
    ├── errors.rs   conditions.rs  reserved_paths.rs  metrics.rs
    └── clients.rs               # aws/rclone/boto3/mc assert_cmd wrappers (ported from
                                 #   tinio-server/tests/e2e/mod.rs)
```

`Cargo.toml` notes: `tinio-e2e` has no library target — only the cucumber test binary; it declares no `[features]` of its own so workspace `--no-default-features` runs are unaffected.

### World & server lifecycle

```rust
#[derive(Debug, Default, World)]
pub struct World {
    backend: Backend,          // @fs | @mem, chosen in the #[before] hook from scenario tags
    server: Option<Server>,    // in-process DataPlane on 127.0.0.1:0 (no external deps)
    client: Client,            // raw HTTP request wrapper (ported from tests/common/mod.rs)
    ext: Option<External>,     // @external scenarios only: spawned serve binary + client session
    last: LastResponse,        // last request/response for Then assertions
}
```

- **Default scenarios**: `#[before]` spawns an in-process `DataPlane` (reuse `tests/common/mod.rs` `Server::fs_at`/`mem` pattern) on an ephemeral port with watch-channel shutdown; `#[after]` tears it down. Fresh server per scenario ⇒ no shared state, no parallel-race hazard, independent of whether cucumber-rs runs scenarios concurrently (`--concurrency` is safe).
- **Server configuration**: scenarios needing non-default config (scanner interval, `[s3] checksum` toggle, pagination caps) map tags (`@cold-listing`, `@checksum-on`, …) to `Config` overrides in the `#[before]` hook; the mapping starts with the known configuration points and grows only when a scenario needs it.
- **Default tag filtering (local run)**: plain `cargo test -p tinio-e2e` excludes `@external` scenarios by default (a `filter_run` / `CUCUMBER_FILTER_TAGS` default in the runner main, preserving the current `#[ignore]` semantics); `--tags @external` or `TINIO_E2E_EXTERNAL=1` opts in. CI jobs always pass explicit tags and are unaffected.
- **@external scenarios**: `#[before]` spawns the `serve` example binary (`tests/e2e/mod.rs` `serve_bin()` + `wait_for_ready`) and asserts the external client binaries exist (aws/rclone for `@interop`; boto3 venv via `TINIO_BOTO3_PYTHON`; mc for `@mc`). Missing binary ⇒ explicit panic telling the user to filter tags.
- Time-dependent scenarios (interrupted-upload cleanup, last-write-wins, out-of-band changes) use the ported `eventually`/`wait_for` (10 s poll) helpers.

### Step layer

- **Given/When** = domain verbs matching feature prose: `I create bucket "b"`, `I upload "k" with {int} bytes`, `I start a multipart upload`, `I upload part {int} of {int} bytes`, `I complete the multipart upload`.
- **Then** = assertion primitives: `the object body equals the uploaded bytes`, `the error code is "NoSuchKey"`, `the ETag matches the composed form`, `the listing shows {int} keys under prefix "p/"`.
- Complex assertions (composed-ETag reference implementation, pagination aggregation, concurrent no-torn-objects via `tokio::join!`) live in private step helpers; steps stay one-line readable.
- Assertion primitives reuse `tinio-util::testing` helpers (`etag()`, `body`, `wait_for`); the conformance harness itself stays in tinio-util (used by benches/doctor).
- Data-driven scenarios use Gherkin `Examples` tables (part counts/sizes, pagination limits, conditional headers). Representative fixed values stand in for proptest's randomized ranges (e.g. multipart: 1/2/16/24 parts, 0/1 MiB/4 KiB/8 MiB sizes — including the >8 MiB interop boundary).

### Tagging

| Layer | Tags | CI use |
|---|---|---|
| Backend | `@fs` `@mem` | the `#[before]` hook reads the scenario's tags and picks the backend; an explicit tag wins, otherwise `TINIO_E2E_BACKEND` (default `fs`). CI quality runs the non-external suite twice — the fs pass (default) and a `mem` pass (`TINIO_E2E_BACKEND=mem` + `--tags 'not @fs'`, so fs-only scenarios are skipped) — mirroring how the conformance harness already runs one suite against every `Storage` impl. If the double run measurably exceeds the timing target, the `mem` pass narrows to `@mem`-tagged scenarios (decided by measurement during implementation). Scenarios whose behavior is backend-specific carry only their backend's tag |
| External deps | `@interop` `@boto3` `@mc`; umbrella `@external` | quality runs `~@external`; interop job runs `@interop`; `@boto3`/`@mc` manual |
| Spec traceability | `@FR-xxx` `@SC-xxx` `@T0xx` (multiple allowed) | traceability check |

## Migration scope

### Migration rule

A test migrates iff it carries a SC/FR/T spec semantic **and** its behavior is observable through the S3 API (conjunction, sharpened at grilling 2026-08-31). API-observable unit tests without a spec ID stay in Rust — they are already covered by the conformance harness or by integration features, and cucumber scenarios do not repeat conformance assertions. Everything else (pure mechanics) stays in Rust.

### Moves to cucumber (replaces the source)

| Existing test | Tests | Destination feature(s) |
|---|---|---|
| `tinio-server/tests/data_plane.rs` | 7 | `objects.feature`, `listing.feature` (@T025) |
| `tinio-server/tests/error_codes.rs` | 8 | `error_codes.feature` (@SC-004) |
| `tinio-server/tests/coverage_gaps.rs` | 8 | split across `tagging.feature`, `metrics.feature`, `multipart.feature`, `listing.feature`, `conditions.feature` |
| `tinio-server/tests/reserved_paths.rs` | 2 | `reserved_paths.feature` (@FR-020) |
| `tinio-server/tests/journey.rs` | 2 #[ignore] | `interop/journey.feature` (@interop @aws @rclone) |
| `tinio-server/tests/advanced.rs` | 1 #[ignore] | `interop/advanced.feature` (@interop) |
| `tinio-server/tests/boto3.rs` | 3 #[ignore] | `interop/journey.feature` @boto3 scenarios |
| `tinio-server/tests/mc.rs` | 1 #[ignore] | `@mc` scenarios |
| `tinio-server/tests/edge.rs` | 2 #[ignore] | split (special-char keys, size boundaries, last-write-wins) |
| Business-behavior unit tests, spec-scenario subset (count decided per file by the conjunction rule) | — | corresponding features via `Examples` tables: tinio-fs `multipart.rs`/`listing.rs` spec subset; tinio-mem `multipart.rs`/`object.rs` subset; tinio-server `backend/multipart.rs`/`conditions.rs`/`listing.rs` subset; tinio-core `bucket.rs`/`object.rs`/`multipart.rs` spec subset |

The exact per-file subset of unit tests is decided during implementation by the migration rule, recorded in the implementation plan's checklist, and reviewed per file.

### Stays in Rust (unchanged)

| Existing test | Reason |
|---|---|
| `tinio-fs/tests/layout.rs` | on-disk state-dir structure — implementation detail |
| `tinio-fs/tests/proptest_meta.rs`, `proptest_multipart.rs`, `tinio-core/tests/validation.rs` | proptest paradigm |
| `tinio/tests/smoke.rs` | one-line facade smoke |
| ~600 pure-mechanics unit tests across all crates | internal detail; Gherkin would be worse |
| `tinio-util/src/testing.rs` conformance harness | used by benches/doctor; helpers feed the step layer |
| `tinio-fs/src/testing.rs` (`fs_options`), `tinio-fs/src/testutil.rs`, `tinio-server/src/backend/testutil.rs` | unit-test/bench fixtures |
| All benches (`criterion`, `dhat`) | not tests |

### Deletion rhythm

1. Per file: port the test to a feature + steps; run old and new side by side locally; verify 1:1 scenario coverage against a checklist; delete the old file.
2. Bash scripts (`journey.sh`, `advanced.sh`, `boto3.sh`, `mc.sh`, `lib.sh`): deleted in-tree once `@interop` runs green locally (WSL2) and the quality suite is green locally. The 3-OS proof happens post-push; a post-push interop failure is recoverable at zero cost because nothing is committed (restore from git).
3. `tinio-server/tests/common/mod.rs` + `tests/e2e/mod.rs`: moved into `tests/steps/` in tinio-e2e, then deleted from tinio-server.
4. `e2e/` directory retires; the FR-025 tiering matrix from `e2e/interop/README.md` folds into the tinio-e2e README.

## Feature files as executable spec

- Each feature file's header comment names its source contract (e.g. `derived from specs/001-s3-local-server/contracts/s3-surface.md`), and each scenario carries `@FR-xxx`/`@SC-xxx`/`@T0xx` tags.
- `contracts/s3-surface.md` gains an **Automated coverage** section: per S3 operation → feature file + tag (e.g. `GET Object → objects.feature @T025`).
- `checklists/compatibility.md` test items reference feature files.
- **Traceability CI check** (new): scans `specs/001-s3-local-server/` for FR/SC/T IDs and asserts every ID appears as a feature tag, and every traceability tag has a spec ID — zero orphans in both directions. Implemented as a Rust test in tinio-e2e (cross-platform, no bash dependency), run in the quality job.
- Workflow rule: a contract change updates the derived features in the same PR.

## CI integration

### quality job (ubuntu/windows/macos)

```yaml
- run: cargo test --workspace --exclude tinio-e2e
- run: cargo test --workspace --no-default-features --exclude tinio-e2e
- run: cargo test -p tinio-e2e -- --tags '~@external' --format json --report-target cucumber-report.json
- run: cargo clippy --workspace --all-targets -- -D warnings        # covers tinio-e2e
- run: cargo +nightly fmt --all -- --check                          # covers steps/*
- run: scripts/traceability-check.sh (or equivalent)                # spec↔tag cross-check
```

`--exclude tinio-e2e` on the plain workspace tests is required: with `harness = false` the cucumber binary would otherwise run *all* features in the default `cargo test --workspace`, including `@external` ones that need client binaries.

### interop job (ubuntu/windows/macos)

```yaml
- run: cargo build -p tinio-server --example serve        # serve_bin() needs the binary
- run: cargo test -p tinio-e2e -- --tags @interop --retry 1 --format json --report-target interop-report.json
```

Replaces `bash e2e/interop/journey.sh && bash e2e/interop/advanced.sh`. aws-cli + rclone installation stays.

### PR test reports

Three-tier delivery (the "CI outputs test report to PR" requirement):

1. **Artifact**: every cucumber run emits `--format json` → `cucumber-report.json` (quality) / `interop-report.json` (interop); `actions/upload-artifact` uploads both. Available on every PR.
2. **PR comment + check**: `dorny/test-reporter` (native cucumber-JSON support) publishes a pass/fail summary table comment on the PR and attaches check status. **Only the ubuntu legs** of quality and interop post comments (one per job per PR); windows/macos legs upload artifacts only. Requires job-level `permissions: checks: write, pull-requests: write` (ci.yml currently declares no `permissions:` block — add it).
3. **Fallback**: the same jobs write a step summary (`actions/github-script`) with pass/fail counts; on failure the failing scenarios are listed. Fork PRs without comment permissions still get artifact + summary.

## WSL2 local workflow (local dev convenience, not CI)

Documented in the tinio-e2e README and wrapped in `crates/tinio-e2e/scripts/wsl-interop.sh`:

- Purpose: run Linux-side tests — especially `@interop` (aws-cli/rclone install and behavior closest to production) — inside WSL2 on the Windows dev machine.
- Environment: `sudo apt install awscli rclone` (or official installers); boto3 venv `python3 -m venv .venv && .venv/bin/pip install boto3` (keeps `TINIO_BOTO3_PYTHON` override); `mc` optional.
- Performance: with the repo on `/mnt/e`, set `CARGO_TARGET_DIR=/home/<user>/tinio-target` (build artifacts on ext4; source reads over 9p acceptable), or clone into the WSL2 native filesystem.
- Script: checks client presence → sets `CARGO_TARGET_DIR` when on `/mnt` → `cargo test -p tinio-e2e -- --tags @interop --retry 1` (optional full-suite flag).

## Migration verification

Per-file step: old tests and new features run **side by side locally** → the migration checklist ticks each old test to its feature scenario(s) 1:1 → old file deleted only when the feature passes and coverage matches.

Final acceptance (before the branch is considered done):

1. `cargo test --workspace --exclude tinio-e2e` green on 3 OS.
2. `cargo test -p tinio-e2e -- --tags '~@external'` green on 3 OS; scenario count ≥ replaced integration-test count.
3. `@interop` green locally in WSL2 — the deletion gate for the bash scripts (§Deletion rhythm); 3-OS CI green is the post-push confirmation. macOS has no local verification path (Windows + WSL2 only) and is accepted as post-push-verified (grilling Q8).
4. Traceability check: zero orphan spec IDs and zero orphan traceability tags.
5. `cargo clippy --workspace --all-targets -- -D warnings` and `cargo +nightly fmt --check` green.
6. PR report visible on a real PR (comment + check + artifact).

Performance target: in-process scenarios are ~ms-scale per server; the full non-external suite stays within the current `cargo test --workspace` duration order of magnitude; `@interop` scenarios are seconds-scale (binary spawn), matching the current bash scripts.

## Docs

- `docs/cargo.md`: tinio-e2e usage — run commands, tag filter table, `--format json`, harness=false note.
- `docs/style.md`: gherkin style conventions — English only; Given/When/Then verb forms; `Examples` tables for data-driven scenarios; steps organized by domain, not by feature.
- `CLAUDE.md`: Testing section updated with the cucumber workflow and the migration rule.
- `specs/001-s3-local-server/contracts/s3-surface.md`: **Automated coverage** mapping section.
- `specs/001-s3-local-server/checklists/compatibility.md`: test items reference feature files.
- `crates/tinio-e2e/README.md`: tiering matrix (from `e2e/interop/README.md`), WSL2 workflow, tag rules.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Gherkin prose drifts from behavior (weak spec link) | traceability CI check + contract→feature workflow rule |
| Step-registry bloat / feature-coupled steps | domain-organized step modules (cucumber-recommended) |
| `@interop` flakiness (external clients) | `--retry 1` in CI; `eventually` polling helpers |
| Scenario count explosion (dual-backend runs) | `@fs`-only / `@mem`-only tags where behavior differs |
| cucumber-rs limitations discovered mid-migration (e.g. missing report format) | JSON format + dorny/test-reporter; fallback summary always available |
| Migration silently drops coverage | per-file 1:1 checklist; final scenario-count gate |
