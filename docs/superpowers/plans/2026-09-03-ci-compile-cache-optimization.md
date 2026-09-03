# CI compile-time & cache-hit-rate improvement plan (2026-09-03)

Status: Phase 0 (measure) done · Phase 1 (structural) in tree, pending one manual commit by the user · Phase 2 (persistence) open.

## Goal

Reduce end-to-end CI wall time on push/PR (baseline ~24 min) and raise the sccache Rust hit rate (baseline 14.0%) toward a healthy >70%, measured per run on the same dev commit, without reducing coverage or gate independence. Wall clock is the primary metric; hit rate is pursued as the means, not the end.

## Baseline (run 33769082799, dev @ ea20b12, 2026-09-03)

All jobs green, wall 14:49 → 15:13 (~24 min). sccache stats per job parsed from the run logs (now reproducible via `.github/scripts/ci-analyze.js`):

- Overall Rust-only hit rate **14.0 %** (1575 hits / 11267 compile requests); the headline 23 % was inflated by C/C++ hits.
- **cache_write_errors are systemic: 4632 across all 21 compiling jobs** (e.g. 245 in the ubuntu default-features test leg alone) — most sccache writes to the single shared ghac store failed, so almost nothing persisted across runs. Every job's miss count carries write errors, which points at shared-store contention (rate-limit/quota under ~20 parallel uploaders) rather than one leg's fault.
- Ubuntu legs reached 20–35 % (they ran after the 6-min `check` gate); windows/macOS legs were 0.6–12 % — every job on an OS cold-started simultaneously at the 14:55 fan-out.
- `check` (the gate everything waits on): 833 requests, 0 hits, ~340 s — always cold.
- bench-smoke: 351 requests, 0 hits, ~475 s every run.
- rust-cache: 12+ `Failed to save ... another job may be creating this cache` — parallel legs of one job raced one cache key.
- Windows `latest` runners are 2 vCPU: the no-default test leg (~17 min job wall) was the tail.
- dev runs immediately before this one mostly failed, leaving the branch store cold.

## Decisions (2026-09-03 review)

- Objective: wall clock first; maximize cache hit rate as the lever.
- Landing: one single commit of the whole working tree (CI stack + wait-for knob + docs + scripts), committed manually by the user.
- Diagnostic loop: first ARMED run root-causes the write errors from `sccache-err-log-*` artifacts (each compile job uploads them, kept on failure); after the fix, re-run the same commit and compare with `ci-analyze.js` — success = write errors < 5 % of misses AND Rust hit rate ≥ 60 % (ubuntu) / ≥ 40 % (windows/macOS); then flip `SCCACHE_ERROR_LOG_UPLOAD` back to `"false"` and remove the diagnostic env/wiring.
- Quota budget: the rust-cache footprint cut landed up front — registry-only (`cache-targets: "false"`) everywhere except the `test` chain head, since per-job target dirs never served the cross-job chain and dominated store volume. Further levers (per-OS ghac names — zero-sum under the repo-wide 10 GB cap) still wait for the write-error diagnosis.
- nextest: two-run observation window — the structural run proves green, the post-fix run is judged on wall + hit rate; roll back the test legs to `cargo test` (one-line change in `run-test-leg`) if they show no improvement or new retries=2-immune flakes.
- Windows larger runners / per-OS e2e/interop chains: re-evaluate from post-fix data, not now.
- Measurement: `.github/scripts/ci-analyze.js` (local `gh` script, before/after delta mode) is the comparison tool for every change.
- Pre-commit gate: `actionlint` clean (`.github/actionlint.yaml` ignores two anchor false-positive classes).
- dev-red note: HEAD 23179e5 failed only on `delete_create_put_hammer_keeps_successful_puts_in_the_live_generation`, a 30 s `wait_for` deadline timeout in `tinio-util`; the uncommitted knob (60 s default + `TINIO_TEST_WAIT_TIMEOUT_SECS`) plus nextest `retries = 2` cover it.
- No branch protection on this repo (Free plan) — job renames carry no required-check cost.

## Work in tree (staged/unstaged, pending the single commit)

Structural changes per the review: same-platform + same compile parameters serialize into chains, different feature sets stay parallel, single source of truth:

- `test` split into ubuntu legs and `test-port` (windows/macOS); `bench-smoke` chains behind ubuntu `test`; `e2e`/`interop` ubuntu legs chain `check → test → e2e → interop`, with `*-port` portable legs gated on `check` only.
- rust-cache single-writer per OS (default-features leg only).
- `interop` serve example builds `--profile ci`, and `serve_bin()` (tinio-e2e) now resolves the example in the running test binary's own profile dir (`<target>/<profile>/examples`) — the previous hardcoded `debug/` path would miss the ci-profile build on a cold cache or, worse, pick up a stale dev-profile binary restored by rust-cache.
- Unit/integration test legs on cargo-nextest (`--cargo-profile ci --profile ci`, `.config/nextest.toml`: `fail-fast = false`, `retries = 2`); doctests preserved via `cargo test --doc`; cucumber (harness = false) legs untouched.
- DRY: step-level YAML anchors, each defined at its first real use (`- &name` in the earliest job that runs it) and aliased (`- *name`) by later jobs, for the preamble and the e2e/interop trios (GitHub's workflow schema rejects unknown top-level keys, so a synthetic `x-steps` holder block is not an option; merge keys `<<:` are unsupported); the 85-line cucumber report script extracted to `.github/scripts/publish-cucumber-report.js` (loaded at runtime via `require`); `run-test-leg` stays a composite action.
- rust-cache: registry-only (`cache-targets: "false"`) everywhere except the `test` chain head, which keeps its target dir (post-review decision 2026-09-03).
- `test` split again: `test` = ubuntu default leg (chain head); `test-extra` = ubuntu no-default/doc legs (leaf — nothing chains behind, so e2e/bench-smoke no longer wait on the rustdoc leg); `test-port` = win/mac default+no-default (doc leg dropped — rustdoc warnings are platform-neutral).
- fmt --check runs on the ubuntu lint leg only (platform-neutral); win/mac lint legs skip the nightly install.
- Diagnostics: `SCCACHE_ERROR_LOG: ${{ github.workspace }}/sccache-err.log` (single definition, writable on every OS — a job-level `${{ runner.temp }}` override is invalid, `runner` is not in job-env context, caught by actionlint) and `SCCACHE_ERROR_LOG_UPLOAD: "true"` (ARMED), with per-job artifact upload.
- Wait-for knob in `tinio-util` (30 s → 60 s default + env override) and its docs note.

## Phase 2 — next actions

1. User commits the tree once and pushes dev. Expectation for the first ARMED run: green (hammer test now has the knob + nextest retries), structure validated (anchors, env-context steps, nextest switch), and `sccache-err-log-*` artifacts produced by every compile job.
2. Root-cause the write errors from those artifacts: quota eviction under the 10 GB repo cap vs write rate-limiting from ~20 concurrent uploaders vs store errors. The 4632-error baseline (every job, not one leg) points at shared-store contention.
3. Apply the fix (levers, chosen from the diagnosis): fewer simultaneous uploaders / rust-cache footprint reduction for quota; per-OS `SCCACHE_GHA_CACHE_NAME` only if quota eviction is confirmed.
4. Re-run the same commit; compare with `node .github/scripts/ci-analyze.js <new-run> 33769082799`. Acceptance: write errors < 5 % of misses, Rust rate ≥ 60 % ubuntu / ≥ 40 % windows-macOS, wall toward ~15–18 min.
5. On acceptance: flip the flag to `"false"` and remove `SCCACHE_ERROR_LOG` + the upload anchor (diagnostics do not stay in the repo).
6. Judge nextest on the two-run window (decision above), then re-evaluate from post-fix data: windows larger runners, per-OS `e2e-port`/`interop-port` chaining (wall vs billing trade-off), nextest partition sharding.

## Verification

- `actionlint` clean on `.github/workflows/ci.yml` (config ignores the two anchor false-positive classes; real errors stay loud).
- `.github/scripts/ci-analyze.js` output for each before/after pair: per-job wall, compile requests, Rust hit rate, write errors; totals.
- Full matrix green: fmt, clippy `-D warnings`, all-features build, per-feature matrix, nextest unit/integration + doctests, cucumber fs/mem/@interop + traceability, bench smoke, audit, semver.
- Cache size stays under the 10 GB repo cap.

## Known limits

- cucumber (harness = false), rustdoc, and clippy are outside the cache/build-chain optimizations (~1/3 of compile volume).
- Cross-platform object reuse does not exist; per-OS stores are the unit of reuse.
- Anchor expansion and expression evaluation are validated locally by actionlint (see `.github/actionlint.yaml` for the two suppressed matrix false-positive classes) and confirmed by the first real run.
