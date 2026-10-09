# Design: panic-free business code

**Date**: 2026-10-09
**Status**: approved — design confirmed 2026-10-09 (scope, exception policy, lock policy, assert gate, config shape). Not yet planned.
**Scope**: production code of all 22 workspace crates + `xtask`; new repo-root `clippy.toml`; `[lints.clippy]` rows per crate; one policy test; `docs/style.md` §Panics. No wire-format, FR/SC, or success-path change.

## Goal

Business code never panics. Every `.unwrap()` / `.expect()` / `unwrap_err` / `expect_err` / `panic!` / `unreachable!` / `todo!` / `unimplemented!` / `assert!` site in code compiled outside test contexts returns the crate's `Result` instead — or carries a written `reason` when unreachable by construction. Enforced mechanically: clippy levels for the unwrap family, a repo policy test for the assert family.

## Non-goals

- Indexing / slicing (`x[i]`) and arithmetic overflow — out by ruling; style.md declares the boundary. Prod indexing sites measured ≈146, unscheduled.
- Test / bench / doc-example / test-support cleanup — ≈9,400 unwrap-family sites stay free.
- `tinio-e2e` (e2e harness) and `vendor/soft-canonicalize` (third-party `[patch]` tree) — outside cleanup and gates; `xtask` stays in scope.
- `debug_assert!` family — release-inert, out of rule.
- Workspace-lints migration — ruled out by measurement (D7).
- New error semantics — conversions reuse existing variants where meaning fits (D5).
- Any behaviour change beyond panic→error — success paths and existing error codes unchanged.

## Evidence base

Measured 2026-10-09 on this tree, clippy 0.1.99 / rustc 1.99.0. Probe for clippy coverage ran in a throwaway workspace, not this repo.

| Fact | Measurement |
|---|---|
| Prod unwrap/expect family | ≈163 raw; ≈90–120 after resolving `#[cfg(test)] mod` gating in parent `lib.rs`/`mod.rs` (redb `contract.rs`, select `struson_contract.rs` are test-gated). Authoritative list = plan task 0 |
| Prod `assert!` / `assert_eq!` | ≈73 raw; ≈14 real (41 test-gated in redb, 18 in select) |
| Prod panic-family macros | ≈18 raw; ≈13 real |
| `clippy.toml` `allow-*-in-tests` coverage | `#[cfg(test)]` mod ✔; `tests/` default harness ✔; `harness = false` bench ✗; `harness = false` test target ✗ |
| `[lints] workspace = true` + extra keys | cargo 1.99 rejects — `cannot override workspace.lints in lints` |
| `unsafe_code` split | 14 crates `forbid` (incl. `xtask`), 8 `deny` with enumerated `#[allow(unsafe_code)]` sites, `tinio-e2e` none — inheritance cannot preserve it |
| Feature-gated test support | `tinio-util::testing` and peers compile outside `cfg(test)` (feature `testing`) — clippy reads them as production; module-level allow needed (D7) |
| Delivery surface | every crate `publish = false`; no `build.rs` anywhere — public-signature changes carry no external contract |
| CI wiring | triggers push(main, dev) + all PRs, no path/label filters; `e2e-trace` runs `-p tinio-e2e --test traceability` on ubuntu + windows + macos — the policy test rides the same wiring |

## Decisions (ruled 2026-10-09)

- **D1 Scope** — production only: code compiled outside tests, benches, doc examples, test-support modules. Test contexts stay free.
- **D2 Exception** — `#[expect(<lint>, reason = "…")]`, four categories, each with a canonical reason prefix fixed in style.md: (a) compile-time literal input; (b) infallible-by-construction conversion (writes into `String`/`Vec<u8>`, derive-only `Serialize`); (c) invariant visible in the same expression with no better shape; (d) argued-unreachable — reason carries the written argument and names the mechanism that catches a violation (clock before epoch, known-range conversion, third-party contract, lazy-static registration with a name-uniqueness test). Runtime-failable (I/O, dynamic parse, env, other threads) never excepted. (d)'s quality rides on review — accepted by ruling. Eliminate over annotate.
- **D3 Locks** — `.lock()` → `.unwrap_or_else(PoisonError::into_inner)`; never unwrap, never propagate. Critical sections are short invariant-preserving mutations.
- **D4 Out of rule** — indexing/slicing, arithmetic overflow; declared in style.md.
- **D5 Error variants** — reuse the existing variant whose meaning fits; else add uniformly named `Invariant(&'static str)` plus the Error→S3/HTTP mapping arm (server: 500) — the internal string stays out of the response body (generic `InternalError`; log/metric carry detail).
- **D6 Fallback** — only where `Result` is structurally impossible (`Display`, `Drop`, foreign trait impl): defined fallback + tracing event, never panic (observability-over-debt ruling).
- **D7 Enforcement** — repo-root `clippy.toml` owns test exemption (`allow-unwrap-in-tests`, `allow-expect-in-tests`, `allow-panic-in-tests`); each crate adds `unwrap_used / expect_used / panic / todo / unimplemented / unreachable = "warn"`; a stated allow (reason citing style.md §Panics) is the mechanism for every test context the in-tests keys cannot reach: `harness = false` targets, feature-gated test-support modules, and file-level helper fns of integration-test targets, for any of the six lints (`todo`/`unimplemented`/`unreachable` have no in-tests key at all; the other three keys do not reach file-level helpers) — measured 2026-10-09; CI `-D warnings` = hard gate. No `[lints] workspace = true` migration: cargo rejects level overrides beside it, and the `unsafe_code` split (Evidence base) cannot inherit.
- **D8 Policy test** — no clippy lint exists for asserts; repo policy test (precedent `tinio-e2e/tests/traceability.rs`) rejects, in production source — a `syn` parse, not a text scan: unannotated assert-family sites; `#[allow(clippy::unwrap_used|expect_used|panic|todo|unimplemented|unreachable)]` (only `#[expect]` allowed); and `#[expect]` reasons whose prefix is not one of D2's four canonical forms. Home: `crates/tinio-e2e/tests/policy.rs`, wired by its own steps in the `e2e` and `e2e-port` jobs (mirroring `traceability`).
- **D9 Rule home** — `docs/style.md` §Panics owns the operative rule (compressed, cites this design); this spec owns rationale and mechanics.

## Shared symbols (one definer each)

| Symbol / fact | Definer | Consumers |
|---|---|---|
| Operative rule | `docs/style.md` §Panics | spec/plan cite only |
| Test exemption config | `clippy.toml` (repo root) | every crate's clippy run, local + CI |
| Site inventory + test-context rule | plan task 0 artifact | plan tasks; policy test cites the rule |
| Assert gate | policy test (skeleton in task 0) | CI |
| Shared scan predicates + repo walk | `tinio-util::testing::{is_test_gated, item_attrs, workspace_root, repo_rel, walk_files}` (behind `testing`) | policy test, spawn guard, traceability |
| `Invariant(&'static str)` | first crate needing it; style.md fixes the name | all crates |
| Lock idiom | `PoisonError::into_inner` (std) | all `.lock()` sites |

## Design

### §1 Exception grammar

`#[expect]`, not `#[allow]`: a cleaned site turns its stale expectation into a compile error, so exceptions cannot rot silently. Statement-position sites may need a `let` to host the attribute. Reason names a D2 category.

### §2 Conversion playbook (per site, in order)

1. Structural elimination — make the invariant unrepresentable (`let-else`, iterator, early return).
2. `?` where the fn already returns a compatible `Result` / `Option`.
3. Signature → `Result`; propagate; map at the top boundary (D5).
4. `ok_or` / `let-else` for Option-shaped failure.
5. D6 fallback.

### §3 Enforcement mechanics

- `clippy.toml` (coverage measured above) — one file, all crates, local + CI + IDE.
- Per-crate `[lints.clippy]` rows; a crate's cleanup and its rows land in the same task so CI never sees a half-gated crate.
- `harness = false` roots (benches that declare it; `tinio-e2e/tests/cucumber.rs`) and feature-gated test-support modules: one allow attribute — file-top `#![allow(…)]` in a target root, `#[allow(…)]` on the module declaration otherwise — carrying a style.md-citing reason.
- Policy test (D8): parses `crates/*/src` + `xtask/src` with `syn` 3 (`full`, `visit`; dev-dep of `tinio-e2e`, pinned at the workspace per `docs/cargo.md` — a parse failure fails the test). Enforces D8's three rules; test contexts skipped by attribute; proven red-then-green with planted probes, one per rule. The inventory itself comes from the compiler, not a scan: clippy's own diagnostics for the unwrap family (command-line lint levels, no manifest edit), the policy test's red run for the assert family. Task 0 also probes `unwrap_err` / `expect_err` lint coverage and plants a sample of every lint to confirm firing.

### §4 Inventory first (plan task 0)

Resolve `mod` gating and confirm the authoritative per-file site list — from clippy's diagnostics (unwrap family) and the policy test's red run (assert family), not from a text scan. Classify each site (already-`Result` / needs signature change / exception candidate). Counts in this spec are estimates and go stale; the plan owns the live list. Task 0 also lands `clippy.toml`, style.md §Panics, the policy-test skeleton, and one pilot crate to validate the mechanics end-to-end before waves start.

## Testing & acceptance

- Per task: `cargo clippy -p <crate> --all-targets -- -D warnings` clean at the new levels; `cargo test -p <crate>` green.
- Per wave (workspace-green checkpoints): `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`.
- Platform legs: Windows + WSL2 Ubuntu-26.04 (`docs/tests.md` recipe) for platform-gated crates; the macOS crate rides the CI macOS clippy leg.
- Final: policy test red-then-green; survivor count equals the inventory's exception list; levels on in every crate.
- Done = zero unannotated production sites; CI green.

## Risks

- Signature ripple beyond estimate → inventory first; tasks split per crate.
- panic→Err changes a test expectation → per-crate test run; a changed expectation needs a one-line justification in the task record.
- Parallel subagents share one tree → disjoint file ownership per task; re-read before every write.
- One commit for the whole plan (ruling) → the review surface is the whole diff; wave-end workspace gates and per-task records are the mitigation.
- Two test-context definitions (clippy's vs policy test's) → policy test cites the task-0 rule.

## Plan decomposition

One plan. Task 0 (inventory + mechanics + style.md + pilot) → wave A (util, core, api, auth, config, select, version, sandbox-core/linux/windows/macos) → wave B (mem, blob, fs, store-redb, store-pg) → wave C (server, cli, tinio, xtask). Waves are dependency order; within a wave ≤4 parallel tasks with disjoint file sets; a task that changes a public signature adapts every in-repo caller in the same task. Two user-directed additions ride wave A: the repo's source-scanning gates convert to a `syn` parse and widen where the 2026-10-09 audit found holes — the spawn guard gains the `src` test contexts (12 sites) and admits `.output()`/`.status()` as sites (19 sites), and `function_body`'s boundary comes from the parse while its one seam-bearing slice (`classified`) becomes a behavioral test — and the traceability gate's allow-list is repaired (Task A7: the audit's four dead reasons, a resolvable-home assertion per entry, tag/allow-list mutual exclusion, `spec.md` into the tag-side corpus). Same goal (a scan that is exact), same mechanics. Delivered as one commit on `dev` (the current branch) carrying spec + plan + implementation + config, operator-approved; agents never touch the index.
