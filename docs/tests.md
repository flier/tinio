# Tests

## Rust

- Async stays async (no blocking-runtime wrap); sync is ordinary. Exempt: runtime-shape. Conformance harness is a non-default test feature; cucumber does not re-assert Rust.
- One-off scripts: any language. Standing unit / e2e: Rust only. Scratch outside the repo and not `target/`; delete at round end; reap spawned processes else the next build cannot replace the binary.
- Placement (2026-10-01, test-placement plan): unit next to the subject (sibling file once the module outgrows one file — not a crate-level gather). Flow cucumber iff the migration rule admits, else integration (crate boundary / foreign oracle / bench-shared / public-API properties). One-module property suite moves inline with its subject.
- Shared harness = lib behind a test feature. Shared include = file-scope (path resolution differs by platform). Spawn sites outside the guard's scan: record at the guard.
- Drop a duplicate only when the survivor covers the same rule; fold missing asserts first. State-dir layout: closed set the fs backend tests pin — `meta.redb`, `tmp/`, `multipart/`, `blobs/` and the tool-state files.

## Standard acceptance (pre-push gate)

- Local cmds cover every CI workflow leg. CI profile = dev. CI unit = nextest; local = cargo test; doctest separate. Out of gate: interop (external tools on `PATH`), audit, semver (constitution VI). Platform: [Platform-gated verification](#platform-gated-verification).
- Doc gate: workspace rustdoc with warnings denied — not doctests. Public docs must not rustdoc-link private items.
- Feature-gate breakage needs each-feature — the default graph cannot see it. Matrix: facade + server + `tinio-fs`'s feature-off leg, not a powerset. Feature-off: lib-only check/clippy (dev-deps re-enable; Unix lints on Linux) — `tinio-fs`'s self dev-dependency unifies `versioning` back on for every test build, so `cargo hack check --package tinio-fs --each-feature` (a check builds no dev-deps) is the leg that compiles the stripped arm; test-run the refusal arm. Versioning graph is a runtime seam (`docs/cargo.md`). Sandbox e2e requires the sandbox feature; the feature gates the body, not the program — the binary must still compile and serve with features off.

## Cucumber (tinio-e2e)

- Layout / tags / env: e2e README. Features ↔ S3 surface contract. `Given`/`When`=act, `Then`=assert; 1st-person; one behavior/scenario.
- Default filter owns platform / external / confinement (`TINIO_E2E_CONFINEMENT`: unset=platform default, force in, force out); replacing it restates every exclusion.
- Gate: fs, mem, parquet, spec-ID traceability. Migrate iff spec + product-surface observable (cucumber migration design); no spec ID → Rust. Keep the shared util harness.

## Product surfaces

- T056 via the Plane API, never raw transport; process legs cucumber. Busy-pipe successor: **T147** — do not paper over. Pipe-pool needs a multi-thread runtime (`open` blocks the calling thread).
- CLI process legs cucumber (build the binary first). Windows console drain stays Rust. Confinement: feature-off refuses at build, never by platform; feature-on must reach readiness; honour is macOS CI only, else operator opt-in.
- Service verbs: supervisor + elevation — no auto leg; logic without elevation: inline unit tests. Doctor (T057): cucumber for repair flows; Rust for JSON key set vs schema. Credential policy stays Rust by ruling — loopback-only built-in fallback.
- Read-only (FR-023): cucumber for backend wire, genuine read-only root (unix), mode / FR-016 / state via the real binary. Derived default state-dir stays Rust — CLI children always get an explicit state dir.

## Platform-gated verification

- Shared: confinement **fails**, never skips, when the host cannot do the leg — skip certifies nothing (`TINIO_SANDBOX_ALLOW_SKIP=1`). Readiness is SSE (decision 19), not a scraped line. Undriveable arms: sandbox bdd plan §7. Guest artifacts: [Concurrency](#concurrency).
- Linux / Windows: operator-owned, not CI (userns / console). CI compiles/lints jail and signal binaries; run is filtered — re-enable via ignore-default-filter or a non-ci profile. Confinement e2e is cucumber except oracles with no step-reachable surface, and feature-off confinement refusal (Rust). Default workspace run excludes confinement off macOS; operator opt-in (`TINIO_E2E_CONFINEMENT=1`).
- Windows: confinement out of CI (cost); confined timeout separate; ACE shape design §6; launcher does not clean stale ACEs; network: unconfined reachability first, then confined denial. End-to-end readiness-timeout induction is undriveable; the core launcher covers the abort.
- macOS: real-Mac gate; off-Mac certifies nothing; fail closed without the host helper. CI records refusals; confinement serial, parallel pass forced off (sandbox bdd plan §6). Two mechanisms (`TINIO_SANDBOX_MACOS_MECHANISM`); both must be green. Nested Seatbelt is environment. Socket-path overflow: harness.

## Concurrency

- Child-spawning suites serial. ≤4 agents; distinct `CARGO_TARGET_DIR`; prefer one suite; serialise when compile-or-confine stacks.
- Guest fork failed → restart the guest, not pkill. Scratch: named list only. Single-thread probe: standalone binary. Otherwise a re-exec of the test binary is allowed.
- Guest failures = **harness artifacts, not product defects**. Sync must carry vendor, config, specs, packaging, crates, and manifests. CRLF on unix unit files: pin LF in attributes; recurs → re-ship packaging. Socket-path overflow → harness depth first. Never two agents on the same VM.
