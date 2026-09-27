# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`.
- Sync: `#[test]`. Exempt: runtime-shape tests.
- Conformance: `tinio-util` `testing` in `[dev-dependencies]`. Cucumber: no repeat asserts.

## Scripts

- One-off scripts: any language.
- Unit / e2e tests: Rust only — no shell/python standing in as a test.

## Standard acceptance (pre-push gate)

Intent: local cmds cover every `.github/workflows/ci.yml` leg. `[profile.ci]` = `dev` → omit `--profile ci`. CI unit = nextest (`.config/nextest.toml`); local = `cargo test`; doctest = `--doc`. Feature-gate breakage needs `cargo hack --each-feature` — default graph alone cannot see it. Platform legs: [WSL2](#platform-gated-verification-wsl2) / [macOS](#platform-gated-verification-macos) / [Windows](#platform-gated-verification-windows).

### CI-only legs

Out of gate: `interop` / `interop-port` (external tools on `PATH`), `audit`, `semver` (constitution VI). Cmds live in ci.yml.

### rustdoc

- Doc gate ≠ `cargo test --doc`. Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` (+ intra-doc broken/private-link denies).
- Public docs: no private `[`foo`]`; `pub(crate)` → `` `foo` ``.

## CI legs

- Unit/doc legs mirror gate. `features`: `--each-feature` on facade + `tinio-server` (no powerset) — catch cfg/`use` drift.
- Sandbox e2e `[[test]]` targets: `required-features = ["sandbox"]`. `tinio` binary: feature gates body, not the program — `--no-default-features` must still compile and serve. Keeper: `cargo hack check --package tinio --each-feature` (design decisions 4, 19).

## Cucumber (tinio-e2e)

Layout / tags / env: `crates/tinio-e2e/README.md`. Features ↔ `specs/001-s3-local-server/contracts/s3-surface.md`.

- `Given`/`When`=act, `Then`=assert; 1st-person; one behavior/scenario.
- Default filter owns platform/external/confinement gates; `--tags` replaces it — re-state every exclusion.
- Gate e2e = fs/mem/`@parquet`/traceability. Spec IDs: `--test traceability`.
- Migrate iff spec+S3 observable; no ID → Rust. Keep `tinio-util`.

## Management plane (tinio-api)

- T056 via `Plane` API, never raw transport. CLI process legs → `@cli` cucumber.
- Pipe successor (`ERROR_PIPE_BUSY`): do not paper over — **T147** owns.

## CLI (tinio)

- Process legs = cucumber `@cli` (build `tinio` first). Windows console drain stays Rust (`lifecycle.rs`).
- `@cli @confinement`: feature-off refuses by build, never by platform; feature-on must reach readiness. Automatic honour arm = macOS `e2e-port` only; Linux/Windows = operator opt-in ([WSL2](#platform-gated-verification-wsl2) / [Windows](#platform-gated-verification-windows)).
- Doctor (T057): `cargo test -p tinio --test doctor` ↔ `contracts/doctor-report.schema.json`.

## Service verbs (manual)

Supervisor + elevation — no auto leg. Pure halves: `tinio-cli/tests/service.rs`.

## Platform-gated verification (WSL2)

- Gate script: `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`. **Owner: operator, not CI** (hosted runner cannot create userns).
- Confinement tests **fail**, never skip, when host cannot confine — green-on-skip certifies nothing. Opt-out: `TINIO_SANDBOX_ALLOW_SKIP=1`.
- CI `default-filter` drops `binary(jail)` from *run* (still compiles/lints). Back into a leg: `--ignore-default-filter` / non-`ci` profile.
- Guest sync / agents: [Guest runs](#guest-runs-wsl2-and-macos-vms).

## Platform-gated verification (macOS)

- Gate script: `crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` (real Mac). CI records refusals; script asserts against `EXPECTED`.
- Crate is macos-only `cfg` — off-Mac certifies nothing; exemption home = script header. Fail-closed without `sandbox-exec`.
- **Two mechanisms, so the gate has two modes**: `TINIO_SANDBOX_MACOS_MECHANISM` (`sandbox-exec` default, `sandbox-init` alternative, any other value a usage error) is read by `crates/tinio-cli/src/sandbox/macos.rs`'s `launcher_backend` and by the crate's own harness, so `cargo test -p tinio-sandbox-macos` and `cargo test -p tinio-server --test sandbox_macos_e2e` each run green under **both** values — the e2e asserts the mechanism's own check off the value it was started with (`preflight` vs `sandbox-init validator`). The in-process mode never invokes `sandbox-exec`; its fail-closed surface is the validator's fork and the child's own apply.
- Readiness = SSE `listening` event (decision 19), not scraped line. Nested Seatbelt → fail as environment limit, not product defect.
- Short roots: `sun_path` 104 bytes — deep `$TMPDIR` is harness, not sandbox ([Guest runs](#guest-runs-wsl2-and-macos-vms) trap 3).
- Crate suites: `startup`, `exposures`, `network`, `relay`, `injection`, `handover` — the last reads a spawned child's fd table via `/usr/sbin/lsof`: control handle crosses `execve` once, launcher's own socket does not.

## Concurrency

- Cap children first — confinement/e2e → `--test-threads=1`; never two child-spawning suites concurrent.
- ≤4 agents; distinct `CARGO_TARGET_DIR`; prefer one suite over `--workspace`; >2 compile-or-confine → serialise.
- Guest `fork failed` → `docker restart` (no `pkill`). Scratch: remove named list only.
- Test/probe never re-execs own binary — watch child via standalone probe.

## Guest runs (WSL2 and macOS VMs)

Guest failures these explain = **harness artifacts, not product defects**. Caps: [Concurrency](#concurrency).

1. Sync carries `vendor/`, `.config`, `specs/`, `packaging/` + `crates` + manifests — else sync artifact.
2. CRLF unit file → 2 guest-only unix false fails. Fixed 2026-09-26: `.gitattributes` pins `packaging/tinio.service` to `eol=lf` and the worktree copy is LF (measured: LF 99 pass / CRLF 97 pass + exactly `the_checked_in_unit_is_this_rendering` + `the_contract_carries_the_same_unit` fail; host sees 89 — those two are `cfg(unix)`). Recurs → the sync carried a CRLF copy: re-ship `packaging/`.
3. `sun_path` overflow → suspect harness depth before environment ([macOS](#platform-gated-verification-macos)).
4. Never two agents on same VM concurrent — serialise or own tree.

## Platform-gated verification (Windows)

- Gate script: `crates/tinio-sandbox-windows/scripts/verify-windows.ps1`. **Owner: operator, not CI** — needs console; CI drops `binary(signals)` from *run*.
- Legs **fail**, never skip, when host cannot do what leg needs (same opt-out as WSL2). Console non-negotiable.
- Confined e2e (`crates/tinio-server/tests/sandbox_e2e_windows.rs`, four legs, `--test-threads=1`): launcher death takes the child and the standing ACE stays; a restarted launcher takes the **residue** over and a clean exit **keeps** it (the leg's third run then proves the profile is gone, because `took_the_profile_over()` is `false` only if the previous clean exit deleted it); an escaping bucket junction is refused as nonexistent inside the container; and **a child that dies before reporting readiness becomes the parent's own `error` frame** — induced by a served root whose own directory the container is not granted (a directory junction), so the child is refused pre-dispatch and writes nothing to stdout; asserted on that stream byte-exactly against the encoder, on the child's own exit code (2, not the launcher's 1 fallback), and on three witnesses that a child ran (`CreateProcessW`, `step="child exit" code=2`, the child's `tinio:` refusal on inherited stderr). That last leg is the Windows half of design §12's launcher-platform assertion; **the macOS arm of the same assertion still has no leg** (`sandbox_macos_e2e` has the spawn-and-read shape, not the induction). Fail-not-skip applies as above, and the leg shares the suite's `TINIO_LAUNCHER_EXIT_TIMEOUT_SECS` bound.
- `@confinement` out of CI: grant/DACL cost has no budget — operator opt-in; confined timeout separate from normal (`CONFINED_RUN_TIMEOUT`). Numbers / ACE shape → design §6. **No stale-ACE cleanup in launcher.**
- Readiness = SSE event (decision 19), not scraped line. Network: prove unconfined reachability first, then confined denial.
