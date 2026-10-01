# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`.
- Sync: `#[test]`. Exempt: runtime-shape tests.
- Conformance: `tinio-util` `testing` in `[dev-dependencies]`; a crate's own harness module is a **non-default feature** its dev edges turn on (`tinio-sandbox-windows`'s `testing`, whose `default` is empty), because the module has to be lib code to cross the `tests/` boundary at all. Cucumber: no repeat asserts.
- Placement (2026-10-01, `plans/2026-10-01-test-placement-cleanup.md`):
  - Unit test — one function/type, local setup: `#[cfg(test)] mod tests` in the file that defines the subject, or in the sibling file that module declares (`#[cfg(test)] mod tests;` → `src/backend/tests.rs`) once the module outgrows one file. Not a `tests/` integration target, and not a crate-level module gathering several unrelated modules' tests.
  - Flow test — multi-step product surface: cucumber iff the migration rule admits it; otherwise `tests/`.
  - Spawn sites may be inline. `spawn_guard` reads `crates/*/tests/**` only, so an inline site leaves its scan by construction; record the hole in that gate's module doc (relay, the Windows harness).
  - Harness shared by inline legs and `tests/` targets: lib code behind a `testing`-style feature — a `tests/` module cannot cross into `src/`. Consequences: its deps move from `[dev-dependencies]` to optional `[dependencies]` behind that feature (`tempfile`), and a child role is named by its full libtest path under `--exact`.
  - **A file shared by `#[path]` must be declared at file scope, never inside an inline module.** A `#[path]` resolves against the declaring file's directory **plus the name of any enclosing inline module**, so `#[path = "…"] mod fixture;` written inside `mod tests` has `src/<module>/tests/` as its base — and a relative path that climbs out of it traverses a directory that does not exist. **Windows folds the `..` away lexically and compiles; Linux and macOS resolve a path component by component and refuse**, so the lib-test target does not compile there at all while the same tree is green on Windows (`tinio-select`'s shared `json_fixture`, 2026-10-02, where the trap was invisible until the WSL leg ran). Declare the module at file scope — its base is the file's own directory, every component of which is real — and let the inline `mod tests` reach it with `use super::fixture;`. A file shared with a bench is declared by each includer, as `benches/select_scan.rs` does.
  - `tests/` keeps the boundary itself (a re-export list, a published-path binding), legs whose oracle is a foreign process or the OS, fixture and generator pairs shared with benches, and a property suite whose subject is the crate's public API rather than one function (`tinio-core/tests/validation.rs`). A property suite of one module moves inline with its subject, seed file and all (`crates/tinio-fs/src/meta.proptest-regressions`).
  - Deleting a duplicate: only when the survivor asserts the same rule over the same subject. An assertion the survivor lacks is folded into it first — never dropped.

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
- Default filter owns platform/external/confinement gates (confinement is three-state: `TINIO_E2E_CONFINEMENT` unset = platform default, `1`/`true` = force in, `0`/`false` = force out, anything else refused); `--tags` replaces it — re-state every exclusion.
- Gate e2e = fs/mem/`@parquet`/traceability. Spec IDs: `--test traceability`.
- Migrate iff spec+product-surface observable; rule: `docs/superpowers/specs/2026-08-31-cucumber-bdd-migration-design.md` §Migration rule. No ID → Rust. Keep `tinio-util`.
- The suite's manifest carries no comments (`docs/cargo.md`); its reasons live here. `http`/`hyper-util`: the `@cli` scrape's request line — `tinio_api::client::Call` takes an `http::Method`, and the Windows pipe arm hands the client's own `exchange` an already-dialed stream through `hyper_util::rt::TokioIo`, both the product's types rather than the suite's. `tinio-api` is `default-features = false`: the steps parse `state` through the type that writes it, and need nothing the router, TLS or OpenAPI layers carry. `tinio-sandbox-windows`'s `testing` feature is the `@cli` Windows legs' `icacls`/`junction` devices, which the steps spell through the facade's re-export (Cargo unifies the two edges; on a host the crate is not built for it is the empty library its root `#![cfg]` makes it). The Linux-only `rustix` dev-dep serves the `I send the server SIGTERM` step (`steps/cli.rs`) and nothing else — a signal cannot be sent from `std`, the in-place arm is the only one where it reaches the server rather than a launcher, and the step is Linux for that reason.

## Management plane (tinio-api)

- T056 via `Plane` API, never raw transport. CLI process legs → `@cli` cucumber.
- Pipe successor (`ERROR_PIPE_BUSY`): do not paper over — **T147** owns.
- Pipe-pool tests need two workers: `ClientOptions::open` blocks the calling thread, so a busy or draining pool is observable only from a multi-thread runtime — the two scenarios are `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` and the crate's **dev** edge carries tokio's `rt-multi-thread`, dev-only (`plans/2026-09-25-sandbox-followups.md` §5 owns the scenarios).

## CLI (tinio)

- Process legs = cucumber `@cli` (build `tinio` first). Windows console drain stays Rust (`lifecycle.rs`).
- `@cli @confinement`: feature-off refuses by build, never by platform; feature-on must reach readiness. Automatic honour arm = macOS `e2e-port` only; Linux/Windows = operator opt-in ([WSL2](#platform-gated-verification-wsl2) / [Windows](#platform-gated-verification-windows)).
- Doctor (T057): two halves. The flows are `doctor.feature` (`@cli`; `@unix` for the socket residue, `@windows` for the held-handle failed repair) — each repairable defect the reclamation matrix names, driven plain → `--dry-run` → `--dry-run --json` → `--fix` against one fixture, so the modes' contrast is asserted rather than assumed. The residue is `crates/tinio/tests/doctor.rs` (`cargo test -p tinio --test doctor`), the `--json` **key set** against `contracts/doctor-report.schema.json` — the schema's `additionalProperties: false` at both levels, which no scenario asserts: the step layer has no JSON reader (`steps/cli.rs`'s `report_severity` reads the one severity field by text and says why). Coverage change with the move: the migrated legs' `#[cfg(not(feature = "api"))]` arms went with them, the cucumber child being a default-feature binary — an `api`-off plain run is no longer process-asserted, and never was a gate (the `no-default-features` keeper is `cargo hack check`, compile-only; the checks themselves are `crates/tinio-cli/src/commands/doctor/tests.rs`).

## Read-only mode (FR-023)

- `read_only.feature`. Wire half (`@fs @read-only`): reads unchanged, one refuser per mutating class, missing-bucket refusal — its read-write reference arm is the untagged `@fs` scenario beside it. Surface halves: a genuinely read-only root = `@unix` (chmod 0500, step owns the restore — a 0500 tempdir cannot be removed); mode channels + FR-016 precedence, relocated state dir, in-root meta view = `@cli` (real binary; the retired `tinio-server/tests/serve_read_only.rs` legs).
- Residue: `crates/tinio-server/tests/read_only.rs` — the derived `<home>/.tinio/roots/<hash>/` layout. Reason: no scenario reaches the derived path (every `@cli` child is handed explicit `TINIO_STATE_DIR`).

## Service verbs (manual)

Supervisor + elevation — no auto leg. Pure halves: `crates/tinio-cli/src/service/mod.rs`'s inline `mod tests` (the `tests/service.rs` file retired 2026-10-01).

## Platform-gated verification (WSL2)

- Gate script: `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`. **Owner: operator, not CI** (hosted runner cannot create userns).
- Confinement tests **fail**, never skip, when host cannot confine — green-on-skip certifies nothing. Opt-out: `TINIO_SANDBOX_ALLOW_SKIP=1`.
- CI `default-filter` drops `binary(jail)` from *run* (still compiles/lints). Back into a leg: `--ignore-default-filter` / non-`ci` profile.
- No product-surface leg and no host here can drive (`plans/2026-09-29-sandbox-bdd-e2e.md` §7 D1/D4/D6): the seccomp-skip **warning** path (needs an arch with no syscall table — the skip's *state* is a metric, `specs/001-s3-local-server/data-model.md` §Metrics); the more-than-one-thread refusal (unreachable from the CLI, which confines before any thread exists); the `EINVAL` → past `CAP_LAST_CAP` arm (needs a pre-5.9 kernel).
- **Confinement e2e now rides the cucumber family** (2026-10-01, `plans/2026-10-01-test-placement-cleanup.md`): `crates/tinio-server/tests/serve_sandbox_channels.rs` retired (its two channel legs are `confinement.feature`'s; the refusal shape lives in `steps/cli.rs`'s dispatch; the third — the feature-off flag refusal FR-036 owes — stays Rust in `crates/tinio-server/tests/sandbox_feature_off.rs`, which the `no-default` leg compiles in and a default-features run compiles out), and `sandbox_e2e.rs`'s control-token leg is that file's `@linux` "A confined run writes its control token to neither stream" (the step layer reads the run's streams and its published `state`). **Coverage change, operator-ruled and accepted**: the family's default filter excludes `@confinement` off macOS, so the default workspace leg (`cargo nextest --workspace --exclude tinio-e2e`) — where both Rust legs ran — no longer runs these arms; here they are operator opt-in (`TINIO_E2E_CONFINEMENT=1`, `--concurrency 1`). `sandbox_e2e.rs`'s `EROFS` leg stays in Rust: the kernel's answer through `/proc/<confined-pid>/root` names no step-reachable surface.
- **Closed 2026-09-30** (product finding opened the same day, WSL2, quiet guest): a confined in-place server ignored `SIGTERM` — alive 60 s after the first one, a second `SIGTERM` ignored, `SIGKILL` needed, where the unconfined control exits in 761 ms. Cause: the signal wake was `signal-hook`'s `libc::send`, i.e. `sendto` on Linux — the number the seccomp filter denies, its error dropped, so the wake byte never landed. Fixed in `crates/tinio-cli/src/signal.rs` (owned socketpair, `signal_hook::low_level::register`, one `write(2)`); leg = `confinement.feature`'s `A confined server stops on SIGTERM`.
- Guest sync / agents: [Guest runs](#guest-runs-wsl2-and-macos-vms).

## Platform-gated verification (macOS)

- Gate script: `crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` (real Mac). CI records refusals; script asserts against `EXPECTED`.
- **The CI leg carries the confinement family, budgeted**: `e2e-port`'s macOS row runs it through the serial `--tags` step (the parallel default pass is forced to exclude it — `TINIO_E2E_CONFINEMENT=0`) — measured 2026-09-30 on the guest (macOS 15.8.1, 6 vCPU, `ulimit -n 256`): 31 elements, 5 s serial and 31/31 green, the leg 33–36 s wall, no tag narrowing needed; commands, tree hash and the run's conditions = `plans/2026-09-29-sandbox-bdd-e2e.md` §6 (M1).
- Crate is macos-only `cfg` — off-Mac certifies nothing; exemption home = script header. Fail-closed without `sandbox-exec`.
- **Two mechanisms, so the gate has two modes**: `TINIO_SANDBOX_MACOS_MECHANISM` (`sandbox-exec` default, `sandbox-init` alternative, any other value a usage error) is read by `crates/tinio-sandbox-macos/src/mechanism.rs` (`Options::from_env`, reached through the facade's `serve::Spec` since 2026-09-27; the CLI names no platform backend) and by the crate's own harness, so `cargo test -p tinio-sandbox-macos` and `cargo test -p tinio-server --test sandbox_macos_e2e` each run green under **both** values — the e2e asserts the mechanism's own check off the value it was started with (`preflight` vs `sandbox-init validator`). The in-process mode never invokes `sandbox-exec`; its fail-closed surface is the validator's fork and the child's own apply.
- Readiness = SSE `listening` event (decision 19), not scraped line. Nested Seatbelt → fail as environment limit, not product defect.
- Short roots: `sun_path` 104 bytes — deep `$TMPDIR` is harness, not sandbox ([Guest runs](#guest-runs-wsl2-and-macos-vms) trap 3).
- Crate suites: `startup`, `exposures`, `network`, `injection`, `handover` — the last reads a spawned child's fd table via `/usr/sbin/lsof`: control handle crosses `execve` once, launcher's own socket does not. The two relay legs are inline unit tests in `src/relay.rs` — outside `spawn_guard`'s scan by construction, and the hole is recorded in `crates/tinio-util/tests/spawn_guard.rs`'s module doc.
- No product-surface leg and no host here can drive (`plans/2026-09-29-sandbox-bdd-e2e.md` §7 D2/D3/D7): the pre-flight **skip** arm (needs a release-profile run); the missing-`sandbox-exec` path (needs the helper renamed or hidden for one leg); the control-descriptor-count refusal and the pre-flight timeout (no CLI route).

## Concurrency

- Cap children first — confinement/e2e → `--test-threads=1`; never two child-spawning suites concurrent.
- ≤4 agents; distinct `CARGO_TARGET_DIR`; prefer one suite over `--workspace`; >2 compile-or-confine → serialise.
- Guest `fork failed` → `docker restart` (no `pkill`). Scratch: remove named list only.
- A probe needing **single-threadedness** (`unshare(CLONE_NEWUSER)` refuses in a multi-threaded process, and libtest runs every test on a worker thread) is a standalone `[[bin]]` — `tinio-sandbox-linux`'s `sandbox-child`, never a re-executed libtest child. A probe owing no such property may re-exec the test binary: `tinio-sandbox-windows`'s roles do, named by full libtest path under `--exact`.

## Guest runs (WSL2 and macOS VMs)

Guest failures these explain = **harness artifacts, not product defects**. Caps: [Concurrency](#concurrency).

1. Sync carries `vendor/`, `.config`, `specs/`, `packaging/` + `crates` + manifests — else sync artifact.
2. CRLF unit file → 2 guest-only unix false fails. Fixed 2026-09-26: `.gitattributes` pins `packaging/tinio.service` to `eol=lf` and the worktree copy is LF (measured: LF 99 pass / CRLF 97 pass + exactly `the_checked_in_unit_is_this_rendering` + `the_contract_carries_the_same_unit` fail; host sees 89 — those two are `cfg(unix)`). Recurs → the sync carried a CRLF copy: re-ship `packaging/`.
3. `sun_path` overflow → suspect harness depth before environment ([macOS](#platform-gated-verification-macos)).
4. Never two agents on same VM concurrent — serialise or own tree.

## Platform-gated verification (Windows)

- Gate script: `crates/tinio-sandbox-windows/scripts/verify-windows.ps1`. **Owner: operator, not CI** — needs console; CI drops `binary(signals)` from *run*.
- Legs **fail**, never skip, when host cannot do what leg needs (same opt-out as WSL2). Console non-negotiable.
- Confined e2e (`crates/tinio-server/tests/sandbox_e2e_windows.rs`, three legs, `--test-threads=1`; **the placement cleanup keeps all three** — `plans/2026-10-01-test-placement-cleanup.md` — each for a checkable reason the step layer lacks: the package-SID DACL oracle, the Windows console control event leg 2's stop needs, leg 3's directory-junction induction): the standing ACE stays on the served root while the server runs and after the launcher is hard-killed (the *launcher death takes the child* half of that leg is `plans/2026-09-29-sandbox-bdd-e2e.md` §5's migration, asserted by `confinement.feature`'s P4 — a step layer has no package-SID oracle except the launcher's log); a restarted launcher takes the **residue** over and a clean exit **keeps** it (the leg's third run then proves the profile is gone, because `took_the_profile_over()` is `false` only if the previous clean exit deleted it); and **a child that dies before reporting readiness becomes the parent's own `error` frame** — induced by a served root whose own directory the container is not granted (a directory junction), so the child is refused pre-dispatch and writes nothing to stdout; asserted on that stream byte-exactly against the encoder, on the child's own exit code (2, not the launcher's 1 fallback), and on three witnesses that a child ran (`CreateProcessW`, `step="child exit" code=2`, the child's `tinio:` refusal on inherited stderr). That last leg is the Windows half of design §12's launcher-platform assertion; its macOS half is `confinement.feature`'s P3 (the `.env`-symlink induction). **The escaping bucket junction the suite used to refuse as nonexistent inside the container is `confinement.feature`'s P5 now** (`plans/2026-09-29-sandbox-bdd-e2e.md` §5 retired this file's `a_confined_server_refuses_an_escaping_bucket_junction_as_nonexistent`), and the plan is where its causal chain lives: `NoSuchBucket` is what `ensure_bucket`'s reparse fallback answers once the containment proof has refused the bucket's directory, while the world where a reverted canonicalizer arm lets that bucket through is caught by the object-level lstat policy as `AccessDenied`/403 — so the *code* is the assertion and the status is its consequence, and the ungranted counter-arm (403) is what makes the scenario's grant step load-bearing rather than decoration. Fail-not-skip applies as above, and the leg shares the suite's `TINIO_LAUNCHER_EXIT_TIMEOUT_SECS` bound.
- `@confinement` out of CI: grant/DACL cost has no budget — operator opt-in; confined timeout separate from normal (`CONFINED_RUN_TIMEOUT`). Numbers / ACE shape → design §6. **No stale-ACE cleanup in launcher.**
- Readiness = SSE event (decision 19), not scraped line. Network: prove unconfined reachability first, then confined denial.
- No leg on either launcher platform: the 30 s readiness bound's end-to-end induction — the arm needs a child alive and silent past the bound, which no fixture-reachable failure leaves; the core launcher's own leg covers the expired bound reaching the abort (`plans/2026-09-29-sandbox-bdd-e2e.md` §7 D8).
