# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`.
- Sync: `#[test]`. Exempt: runtime-shape tests.
- Conformance: `tinio-util` `testing` in `[dev-dependencies]`. Cucumber: no repeat asserts.

## Standard acceptance (pre-push gate)

Gate = local cmds for every ci.yml leg. `[profile.ci]` = `dev`; omit `--profile ci`.
CI unit = nextest (retries 2, `.config/nextest.toml`); local = `cargo test`; doctest = `--doc`.
Unix/windows-gnu: [Platform-gated verification](#platform-gated-verification-wsl2); Windows confinement: [Platform-gated verification (Windows)](#platform-gated-verification-windows).

```
# compile graphs — the default graph cannot see feature-gate breakage
cargo check --workspace --all-targets
cargo hack check --package tinio --each-feature --no-dev-deps
cargo hack check --package tinio-server --each-feature --no-dev-deps

# lints
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# unit tests — the default and no-default graphs; doctests are separate runs
cargo test --workspace --exclude tinio-e2e
cargo test --doc --workspace --exclude tinio-e2e
cargo test --workspace --exclude tinio-e2e --no-default-features
cargo test --doc --workspace --exclude tinio-e2e --no-default-features

# legs no default graph reaches
cargo test -p tinio-select --features parquet
cargo test -p tinio-server --features select-parquet
cargo bench -p tinio-fs -p tinio-server -- --test

# docs
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# e2e — scoping/env rules in Cucumber below
cargo build -p tinio                            # the @cli scenarios spawn the binary
cargo test -p tinio-e2e --test cucumber -- --retry 2
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @unix and not @windows and not @parquet and not @interop and not @boto3 and not @mc and not @confinement' --retry 2
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'
cargo test -p tinio-e2e --test traceability
```

`--features` = `test-parquet` Rust; features job = cargo-hack check (`--no-dev-deps`).

**Otel alignment (re-measured 2026-09-24)**: family pinned together — `opentelemetry*` 0.33 + `tracing-opentelemetry` 0.34 (manual bump; the 0.32/0.33 pairing this line used to record is dead). Measured: `cargo hack check -p tinio-server --each-feature --no-dev-deps` exit 0, 14/14 legs (`otel` and `--all-features` among them). `-p tinio` is covered by the `sandbox = ["api"]` fix, not by that measurement. (`--each-feature --all-features` is not a cargo-hack combination.)

### CI-only legs

Out of gate:

| Leg (ci.yml job) | Command | Needs |
|---|---|---|
| `interop`, `interop-port` | `cargo build --profile ci -p tinio`, then `cargo test --profile ci -p tinio-e2e --test cucumber -- --tags '@interop' --retry 1` | aws cli v2 + rclone on `PATH` |
| `audit` | `cargo audit` | cargo-audit, net; parse Cargo.lock |
| `semver` | `cargo semver-checks check-release --package tinio` | cargo-semver-checks + crates.io (no-op pre-release) |

Constitution VI: `audit` (dev+main), `semver` (API MUST check).

Ephemeral second server: `interop`'s `journey.feature` leg **dials it** (`the client targets the {word} server`, `steps/clients.rs`): a bucket created through its own endpoint is listed there and absent from the first's listing, and the first still answers — "serves alongside" now has an observation point. Only aws reaches it; the `rclone`/`mc`/boto3 legs keep the first server.

### rustdoc

- Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Intra-doc: `-D rustdoc::broken-intra-doc-links`, `-D rustdoc::private-intra-doc-links`.
- **`cargo test --doc` ≠ this gate**.
- Public docs: no private `[`foo`]`; `pub(crate)` → `` `foo` ``; prose `[0]` → `` `parts[0]` `` or `\[0\]`.
- Re-run doc gate after fix.

## CI legs

- `.github/actions/run-test-leg` mirrors gate unit/doc cmds.
- `features`: `--each-feature` on facade + tinio-server (no powerset). Catch cfg/`use` drift.
- `test-parquet`: parquet-only; `tests/parquet.rs` needs `required-features = ["parquet"]`.
- `tinio-server`'s **two** `[[test]]` targets — `sandbox_e2e` and `sandbox_e2e_windows` — carry `required-features = ["sandbox"]`; the `tinio` binary carries none, and its `sandbox` feature gates that arm's body rather than the program. An explicit `--no-default-features` build of `tinio` still compiles and serves, which is the property the gate exists to protect: `required-features` gates a *target*, so a feature-off build must not be the only configuration that links. `cargo hack check --package tinio --each-feature` is what keeps that true (design decisions 4, 19).
- `test-port` / `e2e-port` / `interop-port`: same cmds, windows/macos.

## Cucumber (tinio-e2e)

Layout: `tests/features/`, `tests/steps/`. Tags/FR-025/WSL2: `crates/tinio-e2e/README.md`.

### Cargo

- `cucumber` `"0.23"` once; `output-json`/`tracing` in `[dev-dependencies]` only.
- Targets: `[[test]] cucumber` (`harness = false`) + `traceability` (rejects `--tags`/`--retry`).
- Filter: `@parquet` iff `--features parquet`; `@unix`/`@windows` skip off-platform (else `fail_on_skipped` fails); `@interop`/`@boto3`/`@mc` iff `TINIO_E2E_EXTERNAL=1`; `@confinement` on macOS (its stub owes the refusal), elsewhere iff `TINIO_E2E_CONFINEMENT=1` (a confining target pays the launcher's ACL grant — [Windows](#platform-gated-verification-windows) "Owner" bullet).
- `--tags` replaces filter — re-state every exclusion incl. `not @unix and not @windows and not @confinement`.
- Env: `TINIO_E2E_BACKEND`, `TINIO_E2E_EXTERNAL=1`, `TINIO_E2E_CONFINEMENT=1`, `TINIO_E2E_REPORT`, `TINIO_BOTO3_PYTHON`.

```
cargo test -p tinio-e2e                     # unscoped: the default filter applies
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1
```

Gate e2e = fs/mem/`@parquet`/traceability. `@boto3`/`@mc` need `TINIO_E2E_EXTERNAL=1`; `@confinement` needs macOS or `TINIO_E2E_CONFINEMENT=1`.

### Gherkin

- English; features = `specs/001-s3-local-server/contracts/s3-surface.md`.
- `Given`/`When`=act, `Then`=assert; 1st-person; unanchored `{int}`/`{string}`/`{word}`.
- Steps in `tests/steps/mod.rs`; `metrics`/`tagging` → `common`; shared `World`.
- One behavior/scenario; `Examples` for data-driven.
- Tags: `@FR-xxx`/`@SC-xxx`/`@Txxx`; config `@fs`/`@mem`/…; gated `@parquet`; platform `@unix`/`@windows`; host-gated `@confinement`; CLI `@cli` (spawn own binary — hook skips); ext `@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`. Map: `config_from_tags`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff spec+S3 observable. No ID → Rust. Keep `tinio-util`.

## Management plane (tinio-api)

- `crates/tinio-api/tests/management.rs` = T056. Drive via `Plane` API, never raw transport. Plane acceptance; CLI process legs → `cli_lifecycle.feature` (`@cli`).
- Cover status/stop/metrics/openapi + pipe/unix ACL + reader lifecycle. Local `MetricsSource` double only.
- **Pipe successor window** (`ERROR_PIPE_BUSY`): do not paper over — **T147** owns. Dial fail → false "stopped".
- Facade forwards `openapi`/`tls` in `default`; `--no-default-features` = original graph.

## CLI (tinio)

- Process legs = cucumber `@cli` (`cli_lifecycle.feature` / `steps/cli.rs`); need `cargo build -p tinio` first. Assert readiness, FR-016 L2/L3, daemon, notify, status/stop/state-dir.
- Sandbox channels (`@cli @confinement`): the arm is **dispatched** (`steps/cli.rs`, `owed_arm`) — macOS refuses naming `platform` (never `build`); a feature-off binary refuses by clap or naming the build; a wired backend must reach readiness with no refusal's words. `--sandbox` is probed with `--help` so a known flag cannot serve under the probe. Run: `TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036' --concurrency 1`.
- **Coverage consequence, stated so no reader believes otherwise**: the honour arm runs in **no** CI leg. The pair is selected by exactly one — the macOS `e2e-port` fs pass, where the arm is the stub's deterministic refusal — while no job sets `TINIO_E2E_CONFINEMENT=1` and the mem pass re-states `not @confinement`. A host that can confine is the operator's manual opt-in ([WSL2](#platform-gated-verification-wsl2) / [Windows](#platform-gated-verification-windows) owner bullets); a `--tags` run that does not say `not @confinement` still selects the pair.
- Stays Rust — `crates/tinio/tests/lifecycle.rs`: Windows console leg only (unsafe). Assert drain, not exit code.
- Doctor (T057) — `crates/tinio/tests/doctor.rs`, `cargo test -p tinio --test doctor`:
  - store-less root → 0, meta + private-state rows `ok`, no `meta.redb` created (plain and `--dry-run`).
  - stale `state` file (`Stopped`) and stale unix socket → warn; `--fix` reclaims both.
  - stale-socket reclaim follows recorded `control_name`, never re-derived `socket_path` (`[api.unix] path` shape).
  - `NoStateFile` still probes derived socket name; `Unreadable` → warn `not inspected`, `--fix` clears.
  - live server → state row `ok`; `--fix` refuses (error finding, parseable `--json`).
  - orphaned `OBJECT_META` row, bucket-orphaned multipart subtree, stale `BUCKETS` record → information plain, finding `--dry-run`, gone after `--fix`, `object.txt` intact.
  - `meta.redb` integrity + fragmentation; `compact_needed` → warn, `--fix` compacts; unopenable store → error finding with the report intact.
  - `--json` on each leg = `contracts/doctor-report.schema.json` key sets; `state` token in neither stream.
  - `--read-only` diagnoses the relocated state dir; `--fix` on a store-less root creates the store; `--fix --read-only` repairs the relocated dir and leaves the root alone.
  - `--fix` reports `home root-state dirs … not repaired … T144`; a plain run does not.
  - `tokio` dev-dep + `#[tokio::test]`: the store-seeding legs drive async `tinio_fs` store constructors; `Runtime::block_on` is banned.
- Elsewhere: startup repair; service pure halves; `serve_read_only.rs`.

## Service verbs (manual)

- Needs supervisor + elevation — no auto leg. Pure halves: `tinio-cli/tests/service.rs`. Below = verification gap.
- Linux `--user` / system: install → notify unit + state `WorkingDirectory` + query; stop → stopped; elevated also `is-enabled` + journal `READY`/`STOPPING`. Linger for reboot. No elevation → remedy, never raw `EACCES`.
- macOS: `install` does **not** start (`Disabled`); `start` → running; assert via `launchctl print`.
- Windows elevated: RUNNING + path; stop = drain not kill; `state`/socket gone. Not elevated → Administrator remedy (≠ missing-service 1060); `--user` = usage exit 2.
- Both: uninstall → not installed; second uninstall = backend error.

## Platform-gated verification (WSL2)

- Confinement (`--sandbox`): `bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` — both models, the confined server, the external e2e leg (`TINIO_E2E_EXTERNAL=1`; needs aws cli v2, rclone, mc, the e2e boto3 venv).
- Confinement tests **fail**, never skip, where the host cannot create an unprivileged userns — an `eprintln!` skip is swallowed by libtest and reports green having certified nothing; `TINIO_SANDBOX_ALLOW_SKIP=1` opts out and names the `unshare` command tried. Ubuntu 24.04's AppArmor default is therefore red until userns are allowed or a profile ships: intended, not a flake.
- **Owner: the operator, not CI.** `.config/nextest.toml`'s `[profile.ci] default-filter` takes `binary(jail)` out of the workspace leg's *run* — that leg still compiles and lints it, so a build break stays red — because no hosted runner owes the precondition: an ubuntu job is permanently red, never green. Gate is the script above on a WSL2 host, plus the `@confinement` CLI command ([CLI (tinio)](#cli-tinio)) — no leg runs either, since a hosted runner cannot create the userns. Back into a leg on a host that can confine: `--ignore-default-filter`, or any profile but `ci`. Local gate `cargo test --workspace --exclude tinio-e2e` runs it regardless.
- `#[cfg(unix)]` invisible on Win; unused-import may be CI-load-bearing (E0599).
- WSL2 if Win-clean/CI-red (`/mnt/e/GitHub/tinio`, ext4 `CARGO_TARGET_DIR`).
- crates/tinio-e2e/README.md "WSL2":
  `cargo clippy --workspace --all-targets -- -D warnings`
- Hammer tests (`wait_for`, 60s, `TINIO_TEST_WAIT_TIMEOUT_SECS`) starve on loaded CI — repro WSL2 before flaky-vs-real.
- `lint` `windows-gnu`: ubuntu cross. `cargo clippy --target x86_64-pc-windows-gnu
  --workspace --all-targets -- -D warnings` (`rustup target add
  x86_64-pc-windows-gnu` + mingw-w64).

## Platform-gated verification (Windows)

- Confinement (`--sandbox`): `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1` — AppContainer/DACL suite, event relay, signal relay, confined `serve` e2e, on every volume present (`TINIO_SANDBOX_TEST_VOLUMES`, semicolon-separated, default `C:\;E:\`; locally `C:\` NTFS, `E:\` ReFS). It prints the filesystems it actually covered (NTFS and ReFS agree on every probed ACL case), skips a volume `Test-Path` misses, refuses a selection naming none, and checks every native exit status — the script is the gate. Needs a writable volume; the ACL legs need `icacls.exe` from the system directory.
- **Owner: the operator, not CI — the console is what no hosted runner owes.** `.config/nextest.toml`'s `[profile.ci] default-filter` takes `binary(signals)` out of the workspace leg's *run* (`test-port` still compiles and lints the crate); that leg would otherwise be permanently red, since `tests/signals.rs` **fails** where a console control event cannot be delivered. Gate is the script above on a Windows host with a console, a writable volume and `icacls.exe`, plus the `@confinement` CLI command ([CLI (tinio)](#cli-tinio)); it is also the only ReFS run (no leg reaches ReFS: system temp volume is NTFS). Back into a leg: `--ignore-default-filter`, or any profile but `ci`.
- **Grant cost**: the `(OI)(CI)` ACE is granted before `CreateProcessW` and rolled back after the child exits, each pass walking the served corpus. Measured 2026-09-22 (3 NTFS, 1 ReFS): NTFS grant alone 32.3–37.8 s vs the 30 s readiness budget (`crates/tinio-e2e/tests/steps/clients.rs`'s `START_TIMEOUT`; this crate's `tests/common/mod.rs` `TIMEOUT` is fixed against it), ReFS under a third. Ruling 78 (2026-09-22): the launcher's own directory takes one **non-inherited** `RX` ACE + the `*.dll` beside it non-inherited `RX` ACEs in one `icacls` invocation (`*.dll` pattern — a path list is not a form it accepts); caller exposures keep the inherited form (`tests/backend.rs` pins the exposure set and per-directory shape, `tests/acl.rs` the file shape). Numbers + spread: design §6 and its Risks row; operator guidance: `README.md`.
- **Grant variance is the reason `@confinement` stays out of the CI legs — fact only.** One host, one command (`tinio server <empty root> --port 0 --sandbox`, file-redirected streams, harness-shaped env), six runs back to back: readiness at 4.6 s, 8.7 s, 8.7 s, 18.1 s, 57.7 s, and one run past 120 s with no readiness event (killed). Empty served root, so the walk that costs is the program directory's `*.dll` list — the same directory and list every run; no conclusion about the launcher's cause is drawn here (verified 2026-09-24 — the two bullets below). Impact: no leg can budget it, `steps/cli.rs` therefore carries a confined-run bound (`CONFINED_RUN_TIMEOUT`, 180 s) apart from `RUN_TIMEOUT`, and the pair runs only on the operator's opt-in host.
- **ACE latency, measured 2026-09-24 (Windows sandbox owner) — the residual ACEs are exonerated; the cost is one directory object's descriptor write.** `…\target\debug` carried **57** stale `S-1-15-2-…:(RX)` ACEs (one per hard-killed run; a later run on another root never takes one over). Removing all 57 in one `icacls /remove:g` call (23.5 s; Access 68 → 11, SDDL 6140 → 497, and the cleaned SDDL is **byte-identical** to a fresh dir under the repo root) left grant/rollback at **19–30 s** ⇒ unrelated to ACE count. Decisive pair, **identical SDDL**: `Set-Acl` on `target\debug` **9.208 s** vs fresh dir **0.003 s** (3000×), while creating a file there 0.003 s and writing its attributes 0.023 s ⇒ the cost is writing **those objects'** security descriptors, not their ACL, contents or metadata in general. Controls excluding the rest (same ReFS volume): 56 synthetic package ACEs 0.03 s; 20 ACEs added one at a time 0.034 s each; 5 000 files directly inside 0.19 s; a 5 000-file subtree one level down 0.21 s; an active file writer 0.06 s; a process CWD / a running image inside 0.04–0.05 s; `NotContentIndexed` 0.03 s; **a fresh dir created inside `target\debug` 0.06 s**; fresh dir on either volume 0.03–0.12 s; concurrent builders absent ⇒ still 11–25 s. Candidates, in order: the host's security/filesystem filter (every inherited ACE here names `FLIER-WIN11\CodexSandboxUsers`, new objects unaffected ⇒ per-object verdict/cache), then ReFS per-object state on long-lived objects.
- **ACE latency, impact + handling.** Three `sandbox_e2e_windows` runs `--test-threads=1` after the cleanup still failed, all on the same path — `Grants::rollback` → `icacls /remove:g`, i.e. the harness's 15 s bound below one local DACL write. On such a host: set `TINIO_LAUNCHER_EXIT_TIMEOUT_SECS` (**default stays 15 s**; `crates/tinio-server/tests/support/windows_e2e_harness.rs`'s `exit_timeout`) and keep `--test-threads=1`; the real gate remains the operator's host and the platform scripts. **Do not implement stale-ACE cleanup in the launcher**: zero measured gain, a security-sensitive deletion (which SID is dead?), and a SID belonging to a run started between the check and the delete loses its program-dir ACE while its rollback silently no-ops (`icacls /remove:g` exits 0 on a missing entry).
- Measure it: `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 -AclCost [<dirs> <files>]` — 200×1000 synthetic corpus under `%TEMP%`, both passes timed by `Measure-Command`, asserting `icacls`'s own exit status and the ACE at depth 2 before and after removal (a refused grant still returns a duration).
- Network suite: `cargo test -p tinio-sandbox-windows --test network` — the outbound leg proves the target reachable unconfined first, then unreachable confined, and **fails** rather than skips when the host cannot reach it (`TINIO_SANDBOX_TEST_TARGET`, default `1.1.1.1:53`; `TINIO_SANDBOX_ALLOW_SKIP=1` opts out). LAN leg is half-measured by construction (design §7).
- Confined server e2e: `cargo build -p tinio`, then `cargo test -p tinio-server --test sandbox_e2e_windows` (both `[[test]]` targets carry `required-features = ["sandbox"]` — CI-legs bullet above). The build is the one the Windows script runs (`crates/tinio-sandbox-windows/scripts/verify-windows.ps1`), because the e2e leg resolves the `tinio` binary from its own profile directory.
- Readiness is an event, not a scraped line (decision 19): one SSE frame on stdout per event (`event:` line, `data:` line carrying its JSON, blank line), every log on stderr; a consumer accumulates to the blank line and ignores non-frames. The frames are the `tinio` CLI's, produced by `crates/tinio-cli/src/events.rs`, and every consumer decodes them: `crates/tinio-e2e/tests/steps/clients.rs` and `crates/tinio-e2e/tests/steps/cli.rs`, `crates/tinio-server/tests/serve_read_only.rs`, and `crates/tinio/tests/lifecycle.rs`; the sandbox suites read frames through `crates/tinio-server/tests/common/mod.rs`. No `listening on …` line is printed anywhere, and the example that once printed one is retired.
- Three defects stood between the suite and green, each fixed and measured: the AppContainer `std::fs::canonicalize` refusal (root resolution in `tinio-fs` + the containment proof behind the vendored `soft-canonicalize` route around it); the blocking listener the launcher binds (one `set_nonblocking` in the CLI's rebuilt confined child — `crates/tinio-cli/src/sandbox/mod.rs` — because Windows' tokio cannot detect the mode, so hyper blocked in `recv`); the 15 s clean exit (`pacing::sleep_checked` answered a stop only between chunk-bounded sleeps). Rows: design's verified-evidence table.
- Windows legs **fail** rather than skip when the host cannot do what a leg needs — rule and opt-out are the WSL2 section's; the console is the non-negotiable case.
