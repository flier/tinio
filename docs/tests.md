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
cargo test -p tinio-e2e --test cucumber -- --retry 2
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @parquet and not @interop and not @boto3 and not @mc' --retry 2
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'
cargo test -p tinio-e2e --test traceability
```

`--features` = `test-parquet` Rust; features job = cargo-hack check (`--no-dev-deps`).

### CI-only legs

Need extra tools/baseline; out of gate:

| Leg (ci.yml job) | Command | Needs |
|---|---|---|
| `interop`, `interop-port` | `cargo build --profile ci -p tinio-server --example serve`, then `cargo test --profile ci -p tinio-e2e --test cucumber -- --tags '@interop' --retry 1` | aws cli v2 + rclone on `PATH` |
| `audit` | `cargo audit` | cargo-audit, net; parse Cargo.lock |
| `semver` | `cargo semver-checks check-release --package tinio` | cargo-semver-checks + crates.io (no-op pre-release) |

Constitution VI: `audit` (dev+main), `semver` (API MUST check).

### rustdoc

- Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Intra-doc: `-D rustdoc::broken-intra-doc-links`, `-D rustdoc::private-intra-doc-links`.
- **`cargo test --doc` ≠ this gate**; green `cargo test` ≠ green docs.
- Public `//!`/`pub`: no private `[`foo`]`.
- `foo` `pub(crate)` → `private-intra-doc-links`. Write `` `foo` ``.
- `[0]`/`parts[0]` in prose → `broken-intra-doc-links`; `` `parts[0]` `` or `\[0\]`.
- Re-run doc gate after fix — not just `cargo test`.

## CI legs

- `.github/actions/run-test-leg` `run:` = unit/doc cmds; gate mirrors.
- `features`: `--each-feature` on facade + tinio-server. No powerset.
- Catch `use` out of `#[cfg]` / leftover path.
- `test-parquet`: parquet-only (`parquet`/`select-parquet` off default).
- `tests/parquet.rs` needs `required-features = ["parquet"]` or `pub mod parquet`/`[[test]]` break.
- The `tinio-server` `sandbox_e2e` target requires the `sandbox` feature; **the `serve` example deliberately carries no `required-features`**, and adding one breaks this leg. The `no-default` mode below builds `--example serve --no-default-features` (because `serve_read_only` spawns it), and an explicit `--example` selection whose `required-features` are unmet is a hard error — `exit 101` — while the implicit `cargo test --workspace --no-default-features` silently skips such a target, so the gate does not see it. The example must build and serve in every feature configuration (design decisions 4 and 19); what the feature gates is the `--sandbox` arm's body, not the program's existence.
- `test-port` / `e2e-port` / `interop-port`: same cmds, windows/macos.

## Cucumber (tinio-e2e)

Layout: `tests/features/`, `tests/steps/`. Tags/FR-025/WSL2: `crates/tinio-e2e/README.md`.

### Cargo

- `cucumber` `"0.23"` once; `output-json`/`tracing` in `[dev-dependencies]` only (never lib).
- Targets: `[[test]] cucumber` (`harness = false`) + `traceability`.
- `--test cucumber` only; `traceability` rejects `--tags`/`--retry`.
- Bare `cargo test -p tinio-e2e` OK.
- Filter (tests/cucumber.rs): `@parquet` iff `--features parquet`.
- `@interop`/`@boto3`/`@mc` iff `TINIO_E2E_EXTERNAL=1`.
- `--tags` replaces whole filter — re-state exclusions.
- Env: `TINIO_E2E_BACKEND` (`mem`; `@fs`/`@mem` win), `TINIO_E2E_EXTERNAL=1`.
- Also: `TINIO_E2E_REPORT=<path>`, `TINIO_BOTO3_PYTHON`.

```
cargo test -p tinio-e2e                     # unscoped: the default filter applies
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1
```

Gate e2e = fs/mem/`@parquet`/traceability. `@boto3`/`@mc` need `TINIO_E2E_EXTERNAL=1`.

### Gherkin

- English; features = `specs/001-s3-local-server/contracts/s3-surface.md`.
- `Given`/`When`=act, `Then`=assert; 1st-person; `And`; unanchored `{int}`/`{string}`/`{word}`.
- S3 mods in `tests/steps/mod.rs`: `buckets`, `clients`, `common`, `conditions`, `errors`.
- Also: `listing`, `multipart`, `objects`, `reserved_paths`.
- `metrics`/`tagging` → `common`; shared `World`.
- Data-driven: `Examples`; one behavior/scenario.
- Feature tags: `@FR-xxx`/`@SC-xxx`/`@Txxx` (filter, no hook).
- Config: `@fs`/`@mem`/`@nested-root`/`@checksum-on`.
- Also: `@minimal-caps`/`@cold-listing`/`@max-buckets-3`/`@tagging-off`.
- Gated: `@parquet` + `--features parquet`. Map: `config_from_tags`.
- Ext: `@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff spec+S3 observable. No ID → Rust. Keep `tinio-util`.

## Platform-gated verification (WSL2)

- Confinement (`--sandbox`): `bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` — both models, the confined server, and the external e2e leg (`TINIO_E2E_EXTERNAL=1`; needs aws cli v2, rclone, mc and the e2e boto3 venv, and fails without them).
- The confinement tests **fail** rather than skip when the host cannot create an unprivileged user namespace — a skip through `eprintln!` is swallowed by libtest and would report a green suite that certified nothing. `TINIO_SANDBOX_ALLOW_SKIP=1` opts out deliberately, naming the `unshare` command that was tried. A CI runner on Ubuntu 24.04's AppArmor default is therefore red until unprivileged user namespaces are allowed or the binary carries a profile; that is the intended reading, not a flake.
- `#[cfg(unix)]` (copy/ETag) invisible on Win; unused-import may be CI-load-bearing (E0599).
- WSL2 if Win-clean/CI-red (`/mnt/e/GitHub/tinio`, ext4 `CARGO_TARGET_DIR`).
- crates/tinio-e2e/README.md "WSL2": `cargo clippy --workspace --all-targets -- -D warnings`
- Hammer tests (`wait_for`, 60s, `TINIO_TEST_WAIT_TIMEOUT_SECS`) starve on loaded CI.
- Repro WSL2 before flaky-vs-real.
- `lint` `windows-gnu`: ubuntu cross, no native Win. `cargo clippy --target x86_64-pc-windows-gnu --workspace --all-targets -- -D warnings` (`rustup target add x86_64-pc-windows-gnu` + mingw-w64).

## Platform-gated verification (Windows)

- Windows confinement (`--sandbox`): `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1` — the AppContainer/DACL suite, the event relay, the signal relay and the confined `serve` e2e, on every volume present (`TINIO_SANDBOX_TEST_VOLUMES`, semicolon-separated, defaults to `C:\;E:\`; locally `C:\` NTFS and `E:\` ReFS). The script **prints the filesystem each run actually covers**, because NTFS and ReFS agree on every probed ACL case and a regression that broke one would otherwise be invisible; a volume `Test-Path` does not find is skipped rather than failed, a selection naming no volume is refused, and **every native command's exit status is checked** — the script is the gate, so a red suite cannot exit 0. Every leg needs a writable volume and the ACL legs need `icacls.exe` from the system directory.
- **CI needs no new job**: the portable Windows legs already run `cargo nextest run --workspace --exclude tinio-e2e` (`.github/actions/run-test-leg`, job `test-port`), so this crate's suite is picked up the moment it is a workspace member — on the system temp volume, NTFS only, and with whatever console the runner's session has. ReFS is what the local script adds, and the console requirement is the one that cannot be settled by looking: `tests/signals.rs` **fails rather than skips** where a console control event cannot be delivered (the last bullet's rule), so a runner without one owes either a console or an explicit exclusion of `--test signals`, never a green run that proved nothing. The relay itself needs no windowed console — the interactive shell this phase ran in reports `GetConsoleWindow()` as zero, a ConPTY session, and delivers a stop regardless (measured 2026-09-21) — but the runner's session is not measured here.
- The ACL pass costs two passes over the served tree, and the grant is the one that has to fit the readiness budget: the `(OI)(CI)` ACE propagates by filesystem inheritance to every descendant, so the grant before `CreateProcessW` and the rollback after the child exits each walk the whole corpus. **How it is measured, and the check that makes it a measurement**: `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 -AclCost [<dirs> <files>]` builds a synthetic corpus under `%TEMP%` (200 directories × 1000 files by default), times both passes with `Measure-Command`, and **asserts `icacls`'s own exit status** and that the ACE reached a pre-existing descendant at depth 2, before and after the removal — a refused grant still returns a duration, so a number without those checks is not one. **Ruling 78 (2026-09-22) then took the launcher's own directory out of that arithmetic**, and the two grants differ in shape: the program's directory takes one **non-inherited** `RX` ACE and the `*.dll` files directly beside it take non-inherited `RX` ACEs in **one `icacls` invocation** — the tool's `*.dll` pattern against that directory, because a list of paths is not a form it accepts — so what that pass costs is the directory's name matches rather than its whole contents (decision 22; `tests/backend.rs` pins the exposure set and the per-directory shape, `tests/acl.rs` the file shape). **The caller's exposures keep the inherited form and pay the per-object cost below**, which is deliberate: a pre-existing file under an exposed directory has to stay readable by the child without a `/t` walk. Measured 2026-09-22 (three NTFS runs, one ReFS): **the NTFS grant alone — 32.3 s to 37.8 s — is past the 30 s a confined launch has to fit inside** (this crate's harness allows that: `tests/common/mod.rs`'s `TIMEOUT`, fixed by the design against the e2e harness's `START_TIMEOUT`, `crates/tinio-e2e/tests/steps/clients.rs`'s 30 s), while ReFS spends under a third of it on the same tree. The design's §6 and its Risks row carry the numbers, the spread between runs and the guidance for a large served tree; `README.md` is where an operator meets it.
- The network suite, inside the script's first leg: `cargo test -p tinio-sandbox-windows --test network`. Its outbound assertion needs a baseline — the suite proves the target address is reachable unconfined before asserting it is unreachable confined — and it **fails rather than skips** when the host cannot reach it at all (`TINIO_SANDBOX_TEST_TARGET` overrides `1.1.1.1:53`; `TINIO_SANDBOX_ALLOW_SKIP=1` is the deliberate opt-out). Its LAN leg is half-measured by construction: it shows a confined container answering a same-host connection addressed to this host's own interface address, and cannot say what a client on another machine sees (design §7).
- Readiness is an event, not a scraped line (decision 19): the endpoint is an SSE frame on stdout — an `event:` line, a `data:` line carrying its JSON (`{"addr":"127.0.0.1:9000"}`), and a blank line — and every log is on stderr. A consumer accumulates to the blank line, decodes, and treats anything that is not one of our frames as text to ignore rather than as a failure. **The harness half landed in P1** — `crates/tinio-e2e/tests/steps/clients.rs`'s `READY` constant is gone and `wait_for_ready` returns `Option<Event>` — and there are two more readers of the same rule: `crates/tinio-server/tests/sandbox_e2e_windows.rs` and `crates/tinio-server/tests/serve_read_only.rs`'s `Serve::ready()`, each taking the first event that is its own.
- The confined server end to end: `cargo build -p tinio-server --example serve`, then `cargo test -p tinio-server --test sandbox_e2e_windows` (the target carries `required-features = ["sandbox"]`; the **example carries none**, because a `--no-default-features` build must still serve the harness — line 77 above).
- **All three legs of that suite are green on this host.** Three defects stood between the suite and green, and each is fixed and measured: the AppContainer's `std::fs::canonicalize` refusal (the root resolution in `tinio-fs` and the containment proof behind the vendored `soft-canonicalize` now route around it — the design's verified-evidence table has the row); the **blocking listener the launcher binds** (`std` sockets are non-blocking only off unix, Windows' tokio cannot detect the mode, so the accepted connection inherited it and hyper blocked in `recv` instead of writing its response — one `set_nonblocking` in `serve`'s confined child); and the **15 s clean exit** (`pacing::sleep_checked` answered a stop only between chunk-bounded sleeps, so the child exited 15.03 s after its own start against this suite's 15 s deadline). The suite's own header records what the legs cost on this host; the design's verified-evidence table carries a row for each of the three. A red leg is a statement about one of those three, never about the confinement at large, and `TINIO_SANDBOX_ALLOW_SKIP` covers two host preconditions only — whether the machine can confine at all and whether it has a console — both probed before the suite spawns anything, and never a request that goes unanswered or a server that cannot start (that frame is fatal).
- The confinement tests **fail** rather than skip when the host cannot do what a leg needs — the rule, the opt-out and the reason are the WSL2 section's, above. On Windows the non-negotiable case is the console: a leg that needs a delivered console control event owes either a console or a failure that names it, never a green run that proved nothing.
