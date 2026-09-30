# Sandbox BDD e2e

**Date**: 2026-09-29
**Status**: Phases A–D complete (A0–A8, B1–B7, C1–C6, D1–D5). The controller-created tasks A8, C5, C6, D5 and Z1 sit outside §9's rows.
**Scope**: a cucumber surface for the sandbox — the CLI-observable common flow and the three platforms' differences — plus the migration of the CLI-observable halves of the existing Rust process legs, the **additive observability** the new witnesses need (tracing events, and metric families for what a log line cannot carry), the CI-budget measurements that decide where the new legs run, the recorded debts for the branches no host in this matrix can drive, and the code/doc hygiene this audit turned up. Not touched: the mechanism evidence each crate already owns (§5), any product *behaviour* (the observability work is additive only), and the S3 surface.

## §0 What this plan is, and is not

- It is a **new owner** for exactly: the scenarios in §3, the harness and observability changes in §4, the deletions in §5, the measurements in §6, the debts in §7, the hygiene items in §8, and the metric families it declares.
- It is a **referrer** everywhere else: the test mandate is `design §12`; the launcher-platform list and the shared verification conventions are `sandbox-shared.md §3/§6`; the logging contract is `sandbox-shared.md §7` (this plan adds instances of it, never a second copy of it); the coverage facts, the fail-not-skip rule, the concurrency rule and the per-platform gate scripts are `docs/tests.md`; the readiness event is `decision 19`; the standing grants are `decision 15` and the Windows ACL whitelist is `design §6`; the in-place-vs-launcher split is `decision 6`; the subscriber-install order is `decision 26`; the CLI surface prose is `contracts/cli.md`; the requirement is FR-036.
- The pre-push bar is `docs/tests.md`'s Standard acceptance block **in full** (`sandbox-shared.md §1`).
- **Progress**: tick a task's boxes when the task is done and its review pass is green; leave a box unticked with a one-line why when skipped deliberately (`docs/superpowers/README.md`).

## §1 The observing surface, and the one rule this plan amends

Three carriers, one boundary.

**The CLI/process surface (the primary carrier).** Driving the real `tinio` binary: the three sandbox channels and their precedence, a refusal vs an honour, the `listening` event, `state` and its ordering, the serving surface under confinement, the stop/exit-code path, each platform's observable launcher/in-place difference, and the external clients' own paths.

**Tracing, where the process surface is blind.** A new leg may assert a product `tracing` event — **additive only**: no behaviour change, no new branch, no exit-code change, no default-level change. `sandbox-shared.md §7` stays the contract's owner and these are new instances of it. Two facts the tree dictates:

- Default level is `info` (`tinio-config/src/schema/log.rs`), raised with `--verbosity` / `TINIO_LOG_LEVEL` (`error|warn|info|debug`), available in release builds; `NO_COLOR=1` whenever a leg greps the text for `step="…"`.
- **On the Linux in-place arm the subscriber is installed after the confinement** (`crates/tinio-cli/src/sandbox/confine.rs`'s in-place arm, `decision 26`), so every event emitted *during* confinement is discarded — including the ones `tinio-sandbox-linux/src/enter.rs` already emits (`model`, `exposures`, `retained capabilities`, `no_new_privs`, `capset`) and `seccomp.rs`'s skip warning. On the launcher arms the install precedes the launch, so a new `info!` is visible there by default. The CLI therefore **re-emits the pure facts after `confine()` returns** (the model `tinio_sandbox::requested_model()` resolves once, through the facade, before applying the spec, beside the `local_uid` carry, and travels on the Linux-only `serve::Spec::model` knob the CLI sets rather than being read again — no platform backend is named at the call site — a malformed `TINIO_SANDBOX_MODEL` refused there (`Error::Usage`, exit 2) as `Spec::apply`'s own read would refuse it, never downgraded to `Auto` — and the exposure set's size read off the spec `apply` consumes), and anything that only exists at runtime goes to metrics (below) rather than to a line nobody can see.

**Metrics, for quantities a line cannot carry.** `/metrics` is served by the management plane on the control channel (`crates/tinio-api/src/router.rs`), backed by the `prometheus` crate's default registry; families are declared in the crate that observes the fact (`tinio-sandbox-linux` for both landed families), and the **name list is owned by `specs/001-s3-local-server/data-model.md` §Metrics** (the exposition contract is `contracts/management-api.md`). A new family means an edit in both places. **Only quantities the serving process itself can observe are eligible**: the launcher builds no runtime, has no HTTP surface, and the registry is process-local, so launcher-side numbers (the readiness bound, the abort verdict, the grant phase) are **unreachable by construction** and stay on their existing channels (stderr, `state`, `acl-ledger.jsonl`).

**Rust keeps what none of the three can see**: ACE counts and `icacls` text, Seatbelt refusals and profile text, `lsof` fd tables, seccomp classification internals, salt/ledger internals, job-object semantics, payload frame encoding, adoption and relay internals.

**The rule this plan amends** — `docs/superpowers/specs/2026-08-31-cucumber-bdd-migration-design.md` `§Migration rule`, second conjunct "observable through the **S3 API**". It becomes "observable through a product surface (the S3 API, the CLI/process surface, or the product's own tracing/metrics)". One task owns all three sites: that sentence, `docs/tests.md`'s compressed copy of it, and `CONTEXT.md`'s `Migration rule` glossary entry.

**Exceptions the tree forces**, recorded because they are facts about today:

- `crates/tinio-server/tests/serve_sandbox_channels.rs:53` is `#[cfg(not(feature = "sandbox"))]`; cucumber resolves one binary. It stays Rust.
- The Linux `read-only-arm:` is a `println!` on stdout from the test-only `sandbox-child` binary (`crates/tinio-sandbox-linux/src/bin/sandbox-child.rs:195`) and is not a tracing event at all. Its fact moves to a metric (§4 A5); the crate's own leg stays.

## §2 Where the legs run, and what they may touch

- **Windows: this machine.** `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1`; a real console is a precondition (verified: ConsoleHost, interactive). Volumes are C: NTFS and E: ReFS, so `verify-windows.ps1`'s default set covers both filesystems design §12 asks for.
- **Linux: WSL2, distro `Ubuntu-26.04` explicitly** — the default distro is `docker-desktop`, so a bare `wsl.exe -e …` lands in the wrong place; cargo needs `bash -lc`. `bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` from the repo, whose fourth leg already runs `--tags '@cli and @FR-036' --concurrency 1`.
- **macOS: the macOS guest in docker**, reached over SSH as `flier@127.0.0.1`. The published port is **dead as measured 2026-09-30** — `127.0.0.1:2222` lands in the container where the iptables `QEMU_DNAT` chain is empty, so the route that works is the guest's own address through the container: `ssh -o ProxyCommand='docker exec -i macos nc 172.30.0.2 22' -p 2222 -i ~/.ssh/id_ed25519 flier@127.0.0.1` (repair, in the container: `iptables -t nat -A QEMU_DNAT -p tcp --dport 22 -j DNAT --to-destination 172.30.0.2:22`, or a compose restart). **Start it from its compose file** — `MSYS_NO_PATHCONV=1 docker compose -f D:/docker/macos/docker-compose.yml up -d` (stopped with `docker compose stop`); `docker start` is not the way to run this guest. The host owes `/dev/kvm` in the WSL VM — `nestedVirtualization=true` in `.wslconfig`, with Docker Desktop stopped before `wsl --shutdown` — and the guest disk (55.7 GB) is already installed. I ask before starting it.
- **Every scratch root, state dir and fixture lives in the OS temp dir, never under `target/`**: `icacls` on `target\debug` costs 9.2–30 s per call, and a deep `$TMPDIR` overflows `sun_path` on macOS (`docs/tests.md`'s guest-run trap 3) — the macOS scratch stays short (`/tmp`).
- Fail-not-skip is `docs/tests.md`'s rule and does not move: the harness precondition helpers route through `tinio_util::testing::host_cannot`, which **asserts** `TINIO_SANDBOX_ALLOW_SKIP` before it prints.

## §3 Scenario inventory

Tags: §3a–§3c's scenarios carry `@cli @confinement @FR-036` (`@FR-016` where the scenario is about channel precedence) plus a platform tag where they are platform-bound. §3d's external-client legs carry `@confinement @FR-036` plus their client tag and **not** `@cli`. The two sandbox-channel scenarios of `cli_lifecycle.feature` move into the new `confinement.feature` (§4 A2) and are not duplicated.

**Platform-tag semantics change with this plan** (§4 A1): a scenario carrying positive platform tags runs on a host **whose target matches at least one of them** — so `@macos @windows` runs on both launcher platforms and nowhere else, and `@linux` runs only on Linux. That is what makes one shared launcher scenario expressible; the old "exclude the other platform's tag" rule could not.

**Multi-case rows are split**: a row whose cases differ only in parameters becomes one scenario with a Gherkin `Examples` table (the repo's existing convention), and a row bundling unrelated behaviours is split until each scenario asserts one behaviour (`docs/tests.md`'s Cucumber section).

**Post-run addition, 2026-09-30**: `confinement.feature`'s `A confined server stops on SIGTERM` (`@cli @confinement @FR-036 @linux`) — a row added **after** this run closed, and not an entry in any table below (`G1`–`G22` and `P1`–`P6` are this run's own inventory; nothing here is renumbered). It covers the confined-in-place SIGTERM family `docs/tests.md`'s WSL2 section recorded: the CLI's signal wake was `signal-hook`'s `libc::send`, which on Linux *is* `sendto` — the syscall number the seccomp filter denies outright, with the error the library drops — so a confined server ignored `SIGTERM` and outlived every polite stop. The row asserts the run was confined and that a real `SIGTERM`, sent to the pid the run's own `state` file records, then ends it; the fix it regresses (an owned socketpair in `crates/tinio-cli/src/signal.rs`, woken by `write(2)`) landed with it.

### §3a Channels and precedence

| # | Scenario | Assertion | State |
|---|---|---|---|
| G1 | Each of the three channels is enough on its own | `Examples` over `--sandbox` / `TINIO_SANDBOX=1` / `[server] sandbox = true`; each honours **and** the run serves with a witness (§4 A3) | config-key arm is new in BDD |
| G2 | A higher explicit `false` beats a lower `true` | `Examples` over the three pairs: variable `0` over config `true`; `--sandbox=false` over variable `1`; flag over variable `0` | new |
| G3 | `.env` is the environment channel one level lower | a `TINIO_SANDBOX=1` in `<state dir>/.env` confines, and a refusal names `TINIO_SANDBOX`, never the file | new |
| G4 | An exported variable beats `.env` | dotenvy never overrides an exported value | new |
| G5 | `.env` cannot select the root, the mode or the state dir | it is loaded after the discovery parse, so a `.env` `TINIO_READ_ONLY` does not make the run read-only (the fact FR-036's clause stands for; §8 H4 clarifies that wording) | new |
| G6 | `.env` reaches the model and mechanism variables too | `TINIO_SANDBOX_MODEL` / `TINIO_SANDBOX_MACOS_MECHANISM` supplied by `.env` behave as when exported — read from the process environment after `apply_env_file` (`crates/tinio-sandbox/src/serve.rs`'s arms call `Options::from_env()`, the model through the facade's `requested_model()`) | new |
| G7 | A bad value is refused, never defaulted | `Examples` over `TINIO_SANDBOX=yes` (usage status, `expected 0/1/true/false`), `--sandbox 1` (`require_equals`), a bad `TINIO_SANDBOX_MODEL` on Linux, a bad `TINIO_SANDBOX_MACOS_MECHANISM` on macOS | new |

### §3b Honoured, ready, and what the operator sees

No `G11`: the 30 s readiness bound was a scenario here until the induction hunt found that none exists on any in-surface fixture (§7 D8) — it is a debt plus a strengthened core-level leg, not a leg.

| # | Scenario | Assertion | State |
|---|---|---|---|
| G8 | A confined server completes a real S3 journey | create bucket → put object → get object → list, through the spawned binary; the largest hole in the suite today — no e2e anywhere signs a request under `--sandbox` | new |
| G9 | A confined read-only server refuses the write and still serves the read | the `--read-only --sandbox` pair over the same journey | new |
| G10 | Readiness and `state` agree, and `state` names the right process | the `listening` frame; `state` exists iff the run serves (kept); **plus** the pid — the spawned pid on Linux, the launcher's pid on macOS/Windows. The harness's `state_file()` hardcodes `<root>/.tinio/state` while the harness's own `TINIO_STATE_DIR` is `<scratch>/state`, so a relocated-state assertion needs its own path | partly exists |
| G12 | A failed `state` publish ends the child | **induction: a directory at `<state dir>/state.tmp`** — `State::write`'s tolerant remove fails, `child.abort()` runs, the run exits non-zero. Probe-immune (the pre-flight probe only ever creates and removes its own `state-probe-<pid>-<nanos>`), no race, same on both launcher platforms. **Launcher platforms only** — Linux has no launcher, its in-place publish is a different call site. The fixture is sticky (the launcher never removes it) and must be torn down | new |
| G13 | A launcher run with an HTTP control channel publishes no `state` and is not probed | `--sandbox --api http://…` serves; no `state` file ever appears. **`@windows` only** — the launcher's absence of a record is a launcher fact, which is why the scenario is Windows': Linux is the in-place arm and inverts the file's half (it serves, and the plane publishes its own `state` with the TCP channel as `control_name`), while macOS's profile grants no network operation at all, so the confined child is refused every `bind()` and that arm is inference rather than measurement | new |
| G14 | OTLP plus a confinement warns and serves | the warning names both, the run still serves, nothing is refused (FR-036's ruling; unasserted anywhere today) | new |
| G15 | The exposure set is resolved and refused through the product | `Examples` over: a caller path that is a file (not a directory) · a junction in the exposure chain · a path that does not exist · an empty exposure set. The file-root case pins the fragment both layers share ("is not a directory"): it lands in the Windows whitelist there, while Linux answers it from the storage open | new |
| G16 | The state-dir containment refusals reach the operator | `Examples` over: `TINIO_STATE_DIR` as a strict ancestor of the root (**read-only only** — a read-write run ignores it and merely serves) · the read-only in-root clause (the configuration layer's earlier refusal is the sentence that lands) · macOS's derived-state-dir refusal · macOS's write-probe refusal | partly new |
| G17 | The Windows run's operator-visible contract | split into one scenario per fact, each `Examples`-driven where it varies: the `step="grant the ACLs"` line and its fields · the `step="sweep the stale profiles"` summary · `child exit code=` · `taken_over=` / `absent=` · the salt-degrade warning · the 15 s grant-phase warning · the ACL ledger's `kind=served-root` and `kind=exposure` lines | new |
| G18 | The capability set is chosen through the environment | `Examples` over a valid name and an unknown one (which must fail the run) — a security-relevant parser on the operator's surface, untested at every level today | new |

### §3c The three platforms' differences

| # | Scenario | Platform | Assertion | State |
|---|---|---|---|---|
| P1 | The confinement is in place: no child process | `@linux` | the confined process is the spawned one — same pid, no descendant; `state`'s pid is that pid; cross-checked against `/proc/<pid>/root` pointing at the jail | new |
| P2 | The launcher forwards its child's status | `@macos @windows` | on a clean exit the launcher's code is the child's own — macOS asserts `Some(0)` today, Windows asserts only that a code exists (`crates/tinio-server/tests/sandbox_e2e_windows.rs:184`) | partly exists, gap fixed here |
| P3 | A child that dies before readiness becomes the launcher's own `error` frame | `@macos` | **induction: `<root>/.tinio/.env` is a symlink to an existing, parseable dotenv file outside the root** — the state dir is lexically in-root so the profile never names it, the unconfined launcher reads that file and the confined child cannot, so the child dies with **no frame** and exit 1 while the launcher emits its own single `error` frame. Works under both mechanisms; no root, no race. A symlinked *root* does **not** work (its canonical spelling joins the exposure set). Assert the sentence and the three witnesses (`step="spawn"` with a pid, `step="exit"`, the child's own `tinio: configuration error:` line on inherited stderr) — **not** the errno, which is measured as either `PermissionDenied` or `NotFound` | new — `design §12` bullet 4's missing leg |
| P4 | The launcher's death takes the confined child | `@windows` | the port stops accepting once the launcher is killed — the observable half of `sandbox_e2e_windows.rs:40`; the ACE-count half stays Rust (§5) | migrate |
| P5 | An escaping junction inside the root is refused as nonexistent | `@windows` | `NoSuchBucket` under the junction bucket — the observable half of `sandbox_e2e_windows.rs:281`. The fixture grants the out-of-root target first, and that SID is only obtainable by parsing the launcher's `step="grant the ACLs"` line (§4 A3) | migrate |
| P6 | The launcher does not turn a child's non-frame stdout into a frame | `@macos @windows` | no BDD leg — a child that writes junk cannot be induced from the CLI surface; covered Rust-side (`crates/tinio-sandbox-windows/tests/events.rs:143`, core's observer tests). The dropped-line **count** is no metric: a launcher-side quantity is unreachable by `/metrics` by §1's rule | no leg, Rust-covered |

### §3d The external-client tier under confinement

The external clients — `aws` cli v2 and `rclone` (FR-025's CI-gated tier), `mc` and boto3 (its best-effort tier) — all run against an **unconfined** endpoint today: `crates/tinio-e2e/tests/steps/clients.rs`'s `External::start` spawns the plain server.

| # | Leg | Assertion |
|---|---|---|
| G19 | the `aws` cli v2 and `rclone` journeys against `--sandbox` | the confined twins **sampled** from `interop/journey.feature` and `interop/advanced.feature` (the paragraph below), through a confined server |
| G20 | the boto3 journey against `--sandbox` | the SDK path — SigV4, chunked bodies, the ACL legs (`crates/tinio-server/tests/boto3_journey.py`) |
| G21 | `mc` against `--sandbox` | the other best-effort client's path |
| G22 | the same clients against `--read-only --sandbox` | each client's **own** error shape for the mutating operations `contracts/s3-surface.md` §Read-only mode lists; the refusal set itself is G9's fact and is referred to, not re-asserted |

**G19/G22 are a sample, not a sweep.** `interop/journey.feature`'s 7 unconfined journeys gain **2 confined twins** (the aws+rclone core journey, the boto3 basic one) and 3 read-only legs (aws, rclone, boto3); `interop/advanced.feature`'s 15 gain **3 twins** (the aws multipart/ETag one, the rclone multipart/server-side-copy one, the mc basic journey) and 1 (`mc`'s read-only refusals). Unmirrored in journey: the ephemeral `--port 0`, invalid-credential, aws CRC64NVME-checksum-multipart, `max_buckets`-pagination and boto3-checksum journeys; unmirrored in advanced: the aws server-side copy, both cold-listing halves, the aws and mc special-character outlines, and the size-boundary, range, overwrite-LWW, truncated-listing, missing-object/bucket, shortest-name and mc-deep-nesting legs. G22 samples the same way: its 4 read-only legs pin 17 (client, operation) refusals — aws 5, rclone 4, mc 4, boto3 4 — drawn from 5 of §Read-only mode's 20 mutating operations (`CreateBucket`, `DeleteBucket`, `PutObject`, `DeleteObject`, `CreateMultipartUpload`), one representative per client-visible class, because the refusal set itself is G9's and §Read-only mode's and is referred to, not re-swept per client. Rationale: the confined arms are excluded from CI (`and not @confinement`), so a wider sample buys no CI signal — it only spends the operator gate's wall-clock on WSL2, the macOS guest and this host, where every scenario is a launcher plus a confined child and A4's `--concurrency 1` makes the gate strictly serial. The features' own confined-arm blocks and `steps/clients.rs`'s header already state this sampling.

Boundary and tier are unchanged: FR-025's tiering and the e2e README's matrix own which client is CI-gated; `docs/tests.md`'s "Rust only" rule for unit/e2e tests is not touched (external-client scripts and steps are the sanctioned exception).

**The trap this creates, and its fix**: an explicit `--tags` replaces the runner's whole composed filter, so the CI interop job's `--tags '@interop'` (`ci.yml`'s `interop-cucumber` anchor, reused by `interop-port`) would sweep in an `@interop @confinement` scenario and run a confined server on a hosted runner. The interop legs take `and not @confinement` **in the same task that adds the scenario**; the confinement's real gate stays the operator script on each platform.

## §4 Harness and observability changes

One file owns each edit. A0–A5 run in Phase A; B6 in Phase B.

### A0 The rule amendment and the FR-036 wording

- `2026-08-31-cucumber-bdd-migration-design.md` `§Migration rule` gains the second observable surface (§1); `docs/tests.md`'s compressed copy and `CONTEXT.md`'s `Migration rule` glossary entry follow in the same task.
- H4: FR-036's `.env` clause is clarified in `specs/001-s3-local-server/spec.md` and `contracts/cli.md` — live prose, no semantic change — so the next reader is not tripped by "select the mode" reading as the sandbox mode.
- [x] all three rule sites edited (the spec's `§Migration rule`, `docs/tests.md`, `CONTEXT.md`), plus the two bullets the same spec gained for the amended rule ("No direct-Storage cucumber steps", "HTTP-surface-only level"); the wording clarification landed

### A1 Platform tags and the default filter

- `@linux` and `@macos` join `@windows` and `@unix`; `@unix` keeps its meaning (both unix platforms).
- `steps/tag_filter.rs`'s `default_filter` (called today from `crates/tinio-e2e/tests/cucumber.rs`) becomes the §3 rule: a scenario with positive platform tags is excluded when **none** of them matches `cfg!(target_os)`.
- Three existing owners move with it, in this task: `crates/tinio-e2e/README.md`'s tag table (including H5's missing `@acl` / `@acl-off` / `@cors` and the stale "unused" notes), `.github/workflows/ci.yml`'s mem-pass exclusion list (the `e2e-mem` anchor), and the `--tags` line in `wsl-chroot.sh` / `verify-windows.ps1`.
- [x] filter table edited; every existing tag's behaviour unchanged on this host
- [x] README + mem-pass list + both scripts agree with the new table

### A2 The `confinement.feature` file

- New `crates/tinio-e2e/tests/features/confinement.feature`. The two sandbox-channel scenarios of `cli_lifecycle.feature` move here; `cli_lifecycle.feature` keeps no copy and its header's stale `owed_arm` reference is fixed (H3, folded here).
- [x] file created, pair moved and green, no duplicate text left behind

### A3 Step layer

- `steps/cli.rs`: `SandboxChannel` gains the `Config` arm and the `.env` spelling (G3–G6).
- **Witness step**: one step answering "was this run really confined", built from **two carriers** — the product's own event (§4 A5's lines) and an **independent cross-check** (Linux: `/proc/<pid>/root` points at the jail; Windows: read the package SID's ACE back with `icacls`; macOS: a negative probe that the profile refuses). A self-report alone is what the `answered, never ignored` step's own doc already warns about.
- **pid-identity step** for G10/P1; **stderr-contract step** for G17 (`NO_COLOR=1` is set by the harness so the text is greppable — `sandbox-shared.md §7` owns the colour rule); **package-SID step** for P5.
- **Metric-scrape step**: `/metrics` lives on the **control channel**, not the S3 port, and no cucumber step scrapes it today (`metrics.feature` asserts only the retired data-plane route's 404). The new step resolves the state dir's socket (or the configured control endpoint) and reads the family, with the token where the bind is not local (`contracts/management-api.md` owns the auth rule).
- [x] steps registered; each has a scenario that uses it; `fail_on_skipped()` still on

### A4 Scratch policy and process hygiene

- Every scenario's root, state dir and fixtures come from `tempfile` in the OS temp dir; nothing under `target/` (§2). Sticky fixtures (G12's `state.tmp` directory) are torn down by the scenario.
- Timeouts, concurrency and cleanliness are **reused**: `CONFINED_RUN_TIMEOUT`, `PUBLISH_SETTLE`, `TINIO_TEST_WAIT_TIMEOUT_SECS`, `TINIO_LAUNCHER_EXIT_TIMEOUT_SECS`, `--concurrency 1` for every confined leg, `kill_servers` + `sweep_confined_grandchild`.
- New spawn sites go through `tinio_util::testing::job::spawn`: `crates/tinio-util/tests/spawn_guard.rs` counts raw `.spawn()` sites in both directions, so a new one either uses the guard or updates the allowlist in the same task.
- [ ] no scenario leaves a `tinio` process or a temp tree behind
  *(Not ticked: A4 landed as its ruling's policy/helper/baseline task rather than the scenario sweep, and the claim fails on macOS — one confined server survives each run, the all-green serial run included; that is the product-level SIGTERM family, not a test artifact, recorded in `docs/tests.md`'s WSL2 section as a finding and owned outside this plan.)*

### A5 The observability additions (the new witnesses' carriers)

Additive only: events and metric families, no behaviour, no branch, no exit code, no default-level change.

- **CLI re-emit (Linux in-place)**: after `confine()` returns and the subscriber is installed, the CLI logs the **pure** facts it can state for itself (the model `tinio_sandbox::requested_model()` resolves once, through the facade, before the confinement and reaches the re-emit as the value the CLI put on the Linux-only `serve::Spec::model` — the call site names no platform backend — rather than reading the environment again, the exposure set's size read off the spec `apply` consumes, whether FR-033's `local_uid` mapping is configured) — the ones that would otherwise be discarded by the ordering §1 records. Runtime-only facts are **not** re-derivable and go to metrics instead.
- **Metric families** (declared in the crate that observes the fact — `tinio-sandbox-linux` for both landed families — and **registered in `specs/001-s3-local-server/data-model.md` §Metrics**): the read-only arm actually taken (Linux) and the seccomp-skip state (Linux). Only quantities the serving process itself observes — launcher-side numbers stay on stderr/`state`/`acl-ledger.jsonl` (§1), so P6's dropped non-frame count has no family here (§3c P6).
- **Secret-leak assertion**: each new line/family ships with a leg asserting the control token appears in neither the frame stream (stdout) nor the log/metric text — the sandbox review's STD-01 (a control token reaching CI logs) is the precedent.
- [x] the Linux re-emit lands; the runtime facts are readable through `/metrics` on a Linux run
- [x] families declared and registered in `data-model.md`; the scrape step (A3) reads them
- [x] the secret-leak assertion green; no new line changes behaviour

### B6 The external clients' confined arm

- `steps/clients.rs`: `External::start` gains the sandbox request (spawned through `job::spawn`; the guard's count updated here), `check_presence` keeps its per-tag demands.
- The scenarios carry `@confinement`, inheriting `--concurrency 1` and the platform gates; `boto3_journey.py`'s journey body is unchanged and only its read-only expectations are added.
- `ci.yml`'s interop `--tags` (the `interop-cucumber` anchor and its `interop-port` reuse) gain `and not @confinement`.
- [x] `External::start` can request a confinement; guard count updated
- [x] the interop tags exclude `@confinement` wherever they are reused
- [ ] G19–G22 green in WSL2 and on the macOS guest; tier unchanged
  *(Not ticked: the legs are green in WSL2 and on this host; the macOS guest has no `aws`/`rclone`/`mc`/boto3, so that arm has no host there — B7's finding 4, the tier stays opt-in.)*

## §5 Migration and deletion

One task per row; **the replacement must be green on the platform the old leg belongs to, plus the local default suite, before the old leg is deleted — and the deletion is in the same task**, never a follow-up.

| Existing leg | Verdict | Reason |
|---|---|---|
| `serve_sandbox_channels.rs:55` (feature-off flag refusal) | keep | `#[cfg(not(feature = "sandbox"))]`; cucumber runs one binary (§1) |
| `serve_sandbox_channels.rs:107-164` (the refusal/honour shape) | split | channel dispatch → G1/G3; "stdout is exactly the encoder's three-line frame, and the refusal names exactly one cause" stays — decoding in a step would hide it, and it is `design §12` bullet 4's wire contract |
| `serve_sandbox_channels.rs:227` (a channel switched off is not a refusal) | migrate → G2 | CLI-observable |
| `serve_sandbox_channels.rs:285` (in-root state dir, Windows) | migrate → G16 | the config layer's earlier refusal is the sentence that actually lands |
| `crates/tinio-server/tests/sandbox_e2e.rs:91` | migrate → G8, delete | G8 is strictly stronger (a signed S3 journey, not a bare request) |
| `crates/tinio-server/tests/sandbox_e2e.rs:163` (read-only through `/proc/<pid>/root`) | keep | `/proc/<pid>/root` is mechanism; G9 asserts the outcome and P1 uses it as a cross-check |
| `sandbox_e2e_windows.rs:40` leg 1 | split | launcher-death-takes-child → P4; the ACE-count half stays (a step layer has no SID oracle but the log) |
| `sandbox_e2e_windows.rs:124` leg 2 | keep | `took_the_profile_over()` is a crate API |
| `sandbox_e2e_windows.rs:281` leg 3 | migrate → P5, delete | CLI-observable; the fixture's SID comes from the harness's new step |
| `sandbox_e2e_windows.rs:443` leg 4 | keep | its induction (a directory junction) is Windows-only; P3 is macOS's own |
| `crates/tinio-server/tests/sandbox_macos_e2e.rs:43` | split | serves-a-request → G8; forwards-child-status → P2; refusal attribution, mechanism selection and `sweep_confined_grandchild` stay |
| the crate suites (`tinio-sandbox-{linux,macos,windows}/tests/**`, `tinio-sandbox/tests/facade.rs`, in-crate `#[cfg(test)]`) | keep | mechanism evidence |
| `cli_lifecycle.feature`'s two sandbox-channel scenarios | move → `confinement.feature` | domain cohesion (§4 A2) |

## §6 CI budget: measurements before any edit

- **M1 — macOS (`e2e-port`)**: the default filter already runs `@confinement` on macOS, so every new `@macos` leg lands there. Measure one confined leg's wall clock; accept it or narrow the CI-visible set by tag.
  **Re-measured 2026-09-30 on the guest** (macOS 15.8.1, 6 vCPU, `ulimit -n 256`, container up 3 h 19 m, 1-min load 2.3–2.7; reached over the `docker exec -i macos nc 172.30.0.2 22` ProxyCommand — the published port is dead, §2; tree = this one, `crates/tinio-e2e/tests/features/confinement.feature` **1148 lines / sha256 `5120e751143906b80f08b0249ac48752e32b770691e68a2b4a69b68750bf7e61`** and `crates/tinio-e2e/tests/steps/cli.rs` 3152 lines / sha256 `5af31ec1eba90e1c4542b758759a7f21c48a9b5b96e4a079434e2e4a281b2a3b`, shipped by the ledger's tar/ssh command and verified hash-identical on the guest — the 426 files under `crates specs packaging` match on both sides but for the two the drift below names — **and the tree moved while the runs were in flight**: a concurrent writer's edits landed at 22:45:47 and 22:46:20, so the same two files now sit at 1154 lines / sha256 `ebfcf47b9e1daaedff5245f6cfb5b4628efcc23375e3d3d30922c20a2f67740e`, and every number quoted from those runs belongs to the 1148-line revision this ruling named). The leg is `TINIO_E2E_REPORT=<scratch> cargo test -p tinio-e2e --test cucumber` (the `e2e-fs` anchor `e2e-port` reuses as-is — corrected 2026-09-30: that anchor is `-- --retry 2 --concurrency 1` (`.github/workflows/ci.yml:442`), both flags, not `--retry` alone; `--profile ci` = `dev` locally): on the 1148-line revision **36.2 s** then **32.1 s** wall, 281 scenarios **280 passed / 1 failed** then **278 / 3** — the `cli_lifecycle` 1000 ms readiness bound at 1.100 s, plus `Unsatisfiable ranges answer 416` and `Copy object within a bucket`'s status flake — the class `--retry 2` absorbs. **Re-run on the current revision (2026-09-30) with M1's exact full leg, twice — reports `/tmp/m1-full-1.json` and `/tmp/m1-full-2.json`, `confinement.feature` and `cli.rs` re-hashed on the guest after each leg at the fingerprints below — 45 s then 42 s wall, 281 scenarios / 2181 steps with one red each: `A server starts, reports and stops`'s 1000 ms readiness bound at 3.695 s then 1.425 s, the same class, so the full-leg numbers are substituted onto the current revision.** The family that leg lands by default is **31 elements**; serial, `TINIO_E2E_REPORT=<scratch> cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and not @interop and not @boto3 and not @mc and not ((@linux or @macos or @windows or @unix) and not (@macos or @unix))' --concurrency 1` is **8.3 s / 4.6 s / 7.4 s wall over three runs — 31/31, 30/31 and 31/31 green** (205 / 203 / 205 steps), slowest scenario `A "--sandbox" request is never silently ignored` **0.540 s**, the S3 journey `A confined server completes a real S3 journey` **0.216 s**; under the leg's own unbounded concurrency those 31 elements aggregate **69.7 s** and **68.2 s** of step time (slowest 10.812 s and 12.868 s). **One red class is new against the 1105-line revision: `OTLP plus a confinement warns and serves` failed its stderr-warning step in 2 of the 5 runs it appeared in** — the launcher's `warn!` line is missing from the captured stderr, intermittent rather than deterministic. **Corrected 2026-10-01: that reading is wrong — the product emits the line every time; the harness snapshotted stderr once at the readiness frame, while the warning is emitted after `publish_state` (temp file + fsync + rename, ~11 ms median on APFS), so the capture raced it.** The fix is `crates/tinio-e2e/tests/steps/cli.rs`'s `Run`, which keeps `stderr_path` beside a fallback `stderr_snapshot` and has its `stderr()` accessor re-read the file; measured **0 of 10 guest runs missed after the fix, against 11 of 25 (44%) before**. **Re-run on the current revision (2026-09-30) with M1's exact serial filter, twice, and this time both came back clean — 31 scenarios / 209 steps green, 6 s then 5 s — so the number above is substituted onto the current revision, whose fingerprints are `crates/tinio-e2e/tests/features/confinement.feature` 1154 lines / sha256 `ebfcf47b9e1daaedff5245f6cfb5b4628efcc23375e3d3d30922c20a2f67740e`, `crates/tinio-e2e/tests/steps/cli.rs` 3065 lines / sha256 `ece0d1d76441412c78e211a50bfb2fa6a35a5bbd98eab2b6931aaeda61ce8b33` and `crates/tinio-e2e/tests/steps/common.rs` 731 lines / sha256 `70f84335f2ea4461041fd6e615387c700fa86baa7b39b7c824889b4d8870857a`, all three hash-identical on the guest after the ledger's tar/ssh sync and the 426 files under `crates specs packaging` now matching on both sides.** **Re-established 2026-10-01 on the current tree, whose fingerprints are `crates/tinio-e2e/tests/features/confinement.feature` 1156 lines / sha256 `da6130f3cd01b884b9199954a893e7dcf96e141a48cecdb8b6360e6bc480e493` (git blob `10f8ad59c293159dd3f4d0bbe40767497c3ab90b`), `crates/tinio-e2e/tests/steps/cli.rs` 3092 lines / sha256 `41a158af4b1dbf2509591d5840b9fd4ab9ffe51387ba64251355ef46bcb3a86f` (`e78b2025ac2271307dd4f2fa200e13f92bbf8298`) and `crates/tinio-e2e/tests/steps/common.rs` 731 lines / sha256 `70f84335f2ea4461041fd6e615387c700fa86baa7b39b7c824889b4d8870857a` (`1203743cbe41f31f13fb5eff08a50d1199583c23`), all three hash-identical on the guest after the tar/ssh sync and the 426 files under `crates specs packaging` matching on both sides. The serial filter, twice (`/tmp/tinio-e2e/verify/m1-serial-v1.json`, `-v2.json`) — 31 scenarios / 209 steps, 31/31 and 209/209 green, exit 0, 12 s then 6 s wall, slowest scenario `A "--sandbox" request is never silently ignored` 2.008 s then `The flag answers before the variable` 0.738 s, the S3 journey `A confined server completes a real S3 journey` 0.247 s then 0.179 s; each run left one server orphaned to PID 1 (11509, 11729), both reaped. The full leg, twice (`/tmp/tinio-e2e/verify/m1-full-v1.json`, `-v2.json`) — 281 scenarios / 2190 steps, 281/281 and 2190/2190 green both runs, exit 0, 70 s each, the pre-fix revision's readiness red not recurring (`A server starts, reports and stops` green, its server start 0.116 s then 0.191 s), slowest scenario `A daemon whose startup fails exits one and says where to look` 10.203 s then 10.196 s; one orphan reaped per run (14649, 14931). The numbers before this sentence stay the record of the pre-fix revision they were taken on.** **Verdict: accept — the leg carries the family with no tag narrowing** (the earlier run's ruling, not re-taken here: the new OTLP class is recorded beside it). Each confined run left **one** server orphaned to PID 1 — `pgrep -x tinio` after each of the five runs answered exactly one pid (4127, 4418, 4750, 5015, 5333) — the SIGTERM family `docs/tests.md` records, not a red row's residue; all five reaped.
- **M2 — Windows**: measure one confined leg on `windows-latest`. **If the number fits, a CI leg is added** and `docs/tests.md`'s Windows section is updated by the same task; if not, the number is recorded and the operator gate stays the owner.
  **Not taken (2026-09-30): needs a hosted `windows-latest` runner and this session cannot push, so the number does not exist yet and no CI leg is justified by it.** Command a later run uses, on the `e2e-port` Windows half: `TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and not @interop and not @boto3 and not @mc and not ((@linux or @macos or @windows or @unix) and not @windows)' --concurrency 1`. Local proxy, **not** the runner's number: this machine, the same command on the same tree — **39 elements, 39/39 green, 9.7 s wall** (build fresh in 1.0 s), slowest scenario `The launcher's death takes the confined child` **2.154 s**, `A failed "state" publish ends the child` 2.140 s, the S3 journey 0.183 s (B7's older tree: 37 elements, 8.3 s); the grant cost is the host's own — §2 records `icacls` on `target\debug` at 9.2–30 s per call — and `windows-latest` is a different machine, so this row is a proxy and **not** the number.
- **M3 — Linux**: `docs/tests.md` records that a hosted runner cannot create userns. Falsify or confirm that `TINIO_SANDBOX_MODEL=root` works under the runner's `sudo`. **If it fails, record it as an unavailable path** — a skip is not an option, so no leg is added and the operator gate stays.
  **Not taken (2026-09-30): needs a hosted `ubuntu-latest` runner, the same access M2 lacks — the number does not exist yet and the `root` model stays un-falsified, so no leg is added and the operator gate stays the owner.** Command a later run uses: build the leg as the runner (`cargo test -p tinio-e2e --test cucumber --no-run`), then run it under `sudo` with the model forced — `sudo -n env "PATH=$PATH" TINIO_SANDBOX_MODEL=root target/debug/deps/cucumber-<hash> --tags '@confinement and not @interop and not @boto3 and not @mc and not ((@linux or @macos or @windows or @unix) and not (@linux or @unix))' --concurrency 1` (build it through `sudo` with a `CARGO_TARGET_DIR` outside the runner's tree if the whole leg must run elevated, so no root-owned artifacts land in the shared target). Local WSL2 proxy, **not** the runner's answer: `sudo -n env TINIO_SANDBOX_MODEL=root /home/flier/tinio-target/debug/tinio server /tmp/d1-m3/root --sandbox --port 0` on kernel 6.18.40.1-microsoft-standard-WSL2 confines in place (`step="confined in place" requested=Root count=1 local_uid=false`), reaches readiness and serves (unsigned `GET /` → 403 `AccessDenied`) — so the root model is not broken under `sudo` on this host, whose kernel *can* create userns; the probe therefore says nothing about a runner's restriction, and its own stop re-confirmed the SIGTERM family `docs/tests.md` records (SIGTERM ignored, SIGKILL needed).

## §7 Recorded debts

Branches with no test that no host in this matrix can drive. Each records what would make it testable — an untestable branch silently dropped is the defect, not the debt.

- **D1** Linux's seccomp-skip *warning path* — needs an arch with no syscall table (a cross-build under emulation). The **state** half is no longer a debt: §4 A5 exposes it as a metric on any Linux host.
- **D2** macOS's pre-flight **skip** vs **accept** — the assertion lands in this plan (§9 C3); the *release-build* arm that produces the skip still needs a release-profile run, which is the remaining debt.
- **D3** macOS's missing-`sandbox-exec` path (`tinio-sandbox-macos/src/launch.rs:1270`) — needs the helper renamed or hidden for one leg.
- **D4** Linux's more-than-one-thread refusal (`tinio-sandbox-linux/src/enter.rs:437`) — the CLI always confines before any thread exists.
- **D6** Linux's `EINVAL → past CAP_LAST_CAP` (`enter.rs:1355`) — needs a pre-5.9 kernel.
- **D7** macOS's control-descriptor-count refusal and pre-flight timeout (`launch.rs:1056`, `:1276`) — no CLI route.
- **D8 — the 30 s readiness bound has no end-to-end induction.** The arm needs a child that is **alive and silent** for the bound; every fixture-reachable failure makes the child *exit* (that is P3's arm, not this one), the payload pipe always reaches EOF, and every blockable site is launcher-side and *before* the clock starts — so a fixture would hang the run rather than expire the bound. The bound is covered at the mechanism level by the core's own test (`crates/tinio-sandbox-core/src/launcher/supervise.rs`, the three-answer `Readiness` test), which this plan **strengthens to assert that an expired bound actually reaches the abort**; the end-to-end leg stays a debt.

No `D5`: the number is not a gap in this register — it belongs to the controller-created task id the status line names (A8, C5, C6, D5, Z1, all outside §9's rows), deliberately skipped here and not lost.

## §8 Hygiene this audit turned up

- **H1** `PlatformFacts::control_socket` has **no reader anywhere** — delete the field (this plan's ruling; the task no longer asks); the two paragraphs naming it in `2026-09-27-sandbox-servespec-seam-design.md` are corrected there.
- **H2** `Attribution::Denied` is matched at `crates/tinio-cli/src/commands/start/run.rs:591` and built by nothing; `Enforcement::{Partial, Unimplemented}` and `Refused::Unnamed` are likewise unconstructed in production — delete them.
- **H3** `cli_lifecycle.feature:6` cites the removed `owed_arm` helper — fixed in §4 A2.
- **H4** FR-036's `.env`/mode wording — fixed in §4 A0.
- **H5** the e2e README's tag table — fixed in §4 A1.

## §9 Phases and tasks

### Phase A — the rule, the observability, the harness

- [x] A0 the rule amendment + the FR-036 wording (§4 A0)
- [x] A1 platform tags and the default filter (§4 A1)
- [x] A2 `confinement.feature` + the pair's move + H3 (§4 A2)
- [x] A3 step layer: `Config`/`.env` arm, witness (two carriers), pid identity, stderr contract, package SID, metric scrape (§4 A3)
- [x] A4 scratch policy, timeouts, spawn guard (§4 A4)
- [x] A5 observability additions: the Linux re-emit, the metric families + `data-model.md` registration, the secret-leak assertion (§4 A5)
  *(Follow-up 2026-09-30: the rule's stdout half could not land in-process — fd 1 belongs to the test harness — so it is the subprocess leg `crates/tinio-server/tests/sandbox_e2e.rs`'s `a_confined_run_writes_its_control_token_to_neither_stream`, which reads the run's own token out of the `state` file it published and is green in WSL2; the `/metrics` half stays an argument rather than a needle, because both families' labels are closed enums (`crates/tinio-sandbox-linux/src/metrics.rs`), so a path, a value or a credential has no route into the exposition.)*
- [x] A6 hygiene H1, H2 (§8)
- [x] A7 the core leg strengthened for D8 (the bound reaching the abort) (§7 D8)

### Phase B — the common flow

- [x] B1 G1–G7 (channels, precedence, `.env`, bad values)
- [x] B2 G8–G9 (the S3 journey, read-write and read-only) — the coverage this plan exists for
- [x] B3 G10, G12–G14 (readiness/state identity, publish failure, no-`state` runs, OTLP)
- [x] B4 G15–G16 (exposure and state-dir refusals)
  *(Follow-up 2026-09-30: case 11's refusal-frame assertion — exactly one `Error` frame carrying the sentence the run refuses with, and stdout's three lines and nothing else — was restored to every G16 row through `steps/cli.rs`'s `its stdout is the refusal frame and nothing else, carrying "…"` and is green on Windows native and in WSL2; the two `@macos` rows **now run green on the guest** (macOS 15.8.1 x86_64, six vCPU, reached over the `docker exec -i macos nc 172.30.0.2 22` ProxyCommand), re-measured 2026-09-30 on `crates/tinio-e2e/tests/features/confinement.feature` **1154 lines / sha256 `ebfcf47b9e1daaedff5245f6cfb5b4628efcc23375e3d3d30922c20a2f67740e`** and `crates/tinio-e2e/tests/steps/cli.rs` **3065 lines / sha256 `ece0d1d76441412c78e211a50bfb2fa6a35a5bbd98eab2b6931aaeda61ce8b33`**, both verified hash-identical on the guest — the inspection of the one boundary that frames them (`crates/tinio-cli/src/run.rs`'s serving arm) is no longer the evidence. `TINIO_E2E_REPORT=/tmp/tinio-e2e/g16-macos-r2.json cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and @macos' --concurrency 1` is **8 scenarios / 57 steps, 8/8 and 57/57 green, exit 0, 32 s**, the two G16 rows among the eight elements, both rows' `its stdout is the refusal frame and nothing else, carrying …` steps passing in the report; a tag filter is the tightest selection available because no tag is unique to them (cucumber 0.23's `--tags` and `--name` conflict), so the run also covers the family's other six `@macos` elements — all green. **Re-established 2026-10-01 on the current tree — `crates/tinio-e2e/tests/features/confinement.feature` 1156 lines / sha256 `da6130f3cd01b884b9199954a893e7dcf96e141a48cecdb8b6360e6bc480e493`, `crates/tinio-e2e/tests/steps/cli.rs` 3092 lines / sha256 `41a158af4b1dbf2509591d5840b9fd4ab9ffe51387ba64251355ef46bcb3a86f`, both hash-identical on the guest: `TINIO_E2E_REPORT=/tmp/tinio-e2e/verify/g16-macos-v1.json cargo test -p tinio-e2e --test cucumber -- --tags '@confinement and @macos' --concurrency 1` is **8 scenarios / 57 steps, 8/8 and 57/57 green, exit 0, 2 s wall with a warm build** (the run's own `cargo test` freshness check took 0.25 s), the two G16 rows among the eight elements (`A read-only run that must derive its state directory is refused` 0.029 s, `A state directory that cannot take the write probe is refused` 0.026 s), both rows' `its stdout is the refusal frame and nothing else, carrying …` steps passing in the report.**)*
- [x] B5 G17–G18 (the Windows operator contract, the capability set)
- [x] B6 G19–G22 (the external clients' confined arm, §4 B6)
- [x] B7 three-host run: Windows native, WSL2 (`-d Ubuntu-26.04`), macOS guest; no leg left failing-on-skip

### Phase C — the platforms, then the deletions

- [x] C1 P1 (Linux in place) + P2 (status forwarding, both launcher platforms)
- [x] C2 P4 + P5 (Windows migration) with the SID-from-stderr step
- [x] C3 P3 (macOS) with the `.env`-symlink induction
- [x] C4 the deletions in §5, each with its replacement green on its own platform plus the local default suite

### Phase D — budget, docs, debts

- [x] D1 M1–M3 measurements, each number written into this plan
- [x] D2 CI edits the numbers justify; `docs/tests.md` sections updated by the task that changes the fact
  *(Closed by ruling with nothing to do: M1's verdict is "accept as-is" and M2/M3 produced no number, so no CI edit is justified and no coverage changed; the reasons sit in §6.)*
- [x] D3 the debts in §7 recorded in `docs/tests.md` beside the platform they belong to
- [x] D4 the closing ownership check (`docs/superpowers/README.md` `## The check, before a phase closes`) across this plan and its predecessor documents

## §10 Acceptance

- The full Standard acceptance block (`docs/tests.md`), plus the three operator gates: `verify-windows.ps1` on this machine, `wsl-chroot.sh` in WSL2 (`-d Ubuntu-26.04`), `macos-sandbox-verify.sh` in the macOS guest (started from its compose file, §2, with the operator's go-ahead) — each green, including its leak gate and its refusal allowlist. **Amended 2026-09-30: the three gates were not run end to end for this run, so this bullet is not claimed green.** What the run verified is their component legs and their selection strings on all three hosts (B7's three-host run, C4's platform legs, C6's `@linux` scenario executed by `wsl-chroot.sh`'s own selection, D4's byte-identical tag restatements); the gates stay the operator's to run, and on macOS the leak gate would trip on the SIGTERM family — one confined server orphaned per confined run, the all-green serial run included (`docs/tests.md`'s WSL2 section) — until that product finding is fixed. **Amended 2026-09-30, later the same day — the gates were run after all, on the follow-up tree, and the macOS clause above is falsified by measurement.** The follow-up work landed after this run closed (E1/E2 unified the mistyped-selector exit code, F1 fixed the confined SIGTERM defect and added its `@linux` leg); the three gates then ran against that frozen tree, serially because all three hosts share one machine, and are **all green**: `verify-windows.ps1` exit 0 (`every leg passed`; its `@windows` confinement selection 39 scenarios / 247 steps, the capability row's `exit code 2` among them), `wsl-chroot.sh` exit 0 under `set -euo pipefail` (its confined leg 27 scenarios / 173 steps, `A confined server stops on SIGTERM` among them), and `macos-sandbox-verify.sh` exit 0 in the guest (both crate suites green, the necessity table's four rules `required`, and `no unexpected refusal for the server's own process`). **The prediction that the macOS leak gate would trip was wrong on two counts, both measured**: `macos-sandbox-verify.sh` carries **no process-leak check at all**, and it runs no cucumber — the orphan per run came from the *cucumber* harness's SIGKILL teardown on the launcher platforms (design decision 20's accepted gap, swept by name), a path this gate never walks; the guest held no `tinio` process after it. The run's own account of its state above stays true of the run's tree.
- `cargo test -p tinio-e2e --test traceability` green: every new scenario carries a real spec ID and no ID is orphaned, and `specs/001-s3-local-server/contracts/s3-surface.md`'s coverage prose is updated in the same task.
- `cargo hack check --package tinio --each-feature` green.
- `crates/tinio-util/tests/spawn_guard.rs` green, its counts unchanged or updated in the same task.
- Every deletion in §5 has its replacement asserted on its own platform.
- The external clients' legs (G19–G22) green on their operator hosts; the CI interop legs still pass `--tags '@interop'` without sweeping in a confined scenario.
- The new metric families are registered in `specs/001-s3-local-server/data-model.md` §Metrics, and the secret-leak assertion is green.
- No new skip path: `TINIO_SANDBOX_ALLOW_SKIP` remains the only opt-out, and no scenario added here relies on it.
