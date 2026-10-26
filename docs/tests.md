# Tests

## Rust

- Async stays async (no blocking-runtime wrap); sync is ordinary. Exempt: runtime-shape.
- Conformance harness = non-default test feature; cucumber does not re-assert Rust.
- One-off scripts: any language; standing unit / e2e: Rust only.
- Scratch outside the repo and not `target/`; clear stale leftovers before a round, the round's own at its end.
- Placement: unit next to the subject; cucumber iff the migration rule admits, else integration.
- DB-backed legs pin 4 test threads (`--test-threads=4`; nextest test group) — a 4-conn pool per leg vs the fixture's `max_connections`.
- DB legs (pg, tier): `TINIO_PG_REQUIRED` aborts on a missing `TINIO_PG_DSN`, never skips; the cucumber pg/tier passes panic regardless.

## PostgreSQL fixture

- The fixture is `tools/pg/docker-compose.yml`: postgres 18, host port 55432, user and database `tinio`.
- Start it before the DB legs; the compose healthcheck gates readiness.
- Run the DB legs and the sqlx cache check with the DSN below.
- Set `TINIO_PG_REQUIRED=1` to fail closed; unset means the legs skip.

```sh
docker compose -f tools/pg/docker-compose.yml up -d
export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'
cargo xtask sqlx check
cargo test -p tinio-store-pg -p tinio-pg -- --test-threads=4
docker compose -f tools/pg/docker-compose.yml down
```

- Stop with `down`; add `-v` only to reset the data volume.

## Tiered storage legs

- Conformance: shared `assert_conformance` + versioning suite over real fs warm and pg cold planes — never a fake.
- Harness drain rule: drain a metadata-only GET body before writing or deleting the same key; the tier holds the read lock through it.
- Crash matrix: the rows, placements and files a crashed copy, removal, publish, index clear or tombstone leaves are planted, never mocked.
- Layer tests own the paths a contract cannot reach: read path, write path, batch, stage, listing, capacity/heat mover, index open.
- e2e: `@tier` stays out of the default pass; its own serial `--features tier` leg needs a live database and a `TINIO_PG_DSN`.
- External: `interop/tier.feature`; client presence and the CI requirement are the e2e README's.
- Run commands, tags and harness detail: `crates/tinio-e2e/README.md`; the tier tasks are the design's T25.

## Harness & guards

- Shared harness = lib behind a test feature; shared include = file-scope.
- Integration for crate boundary / foreign oracle / bench-shared / public-API properties.
- One-module property suite inline with its subject; a harness spawns via `job::spawn` only; an outside-guard site: record the reason.
- Drop a duplicate only when the survivor covers the same rule; fold missing asserts first.
- State-dir layout: closed set the fs backend tests pin, owned by `data-model.md` §Reserved Directory.
- State-dir classes: fs reserved dir, read-only home leaf, pg synthetic root — owner `contracts/config.md` (`[storage.pg]`).

## Standard acceptance (pre-push gate)

- Local cmds cover every CI leg; CI profile = dev; CI unit = nextest, local = cargo test, doctest separate.
- Out of gate: interop (tools on `PATH`), audit, semver (constitution VI). Platform legs: below.
- Doc gate: workspace rustdoc, warnings denied — not doctests; public docs must not link private items.
- Doctests compile only under `cargo test` (`--doc` runs just that target) — the `cargo doc` gate sees no broken example.
- Feature-gate breakage needs each-feature; matrix = facade + server + `tinio-fs` feature-off, not a powerset.

## Feature-off legs

- Feature-off: lib-only check/clippy; an each-feature check of the fs crate compiles the stripped arm.
- Test-run the refusal arm; versioning graph is a runtime seam (`docs/cargo.md`).
- `#[cfg(not(unix))]` with `versioning` off needs a no-default-features check (no `--all-targets`).
- Sandbox e2e requires the sandbox feature (gates the body, not the program — the binary still serves off).

## Platform legs

- Platform-gated change ⇒ checked on Linux first: WSL2 `Ubuntu-26.04` (not the default `docker-desktop`).
- There: checkout on the Linux filesystem, artifacts on ext4; cross the boundary with a script file, not inline commands.
- Legs (`--locked`): the fs check and the fs clippy, on that host.
- fs chown success path is **root-only**: one root-side run owed (it self-skips as uid 1000).

## Cucumber (tinio-e2e)

- Layout / tags / env: e2e README; features ↔ S3 surface contract.
- `Given`/`When`=act, `Then`=assert; 1st-person; one behavior/scenario.
- Default filter owns platform / external / confinement (`TINIO_E2E_CONFINEMENT`) / pg; replacing it restates every exclusion.
- Gate: fs, mem, pg (live postgres), parquet, spec-ID traceability; no spec ID → Rust; keep the shared util harness.
- Clean target dir: build the binary before the run, else a large slice of scenarios fails spuriously.

## Product surfaces

- T056 via the Plane API, never raw transport; pipe-pool needs a multi-thread runtime.
- Busy-pipe successor: **T147** — do not paper over.
- CLI process legs cucumber (build the binary first); Windows console drain stays Rust.
- Confinement: feature-off refuses at build; feature-on must reach readiness; honour = macOS CI only.
- Service verbs: supervisor + elevation — no auto leg; logic without elevation: inline unit tests.

## Rust-owned rulings

- Doctor (T057): cucumber for repair flows; Rust for JSON key set vs schema.
- Credential policy stays Rust by ruling — loopback-only built-in fallback.
- Read-only (FR-023): cucumber for backend wire, genuine root, mode / FR-016 / state via the real binary.
- Derived default state-dir stays Rust — CLI children always get an explicit state dir.

## Platform-gated verification

- Confinement **fails**, never skips, when the host cannot do the leg (`TINIO_SANDBOX_ALLOW_SKIP=1`).
- Readiness is SSE (decision 19), not a scraped line; undriveable arms: sandbox bdd plan §7.
- Linux / Windows: operator-owned, not CI; CI compiles/lints, the run is filtered.
- Confinement e2e = cucumber except oracles with no step-reachable surface, and feature-off refusal (Rust).
- Default workspace run excludes confinement off macOS; operator opt-in via `TINIO_E2E_CONFINEMENT`.

## WSL: the Linux side of `cfg`

- The Linux side of a gate runs in WSL, distro `Ubuntu-26.04` — name it: the default distro is Docker's, with no shell tooling.
- Rust comes from the login shell: `wsl -d Ubuntu-26.04 -e zsh -lic '<cmd>'`; a plain `bash -lc` has no cargo.
- Never install a second toolchain: `~/.cargo` is the host's Rust, and a profile edit that re-points it is the failure.
- The checkout is `/mnt/d/Github/tinio`; a Linux build must not write the Windows `target/` — put `CARGO_TARGET_DIR` on the WSL root fs.
- What it buys: the `cfg(unix)` arms compile and their suite runs, and the ubuntu doc/clippy gates see no `cfg(windows)` modules.
- Windows-only legs stay on Windows; WSL covers the other side of `cfg`, not a second run of the same legs.

## Windows: gates & network

- Windows gates never compile `#[cfg(unix)]`: verify such tests/fns on Linux.
- Windows: confinement out of CI (cost); ACE shape design §6; launcher does not clean stale ACEs.
- Network: unconfined reachability first, then confined denial; readiness-timeout induction is undriveable.

## Windows: the low-integrity label

- Low integrity: binaries built in this tree run Low — the confinement failures are that label, not the tool.
- Symptoms: AppContainer profile creation denied (`hr=0x80070005`); temp-directory writes denied (`PermissionDenied`).
- Never a `SeCreateSymbolicLinkPrivilege` problem — not the familiar `1314`.
- Relaunching from another shell or the task scheduler changes nothing — the image's label wins.
- One server e2e leg keeps an unrelated failure (fixture induction).

## Windows: the fix

- Fix — move the build, not the label: target and temp dirs redirected outside the labelled tree.
- Out-of-tree on this host: the fs and sandbox suites pass with nothing skipped or ignored.
- Clearing the label needs elevation and drops containment — operator's call, not a silent fix.

## macOS

- Real-Mac gate; off-Mac certifies nothing; fail closed without the host helper.
- Fixture: `D:\docker\macos` (outside the repo); image preloaded; KVM lives in the `docker-desktop` WSL VM.
- Nested virtualization required (Windows 11 + Docker Desktop); start, use console or SSH, stop gracefully.
- Guest carries Rust and a crates mirror; sync the tree in, then run the macOS legs over SSH.
- Two mechanisms; confinement serial; nested Seatbelt is environment; socket-path overflow: harness.

```sh
wsl -d docker-desktop -- sh -c 'ls -l /dev/kvm'
docker compose -f D:/docker/macos/docker-compose.yml up -d
ssh -p 2222 -i ~/.ssh/id_ed25519 flier@127.0.0.1
docker compose -f D:/docker/macos/docker-compose.yml stop
```
- Two mechanisms (`TINIO_SANDBOX_MACOS_MECHANISM`), both green; confinement serial (parallel pass off).
- Nested Seatbelt is environment; socket-path overflow: harness.

## Concurrency

- Child-spawning suites serial; ≤4 agents; distinct `CARGO_TARGET_DIR`; prefer one suite; serialise when compile-or-confine stacks.
- Guest fork failed → restart the guest, not pkill.
- Scratch: named list only; single-thread probe = standalone binary.
- Guest failures = **harness artifacts, not product defects**.
- Sync must carry vendor, config, specs, packaging, crates, manifests.
- CRLF on unix unit files: pin LF in attributes; recurs → re-ship packaging.
- Socket-path overflow → harness depth first; never two agents on the same VM.
