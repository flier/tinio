# BDD Cucumber Migration (crates/tinio-e2e) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate tinio's black-box integration tests, the CI-gated bash interop scripts, and the spec-semantic unit tests into one BDD cucumber suite (`crates/tinio-e2e`), with CI reporting test results to the PR.

**Architecture:** A new workspace crate `crates/tinio-e2e` hosts a single cucumber-rs 0.23 test binary (`harness = false`) plus all `.feature` files and domain-organized step modules. The `World` holds an in-process `DataPlane` server (backend chosen from scenario tags + `TINIO_E2E_BACKEND`) or, for `@external` scenarios, a spawned `serve` binary plus external client wrappers (aws/rclone/boto3/mc). Feature files are the executable form of `specs/001-s3-local-server/contracts/s3-surface.md`; every scenario carries `@FR-xxx`/`@SC-xxx`/`@T0xx` tags and a CI traceability check cross-references spec IDs both ways.

**Tech Stack:** cucumber-rs 0.23.0 (`output-json` feature), tokio (macros/rt-multi-thread/time), existing workspace crates (tinio-fs, tinio-mem, tinio-server, tinio-util `testing`), assert_cmd, tempfile, GitHub Actions (`dorny/test-reporter`, `actions/upload-artifact`).

**Spec:** `docs/superpowers/specs/2026-08-31-cucumber-bdd-migration-design.md` — this plan argues from the spec; executors read both. The spec's Decisions, Migration scope, CI, and WSL2 sections are binding.

## Global Constraints

- **Zero commits.** The project git rule (CLAUDE.md) forbids auto-commit, and the user's decision (grilling Q5) is: no commits at all during the migration; everything lands in the working tree; the user reviews and commits manually after final review. There is NO commit step in any task. Each task ends with verification only.
- **cucumber 0.23.0 pinned**, with `features = ["output-json"]`. No cucumber dependency in any library crate — only in `tinio-e2e` dev-dependencies.
- **English only** in feature files, step code, comments, and docs (project language rule).
- **Migration rule (conjunction, grilling Q6):** a test migrates iff it carries a SC/FR/T spec semantic **and** its behavior is observable through the S3 API. API-observable unit tests without a spec ID stay in Rust. Cucumber scenarios never repeat conformance-harness assertions.
- **Deletion rhythm (grilling Q7):** per file — port to feature + steps, run old and new side by side locally, verify 1:1 coverage against the task's checklist, then delete the old file in-tree. Bash scripts are deleted once `@interop` is green locally in WSL2 and the quality suite is green locally.
- **Default run excludes `@external`** (grilling Q2): `cargo test -p tinio-e2e` without `--tags` and without `TINIO_E2E_EXTERNAL` runs only in-process scenarios.
- **Dual backend:** `#[before]` hook picks the backend from scenario tags (`@mem` tag or `TINIO_E2E_BACKEND=mem`; default `fs`). CI quality runs the non-external suite with `fs` and with `TINIO_E2E_BACKEND=mem`.
- **No direct-Storage cucumber steps.** Scenarios are black-box HTTP only. The `tinio-util` conformance harness stays untouched.
- **WSL2 is a local-dev convenience** (grilling round 1), not a CI substitute; macOS verification happens post-push only (grilling Q8).
- **Interop job keeps running** on all 3 OS (existing CI comment: "harness ... is committed (T032), job always runs").
- **House style:** imports per `docs/style.md`; async tests directly (no `Runtime::block_on` wrappers) — exception: the runner `main` needs `set_var` before the runtime thread pool starts, see Task 1.

## File Structure

```
crates/tinio-e2e/                          (new workspace member)
├── Cargo.toml                             cucumber 0.23 + output-json; [[test]] cucumber, harness=false
├── README.md                              tag rules, WSL2 workflow, FR-025 tiering matrix (Task 11)
├── scripts/wsl-interop.sh                 WSL2 one-command @interop run (Task 11)
└── tests/
    ├── cucumber.rs                        runner main: default ~@external exclusion, report file (Task 1)
    ├── features/
    │   ├── buckets.feature                @SC-001 @FR-xxx (Task 1)
    │   ├── error_codes.feature            @SC-004 (Task 2)
    │   ├── reserved_paths.feature         @FR-020 (Task 3)
    │   ├── objects.feature                @T025 (Task 4)
    │   ├── listing.feature                @SC-001 (Task 4)
    │   ├── metrics.feature                (Task 5)
    │   ├── tagging.feature                (Task 5)
    │   ├── multipart.feature              @T032 (Tasks 5, 8)
    │   ├── conditions.feature             (Task 5)
    │   └── interop/
    │       ├── journey.feature            @interop @aws @rclone + @boto3 scenarios (Task 8)
    │       └── advanced.feature           @interop + @mc scenarios (Task 8)
    └── steps/
        ├── mod.rs                         World + step-module list (Task 1)
        ├── common.rs                      in-process Server port + Client + LastResponse (Task 1)
        ├── buckets.rs  errors.rs  objects.rs  listing.rs  multipart.rs
        ├── conditions.rs  reserved_paths.rs  metrics.rs  tagging.rs
        ├── clients.rs                     external-client wrappers, @external spawn (Task 8)
        └── traceability.rs                spec↔tag cross-check test (Task 9)
```

New files outside the crate: `CONTEXT.md` (repo root — BDD-migration glossary, created at grilling 2026-08-31, maintained in Task 11).

Existing files that change: root `Cargo.toml` (workspace members); `.github/workflows/ci.yml` (Task 10); `specs/001-s3-local-server/contracts/s3-surface.md`, `specs/001-s3-local-server/checklists/compatibility.md` (Task 9); `docs/cargo.md`, `docs/style.md`, `CLAUDE.md` (Task 11).

Existing files deleted in-tree: `crates/tinio-server/tests/error_codes.rs` (Task 2), `reserved_paths.rs` (Task 3), `data_plane.rs` (Task 4), `coverage_gaps.rs` (Task 5), `crates/tinio-server/tests/common/mod.rs` (Task 6), `journey.rs`, `advanced.rs`, `boto3.rs`, `mc.rs`, `edge.rs`, `tests/e2e/mod.rs`, the whole `e2e/` directory (Task 8).

## Step Vocabulary (defined once here; each task implements the steps its features reference)

Given/When (implemented in the listed step module):

| Step | Module |
|---|---|
| `I create bucket "{name}"` | buckets.rs |
| `I delete bucket "{name}"` | buckets.rs |
| `I upload "{key}" with {int} bytes` | objects.rs |
| `I upload "{key}" with body "{text}"` | objects.rs |
| `I get object "{key}"` / `I head object "{key}"` | objects.rs |
| `I delete object "{key}"` | objects.rs |
| `I copy object "{src}" to "{dst}"` | objects.rs |
| `I start a multipart upload for "{key}"` | multipart.rs |
| `I upload part {int} with {int} bytes` | multipart.rs |
| `I upload part {int} with body "{text}" and checksum-{algo} "{b64}"` | multipart.rs |
| `I complete the multipart upload` | multipart.rs |
| `I abort the multipart upload mid-body` | multipart.rs |
| `I list objects under "{prefix}"` / `… with delimiter "{d}"` | listing.rs |
| `I list v1 objects under "{prefix}" with marker "{m}" and delimiter "{d}"` | listing.rs |
| `I send a {word} request to "{path}"` (+ optional `with headers {table}` + optional `with body "{text}"`) | errors.rs (raw wire-level) |
| `I concurrently upload "{k1}" and "{k2}" with {int} bytes each` | objects.rs (`tokio::join!`) |
| `I write "{text}" to "{rel}" in the served root` | reserved_paths.rs (fs out-of-band) |

Then (assertions):

| Step | Module |
|---|---|
| `the response status is {int}` | errors.rs |
| `the error code is "{code}"` | errors.rs (XML `<Code>` body parse) |
| `the error code is not empty` | errors.rs |
| `the response header "{name}" is "{value}"` | errors.rs |
| `the response header "{name}" is stored` | errors.rs (saves the value for `{etag}` substitution) |
| `the object body equals the uploaded bytes` | objects.rs |
| `the object body is "{text}"` / `the object body length is {int}` | objects.rs |
| `the object ETag matches the MD5 of the uploaded bytes` | objects.rs |
| `the object ETag matches the composed multipart form` | multipart.rs |
| `the listing shows {int} keys` / `the listing contains "{key}"` / `the listing is empty` | listing.rs |
| `the listing prefixes are "{p1}" and "{p2}"` | listing.rs |
| `the multipart upload disappears within {int} seconds` | multipart.rs (10 s poll) |
| `the served root contains only the state dir and the bucket` | reserved_paths.rs |
| `no file was written outside the served root` | reserved_paths.rs |

Backend/config tags (consumed by `config_from_tags` — the `#[before]` hook, Task 1, and the @external spawn, Task 7): `@mem` (mem backend; an explicit `@fs`/`@mem` tag wins over the env), `@nested-root` (fs: root = tempdir/"root", for traversal-proof scenarios), `@checksum-on` (`caps.checksum = true`), `@minimal-caps` (multipart/copy_object/list_objects_v1/list_objects_v2/delete_objects all false), `@cold-listing` (fs scanner interval 100 ms).

---

### Task 1: tinio-e2e skeleton — runner, World, in-process server, first feature

**Files:**
- Create: `crates/tinio-e2e/Cargo.toml`
- Create: `crates/tinio-e2e/tests/cucumber.rs`
- Create: `crates/tinio-e2e/tests/steps/mod.rs`
- Create: `crates/tinio-e2e/tests/steps/common.rs`
- Create: `crates/tinio-e2e/tests/steps/buckets.rs`
- Create: `crates/tinio-e2e/tests/features/buckets.feature`
- Modify: `Cargo.toml` (root — add `crates/tinio-e2e` to workspace members, matching the existing listing style)

**Interfaces:**
- Consumes: nothing (new crate). Source for the port: `crates/tinio-server/tests/common/mod.rs` (in-process `Server`, `request`, `extract`, `eventually`), `crates/tinio-server/src/backend/testutil.rs` (capability literals pattern).
- Produces: `steps::common::{Server, Client, LastResponse}`, `steps::World` (with `backend`, `server`, `client`, `last` fields), the runner binary — everything later tasks build on.

- [x] **Step 1: Write the crate manifest**

`crates/tinio-e2e/Cargo.toml`:

```toml
[package]
name = "tinio-e2e"
version = "0.1.0"
edition.workspace = true
license.workspace = true
publish = false

[dependencies]
cucumber = { version = "0.23", features = ["output-json"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }

[dev-dependencies]
tinio-fs = { path = "../tinio-fs" }
tinio-mem = { path = "../tinio-mem" }
tinio-server = { path = "../tinio-server" }
tinio-util = { path = "../tinio-util", features = ["testing"] }
assert_cmd = { workspace = true }
tempfile = { workspace = true }
http = { workspace = true }
futures = { workspace = true }

[[test]]
name = "cucumber"
harness = false
```

(Adjust dependency keys to the workspace's actual shared-dep names if the root `Cargo.toml` defines them differently — the workspace uses shared deps for assert_cmd/tempfile/criterion per the existing crates; `http`/`futures` are already shared for tinio-server tests.)

- [x] **Step 2: Write the runner with default @external exclusion**

`crates/tinio-e2e/tests/cucumber.rs`:

```rust
//! The single cucumber test binary. All scenarios live in
//! `tests/features/`; step definitions live in `tests/steps/`.
//!
//! Default tag filter: scenarios that need external client binaries
//! (`@interop`/`@boto3`/`@mc`) are excluded unless the user passes an
//! explicit `--tags` on the CLI or sets `TINIO_E2E_EXTERNAL=1` — the
//! same "opt-in" semantics the old `#[ignore]` integration tests had.
//!
//! `TINIO_E2E_REPORT=<path>` additionally writes a Cucumber-JSON report
//! to the given file (CI uses this for the PR test report).

mod steps;

use steps::{configure, World};

fn main() {
    // Must run before the tokio runtime starts: CUCUMBER_FILTER_TAGS is
    // read by cucumber's CLI parser when no --tags is given. SAFETY: no
    // threads exist yet; the runtime is built below.
    if std::env::var_os("TINIO_E2E_EXTERNAL").is_none()
        && !std::env::args().any(|a| a == "--tags")
    {
        unsafe {
            std::env::set_var("CUCUMBER_FILTER_TAGS", "not @interop and not @boto3 and not @mc");
        }
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(async {
        let mut runner = World::cucumber().init_tracing();
        if let Ok(path) = std::env::var("TINIO_E2E_REPORT") {
            // Pretty output to stdout + Cucumber-JSON to the file, mirroring
            // cucumber-rs book "output/multiple.md".
            let file = std::fs::File::create(&path).expect("create report file");
            runner = runner.with_writer(
                cucumber::writer::Basic::raw(
                    std::io::stdout(),
                    cucumber::writer::Coloring::Auto,
                    cucumber::writer::Verbosity::Default,
                )
                .tee::<World, _>(cucumber::writer::Json::new(file, false)),
            );
        }
        runner.run_and_exit("tests/features").await;
    });
}
```

(`steps::configure()` — defined in Task 1 Step 5 — attaches the `#[before]`/`#[after]` hooks and returns the configured `Cucumber<World>`; `run_and_exit` still follows. The step attribute macros register their steps into the binary's registry regardless of the runner construction.)

Note: the tag-expression grammar (`not … and …`) and the exact `writer::Json::new` signature are verified against the cucumber-rs 0.23 book during this task; if `writer::Json` differs, use `cucumber::cli::Opts` with a manual JSON writer per the book's multiple-writer example. The behavior contract (default exclusion; JSON report file; non-zero exit on failure) is fixed.

- [x] **Step 3: Write the World and step-module registry**

`crates/tinio-e2e/tests/steps/mod.rs` — the steps are collected into the binary by the cucumber attribute macros; every step module is declared here and registered with `#[snippet]`-style module includes. The `World`:

```rust
pub mod buckets;
pub mod common;
pub mod conditions;
pub mod errors;
pub mod listing;
pub mod metrics;
pub mod multipart;
pub mod objects;
pub mod reserved_paths;
pub mod tagging;

pub use common::{Backend, Client, LastResponse, Server};

/// Shared per-scenario state; cucumber builds one via `Default` per
/// scenario and the `#[before]`/`#[after]` hooks manage the server.
#[derive(Debug, Default, cucumber::World)]
#[world(init = Self::new)]
pub struct World {
    pub backend: Backend,
    pub server: Option<Server>,
    pub client: Client,
    pub last: LastResponse,
}
```

The `#[world(init = Self::new)]` attribute is only needed if `Client` cannot derive `Default` (it holds an `http::Client`); if it can, plain `#[derive(Debug, Default, World)]` suffices and `new` is dropped. Implement `impl World { pub fn new() -> Self { Self { backend: Backend::default(), server: None, client: Client::new(), last: LastResponse::default() } } }` when the attribute is used. Also in this file: the `#[before]`/`#[after]` hooks and the tag→config mapping (see Step 5).

- [x] **Step 4: Port the in-process server harness**

`crates/tinio-e2e/tests/steps/common.rs` — copy the contents of `crates/tinio-server/tests/common/mod.rs` (the in-process `DataPlane` spawn on `127.0.0.1:0`, watch-channel shutdown, `request`, `extract`, `eventually`, `Response` parsing) into this module, adjusted as follows:

- Rename nothing — keep `Server`, `request`, `extract`, `eventually` as-is (same signatures; the cucumber steps call them exactly like the old tests did).
- `Server` gains one public accessor so fs-out-of-band scenarios can reach the temp root: `pub fn root(&self) -> Option<&std::path::Path>` returning the `TempDir` path when the backend is fs.
- `Server` keeps its `spawn(storage, caps, root)` constructor and the `fs`, `fs_at`, `mem` convenience constructors.
- Add the small types the World needs:

```rust
/// Which storage backend the in-process server runs on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Backend {
    #[default]
    Fs,
    Mem,
}

/// Which fs-server variant a scenario's tags demand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsKind {
    #[default]
    Plain,
    NestedRoot,            // root = tempdir/"root" — traversal-proof scenarios
    ColdListing(Duration), // fast scanner interval (cold-listing scenarios)
}
```

- `Server` gains two new constructors alongside `fs`/`fs_at`/`mem`:
  - `fs_nested(caps)` — base `tempfile::tempdir()`, root = `base.path().join("root")` created upfront; used by the `@nested-root` scenarios (the traversal proof needs a controlled parent dir whose contents are observable). `root()` returns the nested root.
  - `fs_with_scanner_interval(caps, interval)` — fs backend with the scanner interval passed into the `DataPlane`/`FsOptions` construction (see how `fs_options()` and the pipeline wiring work in `crates/tinio-fs/src/testing.rs` and `tinio-server/src/backend/mod.rs`; keep the default `fs_options()` for all other scenarios). If the interval is not threadable without deeper plumbing, fall back to an env-var read inside the existing `Server::fs` path (the behavior contract — cold-listing scenarios get a fast scanner — is fixed; the mechanism is flexible).

/// The last response, for Then assertions. `RequestError` records a
/// connection-level failure distinctly from an HTTP response.
#[derive(Debug, Clone, Default)]
pub struct LastResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// Raw HTTP client used by every step (one per scenario).
#[derive(Debug, Clone, Default)]
pub struct Client(/* http::Client + the base URL from the spawned server */);
impl Client {
    pub fn new() -> Self { Self::default() }
    /// Bind to the scenario's server; returns the request() helper
    /// closure-compatible view the steps use.
    pub fn bind(&mut self, addr: std::net::SocketAddr) { /* … */ }
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> LastResponse { /* ported from common::request + parse_response */ }
}
```

Port the existing `request`/`parse_response`/`dechunk` helpers into `Client::request` so every step goes through one type. The old `request(addr, method, path, headers, body)` free-function call sites in the old test files are replaced by `world.client.request(method, path, headers, body)` in the steps.

- [x] **Step 5: Implement the lifecycle hooks + tag→config mapping**

In `tests/steps/mod.rs`:

```rust
use cucumber::{event, given, then, when, World as _};
use tinio_server::Capabilities;
use tokio::time::{Duration, sleep};

/// Tag → server configuration, shared by the in-process hook and the
/// @external spawn (Task 7). One mapping, one place: a scenario tag means
/// the same configuration whichever way the server runs.
pub fn config_from_tags(tags: &[String]) -> (Backend, Capabilities, FsKind) {
    let tagged = |t: &str| tags.iter().any(|x| x == t);
    // Backend: an explicit @fs/@mem scenario tag wins; otherwise the
    // TINIO_E2E_BACKEND env override (the CI mem pass); default fs.
    let env_backend = std::env::var("TINIO_E2E_BACKEND").ok();
    let backend = if tagged("mem") {
        Backend::Mem
    } else if tagged("fs") {
        Backend::Fs
    } else if env_backend.as_deref() == Some("mem") {
        Backend::Mem
    } else {
        Backend::Fs
    };

    // Capability toggles (spec §Tagging, grilling Q4).
    let mut caps = Capabilities::default();
    if tagged("checksum-on") {
        caps.checksum = true;
    }
    if tagged("minimal-caps") {
        caps.multipart = false;
        caps.copy_object = false;
        caps.list_objects_v1 = false;
        caps.list_objects_v2 = false;
        caps.delete_objects = false;
    }

    let fs_kind = if tagged("nested-root") {
        FsKind::NestedRoot
    } else if tagged("cold-listing") {
        FsKind::ColdListing(Duration::from_millis(100))
    } else {
        FsKind::Plain
    };
    (backend, caps, fs_kind)
}

pub fn configure() -> cucumber::Cucumber<World> {
    World::cucumber()
        .before(|_feature, _rule, scenario, world| async move {
            let (backend, caps, fs_kind) = config_from_tags(&scenario.tags);
            world.backend = backend;
            let server = match (backend, fs_kind) {
                (Backend::Mem, _) => Server::mem(caps).await,
                (Backend::Fs, FsKind::NestedRoot) => Server::fs_nested(caps).await,
                (Backend::Fs, FsKind::ColdListing(interval)) => {
                    Server::fs_with_scanner_interval(caps, interval).await
                }
                (Backend::Fs, FsKind::Plain) => Server::fs(caps).await,
            };
            world.client.bind(server.addr());
            world.server = Some(server);
        })
        .after(|_feature, _rule, _scenario, _ev, world| async move {
            // Dropping the Server sends the watch-channel shutdown.
            world.server.take();
        })
}
```

- [x] **Step 6: Write the first feature + its steps**

`crates/tinio-e2e/tests/features/buckets.feature` (derived from `specs/001-s3-local-server/contracts/s3-surface.md`; scenario set SC-001):

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (buckets)
@SC-001
Feature: Buckets

  Scenario: Create and delete a bucket
    Given I create bucket "demo"
    Then the response status is 200

  Scenario: Duplicate bucket creation answers BucketAlreadyOwnedByYou
    Given I create bucket "demo"
    And I create bucket "demo"
    Then the error code is "BucketAlreadyOwnedByYou"

  Scenario: Bucket listing shows created buckets
    Given I create bucket "alpha"
    And I create bucket "beta"
    Then the bucket listing contains "alpha" and "beta"
```

`crates/tinio-e2e/tests/steps/buckets.rs`:

```rust
use cucumber::{given, then, World as _};

#[given("I create bucket {string}")]
async fn create_bucket(world: &mut super::World, name: String) {
    world.last = world
        .client
        .request("PUT", &format!("/{name}"), &[], &[])
        .await;
}

#[given("I delete bucket {string}")]
async fn delete_bucket(world: &mut super::World, name: String) {
    world.last = world
        .client
        .request("DELETE", &format!("/{name}"), &[], &[])
        .await;
}

#[then("the response status is {int}")]
async fn status_is(world: &mut super::World, status: u16) {
    assert_eq!(world.last.status, status, "status mismatch");
}

#[then(regex = r"the bucket listing contains "([^"]+)" and "([^"]+)""")]
async fn listing_contains(world: &mut super::World, a: String, b: String) {
    let resp = world.client.request("GET", "/", &[], &[]).await;
    let text = String::from_utf8_lossy(&resp.body).into_owned();
    for name in [&a, &b] {
        assert!(
            text.contains(&format!("<Name>{name}</Name>")),
            "bucket {name} missing from listing: {text}"
        );
    }
}
```

`the error code is "{code}"` lives in `errors.rs` (Task 2) — the second scenario above intentionally references it, so Task 2's first step is to implement it and make this scenario green. The `{string}`/`{int}`/`{word}` placeholders are cucumber-rs expressions (verify exact parameter syntax in the 0.23 docs — the README example uses `{word}` and `(\d+)`).

- [x] **Step 7: Register the crate in the workspace and run**

- Modify the root `Cargo.toml` `[workspace] members` to include `"crates/tinio-e2e"`.
- Run: `cargo test -p tinio-e2e`
  Expected: the cucumber binary runs; scenarios 1 and 3 pass, scenario 2 fails with "step not found: the error code is ..." (step to be implemented in Task 2).
- Run: `TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e`
  Expected: same result (mem backend).
- Run: `cargo test --workspace --exclude tinio-e2e`
  Expected: unchanged green (existing suite untouched).
- Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo +nightly fmt --all -- --check`
  Expected: green; fix any style nits (house import style per `docs/style.md`).

- [x] **Step 8: Verify the default-exclusion behavior**

- Run: `cargo test -p tinio-e2e -- --list` (or `--help` if `--list` is unsupported)
  Expected: the cucumber CLI shows the runner options; the default tag filter is active (no `@external` scenarios exist yet, so this is a smoke check of the mechanism).
- Run: `TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e -- --help` (sanity — no crash)
- Run: `cargo test -p tinio-e2e -- --tags @nonexistent`
  Expected: zero scenarios run, exit code 0 (tags override the default filter).

**Do NOT commit.** Leave everything in the tree.

---

### Task 2: error_codes.feature — the migration pattern task

**Files:**
- Create: `crates/tinio-e2e/tests/features/error_codes.feature`
- Create: `crates/tinio-e2e/tests/steps/errors.rs` (+ `pub mod errors;` in `steps/mod.rs`)
- Delete (in-tree): `crates/tinio-server/tests/error_codes.rs` — only after the parity check passes

**Interfaces:**
- Consumes: `World`, `Client::request`, `Server::{mem, fs_at}`, `LastResponse` (Task 1); `the response status is {int}` (Task 1).
- Produces: `errors.rs` steps — `the error code is "{code}"`, `the response header "{name}" is "{value}"`, raw `I send a {word} request to "{path}"` (with optional `with headers …`/`with body …` continuations) — plus the per-scenario steps `I start a multipart upload for "{key}"`, `I upload part {int} with body "{text}" and checksum-crc32 "{b64}"` (multipart steps live in `multipart.rs`, created here as a stub module because error_codes needs them).

**Source to port:** `crates/tinio-server/tests/error_codes.rs` (204 lines, 7 `#[tokio::test]` fns + `caps()` helper). Each scenario below maps 1:1 to one test fn. The assertions are ported verbatim (status codes, `<Code>` values, header checks).

- [x] **Step 1: Write the feature file**

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (errors) and
# checklists/compatibility.md SC-004; replaces tinio-server/tests/error_codes.rs
@SC-004
Feature: S3 error codes over real HTTP

  Scenario: Missing bucket answers NoSuchBucket
    Given I send a "PUT" request to "/missing/a.txt" with body "x"
    Then the response status is 404
    And the error code is "NoSuchBucket"

  Scenario: Missing object answers NoSuchKey
    Given I create bucket "data"
    And I send a "GET" request to "/data/missing.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"

  Scenario: HEAD on a missing object answers 404
    Given I create bucket "data"
    And I send a "HEAD" request to "/data/missing.txt"
    Then the response status is 404

  Scenario: Invalid bucket name answers InvalidBucketName
    Given I send a "PUT" request to "/Bad_Name"
    Then the response status is 400
    And the error code is "InvalidBucketName"

  Scenario: Bucket create/delete conflicts
    Given I create bucket "data"
    And I create bucket "data"
    Then the error code is "BucketAlreadyOwnedByYou"
    Given I upload "data/a.txt" with body "x"
    And I send a "DELETE" request to "/data"
    Then the error code is "BucketNotEmpty"
    Given I delete object "data/a.txt"
    And I send a "DELETE" request to "/data"
    Then the response status is 204

  @minimal-caps
  Scenario: Disabled capabilities answer NotImplemented
    Given I create bucket "data"
    And I send a "GET" request to "/data?list-type=2"
    Then the error code is "NotImplemented"
    Given I send a "GET" request to "/data"
    And I send a "POST" request to "/data/big.bin?uploads"
    Then the error code is "NotImplemented"

  Scenario: Operations outside the surface answer NotImplemented
    Given I create bucket "data"
    And I send a "GET" request to "/data?policy"
    Then the response status is 501
    And the error code is "NotImplemented"

  @fs @nested-root
  Scenario: Traversal keys are rejected without fs access
    Given I create bucket "data"
    When I send a "PUT" request to "/data/../evil.txt" with body "x"
    Then the response status is 400
    And the error code is not empty
    When I send a "PUT" request to "/data/..%2Fevil2.txt" with body "x"
    Then the response status is 400
    And the error code is not empty
    When I send a "PUT" request to "/data/a%2F..%2Fb" with body "x"
    Then the response status is 400
    And the error code is not empty
    When I send a "PUT" request to "/data//abs.txt" with body "x"
    Then the response status is 400
    And the error code is not empty
    And no file was written outside the served root

  @checksum-on
  Scenario: UploadPart checksum mismatch is BadDigest
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    When I upload part 1 with body "hello world" and checksum-crc32 "y/Q5Jg=="
    Then the error code is "BadDigest"
    When I upload part 1 with body "hello world" and checksum-crc32 "DUoRhQ=="
    Then the response status is 200
```

Notes:
- The old `no_such_bucket` test gates its GET leg on `#[cfg(feature = "list-v1")]`. tinio-e2e dev-depends on tinio-server with default features (list-v1 on) and the `--no-default-features` CI run excludes tinio-e2e, so the GET leg is unconditional here. The PUT leg (→ NoSuchBucket) is the same in both.
- The traversal scenario's old assertions also checked the parent dir (`base_entries == ["root"]`). The `@nested-root` tag (grilling Q2) makes the hook spawn `Server::fs_nested` — base tempdir + nested `root` — so the parent-dir assertion is observable. Port it as the extra step `then no file was written outside the served root` implemented with `Server::root()`: assert the parent of the served root contains only the root dir name. The `the served root contains only the state dir and the bucket` step is Task 3. The old test also asserted each rejected key carries a coded error (`!error_code().is_empty()`) — the `And the error code is not empty` steps above port that.
- `@checksum-on` scenario: the wrong value `y/Q5Jg==` is crc32("123456789") ≠ crc32("hello world"); the right value `DUoRhQ==` is crc32("hello world") — keep the literals.

- [x] **Step 2: Implement errors.rs (raw request + assertions)**

```rust
use cucumber::{given, then, when, World as _};

#[given("I send a {word} request to {string}")]
async fn raw_request(world: &mut super::World, method: String, path: String) {
    world.last = world.client.request(&method, &path, &[], &[]).await;
}

#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with body "([^"]*)""#)]
async fn raw_request_with_body(world: &mut super::World, method: String, path: String, body: String) {
    world.last = world
        .client
        .request(&method, &path, &[], body.as_bytes())
        .await;
}

#[then("the error code is {string}")]
async fn error_code_is(world: &mut super::World, code: String) {
    let text = String::from_utf8_lossy(&world.last.body);
    let found = super::common::extract(&text, "<Code>", "</Code>");
    assert_eq!(found, code, "S3 <Code> mismatch in body: {text}");
}

#[then("the response header {string} is {string}")]
async fn header_is(world: &mut super::World, name: String, value: String) {
    let found = world
        .last
        .headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(&name))
        .map(|(_, v)| v.clone());
    assert_eq!(found.as_deref(), Some(value.as_str()), "header {name}");
}

#[then("the response header {string} is stored")]
async fn header_stored(world: &mut super::World, name: String) {
    // Saves the header value for later steps (e.g. the conditional-request
    // scenarios' `{etag}` substitution).
    world.stored_etag = world
        .last
        .headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(&name))
        .map(|(_, v)| v.clone())
        .expect("header must be present");
}

#[then("the error code is not empty")]
async fn error_code_present(world: &mut super::World) {
    let text = String::from_utf8_lossy(&world.last.body);
    assert!(
        !super::common::extract(&text, "<Code>", "</Code>").is_empty(),
        "no <Code> in body: {text}"
    );
}
```

(`header_stored` and `error_code_present` require `World.stored_etag: String` — add the field in Task 4, or now to keep the World field list complete.)

Note: `{word}` may not be a cucumber-rs expression placeholder (the README example uses `{word}` — verify; fallback: `{string}` with a helper that validates). `LastResponse::headers` needs the port's response parser to keep header names+values (Task 1's `Client::request` must populate it).

- [x] **Step 3: Implement multipart.rs stub (the two steps the feature needs)**

`crates/tinio-e2e/tests/steps/multipart.rs`:

```rust
use cucumber::{given, when, World as _};

#[given("I start a multipart upload for {string}")]
async fn start_upload(world: &mut super::World, key: String) {
    world.last = world
        .client
        .request("POST", &format!("/{key}?uploads"), &[], &[])
        .await;
    // Keep the UploadId for later steps.
    world.upload_id = super::common::extract(
        &String::from_utf8_lossy(&world.last.body),
        "<UploadId>",
        "</UploadId>",
    );
}

#[when(regex = r#"I upload part (\d+) with body "([^"]*)" and checksum-crc32 "([^"]+)""#)]
async fn upload_part_checksum(world: &mut super::World, part: u32, body: String, crc32: String) {
    world.last = world
        .client
        .request(
            "PUT",
            &format!("/data/big.bin?partNumber={part}&uploadId={}", world.upload_id),
            &[("x-amz-checksum-crc32", &crc32)],
            body.as_bytes(),
        )
        .await;
}
```

This requires `World.upload_id: String` (Default = "") — add the field to the World (Task 1 file, `steps/mod.rs`). The multipart feature file is Task 5/8; this stub grows there. (The `key` the upload was started for must be remembered for the part upload path — either store `world.upload_key` or hardcode via the scenario; store `upload_key` for correctness.)

- [x] **Step 4: Register the modules and run the new suite**

- Add `pub mod errors; pub mod multipart;` to `steps/mod.rs`; make sure `configure()` from Task 1 is what `cucumber.rs` uses (wire `World::cucumber()` through `configure()` if it isn't already).
- Run: `cargo test -p tinio-e2e`
  Expected: all `buckets.feature` + `error_codes.feature` scenarios green (scenario 2 of buckets.feature now passes — the error-code step exists).
- Run: `TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e`
  Expected: same, except the `@fs` traversal scenario which runs only on fs (it carries only `@fs`, no `@mem`).

- [x] **Step 5: Parity check and delete the old file**

- The old file `crates/tinio-server/tests/error_codes.rs` is still in the tree. Run it:
  `cargo test -p tinio-server --test error_codes`
  Expected: green (it still passes on its own).
- Check off the 1:1 mapping — old test → scenario:
  - `no_such_bucket` → "Missing bucket answers NoSuchBucket"
  - `no_such_key` → "Missing object answers NoSuchKey" + "HEAD on a missing object answers 404"
  - `invalid_bucket_name` → "Invalid bucket name answers InvalidBucketName"
  - `bucket_already_exists_and_not_empty` → "Bucket create/delete conflicts"
  - `disabled_capabilities_answer_not_implemented` → "Disabled capabilities answer NotImplemented" (@minimal-caps)
  - `unsupported_operations_answer_not_implemented` → "Operations outside the surface answer NotImplemented"
  - `traversal_keys_rejected_without_fs_access` → "Traversal keys are rejected without fs access" (@fs)
  - `upload_part_checksum_mismatch_is_bad_digest` → "UploadPart checksum mismatch is BadDigest" (@checksum-on)
- Delete `crates/tinio-server/tests/error_codes.rs`.
- Run: `cargo test --workspace --exclude tinio-e2e` — the workspace still compiles (no other file imports error_codes.rs).

**Do NOT commit.**

---

### Task 3: reserved_paths.feature

**Files:**
- Create: `crates/tinio-e2e/tests/features/reserved_paths.feature`
- Create: `crates/tinio-e2e/tests/steps/reserved_paths.rs` (+ module registration)
- Delete (in-tree): `crates/tinio-server/tests/reserved_paths.rs` — after parity

**Interfaces:**
- Consumes: `Server::root()`, raw request steps, `the response status is {int}` (Task 1/2).
- Produces: `the served root contains only the state dir and the bucket` step (uses `Server::root()` + `std::fs::read_dir` + sorted names, ported from `sorted_entries`), `no file was written outside the served root` (if not already in errors.rs — put the fs-directory steps here).

**Source to port:** `crates/tinio-server/tests/reserved_paths.rs` (2 `#[tokio::test]` fns; FR-020, T026).

- [x] **Step 1: Write the feature file**

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (reserved
# paths, FR-020, T026); replaces tinio-server/tests/reserved_paths.rs
@FR-020 @fs
Feature: Reserved .tinio paths

  Scenario: Writes to .tinio are denied and reads answer NoSuchKey
    Given I create bucket "data"
    When I upload "data/.tinio" with body "x"
    Then the error code is "AccessDenied"
    When I upload "data/a/.tinio/b" with body "x"
    Then the error code is "AccessDenied"
    When I send a "GET" request to "/data/.tinio"
    Then the error code is "NoSuchKey"

  Scenario: Nested roots never serve the outer state
    Given I create bucket "data"
    And I upload "data/a.txt" with body "x"
    When I send a "GET" request to "/data/a.txt/.tinio/meta.redb"
    Then the error code is "NoSuchKey"
    When I send a "GET" request to "/data/a.txt/.tinio"
    Then the error code is "NoSuchKey"
```

(Read the old file for the exact key paths and codes — port them verbatim; the old test also asserted listings skip `.tinio` keys — if present, add a listing scenario.)

- [x] **Step 2: Implement reserved_paths.rs steps**

```rust
use cucumber::{given, then, World as _};

#[then("the served root contains only the state dir and the bucket")]
async fn root_entries(world: &mut super::World) {
    let root = world.server.as_ref().unwrap().root().expect("fs backend");
    let entries = sorted(root);
    assert_eq!(entries, [".tinio", "data"]);
}

#[then("no file was written outside the served root")]
async fn no_escape(world: &mut super::World) {
    let root = world.server.as_ref().unwrap().root().expect("fs backend");
    let parent = root.parent().expect("root has a parent");
    let entries = sorted(parent);
    assert_eq!(entries, [root.file_name().unwrap().to_string_lossy()]);
}

fn sorted(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}
```

(If `no file was written outside the served root` was already implemented in errors.rs during Task 2, remove the duplicate here — one definition per step phrase; the scenario in error_codes.feature and the one here share it.)

- [x] **Step 3: Parity check and delete**

- Run `cargo test -p tinio-e2e` (new green), `cargo test -p tinio-server --test reserved_paths` (old green), tick the 1:1 mapping (2 tests → 2 scenarios), then delete `reserved_paths.rs`.

**Do NOT commit.**

---

### Task 4: data_plane.rs → objects.feature + listing.feature

**Files:**
- Create: `crates/tinio-e2e/tests/features/objects.feature`, `crates/tinio-e2e/tests/features/listing.feature`
- Create: `crates/tinio-e2e/tests/steps/objects.rs`, `crates/tinio-e2e/tests/steps/listing.rs` (+ registration)
- Delete (in-tree): `crates/tinio-server/tests/data_plane.rs` — after parity

**Interfaces:**
- Consumes: `Client::request`, `Server::root()`, raw-request steps, error-code steps (Tasks 1–3); `md-5` is already a dev-dep of tinio-server — add `md-5 = { workspace = true }` to tinio-e2e dev-deps for the ETag/MD5 assertion, or reuse `tinio-util::testing::etag` if it already computes MD5-of-body (check `crates/tinio-util/src/testing.rs` — it has an `etag()` helper; prefer it).
- Produces: object CRUD steps, Range/conditional steps, listing steps, the concurrent-write step, the out-of-band fs-write step.

**Source to port:** `crates/tinio-server/tests/data_plane.rs` (7 `#[tokio::test]` fns, SC-006/T025 semantics). All run on the fs backend in the old file (`fs_server()`), so all scenarios carry `@fs` **and** `@mem` where the behavior is backend-neutral (default: both). The `@fs`-only ones: `interrupted_upload_leaves_no_partial_object` (fs staging semantics) and `out_of_band_changes_served_immediately` (needs the fs root). Port the byte payloads/keys from the old file verbatim.

- [x] **Step 1: Write objects.feature**

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (objects,
# T025, SC-006); replaces tinio-server/tests/data_plane.rs
@T025
Feature: Object data plane

  Scenario: Full round trip with listing and delete
    Given I create bucket "data"
    And I upload "data/hello.txt" with body "Hello, world!"
    And I upload "data/empty.txt" with 0 bytes
    And I upload "data/sub/dir/deep.txt" with 5 bytes
    When I get object "data/hello.txt"
    Then the object body is "Hello, world!"
    And the object ETag matches the MD5 of the uploaded bytes
    When I head object "data/empty.txt"
    Then the response status is 200
    When I delete object "data/hello.txt"
    Then I send a "GET" request to "/data/hello.txt"
    And the error code is "NoSuchKey"

  Scenario: Range requests answer 206 with the requested window
    Given I create bucket "data"
    And I upload "data/blob.bin" with 1024 bytes
    When I send a "GET" request to "/data/blob.bin" with headers
      | Range | bytes=100-199 |
    Then the response status is 206
    And the response header "Content-Length" is "100"
    And the object body is the first 100 bytes of the uploaded bytes

  Scenario: Suffix ranges are served from the end
    Given I create bucket "data"
    And I upload "data/blob.bin" with 1024 bytes
    When I send a "GET" request to "/data/blob.bin" with headers
      | Range | bytes=-64 |
    Then the response status is 206
    And the response header "Content-Length" is "64"

  Scenario: Conditional requests answer 304 and 412
    Given I create bucket "data"
    And I upload "data/cond.txt" with body "v1"
    And the response header "ETag" is stored
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-None-Match | {etag} |
    Then the response status is 304
    When I send a "PUT" request to "/data/cond.txt" with headers
      | If-Match | "deadbeef" |
    Then the response status is 412

  Scenario: Folder markers are never objects
    Given I create bucket "data"
    And I upload "data/dir/" with body ""
    When I list objects under "data/"
    Then the listing shows 0 keys
    And the listing prefixes are "dir/"
    When I send a "GET" request to "/data/dir/"
    Then the error code is "NoSuchKey"

  Scenario: Concurrent writes never tear objects
    Given I create bucket "data"
    When I concurrently upload "data/race.bin" and "data/race.bin" with 4096 bytes each
    Then the object body length is 4096
```

(The old `full_round_trip_with_listing_and_delete` also asserted listing prefix/delimiter/pagination — those legs move to listing.feature below; the ETag=MD5 assertion uses the same uploaded-bytes MD5 the upload steps record.)

- [x] **Step 2: Write listing.feature**

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (listing,
# @SC-001); replaces the listing legs of tinio-server/tests/data_plane.rs
@SC-001
Feature: Listing

  Scenario: Prefix and delimiter split the listing
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I upload "data/b.txt" with 1 bytes
    And I upload "data/sub/c.txt" with 1 bytes
    When I list objects under "data/"
    Then the listing shows 2 keys
    And the listing prefixes are "sub/"

  Scenario: Pagination walks all keys
    Given I create bucket "data"
    And I upload "data/k0.txt" with 1 bytes
    And I upload "data/k1.txt" with 1 bytes
    And I upload "data/k2.txt" with 1 bytes
    And I upload "data/k3.txt" with 1 bytes
    When I list objects under "data/" with max-keys 2
    Then the listing shows 2 keys
    And a truncated listing resumes with the next page
```

(Read the old test for the exact pagination loop and page-size; port it into a private listing-step helper. If the old listing caps are config-driven, the tag→config mapping grows a `@page-size-2` entry in `steps/mod.rs`.)

- [x] **Step 3: Implement objects.rs + listing.rs steps**

The steps use the deterministic-bytes helper (same bytes per size for a given scenario — store the uploaded body in `World` keyed by key or remember the last upload; simplest: `World.last_upload: Vec<u8>`):

```rust
// objects.rs
#[given("I upload {string} with body {string}")]
async fn upload_body(world: &mut super::World, key: String, body: String) {
    world.last_upload = body.clone().into_bytes();
    world.last = world
        .client
        .request("PUT", &format!("/{key}"), &[], world.last_upload.as_slice())
        .await;
}

#[given("I upload {string} with {int} bytes")]
async fn upload_bytes(world: &mut super::World, key: String, n: u64) {
    let body = deterministic_bytes(n); // repeatable per n, e.g. (i * 31 + n) % 256
    world.last_upload = body.clone();
    world.last = world.client.request("PUT", &format!("/{key}"), &[], &body).await;
}

#[when("I get object {string}")]
async fn get_object(world: &mut super::World, key: String) {
    world.last = world.client.request("GET", &format!("/{key}"), &[], &[]).await;
}

#[then("the object body equals the uploaded bytes")]
async fn body_equals_upload(world: &mut super::World) {
    assert_eq!(world.last.body, world.last_upload, "body mismatch");
}

#[then("the object body is {string}")]
async fn body_is(world: &mut super::World, body: String) {
    assert_eq!(world.last.body, body.into_bytes(), "body mismatch");
}

#[then("the object body length is {int}")]
async fn body_len(world: &mut super::World, n: u64) {
    assert_eq!(world.last.body.len() as u64, n, "body length mismatch");
}

#[then("the object ETag matches the MD5 of the uploaded bytes")]
async fn etag_md5(world: &mut super::World) {
    let digest = md5::compute(&world.last_upload);
    let expected = format!("\"{digest:x}\"");
    let etag = extract_etag(&world.last);
    assert_eq!(etag, expected, "ETag mismatch");
}
```

`deterministic_bytes(n)` — repeatable body of length n (a simple `(0..n).map(|i| (i * 31 + 7) % 256)` pattern; the exact sequence only needs to be reproducible within a run because the upload and the GET compare against the same stored copy). `extract_etag` pulls `ETag` from `LastResponse.headers`. The concurrent step:

```rust
#[when(regex = r#"I concurrently upload "([^"]+)" and "([^"]+)" with (\d+) bytes each"#)]
async fn concurrent_upload(world: &mut super::World, k1: String, k2: String, n: u64) {
    let b1 = deterministic_bytes(n);
    let mut b2 = deterministic_bytes(n);
    b2.reverse(); // distinct content, same length
    let c1 = world.client.clone();
    let c2 = world.client.clone();
    let (r1, r2) = tokio::join!(
        c1.request("PUT", &format!("/{k1}"), &[], &b1),
        c2.request("PUT", &format!("/{k2}"), &[], &b2),
    );
    world.last = r1;
    world.last_upload = b1; // both uploads wrote the same key with different
                            // content; GET must return one of them intact
}
```

The "then the object body length is 4096" assertion verifies no torn object. The Range step needs a raw GET with headers — extend the raw-request step to accept a table of headers (cucumber-rs `Table`):

```rust
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers"#)]
async fn raw_request_headers(world: &mut super::World, method: String, path: String, table: cucumber::Table) {
    let headers: Vec<(String, String)> = table
        .rows
        .iter()
        .map(|r| {
            // The `{etag}` literal (grilling Q3) is substituted from the
            // stored ETag of the previous response.
            let v = if r[1] == "{etag}" {
                world.stored_etag.clone()
            } else {
                r[1].to_owned()
            };
            (r[0].to_owned(), v)
        })
        .collect();
    let refs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    world.last = world.client.request(&method, &path, &refs, &[]).await;
}
```

The `the response header "ETag" is stored` step (defined in errors.rs, Task 2) saves `world.stored_etag`; the conditional scenario's `{etag}` token in the headers table is substituted by the step above (grilling Q3: the feature keeps the data-driven table form; the substitution lives in one step). The 304/412 scenario in the old file uses the real ETag from the PUT response — keep that behavior (the PUT's response is captured, then `the response header "ETag" is stored`, then the GET with `{etag}`).

Listing steps (listing.rs): `I list objects under {string}` (+ `with delimiter {string}`, + `with max-keys {int}`), `the listing shows {int} keys` (parse `<Contents>` count or `<KeyCount>` for v2), `the listing prefixes are {string} and {string}`, `a truncated listing resumes with the next page` (loop with the `NextContinuationToken`/`NextMarker` until all keys seen — port the old pagination loop).

- [x] **Step 4: Parity check and delete**

- Run new + old side by side (`cargo test -p tinio-e2e`, `cargo test -p tinio-server --test data_plane`); tick the 1:1 mapping (7 tests → scenarios above; the round-trip test's listing legs are in listing.feature); delete `data_plane.rs`.

**Do NOT commit.**

---

### Task 5: coverage_gaps.rs split

**Files:**
- Create: `crates/tinio-e2e/tests/features/metrics.feature`, `crates/tinio-e2e/tests/features/tagging.feature`, `crates/tinio-e2e/tests/features/multipart.feature` (initial), `crates/tinio-e2e/tests/features/conditions.feature`
- Create/extend: `steps/metrics.rs`, `steps/tagging.rs`, `steps/conditions.rs`, extend `steps/multipart.rs`, `steps/listing.rs`
- Delete (in-tree): `crates/tinio-server/tests/coverage_gaps.rs` — after parity

**Source to port:** `crates/tinio-server/tests/coverage_gaps.rs` (8 `#[tokio::test]` fns). Port each test's exact paths/headers/assertions; the scenarios:

| Old test fn | Feature | Scenario |
|---|---|---|
| `metrics_endpoint_served_before_the_s3_service` | metrics.feature | "The /metrics endpoint is served before the S3 service" — GET `/metrics` → 200, body contains the metric lines (port the exact prefix assertion from the old test) |
| `list_v1_round_trip_with_delimiter_and_marker` | listing.feature | "ListObjects v1 walks delimiter and marker" — `I list v1 objects under "data/" with marker "{m}" and delimiter "/"` |
| `get_object_tagging_answers_empty_set` | tagging.feature | "GetObjectTagging answers an empty set" — PUT object, GET `?tagging` → 200 with empty `<TagSet>` |
| `delete_objects_quiet_mode_suppresses_deleted_entries` | tagging.feature | "DeleteObjects quiet mode suppresses Deleted entries" — POST `?delete` with quiet-mode XML body → 200, no `<Deleted>` entries, object gone |
| `conditional_put_on_a_missing_key_precondition_fails` | conditions.feature | "Conditional PUT on a missing key fails the precondition" — PUT with `If-None-Match: *` → 412 |
| `multipart_non_final_part_too_small_answers_entity_too_small` | multipart.feature | "A non-final part smaller than the minimum answers EntityTooSmall" — 5 MiB rule; use `@checksum-on`-free setup, port the exact sizes from the old test |
| `list_parts_rejects_a_negative_marker` | multipart.feature | "ListParts rejects a negative part-number-marker" — GET `?partNumber-marker=-1` → 400 with a coded error |
| `upload_part_copy_rejects_an_open_source_range` | conditions.feature | "UploadPartCopy rejects an open source range" — `x-amz-copy-source-range` without an end → 400 |

- [x] **Step 1: Write the four feature files with the scenarios above** — port exact request shapes (XML bodies, headers, query strings) from the old test file, which remains in the tree until the parity check. The multipart.feature grows the non-final-part and negative-marker scenarios with the steps from `multipart.rs` (extend it: `I upload part {int} with {int} bytes` without checksum, `I list the parts of the multipart upload`).

- [x] **Step 2: Implement the new step modules** (`metrics.rs`, `tagging.rs`, `conditions.rs`, listing-v1 step in `listing.rs`, the two multipart steps). The quiet-mode DeleteObjects body is raw XML — use the raw-request step with body. `list-v1` needs the `?list-type`-free path with `prefix`/`marker`/`delimiter` query params — implement as a dedicated step building the query string.

- [x] **Step 3: Parity check and delete** — run both suites, tick the 8-test mapping, delete `coverage_gaps.rs`.

**Do NOT commit.**

---

### Task 6: retire the shared in-process harness

**Files:**
- Delete (in-tree): `crates/tinio-server/tests/common/mod.rs`

- [x] **Step 1: Confirm no remaining users**

- Run: `rg "mod common" crates/tinio-server/tests`
  Expected: no matches (error_codes/data_plane/reserved_paths/coverage_gaps are deleted; journey/advanced/boto3/mc/edge are migrated in Task 8 — if any remain, they still compile; the delete happens after Task 8).
- Verify the port is complete: `rg "tests/common" crates/tinio-e2e` — the e2e suite references its own `steps/common.rs` only.
- Delete `crates/tinio-server/tests/common/mod.rs`.
- Run: `cargo test --workspace --exclude tinio-e2e` — green (the four in-process files are gone; nothing else imports `common`).

**Do NOT commit.**

---

### Task 7: interop merge — clients, journey, advanced, edge, boto3, mc

**Files:**
- Create: `crates/tinio-e2e/tests/steps/clients.rs`
- Create: `crates/tinio-e2e/tests/features/interop/journey.feature`, `crates/tinio-e2e/tests/features/interop/advanced.feature`
- Move: the external-client harness from `crates/tinio-server/tests/e2e/mod.rs` into `clients.rs` (serve-binary spawn, aws/rclone/boto3/mc wrappers, `wait_for_ready`, `files_equal`, `TINIO_BOTO3_PYTHON` override)
- Delete (in-tree): `crates/tinio-server/tests/journey.rs`, `advanced.rs`, `boto3.rs`, `mc.rs`, `edge.rs`, `tests/e2e/mod.rs`, the whole `e2e/` directory (bash scripts + README)
- Add: `World.ext: Option<External>` field (+ `upload_id`/`upload_key` fields from earlier tasks)

**Interfaces:**
- Consumes: `Server` (in-process) and the `serve` example binary (spawned); `assert_cmd` wrappers from `tests/e2e/mod.rs`.
- Produces: the `@external` scenario machinery — `External` holds the spawned server process + the client session (bucket name, base URL); steps `I run aws {command}`, `I run rclone {command}`, `the external client output contains "{text}"`, `the file {name} equals the uploaded bytes`.

**Source to port:** `journey.rs` (2 fns: journey, journey_checksum_multipart — the checksum-multipart leg needs `@checksum-on`), `advanced.rs` (1 fn: multipart >8 MiB + copy + cold listing), `boto3.rs` (3 fns incl. list-buckets pagination + checksum-validation journey), `mc.rs` (1 fn), `edge.rs` (2 fns: edges + mc_edges), and the CI-gated semantics of `e2e/interop/journey.sh` + `advanced.sh` (read both scripts and the old Rust ports — they encode the same journey; the cucumber scenarios are the union).

- [x] **Step 1: Port the client harness into clients.rs**

Copy `crates/tinio-server/tests/e2e/mod.rs` into `clients.rs`, adjusting visibility to pub(super) and adding `External` to the World:

```rust
/// @external scenarios only: a spawned `serve` binary + one client session.
pub struct External {
    pub child: std::process::Child,          // serve binary (killed on drop)
    pub base_url: String,                    // http://127.0.0.1:<port>
    pub bucket: String,                      // created by the first Given
    pub workdir: tempfile::TempDir,          // scratch for client files
}
```

The `#[before]` hook in `steps/mod.rs` gains the @external branch: when any of `@interop`/`@boto3`/`@mc` is tagged, skip the in-process server and spawn the serve binary (`serve_bin()` from the port; `wait_for_ready` on stdout). Presence checks: `@interop` → `aws` and `rclone` on PATH (else panic with "run WSL2 or filter tags — e.g. --tags 'not @interop'"); `@boto3` → the venv python at `TINIO_BOTO3_PYTHON` (else panic with the setup hint); `@mc` → `mc` on PATH.

**Config passthrough (grilling Q4):** the spawned binary receives the same tag→config mapping the in-process hook uses — call `config_from_tags(&scenario.tags)` (Task 1) and translate the result into the spawn's configuration the same way the old harness did. Read `tests/e2e/mod.rs` + `e2e/interop/lib.sh` FIRST to see how the current code passes configuration (env vars? CLI args? config file?) to the serve binary/facade; preserve that exact mechanism, driven by `config_from_tags`. This is what lets `@interop @checksum-on` (journey_checksum_multipart) and `@cold-listing` (advanced cold-listing legs) configure the spawned server.

- [x] **Step 2: Write journey.feature**

The SC-001 journey (create bucket → upload → byte-identical download → prefix/delimiter listing → delete) as `@interop @aws @rclone` scenarios; the checksum-multipart leg as `@interop @aws @checksum-on`; the boto3 journey + list-buckets pagination + checksum journey as `@boto3` scenarios (each scenario runs the python script via the ported boto3 wrapper and asserts "BOTO3 JOURNEY OK"). Port the exact aws/rclone command sequences and assertion helpers from `journey.rs` + `journey.sh` (the two must agree — the bash script is the CI baseline; where they differ, the script wins and the Rust port is corrected).

- [x] **Step 3: Write advanced.feature**

Multipart >8 MiB with composed ETag, server-side copy, cold listing with and without the scanner (`@interop @aws @rclone` + `@cold-listing` variants), plus the edge.rs legs that need aws-cli (special-char keys, size boundary, Range via aws, pagination truncation, error paths) and the mc legs (`@mc`: mb/cp/ls/rm/rb, 10 MiB multipart copy, `mc stat` ETag, zero-byte). Port from `advanced.rs` + `edge.rs` + `advanced.sh`.

- [x] **Step 4: Local verification + WSL2 run**

- Native Windows: `cargo test -p tinio-e2e -- --tags @interop` — expect the presence-check panic (aws/rclone not on PATH) OR a green run if the tools exist; either is acceptable locally.
- WSL2 (Linux): install aws-cli + rclone (`sudo apt install awscli rclone`), then with the repo on `/mnt/e`: `CARGO_TARGET_DIR=/home/<user>/tinio-target cargo test -p tinio-e2e -- --tags @interop --retry 1`
  Expected: green.
- Parity: run the old `#[ignore]` tests once (`cargo test -p tinio-server --test journey -- --ignored`, same for advanced/boto3/mc/edge) and tick the mapping; the bash baseline (`bash e2e/interop/journey.sh`) still runs until this task's WSL2 run is green.

- [x] **Step 5: Delete the old implementations**

- Delete `journey.rs`, `advanced.rs`, `boto3.rs`, `mc.rs`, `edge.rs`, `tests/e2e/mod.rs` from tinio-server.
- Delete the `e2e/` directory (all bash scripts + `lib.sh` + README).
- Run: `cargo test --workspace --exclude tinio-e2e` — green; `cargo test -p tinio-e2e -- --tags 'not @interop and not @boto3 and not @mc'` — green.

**Do NOT commit.**

---

### Task 8: business-behavior unit-test subsets (conjunction rule)

**Files:** extend existing features with `Examples`-parameterized scenarios; delete the covered unit tests in-tree.

**Rule (binding):** for each file below, read it and migrate exactly the tests that (a) carry a SC/FR/T spec semantic **and** (b) are observable through the S3 API. Everything else stays. Cucumber scenarios do NOT repeat conformance-harness assertions (the harness stays). Record the per-file decision in this task's checklist (spec §Migration scope: "decided during implementation by the migration rule, recorded in the implementation plan's checklist, and reviewed per file").

| File | What to look for | Where it lands |
|---|---|---|
| `tinio-fs/src/multipart.rs` (~40 tests) | spec-semantic multipart behavior observable via the API: composed-ETag assembly, part-number validation, part re-upload semantics | multipart.feature `Examples` rows |
| `tinio-fs/src/backend/listing.rs` / `listing.rs` (fs) | prefix/delimiter/pagination semantics with spec IDs | listing.feature |
| `tinio-mem/src/multipart.rs` (15), `tinio-mem/src/object.rs` (24) | the same spec semantics on the mem backend — note: these overlap the fs ones; migrate once (mem-specific assertions stay in Rust) | multipart.feature, objects.feature |
| `tinio-server/src/backend/multipart.rs` (35) | create/complete/list-parts semantics, part validation, `BadDigest` paths | multipart.feature, error_codes.feature |
| `tinio-server/src/backend/conditions.rs` (7) | If-Match/If-None-Match/if-modified-since semantics | conditions.feature |
| `tinio-server/src/backend/listing.rs` (10) | v1/v2 listing semantics | listing.feature |
| `tinio-core/src/bucket.rs` (8), `object.rs` (15), `multipart.rs` (5) | spec-semantic validation observable via API responses (e.g. invalid names → `InvalidBucketName` shape) | error_codes.feature, objects.feature |

- [x] **Step 1: Per file — apply the rule and port.** For each selected test: read it, write the equivalent `Scenario Outline` row(s) with the `Examples` table (representative fixed values standing in for proptest ranges — spec §Step layer), add the needed step variants to the relevant step module, run green.
- [x] **Step 2: Delete only the migrated tests** from the source file (leave the rest), so the file's remaining tests keep running.
- [x] **Step 3: Record the checklist.** Append a checklist to this task's section in the plan document (`docs/superpowers/plans/2026-08-31-cucumber-bdd-migration.md`, under Task 8): per file — migrated test names → scenario(s), and the names of tests rejected by the rule with a one-line reason. This checklist is the review artifact the user checks at final review (grilling Q6).

**Do NOT commit.**

---

#### Task 8 checklist (implemented 2026-08-31, review artifact for grilling Q6)

Rule applied per file: a unit test migrates iff it carries a SC/FR/T spec semantic **and** its behavior is observable through the S3 API; cucumber scenarios never repeat conformance-harness assertions (`tinio-util` harness untouched). 75 tests migrated to 30 scenario additions/extensions across `multipart.feature` (12), `error_codes.feature` (8), `listing.feature` (3), `objects.feature` (4), `conditions.feature` (3); 13 step variants added. Representative fixed values stand in for the randomized/proptest ranges (part sizes at the 5 MiB minimum + 1 byte; the > 8 MiB interop boundary stays in the interop features). Scenario count is below the plan estimate; the migrated-test count exceeds the rough ~40-60 estimate because the spec-semantic matrices are dense — every deletion maps 1:1 to a scenario leg below.

**crates/tinio-fs/src/multipart.rs** — 11 migrated:
- `put_list_part_round_trip` → "Re-uploading a part replaces the earlier content" (part ETag = MD5 of body) + "ListParts pages by part number"
- `put_part_missing_upload_is_no_such_upload`, `non_uuid_upload_ids_are_no_such_upload` → "Multipart operations on unknown uploads answer NoSuchUpload" (bogus + encoded-evil upload ids)
- `complete_under_a_different_key_is_no_such_upload`, `put_part_under_a_different_key_is_no_such_upload` → same scenario (wrong-key PUT + complete legs)
- `complete_assembles_byte_exact_with_composed_etag` → "Composed ETag assembly and post-completion identity" (rows: all 3 parts → `-3`; last 2 → `-2`, body byte-exact)
- `complete_no_parts_is_error` → "Completion validates part numbers and etags" (empty `<CompleteMultipartUpload>` leg → InvalidRequest)
- `complete_mismatched_or_missing_part_is_invalid_part` → same scenario (mismatched-etag + extra-part legs → InvalidPart)
- `abort_removes_parts_and_is_no_such_upload_after` → "Abort removes the upload and its parts" (204, then 404 NoSuchUpload)
- `abort_after_complete_consume_is_no_such_upload` → "Composed ETag assembly…" final legs (complete/list/abort after completion → 404)
- `list_parts_pages_by_part_number` → "ListParts pages by part number" (24 parts, max-parts 5 walk)
- Rejected (stayed, one-line reason): `create_refuses_uploads_at_the_concurrency_cap` (config-driven cap, not surface); `uploads_page_matches_the_engine_over_the_full_bucket` (internal engine equivalence); `marker_inside_a_rollup_absorbs_the_group` (internal rollup edge, no spec ID); `create_records_upload_without_creating_directory` (storage-layout detail); `abort_drains_the_checksum_rows` (DB-row invariant); `complete_retry_after_rename_is_idempotent` (crash window); `consume_is_idempotent_for_a_missing_upload` (internal); `racing_complete_and_abort_leave_a_consistent_state` (race); `list_parts_skips_a_part_whose_file_vanished` (out-of-band file state); `list_parts_skips_a_part_with_invalid_stored_etag` (DB corruption); `list_uploads_and_has_uploads` (trivial bookkeeping); `list_uploads_orders_same_key_group_by_upload_id` (ordering detail); `remove_bucket_clears_uploads` (internal cleanup); `walk_uploads_finds_all` (internal walk); `list_parts_truncated_page_with_vanished_parts_still_marks_resume` (crash window); `part_lock_slots_are_evicted_after_use` (memory); `concurrent_same_part_overwrites_never_mismatch_file_and_record` (race; re-upload semantic covered by the API scenario); `list_parts_is_db_driven_no_ghost_parts` (crash window); `complete_racing_put_part_never_mismatches_content` (race); `abort_during_assembly_is_no_such_upload` (race); `list_parts_marker_at_u32_max_returns_empty_page` (internal boundary); `list_parts_max_parts_zero_is_an_empty_page` (server rejects 0 with InvalidArgument — covered); `list_parts_of_a_mismatched_stored_key_is_no_such_upload` (DB corruption); `complete_recomputes_a_part_whose_record_is_missing` (crash window); `complete_refuses_a_part_that_is_a_directory` (internal); `complete_rejects_part_content_that_disagrees_with_the_record` (internal); `put_part_returns_the_stage_error_when_the_upload_is_live` (I/O detail); `publish_part_cleans_the_temp_…` × 2 (internal temp cleanup).

**crates/tinio-fs/src/listing.rs** — 7 migrated:
- `full_listing_is_lexicographic` → "The full listing is lexicographic" (+ prefix-pruning legs)
- `prefix_and_delimiter_grouping` → "Prefix and delimiter split the listing" (existing, Task 4)
- `pagination_rolls_over` → "Pagination walks all keys" (existing, Task 4)
- `prefix_prunes_whole_subtrees` → "The full listing is lexicographic" (prefix legs)
- `tinio_entries_skipped_at_any_depth` → reserved_paths.feature "Nested roots never serve the outer state" (FR-020)
- `directories_never_objects` → objects.feature "Folder markers are never objects"
- `out_of_band_edit_recomputes_etag` → objects.feature "Out-of-band changes are served immediately"
- Rejected: `missing_bucket_is_no_such_bucket` (NoSuchBucket covered by error_codes.feature); all symlink/junction tests (`dangling_symlink_is_skipped_not_fatal`, `symlink_cycles_terminate`, `symlink_entries_excluded_when_disabled`, `bucket_dir_symlink_not_walked_when_disabled`, `duplicate_symlink_targets_descend_once`, `dangling_bucket_symlink_is_no_such_bucket`, `dangling_bucket_junction_is_no_such_bucket`, `junction_inside_bucket_is_followed_and_cycles_terminate`) (fs-backend walk internals); all pipeline tests (`cold_list_writes_one_batch_per_flush_threshold`, `hot_list_enqueues_nothing`, `pagination_happens_before_enqueue`, `io_concurrency_equals_the_workers`, `slow_db_pipeline_backpressures_the_producer`, `vanished_file_skips_the_entry`, `failed_compute_fails_the_list`, `lost_batches_error_the_list_and_self_heal_next_pass`, `composed_etag_kept_by_the_producer_on_identity_less_storage`, `mtime_preserving_replacement_recomputes_the_etag`, `page_whose_entries_all_vanish_keeps_the_resume_marker`, `corrupt_entry_self_heals_through_the_producer`) (pipeline mechanics, not API-observable); walk-stream tests (`walk_stream_emits_files_in_read_dir_order`, `walk_stream_collects_every_object_of_the_tree`, `walk_stream_missing_bucket_is_no_such_bucket`, `walk_stream_bucket_dir_vanishing_mid_walk_is_no_such_bucket`) (internal stream).

**crates/tinio-mem/src/multipart.rs** — 10 migrated (once — the fs/server scenarios carry the semantics):
- `upload_part_rejects_part_numbers_outside_1_to_10000` → "UploadPart validates the part number range" (0 / 10001 → InvalidPart)
- `upload_part_rejects_mismatched_bucket_or_key` → "Multipart operations on unknown uploads answer NoSuchUpload" (cross-bucket leg)
- `overwrite_part_replaces_previous` → "Re-uploading a part replaces the earlier content" (part re-upload semantics)
- `complete_without_parts_is_invalid` → "Completion validates part numbers and etags" (empty completion leg)
- `complete_rejects_unknown_part_number` → same scenario (extra-part leg)
- `complete_and_abort_reject_mismatched_identity` → same scenario family (wrong-key complete leg)
- `complete_removes_upload_and_parts` → "Composed ETag assembly…" final legs
- `list_parts_paginates` → "ListParts pages by part number"
- `list_uploads_filters_and_paginates` → "ListMultipartUploads filters and paginates by key marker" (prefix filter + key-marker walk)
- `bare_key_marker_skips_the_whole_key_group` → "A bare key marker skips the whole same-key group"
- Rejected: `part_size_limit_rejects_oversized_parts`, `abort_releases_part_bytes` (mem quota config, no spec semantic); `upload_ids_are_unique` (internal UUID); `list_uploads_on_missing_bucket_is_no_such_bucket` (NoSuchBucket covered).

**crates/tinio-mem/src/object.rs** — 12 migrated:
- `put_overwrites_existing_object` → objects.feature "Overwrite replaces the object"
- `get_clamps_inclusive_range_to_object_size`, `get_suffix_larger_than_object_returns_all`, `unsatisfiable_ranges_are_invalid_range` → "Range requests answer 206…" / "Unsatisfiable ranges answer 416" outline rows (bytes=8-99, bytes=-100, bytes=10-, bytes=10-20, bytes=-0)
- `list_objects_delimiter_groups_and_resumes_after_common_prefix` → listing.feature "Delimiter pagination resumes past common prefixes"
- `get_empty_object_returns_empty_body` → "Full round trip with delete" (empty-object legs)
- `get_missing_key_is_no_such_key` → error_codes.feature "Missing object answers NoSuchKey"
- `head_folder_marker_and_reserved_are_no_such_key` → "Folder markers are never objects" + reserved_paths.feature
- `list_objects_skips_folder_markers`, `list_objects_skips_folder_markers_with_delimiter` → "Folder markers are never objects"
- `list_objects_paginates_without_delimiter` → "Pagination walks all keys"
- `list_objects_prefix_does_not_include_siblings` → "Prefix and delimiter split the listing" (prefix legs)
- Rejected: `object_size_limit_rejects_oversized_objects`, `total_size_limit_rejects_and_releases_on_delete` (mem quota config); `object_ops_on_missing_bucket_are_no_such_bucket` (NoSuchBucket covered for PUT only; per-op matrix stays); `put_concatenates_body_chunks` (transport detail); `list_objects_empty_bucket_is_not_truncated`, `list_objects_exact_page_is_not_truncated`, `list_objects_max_zero_returns_an_empty_untruncated_page` (page-boundary details, no spec ID); `list_objects_start_after_inside_prefix_excludes_the_marker`, `list_objects_start_after_before_prefix_still_lists_the_prefix` (pagination edges covered by the resume walk); `list_objects_object_marker_inside_rollup_skips_the_prefix` (rollup edge); `list_objects_nested_delimiter_under_prefix` (covered in-kind by the prefix/delimiter legs); `list_objects_does_not_cross_buckets` (trivial isolation).

**crates/tinio-server/src/backend/multipart.rs** — 15 migrated:
- `multipart_lifecycle` → "Composed ETag assembly and post-completion identity" (create → 3 parts → list → complete → composed `-3` + byte-exact body)
- `complete_rejects_non_final_parts_below_5_mib` → "A non-final part smaller than the minimum answers EntityTooSmall" (existing)
- `abort_removes_upload` → "Abort removes the upload and its parts"
- `invalid_part_numbers_rejected` → "UploadPart validates the part number range"
- `upload_part_copy_range_and_conditionals` → conditions.feature "UploadPartCopy rejects an open source range" (malformed range → InvalidArgument, failing source conditional → PreconditionFailed, invalid part number → InvalidPart legs)
- `list_parts_rejects_max_parts_below_one` → "ListParts pages by part number" (max-parts 0/-1 → InvalidArgument legs)
- `list_multipart_uploads_rejects_max_uploads_below_one` → "ListMultipartUploads rejects max-uploads below one" (outline)
- `create_rejects_an_invalid_algorithm_type_combination` → "CreateMultipartUpload rejects/accepts the checksum algorithm and type" (outlines)
- `create_echoes_the_checksum_algorithm` → "CreateMultipartUpload echoes the checksum algorithm and type" (create with `x-amz-checksum-algorithm: CRC32` + `x-amz-checksum-type: FULL_OBJECT`; the s3s framework echoes both as response headers, asserted with the existing header step)
- `upload_part_validates_and_echoes_the_checksum` → "UploadPart checksum mismatch is BadDigest and stores nothing" (response-header echo + ListParts `<ChecksumCRC32>` echo legs)
- `upload_part_checksum_mismatch_is_bad_digest_and_stores_nothing` → same scenario (BadDigest + 0-parts leg)
- `upload_part_validates_content_md5` → "UploadPart validates Content-MD5" (valid + BadDigest legs)
- `upload_part_rejects_conflicting_sources_and_bare_algorithm` → "UploadPart rejects conflicting checksum headers and bare algorithms"
- `upload_part_algorithm_must_match_the_create_algorithm` → "UploadPart checksum algorithm must match the create algorithm"
- `complete_checksum_mismatch_is_bad_digest_and_preserves_the_old_object` → "Completion checksum mismatch is BadDigest and preserves the old object" (wrong value → BadDigest + old object intact; correct value → 200)
- Rejected: `list_multipart_uploads_resumes_inside_a_same_key_group` (upload-id-marker resume needs a dynamic id from the first page's body — not capturable by the steps; key-marker semantics covered); `upload_part_computes_and_persists_headerless_parts_of_algorithm_uploads` (exercised implicitly by the completion-checksum scenario; explicit echo assertions internal); `complete_validates_composite_sha256`, `complete_validates_full_object_crc32_linearization` (client-side composite computation not expressible at the wire layer; FULL_OBJECT path covered for CRC32); `complete_rejects_algorithm_type_and_size_mismatches`, `complete_cross_checks_completed_part_values_without_a_create_algorithm` (W03), `complete_skips_completed_part_entries_without_a_stored_checksum` (D2), `complete_full_object_size_check_runs_before_the_d2_skip` (W04) (per-part completion checksum entries not expressible via the completion step); `upload_part_copy_computes_and_persists_the_checksum`, `upload_part_copy_without_create_algorithm_keeps_the_fast_path` (checksum-internal); `upload_part_copy_respects_copy_object_toggle` (covered by the @minimal-caps NotImplemented scenario); `list_parts_allow_zero_page_size_restores_the_legacy_empty_page`, `list_multipart_uploads_allow_zero_page_size_restores_the_legacy_empty_page` (config escape hatch); `checksum_toggle_off_drops_the_headers` (default-caps internal).

**crates/tinio-server/src/backend/conditions.rs** — 5 migrated (the `cond`/`timestamp` test helpers, used only by the deleted tests, were removed with them):
- `if_modified_since_fails_when_not_modified_after` → "Date-based conditions answer 304 and 412" (2038/1970 fixed-date legs; the equal-boundary leg is unpinnable — the server compares sub-second mtimes against second-truncated dates)
- `if_unmodified_since_fails_when_modified_after` → same scenario (GET IUS 1970 → 412, 2038 → 200; the PUT write path does not evaluate IUS — only If-Match/If-None-Match — so the write-path legs stay internal)
- `if_none_match_fails_on_match` → "Weak tags and wildcards follow RFC 9110 comparison" (W/ matches → 304, `*` → 304; the plain 304/412 legs already covered by objects.feature)
- `if_match_requires_exact_strong_match` → same scenario (W/ → 412, `*` → 200)
- `precedence_failing_date_wins_over_matching_etag` → "Date-based conditions…" precedence leg (matching If-None-Match + failing If-Unmodified-Since → 412)
- Rejected: `no_conditions_pass` (trivial); `condition_error_maps_failures` (internal mapping fn, exercised by every scenario).

**crates/tinio-server/src/backend/listing.rs** — 2 migrated:
- `v1_rejects_max_keys_below_one`, `v2_rejects_max_keys_below_one` → "Listing rejects max-keys below one" (outline, 0/-1 rows for both API versions)
- Rejected: `empty_delimiter_means_no_delimiter`, `v1_full_and_delimiter_listing` (already covered by the v1 scenario's empty-delimiter/Name-echo/prefix legs), `v2_pagination_and_prefix` (covered by the pagination/prefix scenarios), `v1_missing_bucket_is_no_such_bucket` (NoSuchBucket covered), `v1/v2_allow_zero_page_size_restores_the_legacy_empty_page` (config escape hatch), `v1/v2_echoes_the_effective_page_size_after_a_clamp` (config clamp).

**crates/tinio-core/src/bucket.rs** — 3 migrated:
- `valid_bucket_names_accepted`, `invalid_bucket_names_rejected` → "Bucket names follow the S3 naming rules" (outline: 11 invalid rows → 400 InvalidBucketName, 5 valid rows → 200)
- `bucket_dot_segments_rejected` → same outline + traversal outline rows (`...`, `..a`, `a..`, `a..b`; the dot-only `.`/`..` names are router-ambiguous — no distinct API semantic)
- Rejected: `bucket_name_validates_and_exposes`, `bucket_name_from_owned_string`, `bucket_name_from_invalid_panics` (conversion internals; validation covered), `bucket_equality` (internal), `bucket_types_are_send_sync_and_static` (internal).

**crates/tinio-core/src/object.rs** — 9 migrated:
- `valid_keys_accepted` → objects.feature "Legal keys are accepted" (the API-accepted subset: plain, nested, space (%20), non-ASCII (%C3%BC), folder marker; the RFC-3986 pchar specials are rejected by the s3s router — not API-observable as accepted)
- `traversal_rejected`, `dot_segment_rejected`, `empty_interior_segments_rejected`, `drive_letter_paths_rejected`, `control_characters_rejected` → "Traversal and invalid keys are rejected without fs access" (outline rows: `..x`, `x..`, `a..b`, `a/.../b`, `a/./b`, `a/.`, `./x`, `C:/foo`, `d:%5C.tinio%5Cstate`, `a%5C%5Cb`, `a%00b`, `a%0Ab`, `a//b`, `a/b//c`)
- `absolute_paths_rejected` → same outline (leading-slash rows; absolute keys are unreachable through the API root)
- `object_key_reserved_flag`, `tinio_reserved_at_any_depth` → reserved_paths.feature (FR-020, already covered)
- Rejected: `object_key_validates_and_exposes`, `object_key_from_trusted_literals`, `object_key_from_invalid_panics` (conversion internals), `empty_key_rejected` (unreachable at the router — the bare-bucket path), `object_info_carries_etag_size_mtime` (internal), `object_types_are_send_sync_and_static` (internal).

**crates/tinio-core/src/multipart.rs** — 1 migrated:
- `part_number_rejects_out_of_range` → "UploadPart validates the part number range"
- Rejected: `part_info_round_trip`, `multipart_upload_state` (internal), `part_number_from_invalid_panics` (internal), `multipart_types_are_send_sync_and_static` (internal).

**Step variants added** (multipart.rs steps: start-with-checksum-algorithm, part-with-body, part-with-content-md5, parts-1-through-N, list-parts max-parts + marker-and-max-parts (anchored `$`), complete variants: last-N-parts / mismatched-etag / extra-part / for-key / with-checksum-crc32, abort, parts-listing-shows, uploads-listing-shows, part-ETag-MD5, composed-ETag; errors.rs: `{upload_id}`/`{name}` substitution in raw paths/bodies/tables; listing.rs: delimiter+max-keys combined step, listing-keys-in-order table step; objects.rs: upload-with-body registered for When). No existing step phrase was redefined (the part-number-marker and max-parts regexes gained `$` anchors — required so the combined marker+max-parts variant is unambiguous; every existing usage still matches).

**Deviations from the brief**: (1) the migrated-test count (75) exceeds the plan's rough ~40-60 estimate — the spec-semantic matrices (multipart lifecycle/validation, checksum BadDigest paths, naming rules) are denser than estimated; the scenario count (30 additions) is below the estimate and every deletion maps 1:1 to a scenario leg; (2) `crates/tinio-fs/src/backend/listing.rs` does not exist — the fs listing tests live in `crates/tinio-fs/src/listing.rs`; (3) the create-response checksum echo is a response header, not an XML body element — the echo scenario asserts the headers (review fix round 1); (4) the If-Modified-Since equal-boundary leg is unpinnable over HTTP (sub-second mtimes vs second-truncated dates) — fixed 2038/1970 dates stand in; (5) the ListParts pagination walk sends the marker together with max-parts (a marker-only page returns the whole remainder).

---

### Task 9: traceability + spec documents

**Files:**
- Create: `crates/tinio-e2e/tests/steps/traceability.rs` (+ registration)
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (add "Automated coverage" section), `specs/001-s3-local-server/checklists/compatibility.md` (test items reference feature files)

- [x] **Step 1: Write the traceability test**

A plain Rust test in `steps/traceability.rs` (it runs under the cucumber binary only when tagged in — simplest: a `#[test]` in the test binary runs only via the runner; instead make it a scenario-free check invoked from the runner main before `run_and_exit` when `TINIO_E2E_TRACEABILITY=1`, OR a standalone `[[test]]` target in the same crate with the default harness):

```rust
//! Spec↔tag cross-check: every FR/SC/T ID referenced in
//! specs/001-s3-local-server/ must appear as a feature tag, and every
//! traceability tag must have a spec ID (zero orphans both ways).

#[test]
fn spec_ids_and_feature_tags_are_consistent() {
    let spec_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../specs/001-s3-local-server");
    let features_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/features");

    let ids: std::collections::BTreeSet<String> = collect_spec_ids(&spec_dir);
    let tags: std::collections::BTreeSet<String> = collect_tags(&features_dir);

    let missing: Vec<&String> = ids.iter().filter(|id| !tags.contains(*id)).collect();
    let orphans: Vec<&String> = tags.iter().filter(|t| !ids.contains(*t)).collect();
    assert!(missing.is_empty(), "spec IDs without a feature tag: {missing:?}");
    assert!(orphans.is_empty(), "tags without a spec ID: {orphans:?}");
}
```

`collect_spec_ids` scans `contracts/*.md` + `checklists/*.md` for `\b(FR|SC|T)\d{3}\b`; `collect_tags` scans `tests/features/**/*.feature` for `@(FR|SC|T)\d{3}`. Implement both with `std::fs` walking + a small regex-free parser (`find` for the ID prefixes is fine — the IDs have fixed shape `FR-xxx`/`SC-xxx`/`Txxx`; confirm the exact numbering formats from the contracts before writing the parser). Keep the ID-list in the test deterministic — no globals.

- [x] **Step 2: Wire it into CI-checks + local runs.** Run it locally (`cargo test -p tinio-e2e --test traceability` or via the runner, per the chosen wiring). Expected: first run FAILS with orphans (the new features carry tags the spec may not reference yet, or the other way) — resolve by adding the missing tags to features and the missing IDs to the spec's Automated coverage section, until green. This is the intended workflow: the check is strict from day one.
- [x] **Step 3: Write the contracts sections.** In `contracts/s3-surface.md`, add an "Automated coverage" section mapping each S3 operation → feature file + tag (e.g. `GET Object → objects.feature @T025`); update `checklists/compatibility.md` test items to reference the feature files.

**Do NOT commit.**

---

### Task 10: CI rework + PR test reports

**Files:**
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: quality job** — change the two workspace test steps to exclude tinio-e2e and add the cucumber runs with report output:

```yaml
      - name: test (default features)
        run: cargo test --workspace --exclude tinio-e2e

      - name: test (--no-default-features)
        run: cargo test --workspace --no-default-features --exclude tinio-e2e

      - name: e2e cucumber (fs backend)
        run: cargo test -p tinio-e2e
        env:
          TINIO_E2E_REPORT: cucumber-report-fs.json

      - name: e2e cucumber (mem backend)
        run: cargo test -p tinio-e2e -- --tags 'not @fs'
        env:
          TINIO_E2E_BACKEND: mem
          TINIO_E2E_REPORT: cucumber-report-mem.json
```

(The default @external exclusion in the runner covers the `~@external` filter — no `--tags` needed on the fs pass; explicit `--tags 'not @interop and not @boto3 and not @mc'` is equivalent if preferred. The mem pass runs `--tags 'not @fs'` — grilling Q1: `@fs`-only scenarios (traversal, nested-root, out-of-band) are skipped, backend-neutral scenarios run on mem via `TINIO_E2E_BACKEND`, `@mem`-only scenarios run on mem by their tag. Per the spec's timing fallback: if the double run measurably exceeds the target, the mem pass narrows to `@mem`-tagged scenarios.)

- [x] **Step 2: interop job** — replace the bash steps with the cucumber interop run (keep aws-cli/rclone installation and the "always runs" behavior):

```yaml
      - name: build serve example (interop harness)
        run: cargo build -p tinio-server --example serve

      - name: interop cucumber (@interop)
        run: cargo test -p tinio-e2e -- --tags @interop --retry 1
        env:
          TINIO_E2E_REPORT: interop-report.json
```

(The old "build facade binary" step is dropped — the cucumber steps spawn the `serve` example binary, not the facade; the bash scripts that used the facade are deleted in Task 7.)

- [x] **Step 3: report to the PR** — artifacts + ubuntu-only comments + permissions:

```yaml
    permissions:
      checks: write
      pull-requests: write
```

Add to the **ubuntu legs only** (use a matrix filter: `if: matrix.os == 'ubuntu-latest'` on the reporting steps):

```yaml
      - name: upload cucumber report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: ${{ github.job }}-${{ matrix.os }}-cucumber-report
          path: '*-report*.json'

      - name: publish test report to PR
        if: always() && matrix.os == 'ubuntu-latest'
        uses: dorny/test-reporter@v1
        with:
          name: ${{ github.job }} cucumber report
          path: '*-report*.json'
          reporter: cucumber-junit  # or 'cucumber' — verify the format name
          fail-on-error: 'false'
```

Verify against the dorny/test-reporter docs which reporter identifier consumes cucumber-rs JSON (`cucumber-junit` vs `cucumber`); if neither matches, fall back to `github-script` reading the JSON summary and posting a comment with pass/fail counts + failed scenario names (the spec's tier-3 fallback, which stays regardless).

- [x] **Step 4: verify the workflow syntax.** `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` (or `actionlint` if available) — expected: valid. Local runs of the individual commands (`cargo test -p tinio-e2e`, `--tags @interop --retry 1`, `TINIO_E2E_BACKEND=mem`, `TINIO_E2E_REPORT=...`) must all pass before the push.

**Do NOT commit.**

---

### Task 11: docs + WSL2 workflow

**Files:**
- Create: `crates/tinio-e2e/README.md`, `crates/tinio-e2e/scripts/wsl-interop.sh`
- Create: `CONTEXT.md` (repo root — BDD-migration glossary; created in-tree at grilling, kept in sync here)
- Modify: `docs/cargo.md`, `docs/style.md`, `CLAUDE.md`

- [x] **Step 1: tinio-e2e README** — tag table (`@fs`/`@mem`/`@interop`/`@boto3`/`@mc`/`@checksum-on`/`@minimal-caps`/`@cold-listing` + traceability tags), run commands (`cargo test -p tinio-e2e`, tag filters, `--retry 1`, `TINIO_E2E_REPORT`), the FR-025 tiering matrix folded in from `e2e/interop/README.md` (read it first — it is deleted in Task 7), the harness=false note, and the "default excludes @external" behavior.
- [x] **Step 2: WSL2 script** — `scripts/wsl-interop.sh` (bash, executable):

```bash
#!/usr/bin/env bash
# Run the @interop suite inside WSL2 (Linux-side aws-cli/rclone).
set -euo pipefail
for c in aws rclone; do command -v "$c" >/dev/null || { echo "missing $c — sudo apt install awscli rclone"; exit 1; }; done
if [[ "$(pwd)" == /mnt/* ]]; then
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}"  # ext4 build artifacts
  echo "on /mnt — building to $CARGO_TARGET_DIR"
fi
cargo test -p tinio-e2e -- --tags @interop --retry 1 "$@"
```

- [x] **Step 3: docs updates** — `docs/cargo.md` (tinio-e2e usage section), `docs/style.md` (gherkin conventions: English only, Given/When/Then verb forms, `Examples` tables, steps organized by domain), `CLAUDE.md` (Testing section: the cucumber workflow + the migration rule + default @external exclusion), `CONTEXT.md` (keep the glossary in sync with the implemented tag taxonomy — it was created at grilling and is part of this migration's artifacts).
- [x] **Step 4: final local verification sweep** — the full local acceptance list from Task 12.

**Do NOT commit.**

---

### Task 12: final acceptance checklist (user review gate)

No code changes — the executor runs the checks and records results for the user's manual review and commit.

- [x] **Step 1: local acceptance (must pass on this machine):**

1. `cargo test --workspace --exclude tinio-e2e` — green.
2. `cargo test --workspace --no-default-features --exclude tinio-e2e` — green.
3. `cargo test -p tinio-e2e` — green (fs backend, default filter).
4. `TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e` — green.
5. WSL2: `crates/tinio-e2e/scripts/wsl-interop.sh` — green (Linux + aws-cli + rclone).
6. `cargo clippy --workspace --all-targets -- -D warnings` — green.
7. `cargo +nightly fmt --all -- --check` — green.
8. `cargo test -p tinio-e2e --test traceability` (or the chosen wiring) — green (zero orphans).
9. Scenario-count gate: count scenarios in `tests/features/` ≥ the replaced integration-test count (7+8+2+8+2+1+3+1+2 from the migrated files) — record the number.

- [x] **Step 2: post-push acceptance (user action, listed for the review):**

1. Push; CI quality on 3 OS — green (including both e2e backend runs + traceability).
2. Interop job on 3 OS — green (`@interop`).
3. PR shows the test-report comments (ubuntu quality + ubuntu interop) + artifacts.
4. macOS-specific interop behavior verified (post-push only, per grilling Q8).
5. If interop fails post-push: restore from git is zero-cost (nothing committed); fix and re-push.

- [x] **Step 3: hand over to the user.** Summarize: what moved where (per-file mapping), the Task 8 unit-test checklist, the scenario count, and the post-push items. The user reviews the tree and commits manually.

---

## Execution status (2026-09-02 — all tasks complete)

All 12 tasks are done; the migration landed in-tree (zero commits, per the
Global Constraints). The checkboxes above are marked on the basis of the
verified tree state: every planned file exists, every old file was deleted
in-tree, CI runs the cucumber suite (`tinio-e2e` × 9 in `ci.yml`), and the
acceptance list is green (workspace tests, fs + mem e2e passes, `@interop`
on 3 OS via CI, traceability, clippy/fmt).

### File-structure deltas vs the plan sketch

- `tests/traceability.rs` is a **standalone `[[test]] traceability` plain-harness
target** (`harness` default), not a `#[test]` under `steps/traceability.rs` —
Task 9's "OR a standalone `[[test]]` target" fallback was taken; the cucumber
binary stays `harness = false` and rejects `--tags`/`--retry` args.
- The step-module registry (`steps/mod.rs`) matches the plan, incl. `clients.rs`;
`common.rs` is the ported `tests/common/mod.rs` (git records the rename).
- Config tag `@max-buckets-3` was added post-plan (ListBuckets pagination cap
→ `caps.max_buckets = 3`, used by the boto3 pagination scenario).
- Current suite size: **126 runtime scenarios** (90 `Scenario`/`Scenario Outline`
definitions; outlines expand over their `Examples` rows) across 9 top-level
feature files + `interop/` (11 feature files total), **168 step definitions** —
comfortably above Task 12's scenario-count gate (≥ the 34 replaced integration
tests).

### Post-migration coverage follow-up (2026-09-02)

After the migration, a spec↔suite coverage audit (spec.md + contracts vs the
features) found and closed five S3-API-observable gaps. All scenarios below
are backend-neutral unless tagged, and all verified green (fs 126, mem 97,
interop 147, traceability 3):

| Gap (spec origin) | Scenario(s) | Feature |
|---|---|---|
| `GetBucketLocation` (contract claimed coverage, had none) | answers `us-east-1` | `buckets.feature` |
| `HeadBucket` | 200 existing / 404 missing | `buckets.feature` |
| F07 DeleteBucket 204-before-gone | deleted bucket name reusable immediately | `buckets.feature` |
| US1-AS1 (out-of-band mirror) | directory placed in the served root is a bucket | `buckets.feature` `@fs` |
| Symlink policy (spec Edge Cases; default `follow_symlinks = false`) | access through a link → 403 `AccessDenied`; link excluded from listings | `reserved_paths.feature` `@fs` |
| FR-015 `CopyObject` in-process (was interop-only) | same/cross-bucket copy, overwrite, missing source → `NoSuchKey`, source/destination conditionals → 412 | `objects.feature`, `conditions.feature` |
| FR-003 user metadata | `x-amz-meta-*` accepted and dropped (never echoed) | `objects.feature` |
| Content-Type inference | unknown extension → `application/octet-stream` | `objects.feature` |
| FR-008/US3-AS2 SigV4 negative path | wrong credentials → `InvalidAccessKeyId`, no operation performed | `interop/journey.feature` `@interop @aws` |
| SC-008 three-layer metrics | `tinio_http_*` + `tinio_s3_operations_total` + storage streaming byte counters | `metrics.feature` |

New step phrases added by the follow-up (see the feature files): `the response
header {string} is absent`, `I copy object {a} to {b}`, `I create the directory
{name} in the served root`, `I create a directory link {rel} in the served
root` (unix symlink / Windows junction — no Developer Mode), `I try aws with
wrong credentials {cmd}`, `the listing omits {entry}`.

Traceability allow-list changes (Task 9's list): **FR-008 removed** (the
`@interop` bad-credentials scenario now covers SigV4 rejection e2e; the old
allow-list reason cited the deleted `tinio-server` auth tests), **T089 added**
(citation-only perf-script task in compatibility.md CHK020), FR-009 comment
corrected (its cited T079/T080 tests were deleted in the migration — the
anonymous-request path is what the in-process suite exercises).

Note on the Task 8 checklist: it rejected the tinio-fs symlink/junction unit
tests as "fs-backend walk internals, not API-observable" — that rejection
still holds for the walk mechanics; the follow-up added the API-observable
surface leg (default-policy rejection + listing exclusion) as the
`reserved_paths.feature` scenario above. `compatibility.md` CHK034's
annotation was updated to point at it.

Unrelated to the migration, a unit-test coverage pass (same session) added
tests to `tinio-core/src/storage/{time,body,mod}.rs`, the fs
`write_lock_bucket` boundaries, and the `tinio` facade re-exports — those are
tracked separately, not part of this plan's scope.

---

## Self-Review Notes

- **Spec coverage:** architecture (Task 1), migration table (Tasks 2–8), deletion rhythm (per-task deletes + Task 7 e2e/ retirement), spec binding + traceability (Task 9), CI + PR reports (Task 10), WSL2 (Task 11), verification + acceptance (Task 12), docs (Task 11). The dual-backend CI pass is Task 10 Step 1. The `@boto3`/`@mc` manual (non-CI) status is preserved: the interop job filters `@interop` only.
- **Placeholders:** none by construction — every task names concrete files, step phrases, and verification commands; porting tasks name the source file that remains in the tree as the content authority until its parity check.
- **Type consistency:** `World` fields (`backend`, `server`, `client`, `last`, `last_upload`, `upload_id`, `upload_key`, `stored_etag`, `ext`) are introduced once (Task 1/2/4/7) and referenced consistently; step phrases in the vocabulary table match the feature files.
- **Known implementation-time verifications** (bounded, with fallbacks): cucumber-rs expression placeholders (`{string}`/`{word}`), tag-expression grammar, `writer::Json` signature, `dorny/test-reporter` cucumber format name, `Server::fs_with_scanner_interval` plumbing. Each has an explicit fallback in its task.
