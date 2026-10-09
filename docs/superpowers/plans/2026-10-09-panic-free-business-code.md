# Panic-Free Business Code Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every panic-family site in production code returns the crate's `Result` instead — or carries a written `#[expect(…, reason)]` — enforced by clippy levels and one repo policy test.

**Architecture:** One plan, four waves, one commit. Task 0 lands the mechanics (repo-root `clippy.toml`, `docs/style.md` §Panics, the policy test, CI wiring) and pilots them on `tinio-core`; waves A/B/C clean the remaining crates bottom-up, each task landing its own crate's `[lints.clippy]` rows together with its cleanup so every wave ends with the workspace green; wave A additionally converts the repo's two remaining source-scanning gates to `syn` (Tasks A5, A6 — user-directed during plan review); Task V verifies the whole workspace and hands the single commit to the operator.

**Tech Stack:** Rust 1.99 / clippy 0.1.99, cargo `[lints]`, `clippy.toml`, GitHub Actions (`ci.yml`), WSL2 Ubuntu-26.04 leg.

**Spec:** `docs/superpowers/specs/2026-10-09-panic-free-business-code-design.md` — the plan argues from it; executors read both. Decision numbers (D1…D9) below refer to it.

## Global Constraints

- Scope (D1): production code only — no tests, benches, doc examples, `#[cfg(test)]`/`feature = "testing"` modules; `tinio-e2e` and `vendor/soft-canonicalize` exempt; `xtask` in scope. Out of rule (D4): indexing/slicing, arithmetic overflow, `debug_assert!`.
- Exception (D2): `#[expect(<lint>, reason = "…")]` only; prefix one of four — `literal:` compile-time input; `infallible:` cannot fail by construction (write into `String`/`Vec<u8>`, derive-only `Serialize`); `guarded:` invariant visible in the same expression, no better shape; `argued:` unreachable with the argument and the mechanism that catches a violation named in the reason. Runtime-failable never. Eliminate over annotate.
- Lock (D3): `.lock()` → `.unwrap_or_else(PoisonError::into_inner)`; never unwrap, never propagate.
- Error (D5): reuse the fitting variant; else add uniformly named `Invariant(&'static str)` + its Error→S3/HTTP arm (server: generic 500, detail to log/metrics only).
- Fallback (D6): only where `Result` is structurally impossible (`Display`, `Drop`, foreign trait) — defined fallback + `tracing` event, never panic.
- Lints (D7): each crate's `[lints.clippy]` gains `unwrap_used / expect_used / panic / todo / unimplemented / unreachable = "warn"`, same task as its cleanup. Repo-root `clippy.toml` owns test exemption (`allow-unwrap-in-tests`, `allow-expect-in-tests`, `allow-panic-in-tests`). A stated allow (reason citing §Panics) covers every test context the in-tests keys cannot reach: `harness = false` targets, `testing`-feature modules, and file-level helper fns of integration-test targets, for any of the six lints.
- Policy test (D8): `crates/tinio-e2e/tests/policy.rs`, wired into the `e2e` and `e2e-port` jobs.
- Delivery: one commit on `dev` (current branch) carrying spec + plan + implementation + config, operator-approved; agents never `git add`/commit; the index is the operator's review progress.
- Verification: per task `cargo clippy -p <crate> --all-targets -- -D warnings` clean + `cargo test -p <crate>` green (local = `cargo test`; CI = nextest); per wave the workspace gates stay green; platform-gated crates also get the WSL2 legs of `docs/tests.md` §Platform-gated verification.
- Records: tick `- [x]` and append one Evidence line (command + result) in the task that ran it. Never rewrite another task's record.
- Review: every task ends with a fresh reviewer subagent checking its conversions and each `#[expect]` reason against D2 before the next task starts.
- Execution order: Task 0 → wave A (Tasks A1–A7; a ≤4-deep work queue — A7 may run beside Task 0, disjoint files, no shared interface; A6 trails A3 on `sid.rs`) → wave B (B1–B2 in parallel) → Task C1 → Task V. Within a wave, files are disjoint and each parallel task uses its own `CARGO_TARGET_DIR` + `CARGO_BUILD_JOBS=6` (`docs/tests.md` §Concurrency, ≤4 agents).
- Scanners are parsed, not text-matched: every source scan this plan lands or converts (policy test, spawn guard, `function_body`) uses `syn` 3 — one workspace pin, features at the crate (`docs/cargo.md`); parse failure fails the gate.

## Review Focus

Five failure modes this refactor can hide; each has a test in the owning task.

1. A converted site changes a client-visible error code (an S3 op that used to panic now returns a *different* code than the documented one) — Task C1's tests must show the same code as before, or the new 500 where the panic was.
2. An `#[expect(reason)]` whose claim is false — the site can still panic. The lint cannot judge the argument; the reviewer of each task must read every reason and challenge it (D2 categories are the checklist).
3. Policy test misses a production file (false negative) or flags test code (false positive) — Task 0's planted probes (Task 0 step 5) pin both directions, including `#[cfg(all(test, unix))]` modules and bare `#[test]` fns.
4. Lock recovery swallows a poisoned-state bug — Task B1 adds a unit test that poisons `stuck_tombstones` deliberately and pins the recover-and-continue behavior.
5. A crate's gate misses feature-gated code (`parquet`, `versioning`, `api`, `acl`, `cors`, `sqlx`, `openapi`, `render`) — Task 0 step 2 measures the working feature set per crate; every task's gate command uses that set.
6. A converted gate changes what it accepts — spawn_guard's per-file site counts, `function_body`'s slice boundary — Tasks A5/A6 must show the gate red on a planted violation and green on the tree, and keep every `ALLOWED` count identical.

## Inventory (fixed 2026-10-09, verified by Task 0 step 1)

113 production sites. `res` = fn already returns `Result` (use `?`); `lock` = D3 idiom; `impl:` = fallback candidate (D6) unless a `Result` shape exists; `top`/`top-static` = D2 `argued:` candidate with the catching mechanism named. Crate → owning task at the end of each table.

### tinio-server (38) — Task C1

| File | Lines |
|---|---|
| `src/metrics.rs` | 90, 97, 103, 110, 117, 123, 129, 135, 141, 147, 154, 160, 166, 173, 180, 187, 195, 197 (top-static registry; 445, 455 lock) |
| `src/backend/mod.rs` | 375 (rayon pool), 458, 489, 505 (checksum-spec cache poisoned, lock), 582 (identity attached above) |
| `src/backend/checksum.rs` | 686, 794 (`unreachable!` CRC-only), 750 ("just set") |
| `src/pipeline.rs` | 247, 259, 300 (locks) |
| `src/backend/buckets.rs` | 76, 210 (identity attached above) |
| `src/backend/objects.rs` | 763 (`HeaderValue::from_str(…).unwrap()`), 1083 (identity attached above) |
| `src/backend/select.rs` | 150 (lock), 447 (checked above) |
| `src/data.rs` | 684 ("static format") |

Benches: `benches/{pipeline,listing,multipart_assembly}.rs` — file-top allow only if the gate shows them firing (`harness = false` check: C1 step 1).

### tinio-select (29) — Task A1

| File | Lines |
|---|---|
| `src/engine.rs` | 172, 182, 202, 203, 273, 575, 826 (res), 499 |
| `src/json/matcher.rs` | 357, 382, 475, 480, 602 |
| `src/json/document.rs` | 83, 89, 273 |
| `src/parquet.rs` | 459, 526, 571 (feature `parquet`) |
| `src/sql.rs` | 278 (res), 727, 757 |
| `src/output.rs` | 128, 141 (`serde_json` to string — D2 `infallible:` candidates) |
| `src/csv.rs` | 203 |
| `src/events.rs` | 531 (res) |
| `src/json/source.rs` | 95 |
| `src/path.rs` | 55 (res) |
| `src/row.rs` | 283 (`serde_json` — `infallible:` candidate) |

Bench: `benches/select_scan.rs` — allow only if firing.

### tinio-core (15) — Task 0 (pilot)

| File | Lines |
|---|---|
| `src/storage/listing.rs` | 381, 405, 569, 572 |
| `src/etag.rs` | 69, 79 (`FromStr`, res), 160 (`From<&str>` — D2 `literal:` candidate: doc'd literal API) |
| `src/acl.rs` | 90, 99 (sha256 hex — `infallible:` candidate) |
| `src/bucket.rs` | 85, 91 (`From<&str>`/`From<String>`) |
| `src/object.rs` | 98, 104 (`From<&str>`/`From<String>`) |
| `src/blob.rs` | 140 (`From<&str>`) |
| `src/multipart.rs` | 76 (`From<u32>`) |

The `From<…>` family: style.md says `From<&str>` for literals panics (2026-10-09 D2 `literal:`) — keep with `#[expect(reason = "literal: …")]` per site; `From<String>` sites convert (`TryFrom` is not available to the `impl From` contract — if the shape forbids it, `argued:`). Task 0 decides each with the spec in hand and records the choice.

### tinio-fs (11) — Task B1

| File | Lines |
|---|---|
| `src/metrics.rs` | 28, 37, 48, 61, 72 (top-static, feature `versioning`) |
| `src/etag.rs` | 164, 181 (`Drop for HashBuffer` — D6 fallback + tracing) |
| `src/scanner.rs` | 317, 321 (lock) |
| `src/write.rs` | 158 (etag tee cell), 361 (`unwrap_or_else(|join| panic!(…))` — join error → `Result`) |

Benches: `benches/{streaming,meta,set_batch}.rs` (`harness = false`, measured) — file-top allow each. `src/testing.rs` is ungated test support with 0 sites — add the stated allow if Task B1 confirms its nature.

### tinio-sandbox-linux (5) — Task A3

| File | Lines |
|---|---|
| `src/bin/sandbox-child.rs` | 51, 61, 67 — test-purpose binary (its own doc says so); file-top `#![allow(…)]` + reason, no conversion |
| `src/metrics.rs` | 73, 86 (top-static) |

### tinio-store-redb (4) — Task B2

| File | Lines |
|---|---|
| `src/driver.rs` | 257, 389 (`unwrap_or_else(|join| panic!(…))` — join error → `Result`) |
| `src/meta/object.rs` | 147 (`expect_err` on invalid row — convert) |
| `src/meta/version.rs` | 60 (wire name → table kind) |

### tinio-api (2) — Task A2

`src/transport.rs` 545 (lock, "not poisoned"), 556 (`.expect(` on instance slot — convert; both are the same connection-slot invariant).

### tinio-auth (2) — Task A2

`src/identity.rs` 91, 95 (authenticated key must resolve; anonymous canonical ID) — convert to `Result` through the request path.

### tinio-config (2) — Task A2

`src/schema/owner.rs` 54, `src/schema/users.rs` 67 (`canonical_id` validated at parse — either convert at validation site or `argued:` with the parse-time validator named).

### tinio-sandbox-core (2) — Task A3

`src/event.rs` 18 (`const _: () = assert!(MAX_FIELD_LIMIT != 0)` — compile-time constant: D2 `literal:`/`infallible:`), 44 (`None => unreachable!()` in `from_code`-style match — convert or `argued:`).

### tinio-sandbox-windows (1) — Task A3

`src/sid.rs` 352 ("a file name this module built") — convert.

### tinio-store-pg (1) — Task B2

`src/blocks.rs` 327 (res, `last()` on a checked non-empty page) — `?`/restructure.

### Zero-site crates sweep — Task A4 (rows for Task 0 and Task A3 are pointers; those tasks own their lines)

| Crate | Task | Work |
|---|---|---|
| `tinio-util` | Task 0 | rows; `#[allow(…)]` on `pub mod job;` (`any(test, feature = "testing")`) and `pub mod testing;` (`feature = "testing"`) with style.md-citing reason; Cargo.toml also gains the `syn` dev-dep (Task A5) and the `testing`-gated optional `syn`/`proc-macro2` (Task A6) |
| `tinio-blob` | A4 | rows; allow on `testing` module |
| `tinio-sandbox-windows` | A3 | rows (site above); allow on `testing` module |
| `tinio-version`, `tinio-winsec`, `tinio`, `tinio-sandbox`, `tinio-sandbox-macos`, `tinio-cli`, `xtask` | A4 | rows + `clippy -D warnings` clean confirmation |
| `tinio-e2e` | A4 | rows; `tests/cucumber.rs` (`harness = false`) file-top allow; `tests/policy.rs` is a test target (exempt) |

---

### Task 0: Mechanics + pilot (`tinio-core`, `tinio-util` allows)

**Files:**
- Create: `clippy.toml` (repo root)
- Create: `crates/tinio-e2e/tests/policy.rs`
- Modify: `docs/style.md` (new §Panics)
- Modify: `.github/workflows/ci.yml` (two steps, mirroring `e2e-trace`)
- Modify: `Cargo.toml` (root — `[workspace.dependencies]` gains the `syn` pin and a `proc-macro2` pin), `crates/tinio-e2e/Cargo.toml` (dev-deps: `syn` with `full`/`visit`, `proc-macro2` with `span-locations` — line numbers need it), `crates/tinio-util/Cargo.toml` (rows; dev-dep `syn` for Task A5; the `testing` feature gains optional `syn` + `proc-macro2` deps for Task A6 — `docs/cargo.md`'s carrier pattern). The `[lints.clippy]` rows for `tinio-e2e` land in Task A4.
- Modify: `crates/tinio-core/Cargo.toml`, `crates/tinio-core/src/{storage/listing.rs, etag.rs, acl.rs, bucket.rs, object.rs, blob.rs, multipart.rs}`
- Modify: `crates/tinio-util/Cargo.toml`, `crates/tinio-util/src/lib.rs` (two allow attributes), `crates/tinio-util/src/testing/mod.rs` (the shared attribute predicates, step 4b)

**Interfaces:**
- Produces: `clippy.toml` (test exemption for every crate), `docs/style.md` §Panics (the operative rule every later task cites), `crates/tinio-e2e/tests/policy.rs` (gate consumed by Task V), the CI steps that run it, and the per-crate feature set for gate commands (measured in step 2).

- [x] **Step 1: Confirm the inventory from the compiler, not a scan**

Run: `cargo clippy --workspace --all-targets --all-features --message-format=json -- -W clippy::unwrap_used -W clippy::expect_used -W clippy::panic -W clippy::todo -W clippy::unimplemented -W clippy::unreachable` and group the diagnostics by file:line (the command-line levels need no manifest edit). This is the authoritative list — cfg-resolved and macro-aware; diff it against this plan's Inventory tables.
Expected: matches, differences resolved in clippy's favor and recorded here. The assert-family list is confirmed in step 5 (the policy test's red run) — clippy has no lint for it. The Python scratch script (`D:/Temp/panic_inventory.py`) is an optional third opinion, not the authority.

- [x] **Step 2: Measure the working feature set for gates**

Run per crate with feature-gated production code: `cargo clippy -p tinio-select --all-targets --all-features -- -D warnings` (also `tinio-fs`, `tinio-server`, `tinio-cli`, `tinio-core`, `tinio-api`, `tinio-util`, `tinio-sandbox-linux`).
Expected: `--all-features` compiles; where it does not (powerset conflicts), record the crate's working set in this task's Evidence line and each later task's gate uses it. Also run `cargo clippy -p tinio-core --all-targets -- -D warnings` now — expected clean (no rows yet).

- [x] **Step 3: Write `clippy.toml` and the `[lints]` rows for this task's crates**

```toml
# Test contexts stay unwrap-friendly (design 2026-10-09 D7); production code is gated.
allow-unwrap-in-tests = true
allow-expect-in-tests = true
allow-panic-in-tests = true
```

Add to `crates/tinio-core/Cargo.toml` and `crates/tinio-util/Cargo.toml` under `[lints.clippy]`:

```toml
unwrap_used = "warn"
expect_used = "warn"
panic = "warn"
todo = "warn"
unimplemented = "warn"
unreachable = "warn"
```

Run: `cargo clippy -p tinio-core --all-targets -- -D warnings`
Expected: exactly the 15 core sites from the inventory fire, named by file:line. Note anything the gate does *not* flag that the inventory lists (`unwrap_err`/`expect_err` coverage is the open question — `tinio-store-redb/src/meta/object.rs:147` is the probe when Task B2 runs; if clippy misses it, that site is converted by hand and the Evidence line records it).

- [x] **Step 4: Write `docs/style.md` §Panics**

```markdown
## Panics

- Business code returns; never panics. No `.unwrap()` / `.expect()` / `unwrap_err` / `expect_err` / `panic!` / `unreachable!` / `todo!` / `unimplemented!` / `assert!` outside tests, benches, doc examples, test-support modules (design 2026-10-09 D1).
- Exception = `#[expect(<lint>, reason = "…")]`, prefix one of four: `literal:` compile-time input; `infallible:` cannot fail by construction; `guarded:` invariant visible in the same expression, no better shape; `argued:` unreachable, argument + catching mechanism in the reason (D2). Runtime-failable never. Eliminate over annotate.
- Lock: `.lock()` recovers — `.unwrap_or_else(PoisonError::into_inner)`; never unwrap, never propagate (D3).
- Error: reuse the fitting variant; else `Invariant(&'static str)`; detail to log/metrics only, response body generic 500 (D5).
- Fallback only where `Result` is impossible (`Display`, `Drop`, foreign trait): defined fallback + `tracing` event, never panic (D6).
- Out of rule: indexing/slicing, arithmetic overflow, `debug_assert!` (D4).
- Gate: the six lints warn per crate (CI `-D warnings`); assert family, `#[allow]` use and reason prefixes by `crates/tinio-e2e/tests/policy.rs` (D7, D8).
```

- [x] **Step 4b: Land the shared attribute predicates** in `crates/tinio-util/src/testing/mod.rs` (behind the `testing` feature; `syn` with `full` + `proc-macro2` with `span-locations` become optional deps enabled by that feature — step 3's Cargo.toml edit). These three are the one definer; the policy test and the spawn guard import them and no second copy exists anywhere. `tinio-util`'s dev-dependency on `syn` (for `spawn_guard.rs`) enables `full` + `visit`.

```rust
/// `true` when an item is test context: `#[cfg(...)]` naming `test` or the
/// `testing` feature, or a `#[test]`-family fn attribute. Production
/// features (`acl`, `parquet`, `versioning`, …) are not test context.
pub fn is_test_gated(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| {
        let path = a.path();
        if path.is_ident("test")
            || path.is_ident("rstest")
            || path.segments.last().is_some_and(|s| s.ident == "test")
        {
            return true;
        }
        if !path.is_ident("cfg") {
            return false;
        }
        let mut hit = false;
        let _ = a.parse_nested_meta(|meta| {
            if meta.path.is_ident("test") {
                hit = true;
            }
            if meta.path.is_ident("feature") {
                if let Ok(v) = meta.value() {
                    if v.parse::<syn::LitStr>().is_ok_and(|s| s.value() == "testing") {
                        hit = true;
                    }
                }
            }
            // Recurse into `all(...)` / `any(...)`.
            let _ = meta.parse_nested_meta(|_| Ok(()));
            Ok(())
        });
        hit
    })
}

/// The attributes of any item, for the gates that walk them.
pub fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(i) => &i.attrs,
        Item::Enum(i) => &i.attrs,
        Item::ExternCrate(i) => &i.attrs,
        Item::Fn(i) => &i.attrs,
        Item::ForeignMod(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Macro(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        Item::Static(i) => &i.attrs,
        Item::Struct(i) => &i.attrs,
        Item::Trait(i) => &i.attrs,
        Item::TraitAlias(i) => &i.attrs,
        Item::Type(i) => &i.attrs,
        Item::Union(i) => &i.attrs,
        Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

/// The attributes of an impl item.
pub fn impl_item_attrs(item: &syn::ImplItem) -> &[Attribute] {
    match item {
        syn::ImplItem::Const(i) => &i.attrs,
        syn::ImplItem::Fn(i) => &i.attrs,
        syn::ImplItem::Type(i) => &i.attrs,
        syn::ImplItem::Macro(i) => &i.attrs,
        _ => &[],
    }
}
```

- [x] **Step 5: Write the policy test with planted probes**

Add to the repo-root `[workspace.dependencies]` (pin only, no features — `docs/cargo.md`): `syn = "3"`. Add to `crates/tinio-e2e`'s `[dev-dependencies]`, alphabetically: `syn = { workspace = true, features = ["full", "visit"] }`. Measured: syn 3 parses all 395 workspace `.rs` files (let-chains and edition 2024 included).

`crates/tinio-e2e/tests/policy.rs` — a `syn`-parsed scan using step 4b's shared predicates (`use tinio_util::testing::{impl_item_attrs, is_test_gated, item_attrs};` — the three predicate `fn`s shown below live in `tinio-util` and are imported here, never redeclared) plus unit tests that pin red and green on synthetic sources:

```rust
//! Repo policy gate (design 2026-10-09 D8): production code carries no unannotated
//! assert family, no `#[allow]` escape from the panic lints, and only prefixed
//! `#[expect]` reasons.

use std::path::{Path, PathBuf};

use syn::{
    Attribute, ExprMacro, Item, Macro, Meta, StmtMacro, Token,
    parse::Parser,
    punctuated::Punctuated,
    spanned::Spanned,
    visit::Visit,
};
use tinio_util::testing::{impl_item_attrs, is_test_gated, item_attrs};

const LINT_NAMES: &[&str] = &[
    "unwrap_used", "expect_used", "panic", "todo", "unimplemented", "unreachable",
];
const REASON_PREFIXES: &[&str] = &["literal:", "infallible:", "guarded:", "argued:"];

/// Violations in one production file (`rel` is repo-relative for messages).
/// A parse failure is itself a violation: the gate must see every file.
fn scan_source(rel: &str, text: &str) -> Vec<String> {
    let Ok(file) = syn::parse_file(text) else {
        return vec![format!("{rel}: does not parse — fix the file or the scan")];
    };
    let mut scan = Scan {
        rel: rel.to_string(),
        dir: rel.rsplit_once('/').map(|(d, _)| d.to_string()).unwrap_or_default(),
        out: Vec::new(),
    };
    scan.visit_file(&file);
    scan.out
}

struct Scan {
    rel: String,
    dir: String,
    out: Vec<String>,
}

impl Scan {
    /// The lint-level attributes this gate owns (D8): `#[allow]` escapes and
    /// `#[expect]` reasons. `allow_exempt` marks an `#[allow]` on a `mod`
    /// declaration that resolves to a test-support file — the D7 exception.
    fn check_attrs(&mut self, attrs: &[Attribute], allow_exempt: bool) {
        for a in attrs {
            let is_allow = a.path().is_ident("allow");
            let is_expect = a.path().is_ident("expect");
            if !(is_allow || is_expect) {
                continue;
            }
            let Ok(metas) = a.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            else {
                continue;
            };
            let lint = metas.iter().find_map(|m| match m {
                Meta::Path(p) => p.segments.last().and_then(|s| {
                    let name = s.ident.to_string();
                    LINT_NAMES.contains(&name.as_str()).then_some(name)
                }),
                _ => None,
            });
            let Some(lint) = lint else { continue };
            let line = a.span().start().line;
            if is_allow {
                if allow_exempt {
                    continue;
                }
                self.out.push(format!(
                    "{}:{line}: `#[allow(clippy::{lint})]` escapes the gate — use `#[expect]` with a reason",
                    self.rel
                ));
                continue;
            }
            let reason = metas.iter().find_map(|m| match m {
                Meta::NameValue(nv) if nv.path.is_ident("reason") => match &nv.value {
                    syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) => Some(s.value()),
                    _ => None,
                },
                _ => None,
            });
            let ok = reason
                .as_deref()
                .is_some_and(|r| REASON_PREFIXES.iter().any(|p| r.starts_with(p)));
            if !ok {
                self.out.push(format!(
                    "{}:{line}: `#[expect(clippy::{lint})]` reason missing or unprefixed (D2)",
                    self.rel
                ));
            }
        }
    }

    /// `debug_assert*` is out of rule (D4) — only the three assert macros count.
    fn macro_invocation(&mut self, mac: &Macro) {
        let Some(seg) = mac.path.segments.last() else {
            return;
        };
        let name = seg.ident.to_string();
        if matches!(name.as_str(), "assert" | "assert_eq" | "assert_ne") {
            let line = mac.path.span().start().line;
            self.out.push(format!(
                "{}:{line}: assert family in production (`{name}!`) — return a `Result` (D1)",
                self.rel
            ));
        }
    }

    /// Does a `mod name;` declaration in this file resolve to an exempt file?
    fn resolves_to_exempt(&self, name: &str) -> bool {
        is_test_support_module(&format!("{}/{name}.rs", self.dir))
            || is_test_support_module(&format!("{}/{name}/mod.rs", self.dir))
    }
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_item(&mut self, item: &'ast Item) {
        let attrs = item_attrs(item);
        if is_test_gated(attrs) {
            return; // the whole item — a nested `mod tests` included — is test context
        }
        let allow_exempt = match item {
            Item::Mod(m) if m.content.is_none() => self.resolves_to_exempt(&m.ident.to_string()),
            _ => false,
        };
        self.check_attrs(attrs, allow_exempt);
        syn::visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        self.check_attrs(impl_item_attrs(item), false);
        syn::visit::visit_impl_item(self, item);
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        self.check_attrs(&local.attrs, false);
        syn::visit::visit_local(self, local);
    }

    fn visit_stmt_macro(&mut self, m: &'ast StmtMacro) {
        self.check_attrs(&m.attrs, false);
        self.macro_invocation(&m.mac);
        syn::visit::visit_stmt_macro(self, m);
    }

    fn visit_expr_macro(&mut self, m: &'ast ExprMacro) {
        self.macro_invocation(&m.mac);
        syn::visit::visit_expr_macro(self, m);
    }
}

// `is_test_gated` / `item_attrs` / `impl_item_attrs` are defined once in
// `crates/tinio-util/src/testing/mod.rs` (step 4b) and imported above — the
// spawn guard (Task A5) consumes the same three.

fn walk_rs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}

/// Test-support modules that compile outside `cfg(test)` (D7): each carries a
/// stated allow in its crate instead of a cleanup.
fn is_test_support_module(rel: &str) -> bool {
    const EXEMPT: &[&str] = &[
        "crates/tinio-util/src/job.rs",
        "crates/tinio-util/src/testing/mod.rs",
        "crates/tinio-util/src/testing/metadata.rs",
        "crates/tinio-blob/src/testing.rs",
        "crates/tinio-fs/src/testing.rs",
        "crates/tinio-sandbox-windows/src/testing.rs",
        "crates/tinio-sandbox-linux/src/testing.rs",
        "crates/tinio-sandbox-linux/src/bin/sandbox-child.rs",
    ];
    EXEMPT.contains(&rel)
}

#[test]
fn production_assert_is_red() {
    assert_eq!(scan_source("t", "fn f() { assert!(x); }").len(), 1);
}

#[test]
fn cfg_test_module_is_green() {
    assert!(scan_source("t", "#[cfg(test)]\nmod tests { fn t() { assert!(x); } }").is_empty());
}

#[test]
fn cfg_all_test_unix_module_is_green() {
    assert!(scan_source("t", "#[cfg(all(test, unix))]\nmod tests { fn t() { assert!(x); } }").is_empty());
}

#[test]
fn bare_test_fn_is_green() {
    assert!(scan_source("t", "#[test]\nfn t() { assert!(x); }").is_empty());
}

#[test]
fn macro_body_and_debug_assert_are_green() {
    assert!(scan_source("t", "macro_rules! m { () => { assert!(true); } }").is_empty());
    assert!(scan_source("t", "fn f() { debug_assert!(x); }").is_empty());
}

#[test]
fn bare_allow_is_red_and_prefixed_expect_is_green() {
    assert_eq!(scan_source("t", "#[allow(clippy::unwrap_used)]\nfn f() {}").len(), 1);
    assert!(scan_source("t", "#[expect(clippy::unwrap_used, reason = \"literal: fixed\")]\nfn f() {}").is_empty());
    assert_eq!(scan_source("t", "#[expect(clippy::unwrap_used, reason = \"because\")]\nfn f() {}").len(), 1);
}

#[test]
fn workspace_is_clean() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut violations = Vec::new();
    for dir in ["crates", "xtask"] {
        for path in walk_rs(&root.join(dir)) {
            let rel = path
                .strip_prefix(&root)
                .expect("walk starts at the repo root")
                .to_string_lossy()
                .replace('\\', "/");
            if rel.contains("/tests/") || rel.contains("/benches/") || rel.starts_with("crates/tinio-e2e") {
                continue;
            }
            if is_test_support_module(&rel) {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read");
            for v in scan_source(&rel, &text) {
                violations.push(format!("{rel}: {v}"));
            }
        }
    }
    assert!(violations.is_empty(), "policy violations:\n{}", violations.join("\n"));
}
```

Implementation notes: the visitor skips any item whose attributes say test context (`is_test_gated`) — one rule covering `#[cfg(test)]` mods, `#[cfg(all(test, unix))]`, `#[test]` fns and `feature = "testing"` modules, nested mods included. `walk_rs` is a plain recursive `read_dir` over `.rs`. An `#[allow]` on a `mod` declaration that resolves to an exempt test-support file passes (`resolves_to_exempt`, the D7 exception); every other `#[allow]` of the six lints is a violation. `macro_rules!` bodies are token streams, never descended into; `debug_assert!` is out of rule (D4). Documented blind spot: a local macro that expands to `assert!` is invisible to this parse.

Run: `cargo test -p tinio-e2e --test policy`
Expected: probe tests green; `workspace_is_clean` red with exactly the uncleaned crates' assert/allow/reason lines (today: `tinio-sandbox-core/src/event.rs:18` and nothing else — confirms the parsed scan sees production code).

- [x] **Step 6: Wire the policy test into CI**

In `.github/workflows/ci.yml`, add to the `e2e` job beside `e2e-trace` and to the `e2e-port` job's alias/step set (both mirror the existing `--test traceability` step):

```yaml
      - name: e2e-policy
        run: cargo test --profile ci -p tinio-e2e --test policy
```

- [x] **Step 7: Clean `tinio-core`'s 15 sites**

Per the inventory table and spec §2 playbook (structure → `?` → signature → `ok_or`/`let-else` → D6 fallback). `storage/listing.rs` 569/572 restructure to keep the popped value; `etag.rs` `FromStr` uses `?`; `From<&str>` sites follow the style.md literal rule with `#[expect(clippy::expect_used, reason = "literal: …")]`; `acl.rs`/`From<String>` sites convert. Any new error variant: `Invariant(&'static str)` (D5).

- [x] **Step 8: Add the `tinio-util` test-support allows**

On the `mod` declarations in `crates/tinio-util/src/lib.rs`:

```rust
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, reason = "test support behind `testing` (docs/style.md §Panics)")]
pub mod job;
```
(same attribute shape on `pub mod testing;`). Note: `#[allow]` on a `mod` declaration cannot carry `reason` on older syntax — if the compiler rejects `reason` on `allow` (only `expect` guarantees it), use a preceding `// reason: test support behind `testing` (docs/style.md §Panics)` comment instead and keep the bare `#[allow(…)]`.

- [x] **Step 9: Gate + record**

Evidence: task complete on the frozen tree — clippy at the new levels exit 0 for `-p tinio-core`, `-p tinio-util`, `-p tinio-fs`, `-p tinio-mem`, `-p tinio-e2e` and `--workspace`; tests: core 279, util 70, fs 367 lib, mem 99 lib (part-listing filter legs green); policy test 8 probes green, `workspace_is_clean` red on exactly `tinio-sandbox-core/src/event.rs:18`; `--all-features` measured working for every gate crate; `expect_err` confirmed lint-covered. Review (opus): Spec ❌ (two Important) → fix round 1 → re-review all findings addressed — I1: three stored-row callers now take validated paths, `From<u32>` kept with a `literal:` reason; I2: `visit_attribute` is the single check point (incl. `File.attrs`) with the D7 exemption as a flag, probes `file_level_allow_is_red`/`test_support_mod_allow_is_green` added. Deferred minors for the final review: the `From<u32>` reason's "every call site is a literal" clause is overstated by nine compile-time-bounded test callers; `impl_item_attrs` currently unconsumed; `walk_rs` passes vacuously on a missing root; group-level allow names unwatched; the test-module pre-pass has no probe. The workspace `Cargo.toml` pin loosening (`clap`/`tempfile`/`thiserror`) is the operator's own hand edit — kept by their ruling.

Run: `cargo clippy -p tinio-core --all-targets --all-features -- -D warnings` and `cargo clippy -p tinio-util --all-targets --all-features -- -D warnings`
Expected: clean. Then `cargo test -p tinio-core` and `cargo test -p tinio-util` — green. Tick steps 1–9 and append: `Evidence: clippy core/util clean at new levels; policy.rs green on probes, red on event.rs:18; feature sets measured …`.

---

### Task A1: `tinio-select` (29 sites)

**Files:** `crates/tinio-select/Cargo.toml` (rows), `src/{engine.rs, sql.rs, output.rs, csv.rs, events.rs, path.rs, row.rs, parquet.rs, json/{matcher.rs, document.rs, source.rs}}`, `benches/select_scan.rs` (allow only if the gate fires).

**Interfaces:** Consumes Task 0's `clippy.toml` + style.md §Panics. Produces nothing shared.

- [x] **Step 1: Add the six `[lints.clippy]` rows; run `cargo clippy -p tinio-select --all-targets --all-features -- -D warnings`** — expected: the 29 inventory sites fire.
- [x] **Step 2: Convert per the playbook.**
- [x] **Step 3: Gate + tests.**
- [x] **Step 4: Tick + evidence.**

Evidence: 31 sites (29 inventory + 2 gate-found in `row.rs`'s test mod) — 16 converted, 10 structurally eliminated (engine `Slot` pairs key with expression; matcher scopes exhaustified; `position()` narrowed to `JsonStreamReader`), 3 `infallible:` (serde_json on owned values, true for the concrete types), 1 bench allow (`harness = false`) + 1 test-mod allow (no `allow-unreachable-in-tests` key — probed against clippy-driver). New `Error::Invariant(&'static str)`: `Display` generic, pinned by a test; no leak at either server mapping. One reachable panic genuinely fixed (parquet RFC3339 pre-year-0 → `Format`, mirroring the DATE path). Gate: clippy exit 0 both feature sets; tests 413/442 (+parquet 32); policy 9/9. Review Approved (0 Critical; 1 Important **carried to C1**: D5's log carrier for `Invariant` — the server catch-all must log the detail; minors deferred: `json_number` doc overstates reachability, report arithmetic, `row.rs` allow could be `expect`).

---

### Task A2: `tinio-api` + `tinio-auth` + `tinio-config` (6 sites)

**Files:** `crates/tinio-api/{Cargo.toml, src/transport.rs}`, `crates/tinio-auth/{Cargo.toml, src/identity.rs}`, `crates/tinio-config/{Cargo.toml, src/schema/{owner.rs, users.rs}}`.

- [x] **Step 1: Rows + fire check** per crate (`--all-features` where the crate has features: `tinio-api` has `openapi`).
- [x] **Step 2: Convert.** `transport.rs` 545 → lock idiom (D3), 556 → `ok_or`/restructure into `Result`; `identity.rs` 91/95 → `Result` through the request path (auth failure must stay the same S3 code); `owner.rs`/`users.rs` → convert or `argued:` naming the config validator.
- [x] **Step 3: Gate + tests** per crate; `cargo test -p tinio-api -p tinio-auth -p tinio-config` green.
- [x] **Step 4: Tick + evidence.**

Evidence: six sites — `transport.rs` 545 lock idiom (D3 verbatim), 556 `with_instance` → `Poll<io::Result<T>>` (all five callers already that shape; success path unchanged); `identity.rs` 95 → `S3Result<OwnerId>` with the `InvalidAccessKeyId` text now single-homed (`auth.rs` delegates; new test pins it), anonymous arm `infallible:` (the constant compared to itself — true by construction); config 54/67 `argued:` naming `validate_canonical_id(_opt)` (which calls `OwnerId::new`, so a validated value provably cannot fail). Caller adaptation removed tinio-server's four "identity attached above" sites (C1 verifies only). clippy `-D warnings` exit 0 per crate; tests 314 + server lib 301; WSL2 legs green; review Approved (0 Critical/Important; minors: config reason over-claims reachability → deferred to Task V; the test-module allow was avoidable via `panic!` → preference; `transport.rs` 1991/2135 vs D3's "never propagate" → Task V list).

---

### Task A3: sandbox group — `tinio-sandbox-core` + `tinio-sandbox-linux` + `tinio-sandbox-windows` + `tinio-sandbox-macos` + `tinio-sandbox` (8 sites)

**Files:** the five crates' `Cargo.toml`; `tinio-sandbox-core/src/event.rs`; `tinio-sandbox-linux/src/{metrics.rs, bin/sandbox-child.rs, lib.rs}`; `tinio-sandbox-windows/src/{sid.rs, lib.rs}`.

- [x] **Step 1: Rows + fire check.** Note `tinio-sandbox-linux`/`-windows` are `deny(unsafe_code)` crates — leave `unsafe_code` untouched.
- [x] **Step 2: Convert.** `event.rs` 18 is a compile-time constant assert (`literal:`/`infallible:` `#[expect]` or a const-generic-shaped restructure), 44 `unreachable!` → `Result` or `argued:`; `metrics.rs` 73/86 → `argued:` with the metric-name uniqueness mechanism named; `sid.rs` 352 → convert; `bin/sandbox-child.rs` → file-top `#![allow(…)]` + comment citing style.md §Panics (test-purpose binary); `lib.rs` → allow on the `testing` module (`any(test, feature = "testing")`).
- [x] **Step 3: Gate + tests.** `cargo clippy -p <crate> --all-targets --all-features -- -D warnings` per crate; `cargo test -p tinio-sandbox-core` etc. on Windows.
- [x] **Step 4: WSL2 leg** per `docs/tests.md` (script file, `Ubuntu-26.04`, `CARGO_TARGET_DIR=$HOME/tinio-target`): `cargo clippy -p tinio-sandbox-linux --all-targets --all-features --locked -- -D warnings` and `cargo test -p tinio-sandbox-linux --locked`.
- [x] **Step 5: Tick + evidence.**

Evidence: eight sites — `event.rs:18` **eliminated** (the const's own E0080 is the check, probed: `clippy::panic` never fires in a const and the policy gate bans `assert!` unconditionally, so elimination was the only shape), `event.rs:44` `argued:` (const-eval, expectation fulfilled), `metrics.rs` 73/86 `argued:` (prometheus's `AlreadyReg`/`Msg` refusal, read in the registry source; ruled sufficient, optional uniqueness test deferred), `sid.rs:352` → `io::Result<PathBuf>` + `?` (test module untouched for A6); `bin/sandbox-child.rs` file-top allow; both `testing`-module declaration allows. The gate surfaced **26 test-context sites across 8 further files** (supervise 6, events 4, signals 4, jail 8, facade 2, the two mod declarations, the bin) — each a D7-permitted stated allow; `supervise.rs`'s sits on the `mod tests` declaration (narrowest scope). clippy `-D warnings` exit 0 ×5; tests green (core 82+90, windows 127+4+3+4+91, sandbox 7+6+33); policy 9/9 (`workspace_is_clean` green); WSL2 legs green. Review Approved (minors deferred: `sid.rs:353`'s comment misquotes §Panics' four prefixes → Task V polish; redundant lint names in five allows; `panic!`-in-tests preference; a load-correlated WSL2 flake in untouched `mechanism` code).

---

### Task A4: zero-site crates sweep

**Files:** `crates/{tinio-blob, tinio-version, tinio-winsec, tinio, tinio-sandbox-macos, tinio-cli, tinio-e2e}/Cargo.toml`, `xtask/Cargo.toml`, `crates/tinio-blob/src/lib.rs`, `crates/tinio-e2e/tests/cucumber.rs`.

- [x] **Step 1: Add rows** to each crate above; add the `testing`-module allow in `tinio-blob/src/lib.rs`.
- [x] **Step 2: Confirm clean.** `cargo clippy --all-targets --all-features -- -D warnings` per crate; any site that fires and is not in the inventory → stop, report (the inventory is wrong).
- [x] **Step 3: cucumber.** `cargo clippy -p tinio-e2e --all-targets -- -D warnings`; if `tests/cucumber.rs` fires (`harness = false`), add the file-top allow with the style.md-citing reason.
- [x] **Step 4: WSL2 leg** for the crates with `cfg(unix)` code (`tinio-cli`, `tinio-blob`, `tinio-version`): clippy + `cargo test -p <crate> --locked`.
- [x] **Step 5: Tick + evidence.**

Evidence: 21 files — the 8 crate `Cargo.toml`s, `tinio-blob/src/lib.rs` (testing allow), the four gate-mandated out-of-brief paths (`tinio-version/src/testutil.rs`, `tinio/tests/{common,credential_policy,versioning_wiring}.rs`), `tinio-e2e/tests/cucumber.rs`, and 13 stated allows total, each citing §Panics. The review found two half-gated crates and the fix round closed them: the six macOS test files (five targets + `harness/mod.rs` — the CI macOS leg would have gone red) and `tinio-e2e/tests/traceability.rs` (RED exit 101 → GREEN, by ruling). All 8 crates exit 0 on `--all-targets --all-features -- -D warnings` with own targets compiled; WSL2 leg rc=0; policy 9/9. Re-review: all findings addressed. Notes: four macOS target allows are inert (their sites sit in `#[test]` fns — deliberate over-coverage, permitted class); the macOS bodies still ride the CI leg.

---

### Task B1: `tinio-fs` (11 sites)

**Files:** `crates/tinio-fs/Cargo.toml`, `src/{metrics.rs, etag.rs, scanner.rs, write.rs, testing.rs}`, `benches/{streaming.rs, meta.rs, set_batch.rs}`.

- [x] **Step 1: Rows + fire check** (`--all-features`, feature `versioning`).
- [x] **Step 2: Convert.** `metrics.rs` 28…72 → `argued:` (feature `versioning`); `scanner.rs` 317/321 → lock idiom; `etag.rs` 181 (`Drop`) → D6 fallback + `tracing` event, 164 → convert; `write.rs` 361 → join error into the crate's `Result`, 158 → convert or `guarded:` with the tee contract named.
- [x] **Step 3: Poison test.** Unit test in `scanner.rs`: poison `stuck_tombstones` by panicking with the guard held, then assert the recover-and-continue path (Review Focus 4).
- [x] **Step 4: Benches.** File-top `#![allow(…)]` + reason in each of the three (`harness = false`, measured); `src/testing.rs` — confirm nature; if test support, one allow + reason.
- [x] **Step 5: Gate + tests.** `cargo clippy -p tinio-fs --all-targets --all-features -- -D warnings`; `cargo test -p tinio-fs`.
- [x] **Step 6: WSL2 leg** (`tinio-fs` is the `docs/tests.md` worked example): `cargo check -p tinio-fs --all-targets --locked` + the clippy leg there.
- [x] **Step 7: Tick + evidence.**

Evidence: eleven sites, all minimum-shape: 5 metrics `argued:` (prometheus 0.14 registry refusal, source-checked; each name registered once repo-wide), 4 lock sites D3 (one `stuck_tombstones_guard()` helper; no guard held across `.await`), `etag.rs:181` `Drop` → poison recovered + `tracing::warn!` (D6), `write.rs` 158/361 → the new D5 `Error::Invariant` with `tracing::error!` for the join (payload provably off the wire: `storage::Error` → server `s3_error!(InternalError, …)` Display-only). Poison test red-then-green (`scanner::tests::a_poisoned_stuck_tombstone_set_is_recovered_and_the_pass_continues` genuinely poisons under `catch_unwind`). Benches' file-top allows added here (my carry-forward note was wrong — A4 never owned them); the `testing`-module allow sits on the `lib.rs` declaration (the policy test's `resolves_to_exempt` covers it). Legs: clippy exit 0 (re-run by the reviewer too); 369 lib + 19 doc; policy 9/9; nightly fmt; WSL2 `--locked` + all-features + `cargo hack --each-feature`; the stale join-error comment corrected in place (F25 record left as written). Review Approved (minors → Task V: `write.rs:369`'s comment claims a cleanup the join path skips; `write.rs:156-166` lacks a tracing event for parity; the poison test's closing assertion is weaker than its comment). Note: pre-existing feature-off `dead_code` on `MetaStore::engine` reported, not fixed.

---

### Task B2: `tinio-mem` + `tinio-store-redb` + `tinio-store-pg` (6 sites)

**Files:** the three crates' `Cargo.toml`; `tinio-mem/src/multipart.rs`; `tinio-store-redb/src/{driver.rs, meta/object.rs, meta/version.rs}`; `tinio-store-pg/src/blocks.rs`.

- [x] **Step 1: Rows + fire check** per crate.
- [x] **Step 2: Convert.** `mem/multipart.rs` 505 → `guarded:` or restructure; `redb/driver.rs` 257/389 → join error into the store's `Result`; `meta/object.rs` 147 → convert; `meta/version.rs` 60 → `Result`/`ok_or`; `pg/blocks.rs` 327 → `?`/restructure.
- [x] **Step 3: Gate + tests.** `cargo clippy -p tinio-mem --all-targets --all-features -- -D warnings`, same for `tinio-store-redb` and `tinio-store-pg` (the `.sqlx` offline cache is on by default via `.cargo/config.toml`, so compilation needs no database); `cargo test -p tinio-mem -p tinio-store-redb`; `tinio-store-pg`'s DB-backed tests are operator-run (`docs/tests.md`), so run only its non-DB targets here and say so in the Evidence line.
- [x] **Step 4: Tick + evidence.**

Evidence: six sites, **zero `#[expect]`** — `mem/multipart.rs` 505 → `let-else` + `Err(no_parts())` (the arm is unreachable: the entry refusal exists and every iteration pushes exactly one `PartInfo`); `redb/driver.rs` 257/389 → join error lifted into the store's `Result` (`Error::Io` via the new private `blocking_task_failed`; `style.md`'s "extras → `Io`"); `redb/meta/object.rs` 147 → match returning the canonical `InvalidETag` (the one fallible element of `validate` is `ETag::new`); `redb/meta/version.rs` 60 → exhaustive `table_kind(ContractKind) → RowKind` (playbook step 1; deviating from the brief's `Result`, disclosed; adding a kind is now a compile error; wires verified identical); `pg/blocks.rs` 327 → `entries.last()?` (absorbs the empty-page guard). Changed test `a_panicking_write_closure_is_reported_as_an_error` asserts strictly more (class, message, driver still serves). Legs: mem 99+4, redb 127+2+1+2+24; pg lib 108 + doc 29 with DB legs self-skipped; clippy ×3 exit 0; policy 9/9; per-crate fire checks. Review Approved (minors deferred → Task V: no read-side lift test; `Error::Io`'s doc narrower than its uses; `driver.rs:298` "this method" inaccurate; `scanner.rs:416` spells the helper as an associated fn — routed to B1 in-flight). Ruling: `specs/001-s3-local-server/pipeline-spec-review.md`'s re-panic records stay as written (the record rule); the live correction lives in `scanner.rs` and design D1 — named in Task V's report.

---

### Task C1: `tinio-server` (38 sites)

**Files:** `crates/tinio-server/Cargo.toml`, `src/{metrics.rs, data.rs, pipeline.rs, backend/{mod.rs, checksum.rs, buckets.rs, objects.rs, select.rs}}`, benches as needed.

- [x] **Step 1: Rows + fire check** (`--all-features` — `acl`, `cors`, `select-parquet` …).
- [x] **Step 2: Convert.** `metrics.rs` 90…197 → `argued:` with the name-uniqueness mechanism named; 445/455 → lock idiom; `backend/mod.rs` 458/489/505 → lock idiom (poisoned cache), 375 → `Result`/lazy fallback, 582 + `buckets.rs` 76/210 + `objects.rs` 1083 → "identity attached above" — restructure so the identity is carried, else `guarded:`; `checksum.rs` 686/794 → `Result` (a wrong-spec call is a request error, not unreachable), 750 → restructure; `pipeline.rs` 247/259/300 → lock idiom; `objects.rs` 763 → `Result`/`ok_or`; `select.rs` 150 → lock idiom, 447 → restructure; `data.rs` 684 → `infallible:`/convert.
- [x] **Step 3: Error-mapping check.** Any new `Invariant`/error variant: its Error→S3 arm returns the generic 500 with no internal string (D5); add/extend the unit test that pins a client-visible code is unchanged for the converted ops (Review Focus 1).
- [x] **Step 4: Gate + tests.** `cargo clippy -p tinio-server --all-targets --all-features -- -D warnings`; `cargo test -p tinio-server`.
- [x] **Step 5: WSL2 leg** (server carries `cfg(unix)` code): clippy + `cargo test -p tinio-server --locked`.
- [x] **Step 6: Tick + evidence.**

Evidence: 34/38 inventory sites handled + 1 gate-found test-context site; the other 4 (`mod.rs` 582, `buckets.rs` 76/210, `objects.rs` 1083) verified already gone via A2 — no restructure needed. `metrics.rs`: 18 registry `.expect`s → 17 `argued:` with the `lazy_static!`→`LazyLock` migration (the macro hides the initializer from the attribute; the reason's mechanism read in the vendored `registry.rs` — `AlreadyReg`/`Msg`; `registers_all_families` asserts the exact 18-name set and the names are disjoint from fs/linux families); all production `.lock()` → D3 (remaining `.lock().unwrap()`s are inside `#[cfg(test)]`); `checksum.rs` 686/750/794 → total `Option` chain (the wrong-spec request error is `validate_supported`'s 400 upstream, verified doubly dead) — brief said `Result`, deviation adjudicated sound; `data.rs` 684 → `literal:` (compile-time literal input, module test the mechanism) rather than the brief's `infallible:`; `mod.rs` 375 → `select_pool()` lazy `OnceLock` pool, failure logs `%err` and answers generic 500 with no constructor ripple. D5 carriers verified in code and pinned: `select.rs` (`error!` + the documented in-stream generic item, `an_engine_invariant_answers_the_generic_item`) and `errors.rs` (`error = ?err` + message-less `InternalError`, `an_io_detail_stays_out_of_the_body`). Gates: Windows `clippy -p tinio-server --all-targets --all-features -- -D warnings` exit 0 (controller re-ran on the settled tree; the reviewer forced a real re-check), `cargo test -p tinio-server` exit 0 (301 lib + 4 sandbox + 9 doc; reviewer re-ran `--lib` 301), feature-off lib clippy exit 0, nightly fmt clean for this crate, policy 9/9 incl. `workspace_is_clean` (reviewer re-ran), WSL2 clippy exit 0 + `cargo test --locked` exit 0 (302 lib). Review Approved — 0 Critical/0 Important; minors disclosed: the `Io` 500 body text is now message-less generic (code unchanged, no repo artifact pinned the old text — D5-mandated), the pool/Io arms' generic-500 message style differs (cosmetic), the pool build moved from construction to first use (sanctioned by this task's plan line), the pool-failure arm is compile-verified only. Follow-up for Task V: workspace-root `lazy_static` dependency now unused.

---

### Task A5: `spawn_guard` — parsed scan, widened scope, `.output()`/`.status()` admitted

**Files:** `crates/tinio-util/tests/spawn_guard.rs` (scanner + `ALLOWED` + doc) and the test files it converts: `crates/tinio/tests/common/mod.rs`, `crates/tinio-sandbox-linux/tests/jail.rs`, `crates/tinio-sandbox-linux/src/testing.rs`, `crates/tinio-sandbox-macos/tests/{handover.rs,harness/mod.rs}`, `crates/tinio-sandbox-windows/src/testing.rs`, `crates/tinio-server/tests/{sandbox_macos_e2e.rs,support/windows_e2e_harness.rs}`, `crates/tinio-e2e/tests/steps/{cli.rs,reserved_paths.rs}`. No `Cargo.toml` edit: every affected crate already reaches `tinio_util` (verified: dev-dep with `testing`, or a source edge). Ownership rule that keeps wave A disjoint: **a raw site in a file another task owns is allowlisted (an `ALLOWED` edit — no edit to that file) and named in the Evidence line**; conversions happen only in files this task owns.

**Interfaces:** Consumes Task 0's shared predicates (`tinio_util::testing::{is_test_gated, item_attrs}`) and the workspace `syn`/`proc-macro2` pins. Produces the widened gate.

- [x] **Step 1: Baseline.** Run the current text gate; record the old `ALLOWED` list and its found sites (the before-picture the Evidence line compares against).

- [x] **Step 2: Replace the scanner.** Three site classes + the scope rule. Starting inventory (audit 2026-10-09; the widened run is authoritative): **12 raw `.spawn()` in `src` test contexts** — `tinio-util/src/job.rs:569,641` (the adoption proof, deliberately raw), `tinio-sandbox-macos/src/relay.rs:367,404`, `tinio-sandbox-macos/src/launch.rs:1866,1893`, `tinio-sandbox-core/src/launcher/supervise.rs:815,840,963`, `tinio-sandbox-core/src/launcher/probe.rs:355`, `tinio-sandbox-windows/src/sweep.rs:750`, `tinio-cli/src/commands/start/run.rs:745`; **`.output()`/`.status()` sites** — `crates/tinio/tests/common/mod.rs:65` (`run_doctor` — the §13 orphan shape, convert first), `tinio-sandbox-linux/tests/jail.rs:98,398`, `tinio-sandbox-macos/tests/handover.rs:309,357,369`, `tinio-sandbox-macos/tests/harness/mod.rs:688`, `tinio-server/tests/sandbox_macos_e2e.rs:282,310,591,616`, `tinio-server/tests/support/windows_e2e_harness.rs:450`, `tinio-e2e/tests/steps/cli.rs:2819,2954`, `tinio-e2e/tests/steps/reserved_paths.rs:71`, `tinio-sandbox-windows/src/testing.rs:152,188,349,1589`, `tinio-sandbox-linux/src/testing.rs:103-108`.

```rust
/// Raw spawn sites: zero-arg `.spawn()`, the `…::Command::spawn(…)` spelling,
/// and `.output()`/`.status()` whose receiver subtree builds a `Command`
/// (documented heuristic: `let cmd = Command::new(..); cmd.output()` is missed).
fn raw_sites(text: &str, in_tests_dir: bool) -> Vec<(usize, String)> {
    let file = syn::parse_file(text).unwrap_or_else(|e| panic!("a scanned file must parse: {e}"));
    let mut scan = SpawnScan { in_tests_dir, in_test: false, lines: Vec::new() };
    scan.visit_file(&file);
    let mut lines = scan.lines;
    lines.sort_unstable();
    lines.dedup();
    lines
        .into_iter()
        .map(|line| {
            let code = text.lines().nth(line - 1).unwrap_or_default().trim().to_string();
            (line, code)
        })
        .collect()
}

struct SpawnScan {
    in_tests_dir: bool,
    in_test: bool,
    lines: Vec<usize>,
}

impl SpawnScan {
    fn command_receiver(expr: &Expr) -> bool {
        match expr {
            Expr::Call(c) => matches!(&*c.func, Expr::Path(p)
                if p.path.segments.last().is_some_and(|s| s.ident == "new")
                    && p.path.segments.iter().rev().nth(1).is_some_and(|s| s.ident == "Command")),
            Expr::MethodCall(m) => Self::command_receiver(&m.receiver),
            Expr::Paren(p) => Self::command_receiver(&p.expr),
            Expr::Reference(r) => Self::command_receiver(&r.expr),
            _ => false,
        }
    }
}

impl<'ast> Visit<'ast> for SpawnScan {
    fn visit_item(&mut self, item: &'ast Item) {
        let gated = is_test_gated(item_attrs(item));
        if !self.in_tests_dir && !self.in_test && !gated {
            return; // production item in `src/` — outside the gate's scope
        }
        let prev = self.in_test;
        self.in_test |= gated;
        syn::visit::visit_item(self, item);
        self.in_test = prev;
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let method = call.method.to_string();
        let hit = (method == "spawn" && call.args.is_empty())
            || (matches!(method.as_str(), "output" | "status")
                && Self::command_receiver(&call.receiver));
        if hit {
            self.lines.push(call.method.span().start().line);
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let Expr::Path(p) = &*call.func {
            let segs: Vec<String> = p.path.segments.iter().map(|s| s.ident.to_string()).collect();
            let n = segs.len();
            if n >= 2
                && segs[n - 2] == "Command"
                && matches!(segs[n - 1].as_str(), "spawn" | "output" | "status")
            {
                self.lines.push(p.path.span().start().line);
            }
        }
        syn::visit::visit_expr_call(self, call);
    }
}
```

Imports: `syn::{Expr, ExprCall, ExprMethodCall, Item, spanned::Spanned, visit::Visit}` and `tinio_util::testing::{is_test_gated, item_attrs};`.

- [x] **Step 3: Handle every found site** — convert to the guard (`job::spawn(&mut cmd)` then `child.wait_with_output()` / `child.wait()`; `Command::output()` implies piped stdout/stderr, so set `Stdio::piped()` where the builder does not) or allowlist with a reason (the adoption proof in `job.rs`, macOS arms where the guard is a no-op, the ownership rule's sites). Each new `ALLOWED` entry carries its reason; the reverse-direction count check must pass.

- [x] **Step 4: Correct the gate's doc in place** — scope sentence (`crates/*/tests/**` + `src` test contexts), the site definition (three classes, the `Command::new`-receiver heuristic and what it cannot see), and why the macOS entry stays.

- [x] **Step 5: Probes.**

```rust
#[test]
fn the_scan_knows_its_three_classes_and_its_scope() {
    let in_scope = r#"
fn f() {
    let _ = std::process::Command::new("x").spawn();
    let _ = std::process::Command::spawn(&mut cmd);
    let _ = std::process::Command::new("x").output();
    // a comment mentioning .spawn()
    let _ = job::spawn(&mut cmd);
}
"#;
    assert_eq!(raw_sites(in_scope, true).len(), 3, "{:?}", raw_sites(in_scope, true));
    let guarded = "fn f() { let mut c = job::spawn(&mut cmd)?; let _ = c.status()?; }";
    assert!(raw_sites(guarded, true).is_empty());
    let scoped = "fn prod() { let _ = Command::new(\"x\").output(); }\n#[cfg(test)]\nmod t { fn t() { let _ = Command::new(\"x\").spawn(); } }";
    assert_eq!(raw_sites(scoped, false).len(), 1, "{:?}", raw_sites(scoped, false));
}
```

- [x] **Step 6: Gate + legs.** `cargo test -p tinio-util --test spawn_guard` green (both directions); the converted crates' legs this host can run; the Linux-only (`jail`) and macOS-only legs ride the WSL2 and macOS-CI legs (`docs/tests.md`) — each leg named in the Evidence line as run or deferred-with-why.

- [x] **Step 7: Tick + evidence** — before/after counts, every new `ALLOWED` entry with its reason, every conversion, every leg.

Evidence: counts 1 → 47 (13 `.spawn()` + 34 `.output()`/`.status()`) → 35 after 19 conversions; 11 `ALLOWED` entries (35 sites), every reason read and true by the reviewer (incl. the 17 `icacls` sites and the macOS no-op entries; the 18th `acl.rs` `.output()` is production → out of scope, correctly); the doc's three documented holes are self-described and one is probe-pinned; conversions stream-exact (`output()` → null stdin + piped stdout/stderr; `status()` → inherited). Legs: spawn_guard 6/6 (reviewer re-ran it); clippy exit 0 (util with `--features testing` + the four crates touched); behaviour legs; WSL2 `check`/`clippy`/`jail` (4 passed, running a converted spawn); macOS legs → CI. Review Approved (minors → Task V: report §1's class-split label and §97's counts; the hole record omits `testing/mod.rs:3917` (third live instance) and `spawn_guard.rs:44-45`'s coverage sentence is slightly overstated; the harness entry's names (`run_confined`/`Children`'s home) are imprecise).

---

### Task A6: `function_body` boundary comes from the parse; one slice becomes behavioral

**Files:** `crates/tinio-util/src/testing/mod.rs` (the helper), `crates/tinio-sandbox-windows/src/sid.rs` (the `classified` slice → a behavioral test). The audit (2026-10-09) kept the other three callers on source assertions — `tinio-cli/src/sandbox/tests.rs:949`, `tinio-sandbox-windows/src/sid.rs:2269`, `tinio-server/tests/support/windows_e2e_harness.rs:816` — because no runtime seam can observe their properties (a `const fn`/platform API's answers, anti-duplication counts), and the CLI half's behavioral part is already e2e-pinned. API and those three call sites stay untouched; if one breaks, fix it here and record why.

- [x] **Step 1: Rewrite the helper.** Keep the two panics' contract; correct the doc's boundary paragraph in place (the rule becomes: the parse finds the declaration; the doc mention can no longer be mistaken for it, structurally).

```rust
pub fn function_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let file = syn::parse_file(source)
        .unwrap_or_else(|e| panic!("the source must parse before a body is sliced: {e}"));
    let mut finder = BodyFinder { source, signature, found: None };
    finder.visit_file(&file);
    let Some((start_line, end_line)) = finder.found else {
        panic!("no function declaration of this source carries `{signature}`");
    };
    let mut offset = 0usize;
    let mut start = None;
    let mut end = None;
    for (i, line) in source.split_inclusive('\n').enumerate() {
        if i + 1 == start_line {
            start = Some(offset);
        }
        if i + 1 == end_line {
            end = Some(offset + line.len());
            break;
        }
        offset += line.len();
    }
    &source[start.expect("the start line exists")..end.expect("the end line exists")]
}

struct BodyFinder<'s> {
    source: &'s str,
    signature: &'s str,
    found: Option<(usize, usize)>,
}

impl<'s> BodyFinder<'s> {
    fn consider(&mut self, sig: &syn::Signature, block: &syn::Block) {
        if self.found.is_some() {
            return;
        }
        let first = sig.span().start().line;
        let last = sig.span().end().line;
        let header = self
            .source
            .lines()
            .skip(first - 1)
            .take(last - first + 1)
            .collect::<Vec<_>>()
            .join("\n");
        if header.contains(self.signature) {
            self.found = Some((first, block.span().end().line));
        }
    }
}

impl<'s, 'ast> Visit<'ast> for BodyFinder<'s> {
    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        self.consider(&f.sig, &f.block);
        syn::visit::visit_item_fn(self, f);
    }
    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        self.consider(&f.sig, &f.block);
        syn::visit::visit_impl_item_fn(self, f);
    }
}
```

- [x] **Step 2: Replace the `classified` slice with its behavioral test.** In `sid.rs`'s tests, drop `function_body(include_str!("sid.rs"), "fn classified(step: &'static str")` and its `contains` assertion; add:

```rust
#[test]
fn classified_maps_the_denied_hr_to_the_host_refusal_and_anything_else_names_the_step() {
    let denied = classified("CreateAppContainerProfile", "name", capability::E_ACCESSDENIED);
    assert!(matches!(denied, Error::HostCannotConfine { .. }), "{denied}");
    let other = classified("CreateAppContainerProfile", "name", 0x8007_0003u32 as i32);
    match other {
        Error::Launch(detail) => assert!(detail.contains("hr=0x80070003"), "{detail}"),
        other => panic!("a non-denied HRESULT is a launch failure, got {other}"),
    }
}
```

Exact variant names and fields are read from `crates/tinio-sandbox-windows/src/error.rs` at implementation time (the audit named `Error::HostCannotConfine { hr, .. }` and `Error::Launch`).

- [x] **Step 3: Probe the property the text rule hand-patched**

```rust
#[test]
fn a_doc_mention_is_not_a_declaration() {
    let src = "/// fn a does things\nfn b() { b(); }\nfn a() { a(); }\n";
    assert!(function_body(src, "fn a").contains("a();"));
    assert!(!function_body(src, "fn a").contains("b();"));
}
```

- [x] **Step 4: Gate.** `cargo test -p tinio-util --features testing` (helper + doctest + internal uses), `cargo test -p tinio-cli --lib` (its slice site), `cargo test -p tinio-sandbox-windows --lib` (its slice site and the new behavioral test), `cargo test -p tinio-server --all-targets --no-run` (builds the Windows-harness slice site). Any leg this host cannot build is named in the Evidence line.

- [x] **Step 5: Tick + evidence.**

Evidence: `function_body` takes its boundary from `syn::parse_file` + `BodyFinder` (pure, first-match-wins, item before recursion); the doc's boundary/`# Panics`/mention paragraphs corrected in place; the doc-mention probe landed and was measured red→green; the `classified` slice replaced by `classified_maps_the_denied_hr_to_the_host_refusal_and_anything_else_names_the_step` (real variants per `tinio-sandbox-core/src/error.rs`; re-ran 2 passed on this host); the re-pointed second panic is honest (`"fn method() {"` genuinely unparseable) and the indentation shape is pinned as a positive slice; `tinio-util`'s optional `syn` gains `visit` (needed by consumer builds of the `testing` feature — `docs/cargo.md`'s "enable at the crate"). Legs: util 53+25 doctests, cli 116, sandbox-windows 128, server `--all-targets --no-run` exit 0, windows-harness leg 1 passed. Review Approved (minors deferred to Task V: the impl-method fixture isn't nested in an `impl` so `visit_impl_item_fn` is exercised only by the Windows leg; a comment on the `proc-macro2` `span-locations` line and one clause in `# Panics` about the two offset `.expect()`s). ⚠ routed in-flight to A5: the three `absolute_paths` clippy errors in `tests/spawn_guard.rs` from A5's own rewrite.

---

### Task A7: traceability allow-list repair (audit 2026-10-09; operator-added)

**Files:** `crates/tinio-e2e/tests/traceability.rs` only.

**Interfaces:** independent of Task 0; consumes nothing shared; runs beside Task 0 (disjoint files).

- [x] **Step 1: Structure the entries** — `(id, reason, home)` with `enum Home { Path(&'static str), Task(&'static str), Meta }`; the checker fails on a missing `Path`, a `Task` not found unchecked in `specs/001-s3-local-server/tasks.md`, or a `Meta` ID outside the enumerated `{FR-001, T010, T023}`.
- [x] **Step 2: Repairs** — `FR-019` → `Task("T092")` (the named bench never existed); `SC-003`/`T089` → `Task("T089")` (the `e2e/perf/` script never existed); `FR-035` → `Path("crates/tinio-cli/src/service/mod.rs")` (moved 2026-10-01); `FR-010` → the real streaming tests as `Path` (found: `get_stream_chunk_sequence`, `streamed_chunks_own_their_memory`); `FR-016` → deleted (inert); the rest verified against the tree (FR-009 → `Task("T083")`, T018 → `sources.rs`, FR-018 → `transport.rs`), 15 entries.
- [x] **Step 3: `spec.md` into the tag-side corpus** — `collect_spec_ids` gains `specs/001-s3-local-server/spec.md` (widens the valid set only; 0 orphans, green).
- [x] **Step 4: Mutual exclusion** — an allow-listed ID that also appears as a tag is a violation (holds on the real corpus: none of the 15 is tagged).
- [x] **Step 5: Pure checker + unit tests** — `check_allow_list(entries, tagged, unchecked_tasks, repo_root)` + 5 unit tests + the real-list test; `collect_unchecked_tasks` takes only `- [ ]` rows (checked/prose rejected).
- [ ] **Step 6: Gate** — test half green (`cargo test -p tinio-e2e --test traceability` 20/20, re-run after both fix hunks); clippy half red only from Task 0's in-flight dependency lints (`tinio-core` 15× `expect_used`, `tinio-util` 2× `collapsible_if`) — re-checked at Task V.
- [x] **Step 7: Tick + evidence.**

Evidence: audit's four dead entries repaired and FR-016's inert entry deleted (15 entries, every home machine-checked: `Path` exists / `Task` unchecked in `tasks.md` / closed `Meta {FR-001, T010, T023}`); tag↔allow mutual exclusion green; `spec.md` joined the tag-side corpus; fix round 1 (review finding: the mirrored `s3-surface.md` sentence) landed as `:425` rewrite + `:421` clause deletion + the honest "array is the authority" claim at `traceability.rs:96-114`; task review Approved, re-review all findings addressed. Follow-up for the operator: `s3-surface.md:421`'s mirror omits FR-035/T087/T089/T144/T146 (pre-existing drift, ruled out of this task).

---

### Task V: Whole-workspace verification + the single commit

**Interfaces:** Consumes every task's gate evidence; produces the verified tree and the operator-approved commit.

- [x] **Step 1: Full gates.** `cargo clippy --workspace --all-targets -- -D warnings` (ubuntu/CI-native resolution) and, on WSL2, the same plus `cargo test --workspace --locked` (CI-profile parity per `docs/tests.md`).
- [x] **Step 2: Policy test.** `cargo test -p tinio-e2e --test policy` green — including `workspace_is_clean`; confirm the red-then-green record from Task 0.
- [x] **Step 3: Survivor audit.** Every `#[expect(clippy::…, reason)]` in production code: count it, check its prefix, and put the list in this task's record. Count must equal the inventory's exception list (no silent survivors).
- [x] **Step 4: Rows audit.** The six lint names present in all 23 `Cargo.toml`s (`rg -l 'unwrap_used' crates/*/Cargo.toml xtask/Cargo.toml | wc -l`).
- [x] **Step 5: Fresh review.** One reviewer subagent over the whole diff against the spec's D1–D9 (Review Focus list as the checklist).

Evidence (step 5): the opus whole-branch reviewer re-ran the policy test (9/9) and both workspace clippy shapes (exit 0) itself, gave PASS on all of D1–D9, re-read all 37 production `#[expect]` reasons and judged every one true (the 27 `argued:` resolved mechanically: 25 unique metric names across the 24 prometheus sites, every `From<u32>` invocation an integer literal, the `MAX_FIELD` const-eval and garde-validator mechanisms read in source), re-derived the policy test's skip set (21 files, all genuine test modules), scanned every `macro_rules!` body for panic-family tokens (none in production; no assert family in any macro body), and found no silent survivor — commit readiness **YES**, 0 Critical / 0 Important. Minors (latent, false-positive-only direction): the policy test resolves `mod` idents but not `#[path]`, so three store-pg test files are scanned as production (currently green); `select/src/row.rs:295` uses `#[allow]` where `#[expect]` would be stricter (test-gated; A1's recorded preference); the two sandbox-linux metrics `argued:` sites name the refusal rather than a test as mechanism (honest in text; A3 ruled sufficient). Disclosures beyond Task V's list: `impl From<String> for Name`/`Key` were removed (public-API removal, in-scope by the spec's Evidence base — `publish = false`, no `build.rs`, no in-repo caller remains); the two generic-500 message styles differ (cosmetic).
- [ ] **Step 6: Commit (operator-approved only).** Present the diff summary; on approval, one commit on `dev` carrying spec + plan + implementation + config; never `git add` on the operator's behalf before approval.

Step 6 stays unticked by the operator's own ruling: the diff summary was presented 2026-10-09 with the proposed message, and the operator chose to hold the commit (**暂不提交**). The tree is verified commit-ready and left uncommitted at `5304d33b`; the message is ready to paste from the task record. Nothing else is pending.

Post-review addendum (operator-directed two-axis `/code-review`, then an approved fix round): the plain `-p tinio-util` acceptance line is now green (`[[test]] spawn_guard required-features = ["testing"]`); the spec's shared-symbols row was amended (`impl_item_attrs` deleted as unconsumed; the three walk helpers added, traceability named); `tinio-api/src/transport.rs`'s two remaining locks joined D3 (behaviour change, two tests); the three parsed gates share one repo walk from `tinio_util::testing`; `docs/style.md` §Panics states the two test prefixes (`test support:` / `test context:`) with 22 attributes normalised to them; `docs/cargo.md` now allows major-or-major.minor pins and carries the `syn`-edge rationale; the small warts (`identity.rs` tail-let, `row.rs` allow→expect, `reserved_paths.rs` inline uses, the policy test's `#[path]` handling + its new probe) are fixed. Gates re-run on the fixed tree: workspace clippy both feature shapes exit 0, nightly fmt `--check` exit 0, policy 10/10, traceability 20/20, plain `-p tinio-util` clippy/test exit 0, `--features testing --test spawn_guard` 6/6. Kept by ruling: the metrics `argued:` per-site reasons (a macro would hide 24 of them from the syn-parsed gate), the two `sandbox-linux` reasons, and the three operator-directed/`A7`-ruled file edits.
