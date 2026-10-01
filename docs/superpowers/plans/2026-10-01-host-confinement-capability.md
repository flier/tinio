# Host confinement capability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `CreateAppContainerProfile: hr=0x80070005` into one operator sentence with a cause and two remedies, refuse before the first persistent side effect, and report the fact in `tinio doctor`.

**Architecture:** A host-level predicate lives in `tinio-sandbox-windows`'s new `capability.rs` (probe, HRESULT classification, sentence factory), is re-exported through the `tinio-sandbox` facade for all three platforms, and is asked by two gates — the CLI's `refuse_unhonourable` (before any write) and `Backend::confine`'s head (the other embedders, and before the sweep/salt/mutex/profile). One new `tinio-sandbox-core` error variant carries the sentence, one new `tinio-cli` variant renders it at exit 1, and the Windows e2e harness's host precondition takes its sentence from the same factory.

**Tech Stack:** Rust, `windows-sys` (already a dependency of `tinio-sandbox-windows`), `tracing`, `thiserror`, cucumber for the e2e scenario.

**Spec:** `docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md` — the design this plan implements, including its two factual corrections and the six grilling rounds that fixed the placement of every seam.

## Global Constraints

- **English only** in code, comments and docs (`docs/compress.md`).
- **Do not commit.** `docs/git.md` requires asking first, and the operator's ruling for this work is "leave the tree as it is". No step below ends in a commit; do not add one.
- `cargo fmt` and `cargo clippy --workspace --all-targets -- -D warnings` must pass. `tinio-sandbox-windows` carries `unsafe_code = "deny"` (`Cargo.toml:65-69`) and each of its modules opens with `#![allow(unsafe_code)]` where it needs win32; follow that pattern rather than changing the lint.
- The probe's profile name is **exactly** `tinio.sandbox.capability-probe`. It must not come from `package_name` or `package_name_unsalted`: both derive a real root's name, and `package_name` also touches `%LOCALAPPDATA%\tinio\acl-salt`.
- Only `E_ACCESSDENIED` (`0x80070005`) is a capability answer. `ERROR_PATH_NOT_FOUND` (`0x80070003`) and every other HRESULT keep today's path; an unrecognised HRESULT is never cached.
- **An unrecognised probe failure is not a verdict.** It is logged at `warn` and answered `Ok` — the run proceeds, nothing is cached, and the real `CreateAppContainerProfile` classifies its own HRESULT (Task 3's floor). Refusing on it would turn a transient race into a false negative on a host that works.
- The two `#[error]` format strings that join cause and remedy (the core's and the CLI's) are the same line on purpose: the **content** has one author (`capability::cause` and `capability::REMEDY`), the join is a one-line convention stated at both. Change one, change both. **Superseded 2026-10-01 (code-review fix round): there is one join now.** The CLI's variant carries the core error whole, so only the core's `#[error]` spells it — the convention has nothing left to keep in step, and the lockstep test that enforced it went with the second spelling.
- The doctor row's `check` name is exactly `host confinement` and its severity is `warn`; `specs/001-s3-local-server/contracts/doctor-report.schema.json` does not change.

## Review Focus

**One gap this plan does not close, stated here rather than left implicit.** `sandbox::plan` runs at `run.rs:147-155`, but `resolve::server` (`run.rs:119-123`) is above it and, on a root with no `.tinio.toml` yet, `discover::load_or_create` (`tinio-config/src/discover.rs:139-159`) does `create_dir_all(parent)` and writes the config **before** the gate can answer. On a host whose token cannot write that directory the operator therefore reads the filesystem's bare `PermissionDenied` and never the sentence — the exact correlation `CreateAppContainerProfile: hr=0x80070005` was measured under. The design's own comment admits the state-directory half (`run.rs:138-146`). The floor still holds for a host where the tree *is* writable (Task 3 classifies the real call), and `Backend::confine`'s gate (Task 8) is above everything the library does — but "refuse before the first persistent side effect" is a claim about the **library's** first side effect, not about the CLI's resolution. Closing it in the CLI means either a pre-resolution check (which cannot see the config channel) or making `load_or_create` non-creating until after `plan`; both are larger than this change. **Put to the operator in round 2 and accepted (Q12)**: the limit is recorded here, in the spec and in T148's row rather than closed by this change.

The spec's silence on these inputs is not permission for them to misbehave. Each line names the input and the behavior a reasonable person expects; the task named owns the test.

1. **A transient `ERROR_PATH_NOT_FOUND` during a probe.** Two processes creating the same name inside `userenv` produce `0x80070003`; a predicate that cached it would report a healthy machine as permanently incapable. Test: Task 4 Step 1.
2. **A lazy delete under no load.** `DeleteAppContainerProfile` answers `S_OK` for a name that was never created (measured 2026-09-26), so the probe's success path must not be read as "the profile was removed". Test: Task 4 Step 1.
3. **A Low-integrity host running `tinio doctor`.** `doctor` writes its one temporary probe file; on such a host that write fails first, and the capability row must still render. Test: Task 11 Step 4.
4. **A second `tinio` alive while a probe crashes.** The sweep is skipped while another `tinio` runs and a `WAIT_ABANDONED` name is left alone, so the probe's residue must be reaped by a *later* process, never inherited by a live one. Test: Task 4 Step 5.
5. **A facade consumer on unix.** `can_confine()` must exist and answer `Ok(())` on a target with no AppContainer at all, or the facade's own surface stops compiling on two of three platforms. Test: Task 5 Step 1.

**Corrected 2026-10-01 (Task 13 fix round), on the two measurements behind it: item 4's "a *later* process" describes a launcher's sweep and not the `tinio doctor` path.** The sweep runs only when a launcher prepares and is skipped while another `tinio` is alive, so a `tinio doctor` run's probe residue is reaped by nothing at all; the operator therefore chose to make the probe **clear the name before creating it** rather than rely on a reap (Task 4 fix round 3, `reports/task-4-fix3-report.md`, made best-effort by the wave-E round, `reports/wave-e-fix-report.md`), and the inheritance item 4 feared was separately measured **unreachable on this host** - permission precedes existence, so a refused token answers `E_ACCESSDENIED` even with the name present and never reaches the takeover. Both facts are the design's Risks paragraph (`docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md`, the `Probe residue` bullet); Task 4 Step 5 is the round the clear landed in.

**Recorded 2026-10-01 (Task 12), for the next reader.**

- **The unsafe-site ledger is one short, and it is another workstream's file.** `docs/superpowers/specs/2026-09-18-sandbox-design.md:157` says "seven file-level attributes and two function-scoped ones". Grepping `crates/tinio-sandbox-windows/src` for `#![allow(unsafe_code)]` today gives **eight** file-level sites — `handles.rs:22`, `sid.rs:17`, `socket.rs:17`, `process.rs:17`, `control.rs:11`, `acl.rs:79`, `sweep.rs:73`, `testing.rs:1049` (this change's `low_integrity` module) — plus the two function-scoped sites at `lib.rs:202` and `:227`. The design document is not edited here: it belongs to another workstream, and `specs/001-s3-local-server/tasks.md:160` records this phase's rule for exactly this case (pay no cross-workstream edit; leave the debt where the next reader of the two will find it). The brief that named the list also pointed at a `signal.rs` ledger, which does not exist in this crate. Task 11's report enumerates the same eight file-level sites plus the two, and its count sentence calls the total "nine" — the enumeration is right and that sentence is the slip; the design's "seven" was the tree before this change added `testing.rs:1049`.
- **The linux-target check does not reach `tinio-e2e`.** `cargo check`/`clippy --target x86_64-unknown-linux-gnu` fails for that crate because `psm v0.1.32` needs `x86_64-linux-gnu-gcc`, absent on this host. Task 10's linux-target check for `tinio-cli` passed, so the two crates' dependency graphs differ and neither result generalizes: the unix gating of `child_environment` rests on the reproduced dead-code error plus CI, and macOS was not cross-checked at all.
- **No test pins the `ok` sentence's prose truth — a bounded limit, not a defect.** The row and its test read one shared `HOST_CONFINEMENT_OK` const, so rewriting the const's own text keeps the suite green (design Goal 3 forbids a second copy); Task 10 fix round 3 demonstrated exactly that, and the mutation that fails is the row emitting something else. A text edit there is a reviewed decision, held honest by review rather than by a test.
- **The probe's measured cost** and exactly what was timed are in the design's Risks and Evidence sections (Task 12).

---

### Task 1: The core error variant

**Files:**
- Modify: `crates/tinio-sandbox-core/src/error.rs:76` (insert after `Unsupported`)
- Test: the file's own `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: `tinio_sandbox_core::Error::HostCannotConfine { cause: String, remedy: &'static str, hr: i32 }`, whose `Display` is `{cause} — {remedy}`. Matched by Task 4's probe, by Task 3's call site and by Task 6's `sandbox_err`.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 1: landed as written (reports/task-1-report.md); 82 lib + 89 doc tests, clippy clean.

Add to `crates/tinio-sandbox-core/src/error.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn a_host_refusal_renders_both_halves_and_keeps_the_hresult() {
        let err = Error::HostCannotConfine {
            cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
            remedy: "run in an ordinary user session",
            hr: 0x8007_0005u32 as i32,
        };
        assert_eq!(
            err.to_string(),
            "this token cannot create an AppContainer profile (hr=0x80070005) — run in an ordinary user session"
        );
        assert!(matches!(err, Error::HostCannotConfine { hr, .. } if hr == 0x8007_0005u32 as i32));
    }
```

- [x] **Step 2: Run it and watch it fail** — red as predicted — no variant named `HostCannotConfine`.

Run: `cargo test -p tinio-sandbox-core --lib a_host_refusal_renders`
Expected: FAIL — `no variant or associated item named 'HostCannotConfine'`.

- [x] **Step 3: Write the variant** — the variant landed beside `Unsupported`; no exhaustive match needed an arm (surveyed).

Insert after `Unsupported`'s closing brace (`error.rs:76`):

```rust
    /// This host's token cannot confine this process, though the platform can.
    ///
    /// **The split from [`Self::Unsupported`] is time, not severity.** `Unsupported` is a
    /// compile-time fact about the platform — no mechanism is written for it. This is a
    /// run-time fact about the token this process is running under, and it is the operator's
    /// to fix: the platform is not at fault, and a later run in an ordinary session may not
    /// see it at all.
    ///
    /// **The sentence is the platform's**, because it names an API this crate does not know:
    /// `cause` and `remedy` are rendered where the refusal was measured. The join below is the
    /// same one line `tinio-cli`'s own variant carries — see that variant's comment.
    #[error("{cause} — {remedy}")]
    HostCannotConfine {
        /// The fact and its most likely cause, with the raw HRESULT kept verbatim.
        cause: String,
        /// The two actions the operator can take.
        remedy: &'static str,
        /// The platform's raw HRESULT, for support — the sentence carries it in prose.
        hr: i32,
    },
```

- [x] **Step 4: Run it and watch it pass** — green; the counts are the report's.

Run: `cargo test -p tinio-sandbox-core --lib` and `cargo test -p tinio-sandbox-core --doc`
Expected: PASS. No exhaustive `match` on this type exists in the workspace (surveyed), so nothing else is forced to change; Step 5 confirms it.

- [x] **Step 5: Check the workspace still builds** — `cargo check --workspace --all-targets` clean.

Run: `cargo check --workspace --all-targets`
Expected: PASS. If the compiler names a catch-all that was exhaustive, add the arm beside `Unsupported`'s and give it the same path.

---

### Task 2: The classification and the sentence factory

**Files:**
- Create: `crates/tinio-sandbox-windows/src/capability.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs:56-96` (the `pub mod` list, alphabetical: `acl`, `backend`, `capability`, `control`)
- Test: `capability.rs`'s own `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `tinio_sandbox_core::Error` (Task 1).
- Produces: `pub const E_ACCESSDENIED: i32`, `pub const REMEDY: &'static str`, `pub fn cause(hr: i32) -> String`, `pub fn cannot_confine(hr: i32) -> Error`, `pub fn classify(hr: i32) -> Option<Error>`.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 2: landed (reports/task-2-report.md); fix round 1 replaced the vacuous `contains(REMEDY)` assertion with the whole-sentence `assert_eq!` and pinned the format with a letter-bearing HRESULT (`0x800700B7`); the `0x0007_00B7` narrowing rode into Task 4.

Create `crates/tinio-sandbox-windows/src/capability.rs` with the module doc and only this test module:

```rust
//! Whether **this host** can confine a process, and what to say when it cannot.
//!
//! **This is the run-time half of a question two other answers already own.** The build half
//! is the facade's `backend_available()`, a `const fn` over the target list; the failure
//! half is a bare HRESULT an operator cannot act on. What lives here is the third answer:
//! the platform supports confinement, and this token does not get to use it.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_access_denied_is_a_capability_answer() {
        // The one HRESULT that means "this token may not".
        assert!(classify(E_ACCESSDENIED).is_some());
        // `ERROR_PATH_NOT_FOUND` is a concurrent create inside `userenv`
        // (`backend/tests.rs`), and a transient one: caching it would declare a
        // healthy machine permanently incapable.
        assert!(classify(0x8007_0003u32 as i32).is_none());
        // Success is not a refusal, and neither is an unknown code.
        assert!(classify(0).is_none());
        assert!(classify(0x8007_00B7u32 as i32).is_none());
    }

    #[test]
    fn the_refusal_names_the_code_and_the_two_actions() {
        let err = classify(E_ACCESSDENIED).expect("E_ACCESSDENIED is the capability answer");
        // **The whole sentence as one literal.** `contains(REMEDY)` would pass with `REMEDY`
        // rewritten — the text is rendered *from* it — so the remedy half was pinned nowhere.
        assert_eq!(
            err.to_string(),
            "this token cannot create an AppContainer profile (hr=0x80070005), so no confined process could be started — run in an ordinary user session, and check the Low mandatory label on the tree the binary was built in"
        );
        // And `0x80070005` has no A-F digit and is already eight digits wide, so it renders the
        // same under `{:08X}`, `{:X}` and `{:x}`. Pinning the format needs a value that shows
        // both its case **and** its width, and only a value shorter than eight digits shows the
        // width: `{:8X}` and `{:X}` both render `0x0007_00B7` as `700B7`.
        assert!(
            cause(0x0007_00B7u32 as i32).ends_with("(hr=0x000700B7), so no confined process could be started"),
            "the HRESULT is not rendered as the brief's {{:08X}}"
        );
        assert!(
            matches!(err, Error::HostCannotConfine { hr, .. } if hr == E_ACCESSDENIED),
            "{err}"
        );
    }
}
```

- [x] **Step 2: Run it and watch it fail** — red as predicted — `cannot find function 'classify'`.

Run: `cargo test -p tinio-sandbox-windows --lib capability::`
Expected: FAIL — `cannot find function 'classify' in this scope`.

- [x] **Step 3: Write the implementation** — the factory landed: `E_ACCESSDENIED`, `REMEDY`, `cause`, `cannot_confine`, `classify`.

Above the test module, and add `pub mod capability;` to the `lib.rs` list:

```rust
use tinio_sandbox_core::Error;

/// `E_ACCESSDENIED`: the token is not allowed to do this. **The only HRESULT this module
/// treats as a capability answer** — see `classify`.
pub const E_ACCESSDENIED: i32 = 0x8007_0005u32 as i32;

/// The two actions a refused operator has, as one sentence.
///
/// **The second field of the core variant, and the tail of the sentence the CLI prints as
/// `{cause} — {remedy}`.** No `host_cannot` caller interpolates it (the plan-grilling ruling,
/// round 2, Q10): that function appends its third argument with no separator
/// (`tinio-util/src/testing.rs:2946-2953`), so each site keeps a pointer at its own subject
/// instead.
pub const REMEDY: &str = "run in an ordinary user session, and check the Low mandatory label on the tree the binary was built in";

/// The fact and its most likely cause, with the HRESULT kept verbatim.
pub fn cause(hr: i32) -> String {
    format!(
        "this token cannot create an AppContainer profile (hr=0x{:08X}), so no confined process could be started",
        hr as u32
    )
}

/// The error a capability refusal raises, built from the two halves above.
pub fn cannot_confine(hr: i32) -> Error {
    Error::HostCannotConfine {
        cause: cause(hr),
        remedy: REMEDY,
        hr,
    }
}

/// The verdict an HRESULT gets: the error it raises, or `None` when it is not a capability
/// answer at all.
///
/// **The narrowness is the point.** A greedy predicate turns a transient race into a
/// permanent claim about the machine, which is worse than the bare HRESULT it replaces.
pub fn classify(hr: i32) -> Option<Error> {
    (hr == E_ACCESSDENIED).then(|| cannot_confine(hr))
}
```

- [x] **Step 4: Run it and watch it pass** — green; the fix's mutation evidence is the ledger's (progress.md, "Task 2: mutation evidence for the fix").

Run: `cargo test -p tinio-sandbox-windows --lib capability::`
Expected: PASS (2 tests).

---

### Task 3: The real call site classifies, and says so in the log

**Files:**
- Modify: `crates/tinio-sandbox-windows/src/sid.rs:1207-1223` (both `hr != 0` arms of `derive`)
- Test: `crates/tinio-sandbox-windows/src/sid.rs`'s `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `crate::capability::classify` (Task 2).
- Produces: `PackageSid::derive` answers `Error::HostCannotConfine` for `E_ACCESSDENIED` on either Win32 call and emits `step="host cannot confine"`; every other HRESULT keeps its `Error::Launch`.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 3: landed as Step 3 verbatim (reports/task-3-report.md); fix round 1 added the event assertion, the extraction guard and the third structural assertion.

```rust
    #[test]
    fn a_denied_profile_creation_classifies_rather_than_reports_a_bare_hr() {
        // `capability::`, not `crate::capability::`: this crate's `[lints.clippy]` sets
        // `absolute_paths`, and `-D warnings` turns the longer spelling into a build failure.
        let err = capability::classify(capability::E_ACCESSDENIED).expect("E_ACCESSDENIED classifies");
        assert!(matches!(err, Error::HostCannotConfine { .. }), "{err}");
        // And the code that must NOT classify keeps the old path.
        assert!(capability::classify(0x8007_0003u32 as i32).is_none());
        // **The call site itself is pinned by reading it, because nothing else can.** No test
        // forces `userenv` to answer `E_ACCESSDENIED` on a host that works, so the properties
        // this name claims — a denied creation classifies rather than reporting a bare HRESULT,
        // and the refusal says so in the log — are properties of the source.
        let source = include_str!("sid.rs");
        let derive = source
            .split("    pub fn derive(name: &str) -> Result<Self> {")
            .nth(1)
            .expect("derive is in this file")
            .split("\n    pub fn ")
            .next()
            .expect("derive's body ends where the next method begins");
        // The extraction is pinned before it is used, so a rename that made the split hand back
        // the rest of the file fails here rather than counting the wrong region.
        assert!(
            derive.contains("let taken_over = hr == HRESULT_ALREADY_EXISTS;"),
            "the extraction found derive's body"
        );
        assert_eq!(
            derive.matches("capability::classify(hr)").count(),
            2,
            "both of derive's `hr != 0` arms classify"
        );
        // **The log half is pinned too.** Without this the whole `warn!` block can be deleted
        // with the suite green, and the step line is the surface the suites read.
        assert!(
            derive.contains("step = \"host cannot confine\""),
            "the refusal emits the step event"
        );
    }
```

- [x] **Step 2: Run it and watch it fail** — E0433, `cannot find type Error` — the compile-time missing-name class, not the brief's import class (progress.md).

Run: `cargo test -p tinio-sandbox-windows --lib denied_profile_creation`
Expected: FAIL to compile until `use crate::capability;` is in scope — that is the failing state for this step.

- [x] **Step 3: Classify both arms** — both `hr != 0` arms classify; the `step = "host cannot confine"` event stays on the create arm only (the ledger's ruling).

The `DeriveAppContainerSidFromAppContainerName` arm (`sid.rs:1208-1216`) becomes:

```rust
            if hr != 0 {
                return Err(capability::classify(hr).unwrap_or_else(|| {
                    launch(
                        "DeriveAppContainerSidFromAppContainerName",
                        name,
                        io::Error::other(format!(
                            "DeriveAppContainerSidFromAppContainerName: hr=0x{hr:08X}"
                        )),
                    )
                }));
            }
```

and the `CreateAppContainerProfile` arm (`sid.rs:1217-1223`) gains the classification **and** the event:

```rust
        } else if hr != 0 {
            // **The floor.** Whatever a preflight said, the call that actually fails classifies
            // its own HRESULT, so an operator on a host no preflight anticipated still reads a
            // cause and two actions rather than a bare code.
            if let Some(err) = capability::classify(hr) {
                tracing::warn!(
                    step = "host cannot confine",
                    name = name,
                    hr = hr,
                    "the token cannot create an AppContainer profile; the refusal names the cause and the remedy"
                );
                return Err(err);
            }
            return Err(launch(
                "CreateAppContainerProfile",
                name,
                io::Error::other(format!("CreateAppContainerProfile: hr=0x{hr:08X}")),
            ));
        }
```

- [x] **Step 4: Run the tests** — green; 123 lib tests, clippy clean.

Run: `cargo test -p tinio-sandbox-windows --lib`
Expected: PASS, including the existing `sid::tests` legs that assert the creation and deletion events — the new event is emitted only on the refusal path, which no existing leg reaches.

- [x] **Step 5: Confirm the event name is unique** — unique assignment confirmed: `sid.rs` is the only assigner; the phrase survives in three pre-existing prose lines, as the plan's note says.

Run: `rg 'step = "host cannot confine"' crates/`
Expected: `sid.rs` only. **Match the assignment, not the phrase.** The bare phrase already appears in three pre-existing prose lines that are not event names (`tinio-sandbox-linux/tests/jail.rs:51`, `tinio-sandbox-linux/src/testing.rs:100`, `tinio-server/tests/sandbox_e2e.rs:74`), so it cannot answer this question; a second *assignment* is what would mean a second author of the sentence.

---

### Task 4: The probe, the cache and `can_confine()`

**Files:**
- Modify: `crates/tinio-sandbox-windows/src/capability.rs` (append)
- Test: `capability.rs`'s own `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `crate::sid::PackageSid` (Task 3's `derive`).
- Produces: `pub(crate) const PROBE_NAME: &'static str`, `pub(crate) enum Probe { Confined, Cannot(i32) }` (both `pub(crate)`, not `pub`: nothing outside `capability.rs` consumes them, and Constitution I makes an unused public item a must-fix; `can_confine` and `REMEDY` stay `pub`, and `can_confine` carries the runnable doctest example Constitution III requires) (`Clone, Copy, Debug, PartialEq, Eq`), `pub fn can_confine() -> Result<(), Error>`. **No test hook** (the plan-grilling ruling): the first draft carried a `testing`-featured `override_probe` and nothing could consume it — `tinio-cli` has no dev-dependency on this crate at all. The predicate's behavioural proof is Task 11's Low-integrity doctor scenario; its call site's is Task 7's structural test.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 4: landed (reports/task-4-report.md); fix round 1 narrowed `Probe`/`PROBE_NAME` to `pub(crate)` and added `can_confine`'s doctest.

Append to `capability.rs`'s test module:

```rust
    #[test]
    fn the_probe_name_is_its_own_and_sweep_eligible() {
        // Sweep-eligible: the reaper's candidate filter is `PACKAGE_PREFIX`.
        // `PACKAGE_PREFIX` and `io` are imported **inside this test module**: the crate's clippy
        // sets `absolute_paths`, so `crate::sid::PACKAGE_PREFIX` and `std::io::Error` are build
        // failures under `-D warnings` even though they are correct prose.
        assert!(PROBE_NAME.starts_with(PACKAGE_PREFIX));
        // Its own: never a root's derived name, which is the prefix plus 16 hex digits.
        assert_eq!(PROBE_NAME, "tinio.sandbox.capability-probe");
        let leaf = PROBE_NAME.trim_start_matches(PACKAGE_PREFIX);
        assert!(
            !leaf.chars().all(|c| c.is_ascii_hexdigit()),
            "a hex leaf would collide with a real root's name"
        );
    }

    #[test]
    fn only_a_definitive_answer_is_cached() {
        // A fresh cache per test: the production one is a process-wide static.
        let confined = Cache::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = confined.get_or_probe(|| {
                calls += 1;
                Ok(Probe::Confined)
            });
            assert!(answer.is_ok());
        }
        assert_eq!(calls, 1, "a definitive answer is probed once");

        let refused = Cache::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = refused.get_or_probe(|| {
                calls += 1;
                Ok(Probe::Cannot(E_ACCESSDENIED))
            });
            assert!(matches!(answer, Err(Error::HostCannotConfine { .. })));
        }
        assert_eq!(calls, 1, "a definitive refusal is probed once");

        let unknown = Cache::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = unknown.get_or_probe(|| {
                calls += 1;
                Err(tinio_sandbox_core::fail(
                    "probe",
                    io::Error::from_raw_os_error(3),
                ))
            });
            // **`Ok`, not `Err`.** An HRESULT `classify` does not recognise is not a verdict,
            // so the call proceeds and stores nothing — refusing here would turn a transient race
            // into a permanent claim about a machine that works (see `get_or_probe`). The
            // counter below is the assertion that matters: nothing was remembered.
            assert!(answer.is_ok(), "an unrecognised failure is not a refusal");
        }
        assert_eq!(calls, 5, "an unrecognised failure is never remembered");
    }
```

- [x] **Step 2: Run it and watch it fail** — red as predicted — `cannot find type 'Cache'`.

Run: `cargo test -p tinio-sandbox-windows --lib capability::`
Expected: FAIL — `cannot find type 'Cache'` / `cannot find value 'PROBE_NAME'`.

- [x] **Step 3: Write the probe and the cache** — the probe, cache and predicate landed; fix round 2 put `probe_once` under a thread-local no-op dispatcher so a run's log cannot carry the probe's profile.

Add `use crate::sid::{PackageSid, ProfileLock};` to the file's imports, then below `classify`:

```rust
/// The profile the probe creates and deletes. **Its own name, and that is a requirement
/// rather than a convenience.**
///
/// `package_name(root)` derives a real root's exact profile name, so a probe using it would
/// take over a live run's profile and its `Drop` would then delete it — and `package_name`
/// also reads or creates `%LOCALAPPDATA%\tinio\acl-salt`, a side effect a capability answer has
/// no business having. A fixed name under `PACKAGE_PREFIX` is instead reaped by the
/// stale-profile sweep if a crash orphans it, and cannot collide with a root's derived
/// `<prefix><16 hex>` name — the sweep's filter is a bare `starts_with(PACKAGE_PREFIX)` on both
/// of its listings (`sweep.rs:393-401`, `:475-478`) with no hex-shape test at all, so this
/// non-hex leaf is still a candidate. The sweep is skipped while another `tinio` is alive
/// (`sweep.rs:298-300`), so a crashed probe's residue is reaped by a later process.
pub const PROBE_NAME: &str = "tinio.sandbox.capability-probe";

/// What one probe answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// The profile was created; this process can confine.
    Confined,
    /// The creation was refused for the reason that is this host's, not this run's.
    Cannot(i32),
}

/// The verdict, once per process — **and only when it is definitive**.
///
/// A token does not change under a running process, so `Ok` and `E_ACCESSDENIED` are answers
/// worth keeping. An HRESULT `classify` does not recognise is not an answer at all: **nothing is
/// stored and the answer is `Ok`** — see `get_or_probe` for why refusing there would be worse
/// than the bare HRESULT it replaces.
#[derive(Default)]
struct Cache {
    verdict: std::sync::OnceLock<Probe>,
}

impl Cache {
    /// A cell that has not probed yet. `OnceLock::new` is const, which is what lets the
    /// process-wide `VERDICT` below be a `static`; `Default` stays derived for the tests, which
    /// build caches the same way.
    const fn new() -> Self {
        Self {
            verdict: std::sync::OnceLock::new(),
        }
    }

    /// The cached verdict, or this probe's — defaulting its cost to once.
    ///
    /// **A probe that cannot answer lets the run through.** Answering `Err` here would make the
    /// gate refuse a host whose only problem was a transient race inside `userenv`, and a false
    /// negative on a machine that works is worse than the bare HRESULT this change exists to
    /// replace. Nothing is lost by proceeding: the real `CreateAppContainerProfile` classifies
    /// its own HRESULT, so a host that truly cannot confine still gets the cause and the two
    /// remedies — one step later, after the state directory. The `warn` is what makes the
    /// degradation visible instead of silent.
    fn get_or_probe(&self, probe: impl FnOnce() -> Result<Probe, Error>) -> Result<(), Error> {
        if let Some(cached) = self.verdict.get() {
            return verdict(*cached);
        }
        match probe() {
            Ok(probed) => {
                let _ = self.verdict.set(probed);
                verdict(probed)
            }
            Err(err) => {
                tracing::warn!(
                    step = "host cannot confine",
                    error = %err,
                    "the capability probe could not answer, so this run proceeds and the real profile creation will report any refusal"
                );
                Ok(())
            }
        }
    }
}

/// The verdict, as the predicate's answer.
fn verdict(verdict: Probe) -> Result<(), Error> {
    match verdict {
        Probe::Confined => Ok(()),
        Probe::Cannot(hr) => Err(cannot_confine(hr)),
    }
}

/// The process-wide verdict.
static VERDICT: Cache = Cache::new();

/// Whether **this process's token** can create an AppContainer profile.
///
/// **Host-level and process-scoped**: it is asked about the token, not about a root, a path or
/// an access mode, so its answer is one per process rather than one per run.
///
/// # Errors
///
/// `Error::HostCannotConfine` when the token is refused. Any other failure is the probe's own:
/// it is logged at `warn`, nothing is cached, and the answer is `Ok` — a probe that cannot
/// answer must not refuse a host that works.
pub fn can_confine() -> Result<(), Error> {
    VERDICT.get_or_probe(probe_once)
}

/// One probe: create the throwaway profile and drop it.
///
/// `PackageSid`'s lifetime **is** create-then-delete, so this introduces no new side-effect
/// class and leaves no residue on the success path.
fn probe_once() -> Result<Probe, Error> {
    // **Best-effort, and held across the create *and* the delete** (the plan-grilling ruling).
    // The name is fixed, so without it two processes probing at once interleave: the second sees
    // `ALREADY_EXISTS` and takes the profile over, the first's `Drop` deletes it, and the
    // second's `Drop` then reports a deletion for a profile that had existed moments earlier —
    // `DeleteAppContainerProfile` answers `S_OK` for an absent name, so that arm is the
    // `absent = true` **info**, not a warn (`sid.rs:1339-1376`, pinned by `sid.rs:2236-2269`):
    // the log says "deleted" about someone else's probe. A third process can also catch a
    // transient `0x80070003` in the window. **Only the log and that window need the lock, so a
    // lock that cannot be created does not block the answer.** Re-entrant on the owning thread
    // (`sweep.rs:998-1001` states the rule), so `PackageSid`'s own `Drop`, which acquires the
    // same name, counts up rather than deadlocking.
    let _lock = ProfileLock::acquire(PROBE_NAME);
    match PackageSid::derive(PROBE_NAME) {
        Ok(sid) => {
            drop(sid);
            Ok(Probe::Confined)
        }
        Err(Error::HostCannotConfine { hr, .. }) => Ok(Probe::Cannot(hr)),
        Err(other) => Err(other),
    }
}

/// **There is no test hook, and that is the ruling rather than an omission.** The first draft
/// carried a `testing`-featured `override_probe`; it had no possible consumer —
/// `tinio-cli` does not dev-depend on this crate at all — and the `OnceLock` verdict would
/// have made a forced refusal order-dependent inside a shared test binary. What is exercised
/// instead is the real thing: Task 11 runs `tinio doctor` under a genuinely Low token, where
/// this predicate answers `Cannot(E_ACCESSDENIED)` on its own.
```

- [x] **Step 4: Run it and watch it pass** — green; 127 lib + 91 doc tests (the ledger's later counts).

Run: `cargo test -p tinio-sandbox-windows --lib capability::`
Expected: PASS (4 tests) — Task 2 left two and Step 1 adds two.

- [x] **Step 5: Run the probe against this host once and look for residue** — fix round 3 added the clear-before-create; the wave-E round made the clear best-effort and let every unrecognised outcome fall through to the create (reports/task-4-fix3-report.md, reports/wave-e-fix-report.md).

Run: `cargo test -p tinio-sandbox-windows --lib capability::`, then
`Get-ChildItem "$env:LOCALAPPDATA\Packages" -Filter "tinio.sandbox.*"` before and after.
Expected: no `capability-probe` directory remains. This is the residue check the design's Risks section names, and the reason the probe uses the type's own `Drop`.

---

### Task 5: The facade predicate

**Files:**
- Modify: `crates/tinio-sandbox/src/lib.rs` (insert at the blank line 700, between `backend_available`'s end at `:699` and the re-export rule comment at `:701`)
- Modify: `crates/tinio-sandbox/tests/facade.rs` (bind the new public name, per `lib.rs:701-715`'s rule)
- Test: `crates/tinio-sandbox/src/lib.rs`'s `#[cfg(test)] mod tests` and `tests/facade.rs`

**Interfaces:**
- Consumes: `crate::_windows::capability::can_confine` (Task 4), reachable as `tinio_sandbox::windows::capability::can_confine` (the `pub use crate::_windows as windows;` alias at `lib.rs:894-895`).
- Produces: `tinio_sandbox::can_confine() -> Result<(), tinio_sandbox::Error>`, present on all three targets.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 5: landed (reports/task-5-report.md); fix round 1 added a runnable example per cfg arm and rewrote the facade comment (reports/task-5-fix1-report.md).

In `crates/tinio-sandbox/src/lib.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn the_host_predicate_answers_on_every_target() {
        // The facade's surface has to exist where no AppContainer does: a consumer that
        // compiles only on Windows is not a facade.
        let answer = tinio_sandbox::can_confine();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(answer.is_ok(), "unix platforms confine in place or by profile of their own");
        #[cfg(target_os = "windows")]
        let _ = answer;
    }
```

and in `crates/tinio-sandbox/tests/facade.rs`, beside `the_vocabulary_is_re_exported` (`:38`) and `the_launcher_surface_is_re_exported` (`:91`):

```rust
/// The host predicate is the facade's own name, not a core re-export, so it is bound here the
/// way `backend_available` is — the rule at `lib.rs:701-715` is that every public name a
/// consumer may call is nailed down by a test rather than discovered three phases later.
#[test]
fn the_host_predicate_is_published() {
    let _: fn() -> Result<(), tinio_sandbox::Error> = tinio_sandbox::can_confine;
}
```

- [x] **Step 2: Run them and watch them fail** — red as predicted — E0425 at lib.rs:1094.

Run: `cargo test -p tinio-sandbox --lib host_predicate`
Expected: FAIL — `cannot find function 'can_confine' in crate 'tinio_sandbox'`.

- [x] **Step 3: Write the predicate** — the predicate landed on all three targets; its three lint-forced spellings are the ledger's deviations.

```rust
/// Whether **this host** can confine a process, as the platform answers it.
///
/// **The run-time half of the question [`backend_available`] answers at build time.** That one
/// is a `const fn` over the target list and says the backend has landed; this one asks the
/// token this process is actually running under, and on Windows that means creating a
/// throwaway AppContainer profile and deleting it.
///
/// **It cannot be `const`**, unlike its two siblings (`platform_facts`, `diagnostics`): the
/// answer is the machine's, not the build's. **The unix arm is `Ok(())` rather than an
/// absence**: macOS's Seatbelt is an SBPL profile applied to this process and Linux's jail
/// applies in place, so neither has a host-level precondition to fail. The name exists on
/// every target so a consumer compiles and answers one question rather than two.
///
/// # Errors
///
/// `Error::HostCannotConfine` on Windows when this token is refused. The fact's owner is
/// `docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md`.
#[cfg(target_os = "windows")]
pub fn can_confine() -> Result<(), Error> {
    crate::_windows::capability::can_confine()
}

/// Whether **this host** can confine a process — see the Windows arm's doc.
#[cfg(not(target_os = "windows"))]
pub fn can_confine() -> Result<(), Error> {
    Ok(())
}
```

- [x] **Step 4: Run them and watch them pass** — green; the reviewer re-ran 7 lib and 6 facade tests and reproduced the three lint-forced deviations for linux and macOS with scratch rustc/clippy-driver (progress.md).

Run: `cargo test -p tinio-sandbox --lib` and `cargo test -p tinio-sandbox --test facade`
Expected: PASS.

---

### Task 6: The CLI's variant and its mapping

**Files:**
- Modify: `crates/tinio-cli/src/error.rs` (insert after `SandboxDenied`, `:87`)
- Modify: `crates/tinio-cli/src/sandbox/mod.rs:177-196` (`sandbox_err`)
- Test: `crates/tinio-cli/src/error.rs`'s `displays_variants` and `crates/tinio-cli/src/sandbox/tests.rs:884-907`

**Interfaces:**
- Consumes: `_sandbox::Error::HostCannotConfine { cause, remedy, hr }`.
- Produces: `Error::SandboxHostCannotConfine { cause: String, remedy: &'static str }`, exit 1; `sandbox_err` maps the core variant onto it instead of letting it fall through to `Error::from` (`Error::Sandbox`, which would flatten the classification).

- [x] **Step 1: Write the failing tests** — 2026-10-01, Task 6: landed (reports/task-6-report.md); fix round 1 added `the_host_refusal_renders_the_same_sentence_on_both_sides`, the only place both renderings meet (reports/task-6-fix1-report.md).

In `error.rs`'s tests, beside the existing display assertions (`:139-172`):

```rust
    #[test]
    fn a_host_that_cannot_confine_is_operational_and_keeps_its_two_halves() {
        let err = Error::SandboxHostCannotConfine {
            cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
            remedy: "run in an ordinary user session",
        };
        assert_eq!(err.exit_code(), 1);
        assert_eq!(
            err.to_string(),
            "this token cannot create an AppContainer profile (hr=0x80070005) — run in an ordinary user session"
        );
    }
```

and in `crates/tinio-cli/src/sandbox/tests.rs`, beside the leg that pins `Error::Unsupported` → `Error::Sandbox` (`:884-907`):

```rust
    #[test]
    fn a_host_refusal_keeps_its_classification_rather_than_flattening() {
        // The `Unsupported` leg beside this one pins the opposite outcome on purpose: a
        // platform fact flattens into `Sandbox`, a host fact does not, because the remedy is
        // the operator's and the caller may want to know which it was.
        let mapped = sandbox_err(_sandbox::Error::HostCannotConfine {
            cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
            remedy: "run in an ordinary user session",
            hr: 0x8007_0005u32 as i32,
        });
        assert!(
            matches!(mapped, Error::SandboxHostCannotConfine { .. }),
            "{mapped}"
        );
        assert_eq!(mapped.exit_code(), 1);
    }

    /// **The join is one line in two crates, and this is the only place both renderings meet.**
    ///
    /// The plan's Global Constraints make the core's `{cause} — {remedy}` and the CLI's the same line
    /// on purpose ("change one, change both"), and each crate's own test pins only its own. Nothing
    /// fails when the two drift, and nothing else pins that `sandbox_err` carries the fields at all:
    /// the leg above asserts the shape, not the content.
    #[test]
    fn the_host_refusal_renders_the_same_sentence_on_both_sides() {
        let core = _sandbox::Error::HostCannotConfine {
            cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
            remedy: "run in an ordinary user session",
            hr: 0x8007_0005u32 as i32,
        };
        let core_rendered = core.to_string();

        let mapped = sandbox_err(core);

        assert_eq!(
            mapped.to_string(),
            core_rendered,
            "the CLI's join and the core's are the same line (the plan's Global Constraints)"
        );
        assert!(
            matches!(mapped, Error::SandboxHostCannotConfine { .. }),
            "{mapped}"
        );
    }
```

- [x] **Step 2: Run them and watch them fail** — red as predicted — E0599, `no variant named 'SandboxHostCannotConfine'`.

Run: `cargo test -p tinio-cli --lib a_host_that_cannot_confine a_host_refusal_keeps`
Expected: FAIL — `no variant named 'SandboxHostCannotConfine'`; once the variant exists the second test still fails on the `matches!`, because `sandbox_err` flattens it.

- [x] **Step 3: Write the variant and the arm** — the variant and the `sandbox_err` arm landed; the core's join and the CLI's are the same line.

In `error.rs`, after `SandboxDenied`:

```rust
    /// This host's token cannot confine this process, though the platform can — exit code 1.
    ///
    /// **The classification is a value, and its two halves are the sentence's.** `cause` and
    /// `remedy` arrive already rendered from the platform that measured the refusal
    /// (`tinio-sandbox-windows`'s `capability` module), so the text the operator reads and the
    /// text a skipped suite prints have one author. **The join below is the same one line the
    /// core's `HostCannotConfine` carries** — the content has one author, this is its spelling.
    ///
    /// **It does not join [`Self::SandboxBroken`] or [`Self::SandboxDenied`]**: `Broken` says
    /// the mechanism never applied and `Denied` says it applied and refused an operation,
    /// while this says the mechanism **could not be applied on this host** — the operator's to
    /// fix rather than the request's.
    #[error("{cause} — {remedy}")]
    SandboxHostCannotConfine {
        /// The fact and its most likely cause, HRESULT included, in the platform's words.
        cause: String,
        /// The two actions, in the platform's words.
        remedy: &'static str,
    },
```

In `sandbox/mod.rs`'s `sandbox_err`, add the arm before the `other =>` fallthrough:

```rust
        // The host's own answer, kept as a value rather than flattened into the generic
        // `Sandbox`: what a caller does about it is not what it does about a step failure.
        _sandbox::Error::HostCannotConfine { cause, remedy, .. } => {
            Error::SandboxHostCannotConfine { cause, remedy }
        }
```

- [x] **Step 4: Run them and watch them pass** — green; 108 lib tests on the fixed bytes.

Run: `cargo test -p tinio-cli --lib`
Expected: PASS.

---

### Task 7: The CLI gate, and the contract it joins

**Files:**
- Modify: `crates/tinio-cli/src/sandbox/plan.rs:301-345` (`refuse_unhonourable`'s doc and body), plus the three stale counts at `:103`, `:263-267` and `:301-303`
- Modify: `specs/001-s3-local-server/contracts/cli.md:49` and `:60`
- Test: `crates/tinio-cli/src/sandbox/tests.rs` — **`plan.rs` has no test module of its own** (it is 345 lines with no `mod tests`; the CLI's sandbox unit tests live in `tests.rs`, declared at `mod.rs:39-40`, whose `use super::*;` reaches both `plan.rs`'s private items through the parent and `sandbox_err`). `include_str!("plan.rs")` from there resolves to `plan.rs` beside it.

**Interfaces:**
- Consumes: `_sandbox::can_confine()` (Task 5) and `sandbox_err` (Task 6).
- Produces: the third refusal in `refuse_unhonourable`, reached from `plan()` — called at `run.rs:147-155`, before `create_dir_all` at `run.rs:195-201`.

**Why here and not a fourth call site:** `refuse_unhonourable` is already "the fail-closed half of `plan`", the one function every confinement request passes through before anything is written. Adding the fact to it is what gives the ordering guarantee; a new call site beside `create_dir_all` would be a second place the same question is asked.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 7: landed (reports/task-7-report.md).

Append to `crates/tinio-cli/src/sandbox/tests.rs`:

```rust
    #[test]
    fn the_hosts_own_answer_is_a_third_refusal_and_not_a_fourth_path() {
        // **Layer one, and it is the whole of this seam's proof** (the plan-grilling ruling).
        // The workspace's own precedent for a structural test is
        // `crates/tinio-server/src/backend/ops.rs:287-310`.
        //
        // The guarantee is that the three facts are asked in ONE function, so a request that
        // reaches `confine` is one this build, this target and this host all committed to
        // applying. **No seam exists to force the facade's answer** — `backend_available` is a
        // `pub const fn` over `cfg!`, and `tinio-cli` has no dev-dependency on the Windows crate
        // — so the predicate's behaviour is proven where it is real (Task 11's Low-integrity
        // doctor scenario) and this test binds the call site. A text search is not a behaviour,
        // and the plan says so rather than implying otherwise.
        let source = include_str!("plan.rs");
        let body = source
            .split("fn refuse_unhonourable")
            .nth(1)
            .expect("refuse_unhonourable is in this file");
        assert_eq!(
            body.matches("can_confine()").count(),
            1,
            "the host fact is asked once, beside the build and target facts"
        );
        assert!(body.contains("backend_available()"), "and the target fact stays");
    }
```

- [x] **Step 2: Run it and watch it fail** — red as predicted.

Run: `cargo test -p tinio-cli --lib hosts_own_answer`
Expected: FAIL — `left: 0, right: 1`.

- [x] **Step 3: Add the arm and correct the counts** — the third refusal landed in `refuse_unhonourable`; fix round 1 corrected the stale two-fact counts and their "both"/"neither" siblings (reports/task-7-fix1-report.md).

In `refuse_unhonourable`'s `#[cfg(feature = "sandbox")]` block, the `else` branch becomes:

```rust
        } else {
            // **The third fact, and the last this build can answer before it writes anything.**
            // The target has a backend; the token may still be refused one, and on a host where
            // it is, every later step — the state directory, the served root, the listener, the
            // stale-profile sweep, the ACL salt file and the profile itself — is work done for a
            // run that cannot serve. Asking here is what makes the operator read a cause and two
            // actions instead of a `PermissionDenied` on a temp directory.
            //
            // **The ask is the facade's, exactly as `backend_available` is**: the platform that
            // can answer it is the one that owns the probe.
            _sandbox::can_confine().map_err(super::sandbox_err)?;
            Ok(())
        }
```

Correct the three comments that now state a stale count: `:301-303` ("The two facts" → "The three facts"), `:103` ("cleared both the feature and the target" → "cleared the feature, the target and the host"), `:263-267` ("a request this target can honour" → "a request this build, this target and this host can honour").

In `contracts/cli.md:49`, extend the refusal prose with the third class:

```markdown
A confinement request the build or the platform cannot honour is **refused, never ignored**. A request the **host's own token** cannot honour is refused the same way: the cause and two remedies are printed rather than a raw HRESULT, and nothing is written first — the answer's owner is `docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md`.
```

and add its clause to the `Errors (exit 1)` list at `contracts/cli.md:60`:

```markdown
- Errors (exit 1): port in use; control channel bind failure after stale-socket reclaim (second live instance on same root); `api` feature present but every management transport disabled; unreadable storage root; storage root not writable without `--read-only`; invalid config; the host's token cannot create the AppContainer profile a `--sandbox` run needs.
```

- [x] **Step 4: Run it and watch it pass** — green; 108 lib tests.

Run: `cargo test -p tinio-cli --lib`
Expected: PASS.

- [x] **Step 5: Prove the refusal precedes what it can precede** — the gate's limit is the ledger's: on a first run the resolution above it writes first (Review Focus).

Run: `cargo test -p tinio-cli --lib`, then `tinio start <root> --sandbox` by hand on a host that cannot confine.
Expected: the two-part sentence rather than a bare HRESULT, and no state directory minted **when the root already holds a `.tinio.toml`** — that is when the run reaches the gate. **Do not expect an untouched filesystem on a first run**: `resolve::server` creates the state directory and writes the config above the gate (`tinio-config/src/discover.rs:139-159`), so there the operator's first error is `PermissionDenied` from the resolution. That is the limit Review Focus records and the operator accepted in writing; this step proves what the gate covers, and no more.

---

### Task 8: The library gate

**Files:**
- Modify: `crates/tinio-sandbox-windows/src/backend/mod.rs:228-230`
- Test: `crates/tinio-sandbox-windows/src/backend/tests.rs`

**Interfaces:**
- Consumes: `crate::capability::can_confine()` (Task 4).
- Produces: `Backend::confine` asks the predicate **before `prepare_launch`**, so no sweep, no salt file, no mutex and no profile precede the refusal.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 8: landed (reports/task-8-report.md); fix rounds 1-5 hardened the structural test's anchor to the call's own line (reports/task-8-fix5-report.md).

```rust
    #[test]
    fn the_launcher_asks_the_host_before_it_prepares_anything() {
        // Layer one, for the CLI gate's reason: `Backend::confine` is reached by embedders the
        // CLI does not own, so the order is bound here rather than through a caller.
        let source = include_str!("mod.rs");
        let body = source
            .split("fn confine(")
            .nth(1)
            .expect("Backend::confine is in this file");
        let gate = body.find("can_confine()").expect("the host gate is in this body");
        let prepare = body
            .find("prepare_launch")
            .expect("prepare_launch is in this body");
        assert!(gate < prepare, "the gate must precede the crate's first side effect");
    }
```

- [x] **Step 2: Run it and watch it fail** — red as predicted — "the host gate is in this body".

Run: `cargo test -p tinio-sandbox-windows --lib launcher_asks_the_host`
Expected: FAIL — `the host gate is in this body`.

- [x] **Step 3: Write the gate** — the gate landed at the head of `Backend::confine`, above `prepare_launch`; fix round 1 dropped the `#[allow(absolute_paths)]` for an import.

At the head of `Backend::confine` (`mod.rs:228`), before `prepare_launch`:

```rust
        // **The host's own gate, and it sits above everything this crate does.** The CLI asks the
        // same question before it writes its state directory; this one protects every other
        // embedder of this crate, and keeps the stale-profile sweep, the ACL salt file, the
        // profile mutex and the profile itself from happening for a run that cannot be confined.
        crate::capability::can_confine()?;
```

- [x] **Step 4: Run the tests** — green; 32 backend tests.

Run: `cargo test -p tinio-sandbox-windows --lib backend::`
Expected: PASS. The whole suite is the regression guard: every confined-launch leg now goes through the gate on a host that answers `Confined`.

- [x] **Step 5: Run the crate** — green; 127 lib tests for the crate.

Run: `cargo test -p tinio-sandbox-windows`
Expected: PASS.

---

### Task 9: The test harness asks the predicate

**Files:**
- Modify: `crates/tinio-server/tests/support/windows_e2e_harness.rs:601-615` (`host_can_confine`)
- Test: the harness's own legs, plus `crates/tinio-server/tests/sandbox_e2e_windows.rs`

**Scope: this one site, and not the six others** (the plan-grilling ruling). The facts: `host_cannot(what: &str, reason: &str, remedy: &str)` (`tinio-util/src/testing.rs:2946-2953`) prints `remedy` as a **verbatim suffix** of its sentence, and the seven call sites are `sweep.rs:1098`, `backend/tests.rs:649`, `:1313`, `:1574`, `sandbox_e2e.rs:86`, `windows_e2e_harness.rs:559` and `jail.rs:67`. Of those, only `windows_e2e_harness.rs:559` is about this host fact; the rest are a symlink privilege, a reachable address, a LAN address, an unavailable Linux/WSL2 model and a live peer process — each with a remedy that is about *its own* precondition. Giving them this sentence would replace a specific remedy with an unrelated one, so they keep theirs.

**Interfaces:**
- Consumes: `tinio_sandbox::windows::capability::can_confine()` (Task 4, through the facade's `pub use crate::_windows as windows;` at `tinio-sandbox/src/lib.rs:894-895`).
- Produces: the harness's own precondition answers in the same words the CLI prints, so a skipped Windows e2e leg and a refused `tinio start` say one thing.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 9: landed (reports/task-9-report.md); fix round 1 replaced the tautological `source.contains(REMEDY)` assertion with the body-scoped delegate pin (reports/task-9-fix1-report.md).

In the harness's `#[cfg(test)] mod tests` (or its existing host-precondition leg):

```rust
    #[test]
    fn the_harness_precondition_asks_the_production_predicate() {
        // Structural: the harness must not carry a second opinion about the token, and the
        // remedy it prints must be the one the operator reads.
        let source = include_str!("windows_e2e_harness.rs");
        let body = source
            .split("fn host_can_confine")
            .nth(1)
            .expect("host_can_confine is in this file");
        assert!(
            body.contains("capability::can_confine()"),
            "the profile half delegates"
        );
        // **`body`, not `source`, for the second half**: `source` also contains this very
        // assertion's literal, so a `source.contains` here can never fail. `body` ends at this
        // test's own split literal above, so the only thing that can satisfy the assertion is the
        // delegation in the function itself — and the needle carries the newline and indentation
        // that open that line, so a delegate someone commented out with `//` does not satisfy it.
        assert!(
            body.contains("\n    capability::can_confine().map_err(|err| err.to_string())?;"),
            "the reason the harness reports is the predicate's own error"
        );
        // **The boundary that makes the assertion above mean something.** `body` ends at this
        // test's own `.split` literal, which already sits inside `mod tests`, so the module's
        // name is no boundary at all. What must stay outside the slice is the rest of this
        // test: if that `.split` literal were renamed, `nth(1)` would run to EOF, the slice
        // would contain this test's own needle, and the assertion above would satisfy itself.
        assert!(
            !body.contains("host_can_confine is in this file"),
            "the slice still ends before this test's own lines"
        );
    }
```

> **Two of this step's first-draft assertions were wrong and the shipped test is the block above.** The
> module name is not a boundary for this slice at all: `body` ends at the test's own `.split` literal,
> which already sits inside `mod tests`, so `!body.contains("mod tests")` could never pass — the guard is
> the `.expect` message instead, which lies after that literal. And a bare substring needle is satisfied by
> the delegate **commented out**, so the needle carries the newline and the line's own indentation; that is
> what makes the fail-first run below red for the right reason.
>
> **The boundary pin has a residual hole.** It asserts the absence of the `.expect` message rather than
> the shape of the extraction, so renaming that message would disarm it silently. The re-review
> recommended `assert_eq!(source.matches("fn host_can_confine").count(), 2)`, which pins the extraction
> rather than a message; it was not taken, because that would be another round for a test-only hardening.
> What ships is mutation-verified: it fails when the delimiter is removed.

- [x] **Step 2: Run it and watch it fail** — red as predicted.

Run: `cargo test -p tinio-server --test sandbox_e2e_windows the_harness_precondition`
Expected: FAIL — `the profile half delegates`.

- [x] **Step 3: Delegate the profile half** — the profile half delegates; the ACL half grants the caller's SID through `testing::icacls` (the ledger's accepted deviation).

`host_can_confine` (`:601-615`) today builds a scratch temp dir, derives a package SID through `PackageSid::derive` and applies `acl::Grants`. Keep the scratch directory and the `icacls` half — that is the ACL capability, which the predicate does not answer — and replace the profile half with the delegate:

```rust
fn host_can_confine() -> Result<(), String> {
    // **The signature stays `Result<(), String>`**: `host_missing` and `host_ready` consume
    // the reason, and `host_ready` hands it to `host_cannot` (`windows_e2e_harness.rs:555-561`),
    // so the sentence returned here is the sentence a skipped leg prints. Only the profile half
    // changes: one probe, one sentence, one place the answer is decided. A second opinion in this
    // harness is the thing the harness exists to avoid.
    tinio_sandbox::windows::capability::can_confine().map_err(|err| err.to_string())?;
    // ... the scratch directory and the icacls probe stay as they are ...
    Ok(())
}
```

**The remedy stays the call site's** (`:559`, currently `" (design §12)"`): `REMEDY` is a full clause and `host_cannot` appends its third argument with no separator, so passing it here would read `…on a host that genuinely cannotrun in an ordinary user session`. What travels through from the predicate is the **reason** — the cause and the remedies, rendered by `can_confine()` — and the site's own suffix stays a pointer at the design.

- [x] **Step 4: Run the harness's suite** — green; 4 harness tests.

Run: `cargo test -p tinio-server --test sandbox_e2e_windows`
Expected: PASS on this host (it can confine), with the ACL legs unchanged.

- [x] **Step 5: Confirm no second opinion survives** — no `PackageSid::derive` left in `crates/tinio-server/tests/`.

Run: `rg 'PackageSid::derive' crates/tinio-server/tests/`
Expected: no hit in `windows_e2e_harness.rs`. A hit means the harness still derives a real profile of its own rather than asking.

---

### Task 10: The doctor row

**Files:**
- Modify: `crates/tinio-cli/src/commands/doctor/checks.rs` (a new `host_confinement` beside `free_space` at `:313-345`; pushed in `checks()` after `report.push(free_space(...))` at `:460`)
- Modify: `crates/tinio-cli/src/commands/doctor/checks.rs:559-565` (the "one temporary write" comment)
- Modify: `specs/001-s3-local-server/contracts/cli.md:102` (the doctor exit/severity rule, which now has its first `warn`-but-healthy instance)
- Test: `checks.rs`'s own tests and `crates/tinio-e2e/tests/features/doctor.feature`

**Interfaces:**
- Consumes: `_sandbox::can_confine()` (Task 5).
- Produces: a `Finding { check: "host confinement", severity, detail }` — the check name and `Severity` are the only fields the schema sees, so `contracts/doctor-report.schema.json` does not change.

**`Warn`, and `doctor` exits 1 anyway — say so rather than let a reader discover it.** There is no `Fail` in this vocabulary: `Severity` is `Ok | Warn | Error` (`report.rs:21-31`), and both non-`Ok` arms map to exit 1 (`report.rs:180-185`). So on a host that cannot confine, `tinio doctor` fails even when every other check passes. That is the existing severity→exit contract, not something this change introduces, and the contract text is corrected in Step 4 to name it. `Error` would be wrong for a different reason: nothing here is broken or repairable — the operator's session is.

- [x] **Step 1: Write the failing tests** — 2026-10-01, Task 10: landed (reports/task-10-report.md); fix round 2 made the `ok` test assert the detail and added the wiring test (reports/task-10-fix2-report.md).

```rust
    #[test]
    fn a_host_that_cannot_confine_is_reported_as_a_warning() {
        let finding = host_confinement_finding(Err(
            "this token cannot create an AppContainer profile (hr=0x80070005) — run in an ordinary user session".to_string()
        ));
        assert_eq!(finding.check, "host confinement");
        assert_eq!(finding.severity, Severity::Warn);
        assert!(finding.detail.contains("hr=0x80070005"), "{}", finding.detail);
    }

    #[test]
    fn a_host_that_can_confine_says_so() {
        let finding = host_confinement_finding(Ok(()));
        assert_eq!(finding.check, "host confinement");
        assert_eq!(finding.severity, Severity::Ok);
    }
```

- [x] **Step 2: Run them and watch them fail** — red as predicted — `cannot find function 'host_confinement_finding'`.

Run: `cargo test -p tinio-cli --lib host_that_cannot_confine_is_reported`
Expected: FAIL — `cannot find function 'host_confinement_finding'`.

- [x] **Step 3: Write the check** — the row landed; fix round 1 added it to both "root reported clean" filters; fix round 3 rewrote the Windows `ok` detail to "was not refused" (reports/task-10-fix3-report.md).

Beside `free_space_finding` (`:327-345`), matching its shape. **`_sandbox` is not imported in this file** (the alias is crate-wide, `crates/tinio-cli/src/lib.rs:28`), so the call below spells `crate::_sandbox` rather than adding an import:

**The Windows `ok` detail was corrected on 2026-10-01 (Task 10 fix round 3).** `can_confine()`'s `Ok` means *the token was not refused*, not *the token was verified able*: an unrecognised HRESULT is logged at `warn`, answered `Ok` and cached nowhere, so a real creation is never promised. The sketch below carries the shipped shared const (`checks.rs:362`); the fence is a record and the old literal promised a creation the predicate does not establish.

```rust
/// Whether this host's token can confine a process, as a finding.
///
/// **A second self-cleaning write is allowed here, and it is the design's ruling rather than
/// an oversight**: the profile this asks about is created and deleted by the predicate, so
/// `doctor` still leaves the machine as it found it. See `readability`'s "one temporary
/// write" note below, which this check makes two — and correct the note rather than leave it
/// stating a stale count.
fn host_confinement_finding(answer: Result<(), crate::_sandbox::Error>) -> Finding {
    let (severity, detail) = match answer {
        Ok(()) => (Severity::Ok, HOST_CONFINEMENT_OK.to_string()),
        Err(err) => (Severity::Warn, err.to_string()),
    };
    // **`Finding::check`, not a struct literal**: `Finding.check` is a String
    // (`report.rs:39-47`), and the constructor is the shape `free_space_finding` uses beside it.
    Finding::check("host confinement", severity, detail)
}
```

and in `checks()`, after the `free_space` push at `:460`:

```rust
    report.push(host_confinement_finding(crate::_sandbox::can_confine()));
```

- [x] **Step 4: Correct the temporary-write note, and name the exit code in the contract** — the note now names both self-cleaning writes; Task 12 reworded the contract's check-list clause (the note beside the sketch above).

The one temporary write is `writability` (`checks.rs:559-612`): a single `create_new(true)` probe file under the root, removed afterwards, and the check degrades to a finding rather than failing the command (`checks.rs:576-586`). It now has company, and the second is the predicate's — rewrite the comment to name both and keep the promise `contracts/cli.md:45` makes ("leave that root exactly as it found it").

- [x] **Step 5: Run the tests** — green; 27 doctor tests and 109 lib tests; the row reads `ok` on this host.

Run: `cargo test -p tinio-cli --lib doctor::` and `cargo run -p tinio-cli -- doctor`
Expected: PASS, and the human-readable report carries the new row with `ok` on this host.

---

### Task 11: A Low-integrity e2e leg

**Files:**
- Modify: `crates/tinio-sandbox-windows/src/testing.rs` (a `low_integrity` submodule; behind the existing `testing` feature, so no new manifest entry)
- Modify: `crates/tinio-e2e/tests/features/doctor.feature` (one `@cli @windows` scenario) and `crates/tinio-e2e/tests/steps/cli.rs` (the step that reads the finding)
- Test: the scenario itself, plus `testing.rs`'s own `mod tests`

**Interfaces:**
- Consumes: the measured recipe (`CreateRestrictedToken` + `SetTokenInformation(TokenIntegrityLevel)` + `CreateProcessAsUserW`, unelevated).
- Produces: `tinio_sandbox_windows::testing::low_integrity::run(program: &Path, args: &[&str], cwd: &Path) -> std::process::Output`.

**Why this leg and not a Low `tinio`:** a Low `tinio` dies at its first write below the gate — the state directory at `run.rs:195` — so it would prove nothing about the gate. `doctor` is the command whose capability answer is readable without a writable tree, which is why the scenario is a `doctor` one.

- [x] **Step 1: Write the failing test** — 2026-10-01, Task 11: landed (reports/task-11-report.md); fix round 2 fixed the Critical — the leg spelled `System32/cmd.exe` with a forward slash, so it passed with nothing having run (reports/task-11-fix2-report.md).

In `testing.rs`'s `mod tests`:

```rust
    #[test]
    fn a_low_integrity_child_cannot_write_a_medium_directory() {
        let dir = tempfile::tempdir().expect("scratch");
        let target = dir.path().join("probe.txt");
        let out = low_integrity::run(
            Path::new(&std::env::var("SystemRoot").expect("SystemRoot")).join("System32/cmd.exe"),
            &["/C", &format!("echo x > {}", target.display())],
            dir.path(),
        );
        assert!(out.status.code().unwrap_or(-1) != 0, "a Low writer must be refused");
        assert!(!target.exists(), "the Medium directory refuses a Low writer");
    }
```

- [x] **Step 2: Run it and watch it fail** — red as predicted — E0433, `cannot find module 'low_integrity'`.

Run: `cargo test -p tinio-sandbox-windows --lib a_low_integrity_child`
Expected: FAIL — `cannot find module 'low_integrity'`.

- [x] **Step 3: Write the helper** — the helper landed; the working recipe needs `TOKEN_ASSIGN_PRIMARY` on the source handle, the report's corrected recipe.

In `testing.rs`, a `pub mod low_integrity` with its own `#![allow(unsafe_code)]` (this crate's rule) documenting the measured recipe:

```rust
//! Run a program under a **Low** integrity token, so a suite can reach a host state this
//! machine will not otherwise produce.
//!
//! **The recipe, measured on 2026-10-01 and written down because it is not the obvious one:**
//! `icacls <path> /setintegritylevel Low` answers "Access denied" without
//! `SeSecurityPrivilege`, and a Low-labelled *image file* lowers the resulting process's
//! integrity. What works unelevated is
//! `CreateRestrictedToken` + `SetTokenInformation(TokenIntegrityLevel, Low)` +
//! `CreateProcessAsUserW`: the child's writes to an object labelled Medium are refused by
//! no-write-up, which is exactly the state `tinio doctor` must survive.
```

- [x] **Step 4: Write the scenario** — the scenario landed; fix round 1 restored the brief's "a warn finding" through a second step binding, and fix round 2 added the seeded residue scenario.

Append to `doctor.feature`, in the file's own style (2-space scenario indent, 4-space steps, tags `@cli @windows`):

```gherkin
  @cli @windows
  Scenario: doctor reports a host that cannot confine
    Given a storage root
    When I run "tinio doctor {root}" under a low-integrity token
    Then the run fails with exit code 1
    And the report holds a warn finding carrying "host confinement"
    And the report holds a warn finding carrying "hr=0x80070005"
```

**Every step but the `When` already exists.** `the report holds an {word} finding carrying {string}` (`steps/doctor.rs:521-528`) parses a *text* row through `doctor_row` (`:118-122`), and `render_text` writes `{severity:<5} {check}: {detail}` (`report.rs:159`) — so one `warn  host confinement: … hr=0x80070005 …` line satisfies both `And`s. **`--json` would break it**: the JSON is pretty-printed one field per line (`report.rs:172-175`), so a finding's `"severity"` and `"check"` never share a line with its detail, and `doctor_row` reads a leading-space line as severity `""`. Hence the text render here.

**The `When` step must live in `steps/cli.rs`, not `steps/doctor.rs`.** The runner is private to that module — `Cli::command`/`argv`/`run`/`run_paths` are private, and `Cli.last` and `Run`'s fields are private (`steps/cli.rs:171`, `:264-292`, `:448-470`, `:488-506`, `:537-628`) — so a step in another module cannot record into what the `Then`s read. Build the same `Command` (`cargo_bin("tinio")`, the scratch cwd, the scrubbed environment) and spawn it through `low_integrity::run`, recording stdout/stderr into that run.

**Expect exit 1, and for two reasons at once**: the report's severity is `warn` from the new row, while the Low token also fails `writability` (`checks.rs:559-612`), whose finding is `Severity::Error` — and `Report::exit_code` is `1` for either (`report.rs:180-185`). Doctor does **not** abort on a failing check: `checks()` returns a `Report` and every arm of `readability`/`writability` is a finding (`checks.rs:535-586`), and plain doctor never creates the state dir at all (`Pass::Diagnostics`, `checks.rs:45-49`), so a Low token cannot stop the report from printing.

**The scenario's other half is that it must not skip.** Unlike the `@windows` scenario at `doctor.feature:97-107`, this one does not ask whether the host can confine — it *creates* the state it asserts on. If `low_integrity::run` itself fails, the step fails rather than skipping.

- [x] **Step 5: Run the scenario** — green; @windows 19 scenarios / 118 steps on the final bytes.

Run: `cargo test -p tinio-e2e --test cucumber -- --tags '@windows'`
Expected: PASS. **There is no scenario-name option**: the harness inspects only `--tags` (`tests/cucumber.rs:92`) and otherwise installs `tag_filter::default_filter` (`:104-114`; `tag_filter.rs:66-71`, `:84`). A filter that matches nothing runs **0 scenarios and exits 0** (`crates/tinio-e2e/README.md:85`), so check the scenario count, not the exit status.

- [x] **Step 6: Run the tagged suite as CI would** — the default filter 281 scenarios / 2269 steps, run twice; the tagged counts are the reports'.

Run: the workspace's e2e invocation for Windows.
Expected: PASS, and this scenario runs rather than being filtered out.

---

### Task 12: The record

**Files:**
- Modify: `specs/001-s3-local-server/tasks.md:164` (append **T148**)
- Modify: `docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md` (the Evidence section: the probe's measured cost)
- Modify: `docs/superpowers/plans/2026-10-01-host-confinement-capability.md` (this file: tick the boxes as they land)

**Interfaces:**
- Consumes: the measurements taken while Tasks 1-11 landed.
- Produces: the row that names this work in the tracker, and the cost figure the design's Risks section promised to replace its "Not measured" note with.

- [ ] **Step 1: Time the probe on this host**

Run `cargo test -p tinio-sandbox-windows --lib capability:: -- --nocapture` with a temporary `Instant::now()` pair around one cold `probe_once()`, three times; keep the median. Record the wall time, the host, and that it is a **cold** figure — a warm process pays it once.

**Taken 2026-10-01 as an `Add-Type` measurement, not the `Instant` pair above.** That instrument needs a temporary edit to `crates/tinio-sandbox-windows/src/capability.rs`, and source files are outside this bookkeeping task's write scope. The shipped sequence was timed directly instead — `DeleteAppContainerProfile` (the clear), `CreateAppContainerProfile`, the SID render, `DeleteAppContainerProfile` (the `Drop`) and `FreeSid` — over 10 repetitions in a PowerShell host: mean **15.1 ms**, range **12.5–18.2 ms**, every HRESULT `0` and no residue left. That is the API calls alone; the probe's `ProfileLock`, its no-op tracing dispatcher and the process's own startup are outside it, so it is not the run's startup cost.

- [x] **Step 2: Append the tracker row**

At `tasks.md:164`, in the file's existing row format (`- [ ] T### [US2] <imperative>: <body>; unit tests written first`):

```markdown
- [ ] T148 [US2] Refuse a confinement this host's token cannot apply, and report it in `tinio doctor`: a probe in `tinio-sandbox-windows` classifies `E_ACCESSDENIED` from `CreateAppContainerProfile` as a host-level fact with a cause and two remedies, the CLI asks it inside `refuse_unhonourable` (the resolution above that point is a recorded limit, not a gap), the library gates `Backend::confine`, `doctor` reports a `host confinement` row at `warn` and therefore exits 1 on such a host, and the Windows e2e harness's host precondition takes its sentence from the one factory; unit tests written first.
```

- [x] **Step 3: Record the cost in the design, not the old plan**

Add the measured figure to the spec's Evidence section. **The older plan's "Not measured" line stays as it is** (`docs/superpowers/README.md`'s record rule): it was true when written, and this spec is the record of what measuring found.

---

### Task 13: Verification

**Files:**
- Modify: `docs/superpowers/plans/2026-10-01-host-confinement-capability.md` (tick the boxes that landed; append a `## Verification` section)

**Interfaces:**
- Consumes: everything Tasks 1-12 produced.
- Produces: the acceptance the original request asked for — **every test green** — as one reproducible set of commands, plus the record of what they printed.

**Why this is its own task** (the plan-grilling ruling): every other task proves one seam. "All the tests pass" is a claim about the whole workspace, and a claim needs an owner, a command and a result.

- [x] **Step 1: The command the request started from**

Run: `cargo test -p tinio-sandbox-windows --lib -- --test-threads=1`
Expected: PASS. This host was 120 passed / 0 failed on 2026-10-01; the total will differ once Tasks 1-9 add tests, and **"0 failed" is the criterion**, not the number.

- [x] **Step 2: The whole workspace**

Run: `cargo test --workspace --all-targets`
Expected: PASS, including `tinio-server`'s Windows e2e suite (Task 9 changed its harness) and the cucumber suite (Task 11 added a scenario). Anything skipped must be a `host_cannot` skip carrying the factory's remedy, never a silently filtered scenario.

- [x] **Step 3: The gates**

Run: `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`
Expected: both clean.

**Corrected 2026-10-01 (Task 13 fix round): the `fmt` half of that expectation cannot hold on this tree, and the number is not zero.** The gate CI runs is `cargo +nightly fmt --all -- --check` (`rustfmt 1.11.0-nightly`; plain stable `cargo fmt` cannot set this repo's unstable options and under-reports). On this tree it exits 1 with **19 `Diff in` sites, all inherited** - code this change did not write: 14 in files it never touched, and 5 in files it did touch (three of those are HEAD's sites shifted down by its insertions, and two are not shifted at all - `backend/tests.rs:480` and `:1726` sit above this change's EOF append, so their HEAD lines are their own). The list is in this plan's `## Verification` section.

**Corrected 2026-10-01 (final review fix round): the split above was wrong twice, and this is the verified one.** The 19 sites enumerated in the `## Verification` section below are **14 in files this change never modified** (the seven untouched files: `launch.rs` 3, `process.rs` 2, `socket.rs` 1, `tests/confine.rs` 2, `tests/events.rs` 2, `tests/signals.rs` 2, `tinio-util/tests/spawn_guard.rs` 2) and **5 in files it did modify**, of which **3 are line-shifted by its own hunks** - `tinio-sandbox/src/lib.rs:972`/`:978` are HEAD's `:920`/`:926`, shifted +52 by the 51-line insertion at `:698`; `tinio-sandbox/tests/facade.rs:388` is HEAD's `:378`, shifted by the nine-line `the_host_predicate_is_published` block at `:37` plus the one line the final review fix round's comment rewording added above it - and `tinio-sandbox-windows/src/backend/tests.rs:480`/`:1726` are **not shifted**, because this change's only hunk in that file is an end-of-file append beginning at `:1878`. The arithmetic: 14 + 3 + 2 = 19. What the gate means here is met: **this change's own files are clean** - `rustfmt +nightly --check --edition 2024 crates/tinio-cli/src/commands/doctor/tests.rs` exits 0, and the per-file nightly check over the 22 files this change touches reports **ours = 0**, which resolves the two `doctor/tests.rs` sites the previous round's record left open. The clippy half is clean as written.

- [x] **Step 4: Record what they printed**

Append a `## Verification` section to this plan in the shape the sibling plans use (see `docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`'s own section): the date, the host, the four commands and their actual results. A record stays as written (`docs/superpowers/README.md`'s record rule) — if a later run disagrees, the correction is live prose beside it.

- [x] **Step 5: Leave the tree uncommitted**

Run: `git status --short`
Expected: the new plan, the new spec and every modified file, all unstaged. **Do not commit** — `docs/git.md` requires asking first, and the operator's ruling for this work is to leave the tree as it is.

---

## Self-review

- **Spec coverage**: predicate (Task 4) — message (Tasks 2, 4, 9) — gate (Tasks 7, 8) — error surface (Tasks 1, 6) — doctor (Task 10) — testing (Tasks 9, 11) — record (Task 12). Every section of the spec has an owning task.
- **Placeholder scan**: no `TODO`, no `<n>` step, no "similar to Task N". Every step names a command and an expected result.
- **Type consistency**: `Error::HostCannotConfine { cause, remedy, hr }` is constructed only in `capability::cannot_confine` and matched only in `capability::probe_once`, `capability::Cache` and `sandbox_err`; the CLI's `SandboxHostCannotConfine { cause, remedy }` drops `hr` on purpose (the sentence carries it). `can_confine() -> Result<(), Error>` has one signature on all three targets. **Corrected 2026-10-01 (code-review fix round): the CLI variant is `SandboxHostCannotConfine(_sandbox::Error)` — it carries the core value whole and no longer drops `hr` — and `capability::admit()` now also returns an `Admission` witness that `prepare_launch` requires; `can_confine`'s own signature is unchanged on all three targets.**
- **Ordering**: Tasks 1→2→3 are a chain (the variant, the factory, the call site); 4 depends on 3; 5 on 4; 6 and 7 on 5; 8 on 4; 9 on 4; 10 on 5; 11 on 10; 12 and 13 last.
- **Plan-grilling rulings (2026-10-01, round 2)**: no test seam exists, so the predicate's behavioural proof is the Low-integrity doctor scenario and the gate's proof is the structural test (corrected 2026-10-01, cleanup round: **the library gate's proof is now the `Admission` witness `prepare_launch` requires** and its source-reading test is deleted; the CLI gate's is still the structural test); `doctor` exits 1 on a host that cannot confine, and the contract says so; the probe keeps its `ProfileLock` (with a corrected reason); Task 9 is scoped to the harness site alone; `override_probe` is removed; the resolution's pre-gate write is an accepted, recorded limit.
- **Plan-grilling rulings (2026-10-01, round 1)**: the twelve tasks stay unmerged; a thirteenth owns "every test passes"; the two seam tests are layer one, with a behavioural leg following only where a seam exists; the probe holds `ProfileLock` across create-and-delete; an unrecognised probe failure answers `Ok` and caches nothing; the Low-integrity e2e leg ships in this change.
- **Deliberate omissions**: no commit step (operator's ruling), no schema change (`check`/`severity`/`detail` are unchanged), no new cargo feature (`testing` is the existing seam), no change to `WINDOWS_CAPABILITY_USAGE` (that sentence is the platform's, this one is the host's).

## Verification (2026-10-01, frozen tree, this host unless noted)

Task 13 ran the whole change to acceptance on the frozen tree. Nothing outside this section was written. Toolchain: `cargo 1.98.1` / `rustc 1.98.1` (stable, active) and `rustfmt 1.11.0-nightly` (the formatter gate); `HEAD` is `b89d5668`. Pre-flight found **0** orphaned `tinio.exe` processes, and the same check after the run found **0**.

- **The command the request started from**: `cargo test -p tinio-sandbox-windows --lib -- --test-threads=1` -> **127 passed; 0 failed; 0 ignored**, exit 0. That is the crate's acceptance run: the whole lib, not a narrow filter.
- **The workspace leg** (`docs/tests.md`, "Standard acceptance": CI's unit leg is `cargo nextest run --workspace --exclude tinio-e2e`, and `cargo test` is its local equivalent; the sibling verification spells it the same way): `cargo test --workspace --exclude tinio-e2e --no-fail-fast` -> exit 0, **2279 passed / 0 failed / 3 ignored**, summed over the run's 60 `test result:` lines with 18 doc-test blocks among them. `tinio-e2e` is excluded exactly as `.github/actions/run-test-leg/action.yml` excludes it, and runs in its own leg below. The run's repeated `did not finalize incremental compilation session directory ... os error 5` notes are pre-existing `target/` noise (the ledger records them from Task 1); builds and tests are unaffected.
- **The plan's Task 13 Step 2 spelling**, `cargo test --workspace --all-targets --no-fail-fast` -> exit 0, **1808 passed / 0 failed / 1 ignored** over 43 `test result:` lines. It differs from the leg above in exactly the two ways cargo defines: it runs the `tinio-e2e` cucumber target with the default filter (281 scenarios / 2269 steps, green - the same count as the e2e leg below) and the criterion bench targets in test mode, and it runs **no doctests** (`--all-targets` excludes them), which the workspace leg's 18 doc-test blocks carried.
- **Doctests of the changed public surfaces**: `cargo test -p tinio-sandbox-windows --doc` -> **91 passed**, the new example among them as `capability::can_confine (line 152) ... ok` - a plain `ok`, not `- compile`; `cargo test -p tinio-sandbox --doc` -> **32 passed**.
- **Clippy**: `cargo clippy --workspace --all-targets -- -D warnings` -> `Finished`, exit 0, zero warning/error lines.
- **The e2e suites, by scenario and step count, never by exit code** (`cargo build -p tinio` first, `--retry 2`):
  - the default filter, `cargo test -p tinio-e2e --test cucumber -- --retry 2` -> **281 scenarios (281 passed) / 2269 steps (2269 passed)**;
  - `--tags '@windows'` -> **19 scenarios (19 passed) / 118 steps (118 passed)**;
  - `--tags '@T057 and not @unix'` -> **23 scenarios (23 passed) / 203 steps (203 passed)**. Each count is non-zero, so none is the exit-0 empty-filter trap. All three match the counts Task 12 cited.
- **Inherited red, named and not fixed.** `backend::tests::a_confined_launch_holds_the_window_across_its_acl_phase` fails when its file is run alone: `cargo test -p tinio-sandbox-windows --lib -- --exact backend::tests::a_confined_launch_holds_the_window_across_its_acl_phase --test-threads=1` -> **0 passed / 1 failed**, panic at `crates/tinio-sandbox-windows/src/backend/tests.rs:1780` with an empty two-delete buffer, while that run's own sweep log ends `reaped=9 locked=0 failed=0 other_tinio_processes=0`. The launcher's sweep reaps the peer's profile before the window opens, so neither delete the test counts ever happens; with a live neighbouring launcher the sweep's liveness gate skips the reap and the test passes - which is why it passes in the whole-lib run above. The test and the sweep are HEAD code: this change's only hunk in that file is an end-of-file append beginning at 1881, a hundred lines below the failure. Two more fragilities are the same class: the sweep's liveness gate flakes while other `tinio` processes are alive, and narrow `tinio-util` filters can flake on committed recording contention - `tinio-util` is not in this change's file set at all. Both legs passed in the whole-workspace run and neither is this change's.
- **Formatter gate, toolchain named, and it is not green.** The gate is `cargo +nightly fmt --all -- --check` (`rustfmt 1.11.0-nightly`), the command `.github/workflows/ci.yml`'s `fmt` job runs -> **exit 1, 21 `Diff in` sites**. Plain `cargo fmt --all -- --check` (stable 1.9.0) cannot be that gate: it warns that it cannot set six unstable options and checks with stable's own rules. It is red too (exit 1, 9 sites), but every site it names is also a nightly site, so it under-reports rather than disagrees. Of the nightly **19** (**Corrected 2026-10-01, final review fix round: this record read "21" when it was written, and 2 of those sites were this change's own. One later round (the Task 13 fix round) made `crates/tinio-cli/src/commands/doctor/tests.rs:7`/`:14` nightly-rustfmt clean, so the tree carries the 19 below and every one is HEAD's own.**):
  - **19 are pre-existing.** The three the brief names are confirmed against HEAD's own bytes: `backend/tests.rs:480` and `:1726` drift on HEAD at the same lines; `tinio-sandbox/tests/facade.rs:388` is HEAD's `:378` (the `"{capability}"` assertion line) shifted by this change's nine-line `the_host_predicate_is_published` block at `:37` - **Corrected 2026-10-01, final review fix round: this record called that site a trailing blank line shifted by an import hunk, and `git diff HEAD` shows neither; the line is `:388` because the same round's comment rewording added one more line above it.** The other **14** sit in files this change never touched (`launch.rs` 3, `process.rs` 2, `socket.rs` 1, `tests/{confine,events,signals}.rs` 2 each, `tinio-util/tests/spawn_guard.rs` 2). The remaining **5** are in files it does touch, with **3 line-shifted and 2 not**: `tinio-sandbox/src/lib.rs:972/:978` are HEAD's `:920/:926`, shifted +52 by this change's 51-line insertion at `:698` (proven by running nightly rustfmt over a temp mirror of `src/` carrying HEAD's `lib.rs`); `tinio-sandbox/tests/facade.rs:388` is HEAD's `:378`, shifted +9 (above); and `tinio-sandbox-windows/src/backend/tests.rs:480/:1726` sit above this change's end-of-file append at `:1878`, so they are unshifted. **The arithmetic: 14 + 3 + 2 = 19.**
  - ~~**2 are new, and they are this change's**: `crates/tinio-cli/src/commands/doctor/tests.rs:7` and `:14`.~~ HEAD's copy of that file is nightly-rustfmt clean (exit 0, no diff, same mirrored-tree method); the change's added `use super::checks::{...}` sits above the glob `use super::*`, and nightly rustfmt wants named `super` imports before the glob. Recorded, not fixed: this task's write scope is this section, and the ledger's "cargo fmt clean" notes are true of the stable command that cannot see this. **Corrected 2026-10-01 (final review fix round): that record is now the pre-fix state and the two sites are clean.** The fix round imported the names out of `super::checks` one line at a time, so the per-file nightly check over `doctor/tests.rs` exits 0; the tree carries **19** sites and no site is this change's - 17 are inherited in files this change never modified and 2 are inherited line-shifts.
- **File set and index, as found - the operator's hand staging, not a defect.** `git status --short` shows **22 staged entries**: 19 modified tracked files plus the three additions (`crates/tinio-sandbox-windows/src/capability.rs`, this plan, the design spec). One further file, `specs/001-s3-local-server/tasks.md`, is modified and **unstaged** (Task 12 edited it after the hand staging). `HEAD` is `b89d5668`; nothing is committed. No `git add`/`reset`/`restore`/`stash`/`checkout` was run.
- **No number contradicts Task 12.** Every count it cited - 281/2269, 19/118, 23/203, 127 lib + 91 doc - reproduced exactly. T148's technical nouns also check out against the tree: clear-then-create (`capability.rs:202`, `:225`), `classify`'s `E_ACCESSDENIED`-only verdict (`:50-51`), the two gate sites (`crates/tinio-cli/src/sandbox/plan.rs:354` and `crates/tinio-sandbox-windows/src/backend/mod.rs:234`), the two scenario names (`doctor.feature:451`, `:479`), and the `OnceLock` inside the private `Cache` held in `static VERDICT` - so no correction was ordered. The one fact the record did not carry is the formatter finding above; nothing was rewritten to hide it.

**Re-run a reader:** `cargo test -p tinio-sandbox-windows --lib -- --test-threads=1`; `cargo test --workspace --exclude tinio-e2e --no-fail-fast`; `cargo test -p tinio-e2e --test cucumber -- --retry 2` (expect 281 scenarios / 2269 steps); `cargo clippy --workspace --all-targets -- -D warnings`; `cargo +nightly fmt --all -- --check` (red today - 19 sites, all HEAD's own; the two `doctor/tests.rs` sites this line used to add were made clean by the last fix round).

**Corrected 2026-10-01 (code-review fix round): the review's findings were answered in the tree, and four records above are superseded rather than edited.** (a) `capability::REMEDY` now reads "check **or clear** the Low mandatory label" — design §The message's two actions, verbatim; Task 2's fence and Task 4's test literal keep the old sentence, being records. (b) The `tinio_sandbox_core::` paths in Task 2's module fence are `crate::_core::` in the tree (`docs/style.md` §Imports), and `capability.rs`'s five non-entry items are `pub(crate)`, which leaves `can_confine()` the one public entry point design §The predicate states. (c) `can_confine` now emits `step = "host cannot confine"` with the HRESULT — design §The message's log surface, which **no path could reach** before this round: the probe silences its own dispatcher, the verdict is cached, and both gates call this one function, so the emit sits there rather than at each gate. (d) Three documents outside this plan gained the fact the review asked for: `contracts/cli.md`'s refusal states the design's Correction 3 limit instead of "nothing is written first"; the design's §Testing records that the harness's ACL half grants the calling user's SID (`Grants` is typed over `PackageSid`, whose only constructor is the second profile that bullet removes); and `docs/cargo.md` with decision 14 name `testing`'s `low_integrity` as Windows' tenth enumerated site, `spawn_guard.rs`'s module doc recording the raw spawn that left its scan. (e) The two e2e smell fixes: the low-integrity step calls a shared `Cli::read_only`, and `HOST_CONFINEMENT`'s doc no longer claims a sharing with legs that spell the row name themselves. (f) The line references in the two bullets above moved with (a)-(c): `classify`'s verdict is `capability.rs:52-53` and clear-then-create is `:219`/`:242`. (g) Formatter: this round's own files are nightly-clean (0 sites across `capability.rs`, `steps/cli.rs` and `checks.rs`), and `spawn_guard.rs`'s 2 are HEAD's, re-measured on HEAD's own bytes — with the note that (d) is what puts that file into the change set at all, so the bullet above counted it among "files this change never touched" correctly until this round and not after it. Verified by: `cargo check -p tinio-sandbox-windows --all-targets` and `cargo check -p tinio-e2e --test cucumber`, both exit 0.

**Corrected 2026-10-01 (cleanup round, after the fix round above): four shapes moved, and one sentence in the §Verification bullets is superseded by the first of them.** (a) `Cache` is gone — the verdict is a bare `static VERDICT: OnceLock<Probe>` with a free `get_or_probe(&OnceLock<Probe>, ...)`, the workspace's own shape for a process-wide memo (`sid`'s `ensure_state_directory`, `tinio-cli`'s `STOPPER`, `tinio-util`'s `capture`); the bullet above that says "the `OnceLock` inside the private `Cache` held in `static VERDICT`" now describes a `Cache` that does not exist, and the `OnceLock` it names is the whole of the cache. (b) `E_ACCESSDENIED` is `windows_sys::Win32::Foundation::E_ACCESSDENIED` rather than a transcribed `0x8007_0005`. (c) The Low-integrity device sorts its environment block with `sort_by_cached_key`, where `sort_by_key` called the key closure — two allocating `String`s — once per comparison. (d) Both spawn paths end in one `Cli::record`, so the stream → file → snapshot rule and the 9-field `Run` literal have one implementation; the low-integrity step no longer re-types either. Verified by: the `capability::` unit tests (4 passed), `cargo check -p tinio-sandbox-windows --all-targets` and `cargo check -p tinio-e2e --test cucumber` (both exit 0), and the nightly formatter over the three files (0 sites).

**Corrected 2026-10-01 (the operator's follow-up round): four records above became false, and each is corrected in place beside its record.** (a) The CLI's variant carries the core error whole — see this plan's Global Constraints and Self-review, both corrected above, and two tests died with the second spelling (the lockstep one deleted, `error.rs`'s own rewritten to assert the exit code beside the core's rendering). (b) `DeleteAppContainerProfile` is called from one place: `sid`'s `delete_appcontainer_profile` answers `Deleted::{Gone, Refused(hr)}` and the three presentations render from it, so `clear_profile`'s old paragraph — "`Drop` does not call this, and the two calls are not one call" — is rewritten to "one kernel call, three presentations". **The enum has two states and not three**: `S_OK` for a never-created name is the same answer as for one removed, so `Gone` never means "this call removed one". (c) The library gate's order is type-enforced by `capability::Admission` (see §Testing's correction above) and its source-reading test is deleted. (d) The harness's ACL half grants to the probe's own package SID through `capability::with_probe_profile`, not to the calling user's. **The uncontested half of the same round**: `tinio_util::testing::function_body` replaces four hand-rolled source extractors, `host_can_confine` is memoized process-wide, the residue fixture moved into the crate that owns `PROBE_NAME`, the Low-integrity device writes into the caller's capture files and is offloaded with `spawn_blocking`, and `doctor`'s two article steps are one `a(n) {word}` expression. Verified by: `cargo test -p tinio-sandbox-windows --lib` (126 passed), `cargo test -p tinio-server --test sandbox_e2e_windows` (4 passed — the three launching legs really ran), `cargo test -p tinio-cli --lib` (108 passed), `cargo test -p tinio-util --features testing --lib` (42 passed), the `@T057 and not @unix` cucumber leg (23 scenarios / 203 steps), and a forced full clippy rebuild (exit 0, no warnings) after one `absolute_paths` regression this round introduced and fixed.

**Corrected 2026-10-02 (cleanup round): the probe's window moved out of `capability`, and item (d) above is the sentence it supersedes.** (a) `capability::with_probe_profile` is gone: `capability.rs` keeps the one public entry point the design's §The predicate owns — `can_confine()` — with `Window` and `probe_window` `pub(crate)`, and the harness-facing wrapper now lives at `testing::with_probe_profile`, in the crate's non-default `testing` module, which is `docs/tests.md`'s home for a device a `tests/` target must share. The design states that fact once (§The predicate) and its §Testing now refers to the wrapper by its `testing::` path instead of restating the location. (b) `windows_e2e_harness::host_can_confine` no longer asks `can_confine()` before opening the window: the window answers the same refusal (`probe_window` → `cannot_confine`) out of the same memoized probe, so the call was one probe's answer asked twice, and the structural test's needle moved from `capability::can_confine()` to `testing::with_probe_profile` with its subject — the harness's precondition asks the production predicate rather than holding a second opinion — unchanged.

**Acceptance gates, two hosts (2026-10-02).** Both hosts ran the repository's standard acceptance gate against the tree as it stood that morning; every Windows run was force-`touch`ed first so no result was a stale `Fresh`. **Windows host** (`cargo 1.98.1`, `TMP=TEMP=D:\Temp`): `cargo test --workspace --exclude tinio-e2e --no-fail-fast` -> **exit 0, 2281 passed / 0 failed / 3 ignored** over 60 `test result:` lines; `cargo test -p tinio-sandbox-windows --lib -- --test-threads=1` -> **126 passed / 0 failed**; `cargo build -p tinio` then the full default cucumber filter -> **15 features, 281 scenarios (281 passed) / 2269 steps (2269 passed)**; `cargo test -p tinio-e2e --test cucumber -- --tags '@T057 and not @unix'` -> **23 scenarios / 203 steps**; `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0; `cargo check -p tinio --no-default-features` -> exit 0; and the workspace rustdoc gate (`RUSTDOCFLAGS="-D warnings -D rustdoc::private_intra_doc_links -D rustdoc::broken_intra_doc_links" cargo doc --workspace --no-deps`) -> exit 0.

**Linux host** (WSL2 `Ubuntu-26.04`, kernel `6.18.40.1-microsoft-standard-WSL2`, `cargo 1.99.0` / `rustc 1.99.0`, `CARGO_TARGET_DIR` on ext4 so the Windows `target/` is not shared): `cargo check --workspace --all-targets` -> exit 0; `cargo test --workspace --exclude tinio-e2e --no-fail-fast` -> **exit 0, 2208 passed / 0 failed / 3 ignored** over 58 targets; `cargo test -p tinio-select --lib` -> **406 passed / 0 failed / 1 ignored**; `cargo build -p tinio` -> exit 0; and the cucumber `--tags '@linux'` filter -> **5 scenarios / 28 steps**, all passed and non-zero. **The per-platform totals differ on purpose**: a different target set compiles and runs on each host, so 2281 and 2208 are not two measurements of one thing.

**One leg is red, on Linux only, and it is not this change set's.** The rustdoc gate for `crates/tinio-sandbox-windows` fails with four unresolved intra-doc links in that crate's crate-root `//!` docs (`lib.rs:4`, `:7`, `:35`), because the crate is `#![cfg(target_os = "windows")]` - every item vanishes on Linux while the crate-level docs are still resolved. The three failing lines are byte-identical to `HEAD`, the CI-shaped invocation fails identically, and `cargo doc --workspace --keep-going` shows two more pre-existing casualties (`tinio-winsec` four links, `tinio-sandbox-linux` two private-item links at `jail.rs:91` and `seccomp.rs:533`); CI's Docs job is already failing on `dev` (run `36572588571`, commit `7f88fd66`). The consequence to record: because that crate is cfg'd out on Linux, **this leg cannot see the new public item in its `testing` module at all** - the native Windows `cargo doc` is what checks it, and that one is green.

**Two gates that had never been run (2026-10-02).** **Facade feature matrix**: `cargo hack check --package tinio --each-feature` (cargo-hack 0.6.45) -> **exit 0, 17 of 17 legs `Finished`**, every leg printing `Checking tinio`, no error and no warning. Scope, stated honestly: this is the `check` half `docs/tests.md` names as the keeper, so it proves `--no-default-features` still **compiles**; the "and serve" half is a runtime property the gate does not exercise.

**The Linux confinement family, selected the way the operator's gate selects it**: `TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036 and not ((@linux or @macos or @windows or @unix) and not (@linux or @unix))' --concurrency 1`, the tag expression copied verbatim from `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh:122`, after `cargo build -p tinio` -> **exit 0, 28 scenarios (28 passed) / 181 steps (181 passed)**, with no `tinio` process outliving the legs. The run exists because of a selection trap: an earlier attempt used a bare `--tags '@confinement'`, which **replaces** the harness's default filter, so foreign-platform scenarios were selected and reported 25 meaningless failures; the operator's expression restates the platform exclusion, and no `@macos`/`@windows` scenario was selected this time. Two things were deliberately not done: `wsl-chroot.sh` was not run end-to-end, because its external-e2e leg FATALs on a missing boto3 venv before reaching the confinement leg (three conventional venv locations probed, all absent) and its `aws`/`rclone`/`mc` legs were out of scope - the tag expression and the prescribed `cargo build -p tinio` stage were used instead; and the measured consequence is that **an explicit `--tags` makes `TINIO_E2E_CONFINEMENT` inert**, the tag string alone governing selection.

**Line references have drifted (2026-10-02).** The line references inside the earlier round paragraphs - for example `capability.rs:202`/`:225` and `:50-51` - have moved with every round since, most recently with the whole-workspace `cargo +nightly fmt --all` sweep; the symbol and file names in those paragraphs are stable and are what a reader should search for, and the numbers are not worth maintaining.

**Corrected 2026-10-02 (whole-workspace fmt sweep): the formatter gate is green and the tree is cleaner than `HEAD`.** `cargo +nightly fmt --all -- --check` -> **exit 0, zero sites**. Before this round it was red with 19 sites, all inherited, and a `git archive HEAD` comparison in a scratch tree measured **21 sites at HEAD**, of which the two in `tinio-sandbox-windows/src/testing.rs` had already been fixed by this change set; the sweep cleared the 19 inherited sites, so the tree is now cleaner than `HEAD`.


