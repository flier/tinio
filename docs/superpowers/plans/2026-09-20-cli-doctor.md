# `tinio doctor` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `tinio doctor [DIR] [--json] [--dry-run] [--fix] [--read-only]` — an offline diagnosis of a storage root that reports per-check severity, renders the same report as JSON on request, and repairs through the `Cleanup` trait what may be repaired while never touching user data and never creating a store it did not find.

**Architecture:** The command owns no inventory of its own. `Cleanup::repair(RepairKind::Full)` is already "every startup item plus meta-orphan reclamation" (`tinio-core/src/cleanup.rs:121-145`; the home root-state-dir GC the trait's doc still names is not in the fs implementation — `crates/tinio-fs/src/cleanup.rs:14-15` defers it to US2, whose owner is **T144**, not T076 — T076 is delivered), and `CleanupOptions::dry_run` already means "propose, do not touch", so the private-state leg of `--fix` and `--dry-run` is one call with one flag apart — and the two *reportings* differ by one mapping (`Finding::informational` for a plain run's proposals, `Finding::from_action` under `--dry-run`, Task 2's `Proposals`). The checks themselves have three owners: the `Cleanup` trait (`FsCleanup`) owns private state, `tinio-fs`'s `database` module owns the meta store's integrity and fragmentation, and `tinio-api` owns the `state` file and the control socket. What this plan adds is the legs none of them performs (root readability, config validity, on-disk name validity, symlinks while disabled, free space, the control socket's path length, the meta-store integrity and fragmentation check, and the plane's stale `state` file and control socket), one report type, two renderings, and the exit-code mapping the contract fixes — the report, the walks and the exit code are this plan's.

**Tech Stack:** Rust 2024, `tinio-cli`'s existing surface (plan B), `tinio-core::cleanup` (`Cleanup`, `CleanupOptions`, `RepairKind`, `RepairAction`, `RepairActionLevel`) and `tinio_core::{bucket, object}` for the name rules, `tinio_fs::{FsStorage, FsCleanup}` and `tinio_fs::database` (`check_integrity`, `open`, `needs_compact`, `compact_if_needed`), `tinio-api`'s `state`/`client`/`bind` surface in `api` builds, `rustix` on unix and `windows-sys` on Windows for the free-space probe, `serde`/`serde_json` for `--json`.

**Spec:** `docs/superpowers/specs/2026-09-20-cli-integration-design.md` — §4 (`doctor`), decision 14, decision 19 (its read-only resolution). The contract is the doctor section of `contracts/cli.md` (the check list, the severities, the exit codes, `--dry-run`/`--fix`, and the rule that neither flag touches user data). Plan B (`2026-09-20-cli-lifecycle.md`) must have landed: this plan adds the last command to its parser and uses its resolution. Plan A (`2026-09-20-cli-management-plane.md`) must have landed its Task 1 too, for `tinio_api::state::{socket_path, State}`, `tinio_api::client::probe` and `tinio_api::bind::reclaim_stale`, which the state/socket leg and `--fix` call.

## Global Constraints

- **Perform no git write operations whatsoever, and never touch the index** (`CLAUDE.md`): every task closes with a checkpoint step that reports the files pending commit and **does not run** `git add`/`git commit`/`git push`/`git restore`.
- Code, comments, tests, commit messages and documents are always in English. **Documents are not hard-wrapped.**
- **Test-first** (constitution IV): the failing test comes first and its red state is verified.
- No `unwrap()` / `expect()` / `panic!()` in library code; tests may use them. Async tests use `#[tokio::test]`; never `Runtime::block_on` in a test, and no command builds a runtime of its own — the single multi-threaded runtime `run()` builds (`new_multi_thread`, design decision 2) is the only one, and `server`, `status`, `stop` and `doctor` all run on it; `doctor` creates none of its own.
- **No test may mutate the process environment** — the read-only resolution is exercised by spawning the binary (plan B's `tests/lifecycle.rs` pattern), never by `set_var`.
- **Every process-level spawn of the binary pins its state location to the test's own temp dir, and which variable decides is stated because only one of them decides on each platform.** Every spawn sets `TINIO_STATE_DIR` (`tinio_config::state::STATE_DIR_VAR`, `crates/tinio-config/src/state.rs:51`, read by `env_state_dir` at `:63`) **and** `HOME`/`USERPROFILE`. What actually decides in this plan's fixtures: in read-write mode the state dir is `<root>/.tinio`, derived from the root — every fixture root lives inside the test's temp dir, so that is already the test's own directory and `TINIO_STATE_DIR` is inert there for the **served** state dir — the one qualification the owner text states (`crates/tinio-config/src/state.rs:60-70`): a read-write run's served dir is `<root>/.tinio` and the variable never moves it, while the variable *does* answer in place of the home-derived candidate a client probes second, which is what the requirement at the end of this bullet puts on plan B; in read-only mode `TINIO_STATE_DIR` is the override that decides. `HOME`/`USERPROFILE` are the unix arm of the home-derived candidate (`home_state_dir`, `crates/tinio-config/src/state.rs:45-48`) and **do not decide on Windows**: `dirs` 7 resolves the home through `SHGetKnownFolderPath`, so a spawn that set only those two would still see the developer's real profile — and computing that candidate *creates* it, seeding `~/.tinio/roots/`. Hence the standing requirement this bullet puts on plan B: `resolve::state_dir_candidates`'s home-derived candidate must be `TINIO_STATE_DIR` when that variable is set, so no spawn in this plan can reach a real home on any platform. Never `set_var` in-process — the same rule as the bullet above, in the shape a process-level test needs.
- **`doctor` never creates a store.** The **chosen** state dir's `meta.redb` is probed before anything else, and the probe is what decides which of the two traps applies: `tinio_fs::database::open` **creates** the file (and the state dir) when it is absent — `fs::create_dir_all` plus `Database::create`, then the 0700/0600 hardening — while `check_integrity`'s `Database::open` **fails** on the same absent file (`crates/tinio-fs/src/database/open.rs:67-92`, `:177-185`), and `FsStorage::new` reaches the first of them (`crates/tinio-fs/src/backend/mod.rs:337`). So: an absent `meta.redb` is `ok` for the meta legs and `ok` for the private-state leg, and **nothing is opened**; the diagnostics pass opens the store at most once and only through this plan's `open_store` (never the bare `database::open(state_dir)?.stats` an earlier draft carried), handing that one handle to the cleanup leg's storage; and `--fix` opens it once per pass — the repair's own open, then the re-check's, never two at once (the repair's handle is dropped before `checks` runs, which is why the second open is not the collision the first paragraph forbids) — creates it if it is missing (a repair owns the private state it clears), and builds its storage from that same handle with `FsStorage::new_from_db` (`crates/tinio-fs/src/backend/mod.rs:355`, whose own doc names doctor orchestration as its caller). A second handle is not a style question: with an `FsStorage` alive, both `database::open` and `check_integrity` answer `Err: Database already open. Cannot acquire lock.`, and after it is dropped both succeed.
- Style (`docs/style.md`): compressed prose, 3+ segment imports de-qualified (`clippy::absolute_paths`, `-D warnings`), per-crate `Error`. Expect roughly 15 `absolute_paths` sites across this plan's blocks; fix them in place as the lint step reports them.
- **`doctor`'s resolution is plan B's, not a second one**: the root, the mode and the state dir come from `resolve.rs` (decision 19), so a read-only root is diagnosed in its own state dir and its writability check is skipped. The mode resolves from `--read-only` > `TINIO_READ_ONLY` > `[server] read_only` (decision 4), and the state dir is then probed the way `status` and `stop` probe it: the resolved candidate first, and — when it holds no live server — the other one (`<root>/.tinio` ↔ the relocated `~/.tinio/roots/<sha256(canonical root)16>/`), with the report naming which candidate answered. The pair is plan B's `resolve::state_dir_candidates` — called, never re-derived — and its shape is plan B's Task 2 declaration (Task 2's Interfaces in `2026-09-20-cli-lifecycle.md`), which is authoritative over any three-argument shorthand elsewhere: `state_dir_candidates(root, resolved, read_only, override_dir: Option<PathBuf>, derived: impl FnOnce() -> Result<PathBuf, Error>) -> Result<Vec<PathBuf>, Error>`, so this plan's one call site passes `tinio_config::state::env_state_dir()?` for the override (`TINIO_STATE_DIR` is consulted here and nowhere else) and `|| tinio_config::state::default_home_state_dir(&resolution.root)` for the derived candidate; the probe is plan A's `State::read` + `tinio_api::client::probe`; the chosen directory is what every leg below and `--fix` itself run against, which is what keeps a run's findings and a `--fix`'s repairs in the directory the server actually uses (`checks` takes it as a `&StateDir` carrying the directory, which candidate it is and what the probe answered, and its first row says which candidate answered; the probe itself is `liveness`, one function, so the refusal and the report cannot disagree). A read-only root has no `.tinio/` of its own, so `tinio doctor` for such a root needs an explicit `DIR`: the walk-up finds only read-write roots.
- **Execution**: subagent-driven — one fresh subagent per task, reviewed before the next starts; tick a task's step boxes only after its review is accepted.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `crates/tinio-cli/src/commands/doctor/{mod.rs, checks.rs, audit.rs, tests.rs}` (new) | the command: the check list, the store rule (`open_store`, one handle per pass), the repair call, the exit codes — landed as a directory module, split into the entry (`mod.rs`), the check list (`checks.rs`) and its unit tests (`tests.rs`), and one further split of the check list itself (`audit.rs`, the any-depth name walk and the two rows it feeds — 2026-09-23, code-review round: the walk is a `read_dir` traversal with rules of its own, and `checks.rs` is the list that calls it) | 1–3 |
| `crates/tinio-cli/src/report.rs` (new) | `Report`/`Finding`/`Severity`, the human and `--json` renderings | 1 |
| `crates/tinio-cli/src/cli.rs` | plan B's `DoctorArgs` as this command consumes it — `dir` and `--read-only` flattened from plan B's `DirArgs`, plus plan B's `--json`, `--dry-run`, `--fix`; this plan's edits in that file are the `--dry-run`/`--fix` exclusivity and their `#[cfg(feature = "api")]` gate, and nothing else | 1 |
| `crates/tinio-cli/Cargo.toml` | `serde` (derive) + `serde_json` in `[dependencies]` — the report's renderer is `src/` code and neither is declared today; and, in Task 3, `rustix = { workspace = true, features = ["fs"] }` under `[target.'cfg(unix)'.dependencies]` (the workspace already pins `rustix = "1.1"`, but the `fs` feature is what makes `rustix::fs::statvfs` exist) plus `Win32_Storage_FileSystem` on the `cfg(windows)` `windows-sys` arm plan B declares — `GetDiskFreeSpaceExW` lives behind that feature and neither plan names it | 1, 3 |
| `crates/tinio-cli/src/test_support.rs` (new) | the `#[cfg(test)] pub(crate) mod` the check list's unit tests run through: the seeders and `run_checks`, registered in `lib.rs` | 2 |
| `crates/tinio-fs/src/database/mod.rs`, `crates/tinio-fs/src/database/error.rs` | `needs_compact` promoted **and re-exported** (`mod compact;` is private, so `pub` alone publishes nothing), and `Error::is_already_open` added so `doctor` asks the crate that owns the error instead of matching redb's text | 2 |
| `crates/tinio-fs/src/fsutil.rs`, `crates/tinio-fs/src/lib.rs` | `is_symlink_or_reparse` promoted **and re-exported** for the same reason (`crates/tinio-fs/src/lib.rs:30` is `mod fsutil;`), so the audit walk calls the crate's one reparse-point rule instead of copying it | 2 |
| `crates/tinio-cli/src/error.rs` | **verified, not edited**: plan B's Task 2 lands `Error::Fs(#[from] tinio_fs::Error)` (beside `Server`, `Sandbox`, `Log`) and that is the variant the repair path's `?` needs — adding a second one here would be an `E0428` | 3 |
| `crates/tinio/tests/doctor.rs` (new), `crates/tinio/tests/common/mod.rs` (new) | T057's matrix, driven through the binary — the facade crate owns the `tinio` binary and therefore its process-level tests. The matrix is the file; the fixture it is built from (the roots, the spawn shape, the store seeds) is the `common` module beside it, split out 2026-09-23 (code-review round) so the legs and what they are built from are two files — the shape `crates/tinio-server/tests/common/` already uses | 3–4 |
| `crates/tinio/Cargo.toml` | the facade's `[dev-dependencies]` rows this test file needs: `serde_json`, and `tinio-fs` for the fixture's store (a valid redb database cannot be hand-written). Plan B's Task 10 owns that file's feature rows and dependency table and already adds `tempfile` there, so this plan adds two keys beside it | 3 |
| `specs/001-s3-local-server/contracts/doctor-report.schema.json` (new) | the normative shape of `--json`: a top-level `severity` + `findings`, `check`/`severity`/`detail` per finding, `additionalProperties: false` at both levels. Its content is spelled out in Task 4 Step 3, and Task 1's key-set assertion is what keeps the renderer from drifting off it | 4 |
| `docs/tests.md`, `specs/001-s3-local-server/tasks.md`, `specs/001-s3-local-server/contracts/cli.md` | the legs; T057/T073/T074; the doctor section's five edits (the exit-code rule, the `--dry-run`/`--fix` exclusivity, the `--dry-run` run-semantics sentence, `[--read-only]` in the synopsis, and the no-api-note frame pointing at the schema). `docs/superpowers/specs/2026-09-20-cli-integration-design.md` is **read** in Task 4 and not modified | 4 |

**`report.rs` is this plan's one placement for the report type and its two renderings.** The report gets a module of its own — one `Report`/`Finding`/`Severity` surface, reachable from `commands/doctor/` and testable without a binary — rather than living beside the command. Design §2's file table **already carries the corrected `src/report.rs` row**, so the two documents agree without an edit: Task 4's docs step reads that row and verifies it instead of promising a change.

**The process-level tests live in the crate that owns the binary.** `env!("CARGO_BIN_EXE_tinio")` is defined only for the binaries of the package under test, so a test file under `crates/tinio-cli/tests/` cannot use it: `cargo test -p tinio-cli --test doctor` fails to build, however correct the test is. `tinio` is the facade package's binary (`crates/tinio`), so the spawning legs live in `crates/tinio/tests/doctor.rs` and run as `cargo test -p tinio --test doctor` — the crate that owns the binary owns the process-level tests. Plan B's Tasks 3/5/8/9/10 moved their spawning legs into the facade crate the same way. `tinio-cli` keeps unit-level tests only (this plan's `report.rs` and `commands/doctor/` `#[cfg(test)] mod tests`), which need no binary and stay under `cargo test -p tinio-cli`.

---

### Task 1: The report type and its two renderings

**Files:**
- Create: `crates/tinio-cli/src/report.rs`
- Modify: `crates/tinio-cli/src/lib.rs`, `crates/tinio-cli/src/cli.rs` (the `DoctorArgs` wiring this command consumes, never a second definition: `dir` and `--read-only` arrive with plan B's Task 2, which owns `cli.rs` and gives `DoctorArgs` its flattened `DirArgs`, and `--json`, `--dry-run`, `--fix` are fields on that same struct. This task's own edits in that file are the pair's exclusivity — `--dry-run` declared to conflict with `--fix`, so the pair is a usage error rather than a silent win for one of them — and their `#[cfg(feature = "api")]` gate, because both flags are meaningless without the plane (Task 3).), **`crates/tinio-cli/Cargo.toml`** (the two dependency rows `report.rs` compiles against and no plan adds today: `serde = { workspace = true, features = ["derive"] }` for the `Serialize` derives, and `serde_json.workspace = true` for `to_string_pretty` — both are workspace-pinned already (`Cargo.toml:56,58`), and this crate declares neither; `serde_json` is a normal dependency here and not a dev one, because the renderer lives in `src/` and its tests use `serde_json::from_str`/`json!` through it) (2026-09-23 correction, code-review round: **the wiring is not in `cli.rs`.** `cli.rs` declares `Doctor(doctor::Args)` and nothing else about this command; the `Args` type, its `--dry-run`/`--fix` pair, the `conflicts_with` exclusivity and their `#[cfg(feature = "api")]` gate all live in `crates/tinio-cli/src/commands/doctor/mod.rs`, so the two edits this row claims for `cli.rs` belong to that file. Three dependency rows this row does not name also landed: `serde` became **unconditional** in `tinio-cli` — the report type is in every build, not only the `api` one — `tinio-util`'s `testing` feature gained `serde_json` and the shared `json_keys` helper both test trees compare their key sets through, and the facade's `[dev-dependencies]` took `tokio` and `tinio-util` beside plan B's `tempfile`, and `parse-display.workspace = true` joined `tinio-cli`'s external group for `Severity`'s derived `Display`)

**Interfaces:**
- Consumes: `tinio_core::cleanup::{RepairAction, RepairActionLevel}`.
- Produces: `tinio_cli::report::{Report, Finding, Severity}` with `Severity::{Ok, Warn, Error}`, `Finding::from_action(RepairAction) -> Finding` and `Finding::informational(RepairAction) -> Finding` (the same row at `Severity::Ok` — what a plain `doctor` renders a cleanup *proposal* as; Task 2's `Proposals` is the flag that chooses between them. Both take the action **by value**: it is consumed into the row and every caller is done with it afterwards, so the description moves rather than being copied — 2026-09-23 correction, code-review round), `Report::findings(&self) -> &[Finding]` (the only way to read them; `push` and `extend` are the only ways in, and `severity()` is **derived** from the findings rather than cached beside them — 2026-09-23 correction, code-review round: the cached field the sketch below carries was dropped), `Report::severity(&self) -> Severity` (the worst finding), `Report::render_text(&self) -> String`, `Report::render_json(&self) -> Result<String, Error>`, and `Report::exit_code(&self) -> u8` implementing the contract's rule. `Report` is deliberately this crate's own type and not plan A's: `tinio_api::client::Report` (the `status`/`stop` answer — `running`, `state`, `status`) is a different type with the same name, and no file imports both, because `commands/doctor/` takes `client::{probe, State}` and never `client::Report`; the two coexist without a rename, and neither re-exports the other.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-cli/src/report.rs`:

```rust
#[cfg(test)]
mod tests {
    use tinio_core::cleanup::RepairActionLevel;

    use super::*;

    fn report() -> Report {
        let mut report = Report::default();
        report.push(Finding::check("storage root", Severity::Ok, "readable and writable"));
        report
    }

    #[test]
    fn lint_and_info_read_as_ok_and_the_rest_as_themselves() {
        assert_eq!(Finding::from_action(action(RepairActionLevel::Lint)).severity, Severity::Ok);
        assert_eq!(Finding::from_action(action(RepairActionLevel::Info)).severity, Severity::Ok);
        assert_eq!(Finding::from_action(action(RepairActionLevel::Warn)).severity, Severity::Warn);
        assert_eq!(Finding::from_action(action(RepairActionLevel::Error)).severity, Severity::Error);
        // A plain run's proposal row: the same action the `Warn` arm above maps, rendered as
        // information because a "would …" is what a fix *would* change, not a defect yet
        // (Task 2's `Proposals` is the flag that picks this mapping).
        assert_eq!(Finding::informational(action(RepairActionLevel::Warn)).severity, Severity::Ok);
        assert_eq!(
            Finding::informational(action(RepairActionLevel::Warn)).detail,
            Finding::from_action(action(RepairActionLevel::Warn)).detail,
            "the two renderings differ in severity alone"
        );
    }

    fn action(level: RepairActionLevel) -> tinio_core::cleanup::RepairAction {
        tinio_core::cleanup::RepairAction { level, description: "pruned a stale temp file".into() }
    }

    #[test]
    fn the_worst_finding_decides_the_severity() {
        let mut report = report();
        assert_eq!(report.severity(), Severity::Ok);
        report.push(Finding::check("temp files", Severity::Warn, "3 stale files"));
        assert_eq!(report.severity(), Severity::Warn);
        report.push(Finding::check("meta store", Severity::Error, "unreadable"));
        assert_eq!(report.severity(), Severity::Error);
    }

    #[test]
    fn the_human_rendering_names_every_check_and_its_severity() {
        let text = report().render_text();
        assert!(text.contains("ok"), "{text}");
        assert!(text.contains("storage root"), "{text}");
    }

    /// The keys of one JSON object, sorted.
    fn keys(value: &serde_json::Value) -> Vec<String> {
        let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
        keys.sort();
        keys
    }

    #[test]
    fn the_json_rendering_is_the_same_report() {
        let text = report().render_json().expect("json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert_eq!(parsed["findings"][0]["check"], "storage root");
        assert_eq!(parsed["findings"][0]["severity"], "ok");
        assert_eq!(parsed["severity"], "ok");
        // The key set is the contract's, not the implementation's: `doctor-report.schema.json`
        // (Task 4) declares `additionalProperties: false` at both levels, so a field added on
        // one side only must fail here rather than ship as a schema break.
        assert_eq!(keys(&parsed), ["findings", "severity"]);
        assert_eq!(keys(&parsed["findings"][0]), ["check", "detail", "severity"]);
    }

    #[test]
    fn an_empty_report_renders_a_concrete_severity() {
        let text = Report::default().render_json().expect("json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");
        assert_eq!(parsed["findings"], serde_json::json!([]));
        assert_eq!(parsed["severity"], "ok", "never `null`: a consumer reads one concrete severity");
    }

    #[test]
    fn the_exit_code_follows_the_contract() {
        // `contracts/cli.md`'s doctor section: 0 = no problems, 1 = warnings/errors found — with `--dry-run`
        // and with `--fix` alike, where the report holds what the repair left behind. The mode
        // is not an argument to this function: the flags decide which findings the report has
        // (Task 3, through Task 2's `Proposals` — a plain run renders the cleanup proposals as
        // information, `--dry-run` renders them as findings), and the arithmetic is the
        // severity's.
        assert_eq!(report().exit_code(), 0, "0 = no warn and no error");
        let mut dirty = report();
        dirty.push(Finding::check("temp files", Severity::Warn, "3 stale files"));
        assert_eq!(dirty.exit_code(), 1, "a warn is a problem, in every mode");
        dirty.push(Finding::check("meta store", Severity::Error, "unreadable"));
        assert_eq!(dirty.exit_code(), 1, "so is an error");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli report::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! The doctor's report: one finding per check, two renderings, one exit code.
//!
//! `--json` is the same report as `render_text`, not a second pass over the filesystem
//! (design §4): the checks run once and the renderer is what differs.

use serde::Serialize;
use tinio_core::cleanup::{RepairAction, RepairActionLevel};

use crate::Error;

/// The severity a finding carries, mapped from the engine's four levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Nothing to do.
    Ok,
    /// Something the operator should look at, repaired or not.
    Warn,
    /// Something that could not be repaired.
    Error,
}

/// One check's answer.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    /// What was checked.
    pub check: String,
    /// How bad it is.
    pub severity: Severity,
    /// What was found, in one line.
    pub detail: String,
}

impl Finding {
    /// A check's own finding.
    pub fn check(check: impl Into<String>, severity: Severity, detail: impl Into<String>) -> Self {
        Self { check: check.into(), severity, detail: detail.into() }
    }

    /// A repair action as a finding (`Lint`/`Info` are findings, `Warn`/`Error` are problems).
    pub fn from_action(action: RepairAction) -> Self {
        let severity = match action.level {
            RepairActionLevel::Lint | RepairActionLevel::Info => Severity::Ok,
            RepairActionLevel::Warn => Severity::Warn,
            RepairActionLevel::Error => Severity::Error,
        };
        Self { check: "private state".to_string(), severity, detail: action.description }
    }

    /// A cleanup **proposal** as a plain run's informational row: the same row as
    /// [`Self::from_action`], at [`Severity::Ok`], because a "would …" describes what a fix
    /// would change rather than a defect the root has. `--dry-run` renders that same action
    /// through [`Self::from_action`] instead, so the two modes differ by this one mapping and
    /// by nothing else — which is what makes `--dry-run`'s list exactly `--fix`'s list
    /// (`contracts/cli.md`'s doctor section). A stage that *failed* never comes through here: failures ride
    /// the action stream's `Err` side and are `Severity::Error` in both modes.
    pub fn informational(action: RepairAction) -> Self {
        Self { severity: Severity::Ok, ..Self::from_action(action) }
    }
}

/// Every finding of one run.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// The findings, in the order the checks produced them. Private, so `findings()` is the only
    /// way to read them.
    findings: Vec<Finding>,
}

/// The report as `--json` writes it: the same findings, with a severity that is never absent
/// (a report with no findings still serializes a concrete `"ok"`, never `null`).
#[derive(Serialize)]
struct Rendered<'a> {
    // `severity` first, the schema's own order (`contracts/doctor-report.schema.json`'s
    // `required` list): the emitted object reads the same way the document does.
    severity: Severity,
    findings: &'a [Finding],
}

impl Serialize for Report {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Rendered { findings: &self.findings, severity: self.severity() }.serialize(serializer)
    }
}

impl Report {
    /// The findings, in the order the checks produced them.
    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// Adds a finding. The only way a finding enters a report, which is what keeps the cached
    /// severity true of the vector it caches.
    pub fn push(&mut self, finding: Finding) {
        self.severity = Some(match self.severity {
            Some(current) => current.max(finding.severity),
            None => finding.severity,
        });
        self.findings.push(finding);
    }

    /// The worst severity, `Ok` when there were no findings at all.
    pub fn severity(&self) -> Severity {
        self.severity.unwrap_or(Severity::Ok)
    }
}
```

`render_text` prints one line per finding as `{severity:<5} {check}: {detail}` plus a summary line, and `render_json` is `serde_json::to_string_pretty(self)` — the same report, with the severity always concrete: `"ok"` for an empty report, never `null`, which is why `Report`'s `Serialize` goes through the `Rendered` view rather than the `Option` field. That call is the one place a serialization failure could surface, and its mapping is fixed here rather than left to the implementer: `tinio_cli::Error` has no `From<serde_json::Error>` and one is not wanted — a report that cannot be encoded is an operational failure of this process, not a usage or storage error — so `render_json` answers `Error::Operational(format!("the report could not be encoded: {err}"))`. (`Serialize` for this type cannot actually fail — three strings and a lowercase enum per row — so the arm is a floor, not a path.) The finding vector is private and read through `findings()` so that no caller can append past `push` and leave the cached severity describing a report that no longer exists; `Rendered` keeps borrowing `&self.findings` because it lives in the same module, and the borrow is a field read rather than a second path into the type.

**The JSON shape is a schema, and the schema is a file this plan creates.** `--json` renders `{"severity": "ok|warn|error", "findings": [{"check": …, "severity": …, "detail": …}]}`, with `additionalProperties: false` at both levels, and an empty report still carries a concrete `"ok"` rather than a `null`. The normative statement of that shape is `specs/001-s3-local-server/contracts/doctor-report.schema.json` (Task 4 spells its content out verbatim so the implementer copies it); `the_json_rendering_is_the_same_report`'s key-set assertion is the other half of the same rule — it fails the moment a field is added on one side only, so the renderer and the schema cannot drift apart in silence.

`cli.rs` is plan B's file and `DoctorArgs` is plan B's type (its Task 2): this task **consumes the fields it carries** and declares no second copy of any of them — `dir` and `--read-only` come from the `DirArgs` plan B flattens into `DoctorArgs` (the same `TINIO_READ_ONLY` environment name `ServerArgs` carries), and `--json`, `--dry-run` and `--fix` are already fields on that same struct, put there by plan B's Task 2. What this task owns in `cli.rs` is two things and nothing else — **except that it owns them in `commands/doctor/mod.rs`**: the delivery put `Args` beside the command body, so `cli.rs` only names `Doctor(doctor::Args)` and the exclusivity, the gate and the flag fields live with the type (2026-09-23 correction, code-review round). First, the pair's exclusivity: `--dry-run` and `--fix` are declared **mutually exclusive** in clap (`conflicts_with`), so `--dry-run --fix` is a usage error and exits 2, rather than one flag silently winning over the other; the two are one call with one flag apart (Task 3), which is a statement about the code path and not a licence to pass both. Second, the gate: both flags carry `#[cfg(feature = "api")]`, because neither can do anything in a build with no plane to stop, no `state` file to clear and no socket to reclaim — plan B's struct is where they live, and their absence from a `--no-default-features` build is a compile-out and not a runtime refusal (Task 3).

`exit_code` implements the contract's rule in one place (`contracts/cli.md`'s doctor section): **`0` when the report holds no warn and no error, `1` when it holds either** — with or without `--dry-run`/`--fix`, because the mode is not an argument to this function; it changes which findings the report has, not the arithmetic. With `--fix` that means the report holds what the repair *left behind* (Task 3), so a run that cleared everything exits 0 while a finding no repair addresses exits 1. **The two cleanup renderings are why a plain run and `--dry-run` can differ without this function knowing about either**: a plain run renders the cleanup leg's proposals through `Finding::informational`, so "would clear …" is an `ok` row that leaves the exit code alone, while `--dry-run` renders the same rows through `Finding::from_action` — a `warn` — so a root with something to fix exits 1 there and 0 without the flag (Task 2's `Proposals`, Task 3's `run`). `2` is never returned here: usage errors are clap's own and reach the process through `Error::exit_code`. The contract line's parenthetical today attaches `1` to `--dry-run` alone and would have a plain `doctor` answer `0` forever; this feature rewrites it, and the edit lands in Task 4's docs step — `specs/001-s3-local-server/contracts/cli.md` is not edited by this task.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli report::`
Expected: PASS — seven tests.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 2: The checks

**Files:**
- Create: `crates/tinio-cli/src/commands/doctor/`, `crates/tinio-cli/src/test_support.rs` (the `#[cfg(test)] pub(crate) mod` the tests below call, registered in `lib.rs` beside the module list)
- Modify: `crates/tinio-cli/src/commands/mod.rs`, `crates/tinio-cli/src/lib.rs`, `crates/tinio-fs/src/database/mod.rs`, `crates/tinio-fs/src/database/error.rs` (the re-export and the predicate the meta legs need), `crates/tinio-fs/src/lib.rs` **and** `crates/tinio-fs/src/fsutil.rs` (the walk's reparse-point rule, all three listed in the Interfaces below)

**Interfaces:**
- Consumes: `Report`/`Finding`/`Severity` including `Finding::informational` (Task 1), `Resolution` (plan B's `resolve` module), `resolve::state_dir_candidates` (the ordered candidate pair, plan B's Task 2), `tinio_fs::{FsCleanup, FsStorage, is_symlink_or_reparse}` (the last re-exported by this task's `lib.rs` edit — the walk calls it rather than copying the Windows rule), `tinio_fs::database::{Integrity, Open, Stats, check_integrity, open, needs_compact}` (the last one **re-exported** by `crates/tinio-fs/src/database/mod.rs` — `mod compact;` is private, so the visibility change alone publishes nothing), `tinio_core::cleanup::{Cleanup, CleanupOptions, RepairKind}`, `tinio_core::{bucket, object}`, and — in `api` builds — `tinio_api::state::{socket_path, State}`, `tinio_api::client::probe` and `tinio_api::bind::{SUN_PATH_MAX, reclaim_stale}`.
- Consumes three things other crates must add, named here rather than edited elsewhere. **`Resolution` must carry the configuration's load outcome beside the parsed config** — `pub config: Config` plus an added `pub config_error: Option<String>` (or an equivalent) which plan B's `resolve::for_doctor` fills instead of failing with `?`: a root whose config does not parse is the case `doctor` exists for, and `for_doctor` returning `Err` there would make the "configuration valid" check unreachable and hand the operator a bare error where the contract promises a report (plan B's Task 2 owns the field; this plan consumes it, and says so in Task 3's `run` sketch too). **`tinio_api::bind` must export `SUN_PATH_MAX`** (plan A's own `const`, 104 on macOS and 108 elsewhere, plan A Task 2's `bind.rs`), so this plan's socket-path check compares against the limit the bind enforces instead of re-spelling a number that is wrong on one of the three platforms. That is a request to that task rather than a given: the constant is private today and plan A's Task 2 `Produces` line names only `Channel`, `bind_unix`, `reclaim_stale` and `bind_pipe`, so whichever task owns the bind block makes it `pub` and lists it there. **`tinio_fs::fsutil::is_symlink_or_reparse` must be promoted to `pub` and re-exported** (`crates/tinio-fs/src/fsutil.rs:28-33`), so the audit walk's Windows branch tests the same reparse-point rule the crate's own walkers use — a symlink, or `FILE_ATTRIBUTE_REPARSE_POINT` — rather than copying it into a second place where only one of the two would get fixed. The promotion alone would publish nothing, and this is the same trap as `needs_compact` above: `crates/tinio-fs/src/lib.rs:30` declares `mod fsutil;` privately, so the fix is `pub fn` in `fsutil.rs` **plus** `pub use self::fsutil::is_symlink_or_reparse;` in `lib.rs` — a re-export rather than `pub mod fsutil;`, because the module's other helpers are internal and this check needs the one rule, not the surface.
- Produces: `tinio_cli::commands::doctor::{checks, free_space, below_floor, liveness, open_store, socket_path_of, Liveness, Pass, Probe, Proposals, StateDir, Store, DEFAULT_FLOOR_BYTES}` with `pub async fn checks(run: &Run<'_>, proposals: Proposals) -> Report` — pure enough to test against a temp root, the single place the check list lives, and the single place the store is probed and opened — plus `pub fn free_space(root: &Path) -> Finding`, `pub fn below_floor(free: u64, total: u64) -> bool` (the floor arithmetic on its own, so the unit test pins it with explicit numbers rather than a filesystem), `pub async fn liveness(state_dir: &Path) -> Result<Probe, Error>` (the one probe: plan A's `State::read` + `client::probe`, carrying the recorded `control_name` out with the verdict), `pub fn socket_path_of(run: &Run<'_>) -> Result<PathBuf, Error>` (`#[cfg(all(feature = "api", unix))]` — the recorded name else the derived one, and the one place the socket's path is decided, for the check and for `--fix` alike), `pub fn open_store(dir: &Path, pass: Pass) -> Result<Store, Error>` (the one access to `meta.redb`, and it takes the **chosen** state dir — `StateDir.dir`, never `resolution.state_dir` on its own, or the store would be opened in a directory the run decided not to use), and `Proposals::{Informational, Findings}`, the plain/`--dry-run` distinction. The legs also take **`Run<'_>`** — the resolution and the chosen state dir as one value (2026-09-23 correction, code-review round): almost every leg is a function of both, and passing them as two parameters makes it possible to hand a leg one run's resolution and another's state dir with the compiler none the wiser, so the pair travels as one `pub struct Run<'a> { resolution: &'a Resolution, state: &'a StateDir }` and `checks`, `fs_options`, `state_findings`, `socket_findings` and `socket_path_of` all take it. **Every type in those signatures is `pub`** — `Proposals`, `Pass`, `Store`, `Probe`, `Liveness`, `StateDir`, `Run` — because a private type in a public signature is `private_interfaces`, which `-D warnings` denies.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-cli/src/commands/doctor/`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_clean_root_reports_ok() {
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Ok, "{:?}", report.findings());
    }

    #[tokio::test]
    async fn a_symlink_while_following_is_disabled_is_reported() {
        #[cfg(unix)]
        {
            let dir = tempfile::TempDir::new().expect("temp");
            crate::test_support::seed_config(dir.path());
            crate::test_support::seed_bucket_with_symlink(dir.path());
            let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
            assert_eq!(report.severity(), Severity::Warn);
            assert!(
                report.findings().iter().any(|f| f.check.contains("symlink")),
                "{:?}",
                report.findings()
            );
        }
    }

    #[tokio::test]
    async fn a_read_only_root_skips_the_writability_check() {
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).expect("read-only");
        }
        let report = crate::test_support::run_checks(dir.path(), true, Proposals::Informational).await;
        assert!(
            !report.findings().iter().any(|f| f.check.contains("writable") && f.severity != Severity::Ok),
            "a read-only mode run must not complain about a read-only root: {:?}",
            report.findings()
        );
        #[cfg(unix)]
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).expect("restore");
    }

    #[tokio::test]
    async fn an_unparseable_config_is_an_error_finding() {
        let dir = tempfile::TempDir::new().expect("temp");
        std::fs::create_dir_all(dir.path().join(".tinio")).expect("reserved dir");
        std::fs::write(dir.path().join(".tinio/.tinio.toml"), "version = 1\n[server]\nport = \"nine\"\n").expect("seed");
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Error, "{:?}", report.findings());
        assert!(report.findings().iter().any(|f| f.check.contains("config")), "{:?}", report.findings());
    }

    #[tokio::test]
    async fn an_invalid_name_several_levels_down_is_reported() {
        // The rule is about **any depth** (`contracts/cli.md`'s doctor section, T073): a `.tinio` segment
        // below the bucket level is exactly the case it exists for, and a top-level-only walk
        // would silently pass it.
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        let deep = dir.path().join("bucket/a/b/.tinio");
        std::fs::create_dir_all(&deep).expect("deep reserved segment");
        std::fs::write(deep.join("object.txt"), b"data").expect("object");
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Warn, "{:?}", report.findings());
        assert!(
            report.findings().iter().any(|f| f.check.contains("name") && f.detail.contains(".tinio")),
            "{:?}",
            report.findings()
        );
    }

    #[tokio::test]
    async fn a_missing_config_is_a_warning_and_never_created() {
        // `doctor` is offline diagnostics: the file's absence is a warn (`a first start would
        // auto-create it`), and the run must leave the root exactly as it found it. Nothing
        // creates `.tinio` here on either count: the config leg goes through `Config::load`,
        // never `load_or_create`, and the store legs answer an absent `meta.redb` without
        // opening anything (the Global Constraints' store rule).
        let dir = tempfile::TempDir::new().expect("temp");
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Warn, "{:?}", report.findings());
        assert!(report.findings().iter().any(|f| f.check.contains("config")), "{:?}", report.findings());
        assert!(!dir.path().join(".tinio").exists(), "doctor never auto-creates anything");
    }

    #[test]
    fn the_floor_is_the_larger_of_one_gib_and_five_percent() {
        // The floor is arithmetic over the probed totals, so it is pinned with explicit
        // numbers instead of a filesystem: `free_space` computes
        // `max(DEFAULT_FLOOR_BYTES, total / 20)` and `below_floor` is that rule on its own,
        // which is the whole point of splitting it out — two arms, both driven here.
        const GIB: u64 = 1024 * 1024 * 1024;
        // A small filesystem: the fixed 1 GiB arm decides, never the percentage.
        assert!(below_floor(GIB - 1, 4 * GIB), "just under 1 GiB free");
        assert!(!below_floor(GIB, 4 * GIB), "1 GiB free is at the floor, not below it");
        // A large filesystem: 100 GiB total puts 5% (5 GiB) above the fixed arm, so this is
        // the percentage arm and only this arm.
        assert!(below_floor(5 * GIB - 1, 100 * GIB), "just under 5% of 100 GiB");
        assert!(!below_floor(5 * GIB, 100 * GIB), "5% is at the floor, not below it");
        assert!(!below_floor(50 * GIB, 100 * GIB), "half the volume free");
        assert!(below_floor(0, 0), "an empty filesystem is below any floor");
    }

    #[test]
    fn free_space_reports_the_amount_and_its_percentage() {
        // The probe itself, on whatever filesystem the test runs from: this asserts the
        // shape of the answer — both numbers are in the detail — and never a byte count.
        let dir = tempfile::TempDir::new().expect("temp");
        let finding = free_space(dir.path());
        assert_eq!(finding.check, "free space");
        assert!(finding.detail.contains('%'), "the amount and its percentage: {}", finding.detail);
        assert_eq!(finding.severity, Severity::Ok, "a test host is not below the floor: {}", finding.detail);
    }

    #[tokio::test]
    async fn a_missing_meta_store_is_not_a_finding_and_is_never_created() {
        // A root that has never opened a store has nothing to check: absent is `ok`, not a
        // warn that would nag every fresh root — **and it is not opened either**, which is the
        // half that cannot be seen in the report. `tinio_fs::database::open` would create the
        // file, so the leg probes first; this assertion is the only thing that fails if that
        // probe is ever replaced by an open.
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        let meta: Vec<&Finding> = report.findings().iter().filter(|f| f.check.contains("meta.redb")).collect();
        assert!(!meta.is_empty(), "the leg reports even when there is nothing to check: {:?}", report.findings());
        assert!(meta.iter().all(|f| f.severity == Severity::Ok), "{:?}", report.findings());
        assert!(
            !dir.path().join(".tinio/meta.redb").exists(),
            "a diagnostics run over a store-less root creates nothing: the probe is a `symlink_metadata`, not an open"
        );
    }

    #[tokio::test]
    async fn a_corrupt_meta_store_is_an_error() {
        // A file that is not a redb database: `check_integrity` cannot even open it, so the
        // leg takes the **error** arm (`Corrupted` is the arm for a database that opens and
        // fails its check; this one never opens) — and the run still produces a report.
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        std::fs::create_dir_all(dir.path().join(".tinio")).expect("reserved dir");
        std::fs::write(dir.path().join(".tinio/meta.redb"), b"not a redb file").expect("seed");
        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Error, "{:?}", report.findings());
        assert!(
            report.findings().iter().any(|f| f.check.contains("meta.redb") && f.severity == Severity::Error),
            "{:?}",
            report.findings()
        );
    }

    #[tokio::test]
    async fn a_store_held_by_another_process_is_a_warn_not_an_error() {
        // The test holds the store open first (that `FsStorage::new` also creates the file is
        // the point: the leg's probe finds it and tries to use it), so neither
        // `check_integrity` nor the diagnostics pass's single open can take it: the redb lock
        // answers `Database already open`, `tinio_fs::database::Error::is_already_open` is the
        // predicate that recognises it, and that is "not inspected" — a warn carrying the
        // reason, never an error it cannot substantiate.
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        let _held = tinio_fs::FsStorage::new(dir.path(), tinio_fs::testing::fs_options()).expect("take the store");

        let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        let meta = report.findings().iter().find(|f| f.check.contains("meta.redb")).expect("the leg reports");
        assert_eq!(meta.severity, Severity::Warn, "{:?}", report.findings());
        assert!(meta.detail.starts_with("not inspected:"), "{}", meta.detail);
    }
}
```

`crate::test_support` is a `#[cfg(test)] pub(crate) mod` holding the seeders and `run_checks(root, read_only, proposals)`, which builds a `Resolution` for the temp root — config discovery through `discover::resolve_path` and `Config::load`, **never `load_or_create`** — fills the `config_error` field the Interfaces require (from `Config::load`'s failure instead of propagating it, which is what lets a `Resolution` exist for a root whose config does not parse), builds a `StateDir` whose `dir` is the resolved candidate (`resolved: true`) with its `liveness` from the one probe, and calls `checks`; it exists so the check list is testable without a binary. `seed_config` writes a **complete** configuration — the `[auth]` pair included — because "no credentials at all" is a `warn` of its own: a seeded root that omitted the pair would make `a_clean_root_reports_ok` unreachable for a reason that has nothing to do with the check it tests. `seed_bucket_with_symlink` plants a link entry inside a bucket (`#[cfg(unix)]` where the tests use it, since it needs a real symlink).

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli doctor::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

`checks` runs the contract's list in one pass against the **chosen** state dir; the simple checks contribute exactly one `Finding` each, the private-state leg one per action and the meta-store leg one per answer. Its first row is the run's own frame, because decision 19's dual-candidate probe has to be visible in the product and not only in the code that chose: `Finding::check("state directory", Severity::Ok, …)` names the directory every state-dependent leg read, and whether it is the one plan B's resolution named.

| Check | Source | Severity |
|---|---|---|
| state directory (the run's frame) | the `StateDir` this run chose — the resolution's own candidate, or the other one when nothing answered there (`resolve::state_dir_candidates`, probed by `liveness`) — named with which of the two answered | ok |
| storage root exists, readable, writable | `std::fs::metadata` + a probe file; **skipped when read-only** (`contracts/cli.md`'s doctor section) | error / ok |
| configuration valid | the `config_error` plan B's `for_doctor` now carries (**error**, and every other leg's config reads fall back to defaults with this row explaining why) — else `discover::resolve_path` then `Config::load` (`config.md:138`), three outcomes: **missing → warn** (a first `start` would auto-create it), **unknown key or schema-invalid, including half a credential pair → error**, **no credentials at all → warn** (`"a start would generate session credentials (printed once)"`, `config.md:141`) | warn / error |
| private state integrity | one store access (`open_store`), then `FsCleanup::new(&storage, CleanupOptions { dry_run: true, access }).repair(RepairKind::Full)`, streamed: each `Ok(RepairAction)` through `Finding::from_action` under `Proposals::Findings` and through `Finding::informational` under `Proposals::Informational`, each `Err(err)` as an `error` finding in **both** modes — the engine emits only `Warn` actions and reports failures on the stream's error side. No store → `ok` ("nothing to clear"), no actions at all → `ok` ("nothing to clear"), store held by another process → `warn` "not inspected" | warn / error; ok in a plain run |
| `meta.redb` integrity and fragmentation | `symlink_metadata` first: **absent → `ok` and return, opening nothing**. Present → `tinio_fs::database::check_integrity(state_dir)` for the three `Integrity` arms (`Healthy` → ok, `Repaired` → warn, `Corrupted(error)` → error, whose remedy the detail names: delete and rebuild, the metadata is derivable), and `needs_compact(&open.stats, threshold)` from the **same single open** the cleanup leg's storage is built on — `threshold` = `[storage.fs] compact_threshold_percent`, never a number this leg picked. `database::Error::is_already_open()` → the "cannot run" warn; any other open failure → error | ok / warn / error |
| on-disk bucket names and object keys, at **every depth** | this plan's own audit walk, every name through `tinio_core::bucket::name` / `object::key` / `is_reserved_key` (`contracts/cli.md`'s doctor section, T073) | warn |
| symlinks present while `follow_symlinks` is disabled | the same walk's `symlink_metadata` observations | warn |
| the unix control socket's path length, `#[cfg(all(feature = "api", unix))]` | `socket_path_of(run)` — the recorded `control_name`, else the derived `state::socket_path`, one function (never a number or a name re-spelled here) — against `tinio_api::bind::SUN_PATH_MAX`, plan A's own constant; the remedy names `[api.unix] path`. The gate is the same pair as the row below and for the same reason: the check calls `tinio_api`, and `tinio-api` is an optional dependency, so `#[cfg(unix)]` alone would break the `--no-default-features` graph. When the configuration carries an `[api.unix] path` and no state file has recorded one yet, the check measures that configured path instead, because that is the path a bind would use and the path the warning's remedy writes — plan B's `api_options` is the one owner of that resolution (relative to the state dir, absolute verbatim; `contracts/config.md:153`) | warn |
| low free disk space | `free_space(root)`, reported as both the free amount and its percentage | warn |
| stale `state`/control socket, `#[cfg(all(feature = "api", unix))]` | the run's own `liveness`: `Live` → `ok` (a server is running — this is a *diagnosis*, not a refusal; only `--fix` refuses); `Stopped` and `NoStateFile` → `tinio_api::bind::reclaim_stale(socket_path_of(run)?, true)` for the socket — the name the `state` file recorded in the first case, plan A's derived name in the second, and the one function owns the choice — (`Ok(true)` → warn "a stale control socket", `Ok(false)` → ok, `Err` = a listener that answered anyway → warn "not inspected: …", never an error for a healthy root); `Unreadable(reason)` → warn "not inspected: <reason>" (the state file is there but cannot be decoded: the live/stale question cannot be answered, so it is not answered). **Windows has no such leg**: the local channel is a named pipe, a pipe's live/stale state is the plane's bind rather than a file to probe, and `reclaim_stale` is `#[cfg(unix)]` — so the leg's gate is `all(feature = "api", unix)`, not the feature alone. (**Correction, 2026-09-20, R15:** the *whole* leg is not that pair — the stale-`state` row is api-wide and only the socket half is unix-gated; `contracts/cli.md`'s doctor section states the shape.) | ok / warn |

Every check that needs the state runs on that **chosen** state dir, which is what makes a read-only root diagnose correctly (decision 19) and what keeps `--fix` from being pointed at the wrong directory. `open_store` is the one exception in shape, not in rule: it is the check list's *only* access to `meta.redb`, and the storage the private-state leg repairs through is built from the very handle it returns (`FsStorage::new_from_db`), so there is no moment at which two handles to the same store exist. The cleanup leg is constructed with `dry_run: true` in every mode: the check reports, the `Proposals` mapping decides how loudly, and only `--fix` (Task 3) constructs it with `dry_run: false`.

The machinery the table names, as one block, because three of its items are the ones a later edit could quietly undo:

```rust
/// How the cleanup leg's dry-run proposals are reported — the whole difference between a
/// plain `doctor` and `--dry-run` (Task 3 sets it from the flag).
///
/// `pub` with its siblings below, and not for show: all three appear in the signature of a
/// `pub fn`, and a private type in a public signature is `private_interfaces` — a deny-by
/// `-D warnings` lint, which is what the lint steps of this plan run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proposals {
    /// A plain run: a "would …" row is information about what a fix would change, not a
    /// defect, so it renders `ok` and leaves the exit code alone.
    Informational,
    /// `--dry-run`: the same rows render as findings, so the run lists exactly what a fix
    /// would change and exits 1 when there is something to change (`contracts/cli.md`'s doctor section).
    Findings,
}

/// The state dir this run chose, and what answered there.
#[derive(Debug, Clone)]
pub struct StateDir {
    /// The directory every state-dependent leg reads, and the one `--fix` repairs.
    pub dir: PathBuf,
    /// `true` when `dir` is the candidate plan B's resolution named, `false` when the other
    /// one answered instead (decision 19) — the report's first row says which.
    pub resolved: bool,
    /// What `liveness` found there, so the control-channel refusal (Task 3) and the
    /// stale-state leg cannot disagree about the same directory.
    pub liveness: Liveness,
    /// The `control_name` the `state` file recorded, from the same probe: `Some` whenever a
    /// state file was read (whatever the verdict), `None` when there was none or it did not
    /// decode. It is what the socket probe and a `--fix`'s unlink must use — `[api.unix] path`
    /// may override the derived name, so re-deriving it aims at a file that was never the
    /// channel (decision 24, and [`socket_path_of`] is the one place the choice is made).
    pub control_name: Option<String>,
}

/// What the `state` file and the control channel say about a state dir.
#[derive(Debug, Clone)]
pub enum Liveness {
    /// A server answered its control channel.
    Live,
    /// A `state` file is there with nothing answering behind it: the crashed state a repair may
    /// clear, and the case whose recorded `control_name` the socket probe must use.
    Stopped,
    /// No `state` file at all — a root that has never run a server, which is why the socket, if
    /// anything is asked about it, falls back to plan A's derived name.
    NoStateFile,
    /// The `state` file is there but cannot be read or decoded — a version this build does
    /// not read, or bytes that are not a document. Nothing about the server is known, so this
    /// is neither `Live` nor `Stopped`, and the leg that meets it reports
    /// "not inspected: <reason>" rather than guessing. `State::read` answering `Err` here is a
    /// fact about the file, not an operational failure of the run. The name is unknown with it,
    /// so the socket falls back to the derived one.
    Unreadable(String),
}

/// One probe's answer: the verdict, and the name the `state` file recorded when it was read.
#[derive(Debug, Clone)]
pub struct Probe {
    /// The verdict.
    pub liveness: Liveness,
    /// The recorded `control_name`, for [`socket_path_of`].
    pub control_name: Option<String>,
}

/// The one probe: plan A's `State::read` plus `client::probe`, called by the run that picks
/// the candidate and by the check that reports on it — never written twice, so the refusal,
/// the report and the repair cannot disagree. It carries the recorded `control_name` out with
/// it, because that name is only knowable here: reading the file a second time to get it would
/// be the second probe this function exists to prevent.
#[cfg(feature = "api")]
pub async fn liveness(state_dir: &Path) -> Result<Probe, Error> {
    Ok(match State::read(state_dir) {
        Ok(None) => Probe { liveness: Liveness::NoStateFile, control_name: None },
        Ok(Some(state)) => Probe {
            liveness: if tinio_api::client::probe(&state).await {
                Liveness::Live
            } else {
                Liveness::Stopped
            },
            control_name: Some(state.control_name),
        },
        Err(err) => Probe { liveness: Liveness::Unreadable(err.to_string()), control_name: None },
    })
}

/// Without the plane there is no `state` file and no control channel: nothing to probe.
#[cfg(not(feature = "api"))]
pub async fn liveness(_state_dir: &Path) -> Result<Probe, Error> {
    Ok(Probe { liveness: Liveness::NoStateFile, control_name: None })
}

/// The control channel's path for the chosen state dir: the name the `state` file recorded
/// when there is one, and plan A's `state::socket_path` derivation when there is not.
///
/// One place, because the two callers are the socket check's probe and `--fix`'s reclaim, and
/// they must ask about the same file: an `[api.unix] path` override makes the recorded name the
/// only truth, so a `--fix` that re-derived would unlink a path that was never the socket and
/// leave the stale one behind (decision 24: clients read the name, they never re-derive it).
#[cfg(all(feature = "api", unix))]
pub fn socket_path_of(run: &Run<'_>) -> Result<PathBuf, Error> {
    match state.control_name.as_deref() {
        Some(recorded) => Ok(PathBuf::from(recorded)),
        None => socket_path(&state.dir, &resolution.root).map_err(Error::Api),
    }
}

/// Which pass is asking for the store — the one thing that decides whether it may be created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// The diagnostics pass: **never creates a store**. An absent `meta.redb` is answered
    /// [`Store::Absent`] without opening anything, because `database::open` would create the
    /// file and `check_integrity` would fail on it, and a run that diagnoses a root must
    /// leave it exactly as it found it.
    Diagnostics,
    /// The repair path: creates the store when it is missing. The store is private state, a
    /// repair owns it, and a first server start would create it anyway.
    Fix,
}

/// What `open_store` answered.
pub enum Store {
    /// There is no store and this pass must not create one: the meta legs answer `ok`
    /// ("nothing to check") and the private-state leg `ok` ("nothing to clear").
    Absent,
    /// The store this pass may use, opened once, plus the verdict taken before it was opened.
    Opened {
        /// The `Open` `tinio_fs::database::open` returned: its `db` is the handle the compact
        /// takes its `&mut` window from and the storage is built from, and its `stats` and
        /// `compact_needed` are the fragmentation leg's (and `--fix`'s compact's) inputs. The
        /// whole struct is kept, so this crate never has to name `redb`.
        open: Open,
        /// `check_integrity`'s answer, from before the handle above existed.
        integrity: Integrity,
    },
}

/// The store, opened at most once — this command's only access to `meta.redb`.
///
/// **Measured on 2026-09-20** (a scratch crate against the locked crates): `check_integrity`
/// on a state dir with no `meta.redb` answers `Err` ("path not found"); `FsStorage::new`
/// creates `<root>/.tinio/` and `meta.redb`; and with an `FsStorage` alive, both
/// `database::open` and `check_integrity` answer `Err: Database already open. Cannot acquire
/// lock.` — after it is dropped, both succeed. Hence the probe, the single open, and the one
/// handle the storage is built from.
pub fn open_store(dir: &Path, pass: Pass) -> Result<Store, Error> {
    let present = std::fs::symlink_metadata(dir.join("meta.redb")).is_ok();
    if !present && pass == Pass::Diagnostics {
        return Ok(Store::Absent);
    }
    // The integrity check opens the file exclusively and is done with it before the handle
    // below is taken. A store that is absent is `Healthy` for the pass that is about to
    // create it; `--fix` reports the arms through its re-check (Task 3).
    let integrity = if present {
        check_integrity(dir).map_err(tinio_fs::Error::Database)?
    } else {
        Integrity::Healthy
    };
    let open = database::open(dir).map_err(tinio_fs::Error::Database)?;
    Ok(Store::Opened { open, integrity })
}

/// Whether the failure is "another process holds the store" — the one `database` failure that
/// is a fact about this run rather than about the store. The predicate belongs to the crate
/// that owns the error (`tinio_fs::database::Error::is_already_open`, this task's tinio-fs
/// edit), so this crate asks it instead of matching redb's message text or depending on
/// `tinio-store` and `redb` to reach the variant.
fn store_is_held(err: &Error) -> bool {
    matches!(err, Error::Fs(tinio_fs::Error::Database(inner)) if inner.is_already_open())
}

/// The fixed arm of the disk-space floor: `max(1 GiB, 5%)`, whose percentage arm needs the
/// filesystem's total and therefore lives in [`below_floor`], not in a constant.
pub const DEFAULT_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

/// Whether `free` bytes on a `total`-byte filesystem are below the floor — the whole rule on
/// its own, so the unit test pins both arms with explicit numbers instead of a filesystem.
pub fn below_floor(free: u64, total: u64) -> bool {
    free < DEFAULT_FLOOR_BYTES.max(total / 20)
}
```

**Free space is one function whose floor is arithmetic over what it probed.** `checks` calls `free_space(root) -> Finding`, which probes the filesystem's free and total bytes and answers `ok` or `warn` by `below_floor(free, total)` — `max(1 GiB, 5% of the filesystem)`, written down once, in a function a unit test drives without a filesystem. The finding carries both the free amount and its percentage, which is what makes the warn actionable on a filesystem whose size the operator does not have in their head. Unix reads the numbers through `rustix::fs::statvfs` (`f_bavail` × `f_frsize` free, `f_blocks` × `f_frsize` total); Windows reads them through `windows-sys`'s `GetDiskFreeSpaceExW` (`lpFreeBytesAvailableToCaller`, `lpTotalNumberOfBytes`) — an `unsafe` call, and therefore a new enumerated site on the allow-list plan B already keeps for the console handler, enumerated there and not exempted here. **Both dependencies are manifest edits, and Task 3's file list is where they are named**: the workspace pins `rustix` but no `tinio-cli` row declares it, and `GetDiskFreeSpaceExW` lives behind the `Win32_Storage_FileSystem` feature of the `windows-sys` arm plan B declares without a Win32 feature list.

**A leg that does not run is never silent.** Two shapes, and they are not interchangeable. A check that **cannot** run — the store is held open by another process, the root could not be read — reports `warn` with `detail: "not inspected: <reason>"`, because a diagnosis is missing and the operator has to know it is missing. A check that **does not apply** — there is no `meta.redb` yet, or read-only mode skipped the writability probe — reports `ok` carrying the reason, because nothing was skipped that could have found anything. Neither is silence, and the difference is what keeps a partial diagnosis from reading as a clean bill: `severity()` is the worst finding, so the un-inspected leg's `warn` is what stops a half-diagnosed root from exiting 0. A plain run's proposals are the third case and they are published the same way: the row is there, through `Finding::informational`, because the finding exists to be read and not to be counted.

**A stage that failed is a finding, not an early return.** The `FsCleanup` stream yields `Err` items; each becomes a finding with `Severity::Error` rather than a `?` that aborts the run. The report is the product: an operator asking what is wrong with a root gets every other check's answer in the same document, where propagating the first error would turn one broken stage into a one-line message, no report and no exit-code table. This rule is the check list's; what the repair path does with a failure of its own is Task 3's business. (**Correction, 2026-09-23, code-review round: Task 3's business turned out to be the same rule.** The repair path used to propagate — `compact_if_needed`, `FsStorage::new_from_db`, the cleanup stream and its items all returned through `?` — so a failure anywhere in it left `--fix --json` with an **empty stdout** beside one stderr line, and skipped the plane's `state`/socket reclaim with it. That also contradicted the adjacent `open_store` arm, which folds its failure into a note and carries on. `repair` now follows this rule too: every stage failure becomes a `Severity::Error` finding, the stages behind it still run, the plane's reclaim still runs, and the summary row still reports the count. `repair` no longer returns `Result` at all — the only paths left are notes and findings, so the report is the product on every `--fix` invocation.)

**The meta-store leg is `tinio_fs::database`'s answer, reported three ways — through the one handle `open_store` takes.** An **absent** `meta.redb` is `ok` and nothing is opened: a root that has never opened a store has nothing that could be wrong with one, and opening would *make* one (`database::open` creates; the probe is what stops it). A **present** store goes through `check_integrity(state_dir)`, whose three arms are `Healthy` → `ok`, `Repaired` → `warn`, `Corrupted(error)` → `error`, and whose detail names the remedy the doc names (`fs-backend.md:89`: report it for deletion and rebuild — the metadata is derivable). An **occupied** store is the "cannot run" shape above — `warn` plus `"not inspected: <reason>"`, never an error it cannot substantiate, because "another process holds it" is a fact about this run and not about the store — and the distinction is made by asking the crate that owns the error (`tinio_fs::database::Error::is_already_open`, this task's edit) rather than by matching redb's message text or by depending on `tinio-store`/`redb` to reach the variant. Any **other** open failure is the `error` arm. Fragmentation is the same module's second answer, taken from the **same** `Open` the storage is built from: `open.stats` evaluated by `needs_compact(&stats, threshold)`, with `threshold` the configured `[storage.fs] compact_threshold_percent` — never a number this plan picked. That is why `needs_compact` is promoted from `pub(crate)` to `pub` **and re-exported** from `crates/tinio-fs/src/database/mod.rs`: `mod compact;` is private, so the visibility change alone publishes nothing and `tinio_fs::database::needs_compact` would still not exist. The ratio rule and its 64 MiB floor keep **one** owner (`crates/tinio-fs/src/database/compact.rs:10`, `specs/001-s3-local-server/meta-redb-spec.md:165-166`, `fs-backend.md:119`) and `doctor` consumes that owner instead of comparing two byte counts it read itself. `snapshot` stays `pub(crate)`: nothing in this plan calls it (`Stats` arrive on `Open`), so publishing it would put `redb::DatabaseStats` on the public surface for no caller. `--fix` opens the store once per pass for its **own** two steps — `check_integrity` first and then the same module's `compact_if_needed` (Task 3), never two handles at once, which is what `fs-backend.md:89`'s "`--fix` runs `check_integrity` plus an offline compact" asks for in full.

**The stale state/socket leg reads the run's own liveness answer and calls plan A's code rather than re-probing.** `liveness(state_dir)` — `State::read` plus `tinio_api::client::probe`, the same call that chose the candidate — decides whether the `state` file is live, stale or unreadable, and `tinio_api::bind::reclaim_stale(path, dry_run)` is the socket's single probe-then-reclaim implementation: `bind_unix` calls it with `dry_run: false` on the startup path, this report calls it with `true`, and `State::remove` (plan A's Task 1) is what clears the file. The socket's path is never spelled out here nor re-derived there: `socket_path_of(run)` is the one function that picks it, the recorded `control_name` when the `state` file carried one and plan A's derived `socket_path` when it did not, and the check and `--fix` both call it. The liveness answers map onto three findings, and none of them is an error for a healthy root: `Live` → `ok` ("a server is running for {root}" — `doctor` diagnoses a running server; only `--fix` refuses), `Stopped` and `NoStateFile` → the socket's probe through [`socket_path_of`], which is the recorded `control_name` in the first case and the derived name in the second (`Ok(true)` → `warn` "a stale control socket", `Ok(false)` → `ok`, `Err` = a listener that answered after the probe said stopped → `warn` "not inspected: <reason>", because a `state` file claiming stopped beside a live listener is itself the finding), `Unreadable(reason)` → `warn` "not inspected: <reason>" — the name is unknown with it, so a socket probe here would be about a path nothing recorded, and the leg says so instead. `NoStateFile` still probes: a crashed instance can leave the socket behind with no `state` file at all, and the derived name is then the only candidate there is. The whole leg is `#[cfg(all(feature = "api", unix))]`, and both halves matter: without the feature the report says nothing about the plane at all (the allowed half of decision 10's rule — an ignore that makes the build smaller rather than the run weaker), and on Windows there is no socket to probe, because the local channel is a named pipe whose live/stale state is the plane's own bind rather than a file anything can inspect (`reclaim_stale` is `#[cfg(unix)]`; a pipe cannot outlive its process, so there is never residue for a Windows run to reclaim). The state-file half stays in both: an absent `state` file is `ok` there too. (**Correction, 2026-09-20, R15:** the whole-leg gate named above is wrong — the stale-`state` row is api-wide and only the socket half is unix-gated; `contracts/cli.md`'s doctor section states the shape.)

**The socket name is plan A's derivation, called and never re-spelled.** `tinio_api::state::socket_path(state_dir, root)` is public in plan A's management-plane Task 1 and is the single definition of the root-derived socket file name; the length check joins the **chosen** state dir to it, and Task 3's `crashed_root` fixture and Task 4's sibling call it too, so neither spells the name pattern out again. A second definition here is exactly the defect Task 4's ownership check exists to catch.

**`doctor` never auto-creates a configuration.** It is offline diagnostics, so the config check goes through `discover::resolve_path` + `Config::load` and reports an absent file as a `warn` — "a first `start` would auto-create it" is information for the operator, not an action for this command. `discover::load_or_create` (plan B, Task 1) belongs to `server`'s first start and to `service install`, and a `doctor` run must leave the root byte-for-byte as it found it.

**Nothing the report renders is a secret.** The leg that reads `state` reads the file that holds the control channel's token (`State::token`, plan A's Task 1), and the config leg reads the file that holds `[auth]`; neither value may reach a `check`, a `detail` or `--json`. The rule is one sentence with a leg behind it (Task 4): a finding names *what* it found and never the bytes it read — the state/socket detail is built from the path, the pid and the mode, the config leg's "no credentials at all" row states the absence rather than any value, and **no rendering of `State` goes into a finding at all** (plan A's `State` derives `Debug` with a public `token` field, so `format!("{state:?}")` into a `detail` is the exact accident this forbids). The test is Task 4's: a fixture whose `state` file carries a distinctive token, then `--json` on that root, and the token string must appear nowhere in stdout or stderr.

**The name check owns a deliberate audit walk, because the walkers `tinio-fs` has skip exactly what it must report.** The contract's rule is about any depth (`contracts/cli.md`'s doctor section, T073), and the case it exists for is precisely a reserved `.tinio` segment below the bucket level, which a top-level-only walk would pass in silence. The two walkers already written cannot serve it: `FsStorage::bucket_names` and `FsListing::walk_files_streaming` are `pub(crate)`, and both are enumerators — `bucket_names` `continue`s past an invalid name (`crates/tinio-fs/src/backend/mod.rs:705-707`), and `walk_files_streaming` skips a `.tinio` segment at any depth and skips symlink entries while `follow_symlinks` is disabled. So the traversal is this check's own: `read_dir` only, every entry typed **without following the link** (`file_type()`, or `symlink_metadata` — lstat semantics), the root's own `.tinio` skipped whole, and **no depth limit and no entry cap**: the any-depth rule is the contract's, and an entry cap would make the check lie about a deep root exactly where the contract says it must not. What a legal name **is** stays `tinio_core`'s — every name goes through `tinio_core::bucket::name` / `object::key` / `is_reserved_key`, so "what a legal name is" has one owner — and only the traversal is new, new because its purpose is the opposite of an enumerator's: it is looking for the entries an enumerator must not return.

**Three things about that walk are load-bearing, and one of them is a platform bug if it is left implicit.** (1) **The collector stops at the cap, not just the printer.** The check collects at most the first 20 offending names and then counts the rest into a plain `u64` — the finding is one `warn` however many names it finds, the severity is unaffected, the exit code follows the severity, and both renderings print the finding's `detail` verbatim, so one cap bounds the text *and* the memory. "Caps the list it prints" is not enough: a walk that collects every offender and truncates at render time is the unbounded version of the same check on a multi-million-entry root. (2) **The walk runs off the runtime's workers.** It is a synchronous `std::fs::read_dir` loop, and `checks` is `async`: unfixed, it occupies one worker of the single multi-threaded runtime for the whole traversal (`new_multi_thread`'s pool is small, and this command also drives the cleanup leg's `tokio::fs` calls on the same pool). So the walk is one `tokio::task::spawn_blocking` for the whole traversal — one blocking task, not one per entry — and its result comes back as a value. There is no entry cap and therefore no wall-clock bound on a pathological root; what `spawn_blocking` guarantees is that the bound is the operator's patience rather than the process's ability to serve, and what the cap guarantees is that the *report* stays bounded. (3) **On Windows the type test must count reparse points, not just symlinks.** `file_type().is_symlink()` is `IO_REPARSE_TAG_SYMLINK` only, so a junction (`IO_REPARSE_TAG_MOUNT_POINT`) reads as a plain directory and the walk would descend into it — out of the storage root, with no depth limit, and the symlink leg below would miss the entry it exists to report. The rule is the one `tinio-fs`'s own walkers use (`fsutil::is_symlink_or_reparse`, `crates/tinio-fs/src/fsutil.rs:28-33`): a symlink, or on Windows `MetadataExt::file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT`. That helper is `pub(crate)` today inside a private module, so this plan **asks for both halves** — `pub fn` in `fsutil.rs` and a **re-export** in `lib.rs` (`pub use self::fsutil::is_symlink_or_reparse;`; `crates/tinio-fs/src/lib.rs:30` is `mod fsutil;`, so the visibility alone publishes nothing, exactly as with `needs_compact`) — and Task 2's Interfaces spells both, instead of copying the rule: a reparse-point test written twice is the second copy the document set's ownership check exists to catch, and the Windows branch is the one nobody runs by accident.

**Without the `api` feature the state and socket leg is compiled out, not reported `ok` — and so are `--fix` and `--dry-run`.** There is no `state` file, no control socket and no plane that would have written them, so the report says nothing about them at all (`contracts/cli.md`'s doctor section); the two flags are `#[cfg(feature = "api")]` fields of plan B's `DoctorArgs` (Task 1) and simply do not exist in such a build, because a `--fix` with no plane to stop and no socket to reclaim could not honor its own contract. That is the only shape an ignore may take here: it makes the build smaller rather than the run weaker. The checks that do not depend on the plane — root, config, names, symlinks, free space, and the `FsCleanup` and `tinio_fs::database` legs — run in both builds, and the command's own body compiles in both because it reads the flag pair once behind the same gate (Task 3's `run`): nothing below that line names a field a feature-off build does not have. **The two socket-related legs are not in that list**: the control socket's path length and the stale `state`/socket check both call `tinio_api` — one for `socket_path`/`SUN_PATH_MAX`, one for `State` and `reclaim_stale` — so both are `#[cfg(all(feature = "api", unix))]` and neither exists in a build without the plane, where the nearest thing to them is the state-file half the table describes.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli doctor::`
Expected: PASS — twenty-one tests on Windows (twenty-two defined; one is `#[cfg(unix)]`).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 3: `--dry-run`, `--fix`, and the run itself

**Files:**
- Create: `crates/tinio/tests/doctor.rs` — the facade crate's test target, because the facade crate owns the `tinio` binary (the File Structure note above)
- Modify: `crates/tinio-cli/src/commands/doctor/`, `crates/tinio-cli/src/commands/mod.rs`, `crates/tinio-cli/Cargo.toml` (**the two manifest edits the free-space probe compiles against**: `rustix = { workspace = true, features = ["fs"] }` under `[target.'cfg(unix)'.dependencies]` — the workspace pins `rustix = "1.1"` but no `tinio-cli` row declares it, **and the `fs` feature is not optional decoration**: rustix's default feature set is `std` alone, so `rustix::fs::statvfs` does not exist without it, which is why every other user in this tree (`crates/tinio-fs/Cargo.toml:40`) enables it; and `Win32_Storage_FileSystem` added to the `cfg(windows)` `windows-sys` arm plan B declares, because `GetDiskFreeSpaceExW` lives in `windows_sys::Win32::Storage::FileSystem` behind exactly that feature and neither plan names it), `crates/tinio/Cargo.toml` (the `[dev-dependencies]` rows this test file needs: `serde_json`, and `tinio-fs` for the fixture — `crashed_root` creates the store a crashed run leaves behind with `FsStorage::new`, because a valid redb database cannot be hand-written and a fixture that omits it cannot exercise the cleanup leg at all. The facade depends on `tinio-fs` in no table today, so this is a dev-only row. Plan B's Task 10 owns that file's feature rows and its dependency table and already adds `tempfile` there, so this plan adds these two keys beside them and edits nothing else)

**`crates/tinio-cli/src/error.rs` is verified here, not edited.** Plan B's Task 2 lands the crate's error variants — `Fs(tinio_fs::Error)` beside `Server(...)`, `Sandbox(...)` and `Log(String)` — and `Fs` is exactly what this task's two mappings need: the repair path's `?` over the `tinio_fs::Error` values (the cleanup stream's `try_collect`, `FsStorage::new_from_db`) and the `database` results, which go through the public `tinio_fs::Error::Database` variant (it carries no `#[from]` of its own, `crates/tinio-fs/src/error.rs:47-51`, so the call sites map explicitly). One hop, no new variant: the check this task owes is that plan B's variant exists and is spelled `Fs`, because a second `Fs` here would be `E0428` and a second conversion for the same type would be the duplicate this plan's ownership check exists to catch.

**Interfaces:**
- Consumes: `checks`, `liveness`, `open_store`, `socket_path_of`, `Liveness`, `Pass`, `Probe`, `Proposals`, `StateDir`, `Store` (Task 2), `Report`/`Finding`/`Severity` (Task 1), plan B's `resolve::{for_doctor, fs_options, state_dir_candidates}` and plan B's `DoctorArgs` (its `cli.rs` type, `--read-only` included) **including the `pub config_error: Option<String>` field plan B's Task 2 lands** — the resolution carries it out of `for_doctor` instead of failing, and Task 2's config leg is the reader (this task only has to not lose it: `run` passes the whole `&Resolution` through), `tinio_fs::FsStorage` (`new_from_db`, never `new`), `tinio_fs::database::{check_integrity, compact_if_needed}`, and — in `api` builds — `tinio_api::state::{socket_path, State}` and `tinio_api::bind::{SUN_PATH_MAX, reclaim_stale}`.
- Produces: `tinio_cli::commands::doctor::run(args: DoctorArgs) -> Result<ExitCode, Error>`, **async**, dispatched inside the runtime the CLI has already built — `doctor` creates no runtime of its own (design decision 2: the single multi-threaded runtime `run()` builds, `new_multi_thread`, is the only one, and this command is not a second place one is created; it is the same runtime `server`, `status` and `stop` run on). The module's own helpers are defined here and nowhere else, one line each: `async fn state_dir_for_run(resolution: &Resolution) -> Result<StateDir, Error>` — the two candidates from plan B's `resolve::state_dir_candidates(&resolution.root, &resolution.state_dir, resolution.read_only, tinio_config::state::env_state_dir()?, || tinio_config::state::default_home_state_dir(&resolution.root))` (five arguments; plan B's Task 2 Interfaces in `2026-09-20-cli-lifecycle.md` is the authority, and this is the only place the pair is asked for) (2026-09-23 correction, code-review round: **the pair is asked for in plan B's shared client resolution, not here.** `state_dir_for_run` takes `(resolution, candidates)` and only probes them — the traversal moved to `commands::client_target` (whose body is `client_resolution`, the gate-free split both feature graphs compile), which passes `Located.state_dir_override` and the **non-creating** `default_home_state_dir_path`, so a client probe still creates nothing; the plan B correction of the same date records that change. Two helpers this line names are also gone: `access_for` and `threshold_percent` re-derived two fields of the one `Resolution → FsOptions` mapping, which `checks::fs_options(run)` builds once and which now supplies the walk's follow policy, the fragmentation threshold, the cleanup's write policy and the storage itself), probed with Task 2's `liveness` in order, the first live one winning and the resolved candidate answering when neither is live; `#[cfg(feature = "api")]` around that body, with the no-api arm answering the resolved dir, `Liveness::NoStateFile` and no recorded name (no plane, no probe); `fn access_for(resolution: &Resolution) -> AccessMode`, from `resolution.read_only`; and `fn threshold_percent(resolution: &Resolution) -> u8`, from the resolved `[storage.fs] compact_threshold_percent`. **There is no `storage_for`.** An earlier draft built a storage with `FsStorage::new` before the checks and opened the store again inside them; that is the shape the measured facts rule out (Global Constraints' store rule), so this task never constructs an `FsStorage` of its own: the diagnostics pass builds it inside `checks` from `open_store`'s handle, and the repair path builds it here from the same kind of handle. `resolve::fs_options(resolution, None)` is still the one mapping and the `None` is still the point (an offline command builds no pipelines, so the configured `[pipeline.*]` values apply as they stand).

- [x] **Step 1: Write the failing test**

Create `crates/tinio/tests/doctor.rs` — the facade package's test target, run as `cargo test -p tinio --test doctor`, because `CARGO_BIN_EXE_tinio` exists only for the crate that owns the binary:

```rust
//! T057: the doctor through the binary.

use std::{path::Path, process::Command};

fn tinio() -> std::path::PathBuf {
    // `CARGO_BIN_EXE_*` exists for bins of the crate under test, and this file is in the facade
    // crate for exactly that reason.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_tinio"))
}

/// Every spawn in this file goes through here (Global Constraints): the state location is
/// pinned to the test's own temp dir on every platform. `TINIO_STATE_DIR` is what decides
/// where the resolver may look (read-only mode's override), and `HOME`/`USERPROFILE` are the
/// unix arm of the home-derived candidate — `dirs` resolves the home through
/// `SHGetKnownFolderPath` on Windows, so those two alone would leave a Windows spawn reading
/// the developer's real profile and creating a state dir there.
fn spawn(dir: &Path) -> Command {
    let mut cmd = Command::new(tinio());
    cmd.env("TINIO_STATE_DIR", dir.join("home-state"))
        .env("HOME", dir)
        .env("USERPROFILE", dir);
    cmd
}

/// The control channel name the fixture's `state` file carries: the derivation itself where
/// the plane is compiled in — `tinio_api::state::socket_path` for this root, never a name
/// spelled out by hand, the same call Task 4's sibling and the socket-length check make — and a
/// path under the fixture where it is not, because without `api` nothing reads it (the leg that
/// would is compiled out with it).
#[cfg(feature = "api")]
fn control_name(root: &Path) -> String {
    tinio_api::state::socket_path(&root.join(".tinio"), root)
        .expect("socket path")
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(feature = "api"))]
fn control_name(root: &Path) -> String {
    root.join(".tinio/no-such-socket.sock").to_string_lossy().into_owned()
}

/// A root with the private state a crashed run leaves behind: a store (a run that crashed had
/// opened one — and a valid redb file cannot be hand-written, so the fixture asks `tinio-fs`
/// for it rather than pretending), a stale `state` file, a leftover temp file, and a
/// configuration complete enough that nothing but the private state is wrong — the `[auth]`
/// pair included, because "no credentials at all" is a `warn` of its own and a fixture without
/// it could never reach the exit-0 leg below. The token is distinctive on purpose: Task 4's
/// redaction leg asserts this exact string never reaches stdout or stderr.
fn crashed_root(dir: &Path) -> std::path::PathBuf {
    let root = dir.join("root");
    std::fs::create_dir_all(root.join(".tinio/tmp")).expect("reserved dirs");
    std::fs::write(
        root.join(".tinio/.tinio.toml"),
        "version = 1\n[auth]\naccess_key = \"tinio-access-key\"\nsecret_key = \"tinio-secret-key\"\n[server]\nport = 9000\n",
    )
    .expect("config");
    std::fs::write(root.join(".tinio/tmp/leftover"), b"partial").expect("stale temp");
    // The store, created and immediately closed: `database::open` runs inside, which is the
    // same call a `--fix` run makes, and dropping the handle here is what leaves it openable
    // for the binary under test.
    drop(tinio_fs::FsStorage::new(&root, tinio_fs::testing::fs_options()).expect("store"));
    // Built as a document, not as a string literal: a Windows path in a hand-written literal
    // would need its backslashes escaped to be JSON at all.
    let state = serde_json::json!({
        "version": 1,
        "pid": 999999,
        "token": "doctor-must-never-print-this-token",
        "control_name": control_name(&root),
        "port": 9000,
        "root": root.to_string_lossy(),
        "started_at": "2026-09-20T00:00:00Z",
    });
    std::fs::write(root.join(".tinio/state"), state.to_string()).expect("stale state");
    root
}

#[test]
fn a_root_with_stale_state_is_diagnosed_and_left_alone() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = spawn(dir.path()).arg("doctor").arg(&root).output().expect("doctor");
    // The stale `state` file is the only defect this fixture holds, and the plane is the layer
    // that reports it — so the exit code is asserted per build, while what is diagnostic is
    // asserted in both (which is what keeps this test ungated). Underneath it, the fixture's
    // leftover temp file is a cleanup *proposal*, and a plain run renders those as information
    // (`Proposals`), which is the other half of the same distinction.
    #[cfg(feature = "api")]
    assert_eq!(out.status.code(), Some(1), "the stale state file is a defect: 1");
    #[cfg(not(feature = "api"))]
    assert_eq!(out.status.code(), Some(0), "proposals only where the plane is absent: 0");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("private state"), "{text}");
    assert!(root.join(".tinio/tmp/leftover").exists(), "a plain run never touches anything");
    assert!(root.join(".tinio/meta.redb").exists(), "and the store it found is still the store it left");
}

#[test]
#[cfg(feature = "api")]
fn dry_run_lists_the_repairs_where_a_plain_run_reports_them_as_information() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());

    let dry = spawn(dir.path()).arg("doctor").arg(&root).arg("--dry-run").output().expect("doctor");
    assert_eq!(dry.status.code(), Some(1), "the proposals are findings here: 1");
    let dry_text = String::from_utf8_lossy(&dry.stdout);
    assert!(dry_text.contains("would clear leftover temp file"), "{dry_text}");
    assert!(root.join(".tinio/tmp/leftover").exists(), "dry-run never touches anything");

    // The same fixture without the flag — except for the state file, which is a defect this
    // test is not about and which the state/socket leg reports either way. What is left is a
    // root whose only issue is a cleanup proposal, and that is exactly the pair `--dry-run`
    // exists to separate: the identical row, informational here and a finding above.
    std::fs::remove_file(root.join(".tinio/state")).expect("drop the stale state file");
    let plain = spawn(dir.path()).arg("doctor").arg(&root).output().expect("doctor");
    assert_eq!(plain.status.code(), Some(0), "the same proposals as information: 0");
    let plain_text = String::from_utf8_lossy(&plain.stdout);
    assert!(plain_text.contains("would clear leftover temp file"), "{plain_text}");
    assert!(root.join(".tinio/tmp/leftover").exists(), "and a plain run touches nothing either");
}

#[test]
#[cfg(feature = "api")]
fn dry_run_and_fix_together_are_a_usage_error() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = spawn(dir.path())
        .arg("doctor")
        .arg(&root)
        .args(["--dry-run", "--fix"])
        .output()
        .expect("doctor");
    assert_eq!(out.status.code(), Some(2), "the pair is a usage error, not a silent win for one flag");
    assert!(!out.stderr.is_empty(), "and it names the flags that clash");
}

#[test]
#[cfg(feature = "api")]
fn fix_repairs_the_private_state_and_never_the_data() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let object = root.join("bucket/object.txt");
    std::fs::create_dir_all(object.parent().expect("parent")).expect("bucket");
    std::fs::write(&object, b"user data").expect("object");

    let out = spawn(dir.path())
        .arg("doctor")
        .arg(&root)
        .arg("--fix")
        .output()
        .expect("doctor");
    // Exit 0 is only reachable because `crashed_root`'s configuration is complete — credentials
    // included, because "no credentials at all" is a `warn` this plan deliberately keeps and no
    // repair addresses it. Everything the repair *could* clear it cleared: the leftover temp
    // file, the stale state file, and the re-check finds nothing else wrong.
    assert_eq!(
        out.status.code(),
        Some(0),
        "0: the repair cleared every finding, so none remains — {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!root.join(".tinio/state").exists(), "the stale state file is reclaimed");
    assert_eq!(std::fs::read(&object).expect("object"), b"user data", "user data is untouched");
}

#[test]
#[cfg(feature = "api")]
fn fix_refuses_while_a_server_is_running() {
    // A live control channel is an error for `--fix` (`contracts/cli.md`'s doctor section) and the error is a
    // **finding in the report**, never a bare `Err`: stdout carries it, the exit code is the
    // report's, and `--fix --json` on the same fixture is still a document a consumer can parse.
    // The server is started the way plan B's lifecycle test starts one.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let mut child = spawn(dir.path())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let mut line = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(child.stdout.take().expect("stdout")),
        &mut line,
    )
    .expect("ready");

    let out = spawn(dir.path())
        .arg("doctor")
        .arg(&root)
        .arg("--fix")
        .output()
        .expect("doctor");
    assert_eq!(out.status.code(), Some(1), "the refusal is an error finding, so 1");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("control channel"), "the check names what refused: {text}");
    assert!(text.contains("running"), "the detail names the reason: {text}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("control channel"), "the refusal is a finding, not a bare error: {stderr}");

    let json = spawn(dir.path())
        .arg("doctor")
        .arg(&root)
        .args(["--fix", "--json"])
        .output()
        .expect("doctor");
    assert_eq!(json.status.code(), Some(1), "a refused run still produces a report");
    let parsed: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("a refused `--fix --json` is still a document");
    assert_eq!(parsed["severity"], "error", "the refusal is the report's worst finding");

    let _ = spawn(dir.path())
        .arg("stop")
        .arg(&root)
        .output();
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn json_is_the_same_report() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = spawn(dir.path())
        .arg("doctor")
        .arg(&root)
        .arg("--json")
        .output()
        .expect("doctor");
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert!(parsed["findings"].as_array().expect("findings").len() > 1);
}
```

Every spawn in this file goes through `spawn(dir)`, which pins the state location to the test's temp dir on every platform — `TINIO_STATE_DIR` **and** `HOME`/`USERPROFILE`, because which one decides differs by mode and by platform (the Global Constraints bullet): on Windows `dirs` reads the home through `SHGetKnownFolderPath`, so those two alone would leave the child looking at the developer's real profile, and computing a candidate *creates* it. `TINIO_STATE_DIR` is present on every spawn for the same reason it must be: it is the variable that decides wherever the resolver consults an override, and the day a leg here runs in read-only mode it is the only one. The `#[cfg(feature = "api")]` gates are there because `crates/tinio/tests/doctor.rs` is also compiled and run under `--no-default-features` by the acceptance block (Task 4 Step 4): in that build `--dry-run` and `--fix` are not fields of `DoctorArgs` at all, so a test that passed them would fail on an unknown argument rather than on a doctor defect. `crashed_root` uses `#[cfg]`-split helpers for the same reason: `tinio_api` does not exist in that build, so the fixture's `control_name` comes from plan A's derivation where the plane is compiled in and from a path under the fixture where it is not. The two ungated tests are the ones that only read a report; the exit-code assertion inside the first one is per build, because the defect it counts (the stale `state` file) belongs to the plane.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio --test doctor`
Expected: FAIL — `doctor` is not a subcommand yet.

- [x] **Step 3: Write the implementation**

`run` is **async** and is dispatched inside the runtime the CLI already built — the single multi-threaded runtime `run()` builds — so this command constructs no runtime of its own, and it is the same runtime `server`, `status` and `stop` run on (design decision 2; the plan's Global Constraints). It resolves the root, the mode (`--read-only` > `TINIO_READ_ONLY` > `[server] read_only`) and the state dir by calling **plan B's `resolve::for_doctor(&args)`** — exported by plan B's `resolve` module, not a helper of this plan's own — then:

**Correction, 2026-09-23 (code-review round): the sketch below is the plan's, and the delivery diverges in six places.** Read it against these, not literally. (1) The signature is `run(args: Args, located: Located)`: `lib.rs` resolves the `Located` once for every command, so `run` is handed it rather than re-deriving it. (2) The resolution is the shared client one — `super::client_resolution(args.dir, located)` in an `api` build, `for_doctor(located, &args.dir)` in a no-api one — never `resolve::for_doctor(&args)`. (3) `state_dir_for_run` takes `(resolution, candidates)`: it probes the pair the resolution produced and does not ask for it. (4) The check list takes `&Run`, the resolution-and-chosen-state-dir pair, so this body builds one before each `checks` call. (5) `--fix` **re-probes** after the repair (`liveness`) and re-checks against the refreshed `StateDir`: the repair rewrote the `state` file and the socket, so the pre-repair verdict is the refusal's input alone (U1). (6) `--fix` pushes a second row beside `repairs applied` — `home root-state dirs`, `"not repaired: the home root-state-dir GC lands with T144"`, at **`warn`**, which is what floors every `--fix` exit code at 1 (`contracts/cli.md`'s exit-code rule states that per mode; the `Some(0)` a leg below asserts is the pre-floor expectation). The repair path also calls `prepare_home_derived` before `open_store`'s `Pass::Fix`, which may create the store — plan B's 2026-09-23 correction records why.

```rust
pub async fn run(args: DoctorArgs) -> Result<ExitCode, Error> {
    // The two flags are `#[cfg(feature = "api")]` fields of plan B's `DoctorArgs` (Task 1), so
    // they are read once, here, behind the same gate: everything below names the local pair and
    // never the fields, which is what keeps this body compiling in a build without the plane.
    #[cfg(feature = "api")]
    let (fix, proposals) = (
        args.fix,
        if args.dry_run { Proposals::Findings } else { Proposals::Informational },
    );
    #[cfg(not(feature = "api"))]
    let (fix, proposals) = (false, Proposals::Informational);

    let resolution = resolve::for_doctor(&args)?;
    // The state dir the server for this root actually uses: the resolution's own candidate
    // first, then the other one, probed by Task 2's `liveness`, so a run's findings and a
    // `--fix`'s repairs land where the server lives (decision 19). The report's first row names
    // which candidate answered.
    let state = state_dir_for_run(&resolution).await?;
    let report = if fix {
        if matches!(state.liveness, Liveness::Live) {
            // A live control channel is a finding, never a bare `Err` (`contracts/cli.md`'s doctor section):
            // the report is the product, `--json` stays a parseable document, the exit code is
            // the error finding's, and **no repair runs at all**.
            let mut report = Report::default();
            report.push(Finding::check(
                "control channel",
                Severity::Error,
                format!("a server is running for {}; stop it before --fix", resolution.root.display()),
            ));
            report
        } else {
            // Repair first, then check: with `--fix` the report holds what the repair left
            // behind (`contracts/cli.md`'s doctor section), and the actions it applied are summarised as one
            // `ok` finding so the operator still reads what changed. The re-check runs with
            // `Proposals::Findings`: whatever a dry run would still propose after a real repair
            // is exactly the residue the repair did not clear.
            let applied = repair(&resolution, &state).await?;
            let mut report = checks(&resolution, &state, Proposals::Findings).await;
            report.push(Finding::check("repairs applied", Severity::Ok, applied));
            report
        }
    } else {
        // Plain and `--dry-run` run the same check list, and `Proposals` is the whole
        // difference: a plain run renders the cleanup leg's "would …" proposals as
        // informational rows that leave the exit code alone, and `--dry-run` renders them as
        // findings — so the dry run lists exactly what a fix would change
        // (`contracts/cli.md`'s doctor section) and exits 1 when there is something to change.
        checks(&resolution, &state, proposals).await
    };
    println!("{}", if args.json { report.render_json()? } else { report.render_text() });
    Ok(ExitCode::from(report.exit_code()))
}

/// The state dir this run uses: the resolution's own candidate first, then the other one, each
/// probed by `liveness` — the same probe `status` and `stop` run, so the refusal, the report's
/// first row and the repair cannot end up speaking about different directories (decision 19).
///
/// Without the plane there is no `state` file and no control channel: nothing to probe, and no
/// second candidate that could answer, so the resolved directory is the answer.
async fn state_dir_for_run(resolution: &Resolution) -> Result<StateDir, Error> {
    #[cfg(feature = "api")]
    {
        let candidates = resolve::state_dir_candidates(
            &resolution.root,
            &resolution.state_dir,
            resolution.read_only,
            tinio_config::state::env_state_dir()?,
            || tinio_config::state::default_home_state_dir(&resolution.root),
        )?;
        for (index, dir) in candidates.iter().enumerate() {
            let probe = liveness(dir).await?;
            if matches!(probe.liveness, Liveness::Live) {
                return Ok(StateDir {
                    dir: dir.clone(),
                    resolved: index == 0,
                    liveness: probe.liveness,
                    control_name: probe.control_name,
                });
            }
        }
        // Nothing answered anywhere: the resolved candidate is the one to diagnose and, in a
        // `--fix` run, the one to repair — a stale `state` file and a stale socket are exactly
        // what a repair may clear *because* nothing is live.
        let dir = candidates
            .first()
            .cloned()
            .unwrap_or_else(|| resolution.state_dir.clone());
        let resolved = dir == resolution.state_dir;
        let probe = liveness(&dir).await?;
        Ok(StateDir {
            dir,
            resolved,
            liveness: probe.liveness,
            control_name: probe.control_name,
        })
    }
    #[cfg(not(feature = "api"))]
    {
        Ok(StateDir {
            dir: resolution.state_dir.clone(),
            resolved: true,
            liveness: Liveness::NoStateFile,
            control_name: None,
        })
    }
}

/// Everything `--fix` applies, in the order it applies it, as the one line the summary finding
/// carries.
async fn repair(resolution: &Resolution, state: &StateDir) -> Result<String, Error> {
    let mut applied = 0usize;
    let mut notes: Vec<String> = Vec::new();

    // 1. The meta store, opened **once** for the whole repair, and never with a second handle
    //    alive. `check_integrity` comes first — it opens the file exclusively and is done with
    //    it before the handle below is taken, and `fs-backend.md:89` names it as `--fix`'s own
    //    step — the offline compact takes the `&mut` window the open leaves free
    //    (meta-redb-spec §5.9), and the storage the cleanup leg repairs through is built from
    //    that same handle (`FsStorage::new_from_db`, whose own doc names doctor orchestration
    //    as its caller). A second `database::open` here is the measured failure the Global
    //    Constraints record.
    match open_store(&state.dir, Pass::Fix) {
        Ok(Store::Opened { open, .. }) => {
            let Open { mut db, compact_needed, stats } = open;
            compact_if_needed(&mut db, compact_needed, stats, threshold_percent(resolution))
                .map_err(tinio_fs::Error::Database)?;
            let storage =
                FsStorage::new_from_db(&resolution.root, resolve::fs_options(resolution, None), db)
                    .map_err(Error::Fs)?;
            // The engine's own scope: every startup item plus meta-orphan reclamation. This
            // function must not re-implement any of it by hand.
            applied = FsCleanup::new(
                &storage,
                CleanupOptions { dry_run: false, access: access_for(resolution) },
            )
            .repair(RepairKind::Full)
            .try_collect::<Vec<RepairAction>>()
            .await
            .map_err(Error::Fs)?
            .len();
        }
        // `Pass::Fix` creates, so this arm is unreachable; it is answered rather than unwrapped,
        // because this crate forbids panics in library code.
        Ok(Store::Absent) => notes.push("no state store to repair".to_string()),
        // A store that cannot be opened is a finding, not a note on the ok summary and not fatal:
        // another process holding it is the diagnostics "not inspected" warn; every other open
        // failure is an `error`. The rest of the repair still runs, and the re-check below takes
        // the error arm.
        Err(err) => failures.push(store_open_finding(&err)),
    }

    // 2. What the trait does not own: the plane's own leftovers, in `api` builds; the socket
    //    half is unix-only, because `reclaim_stale` is and a Windows pipe cannot outlive its
    //    process (so there is never residue there to reclaim).
    #[cfg(feature = "api")]
    {
        // The socket's path is taken **before** the state file goes, and it is taken from the
        // run's own probe (Task 2's `socket_path_of`): the name the `state` file recorded is
        // the channel's, `[api.unix] path` may have overridden the derived one, and re-deriving
        // it here would unlink a path that was never the socket while leaving the stale one
        // behind (decision 24 — the defect the Task 4 leg with an overridden path exists to
        // catch). Reading it from the carried name also means the order below is no longer
        // load-bearing; it stays in this order because it reads as the sequence.
        #[cfg(unix)]
        let socket = socket_path_of(run)?;
        State::remove(&state.dir)?; // plan A's Task 1 — removal needs no parse
        #[cfg(unix)]
        {
            // Plan A's factor-out of `bind_unix`, called with `dry_run: false` — the same call
            // the server makes at startup, so both agree on what a stale socket is. Nothing
            // answered this run's probe, so the only way this fails is a listener that appeared
            // in between: logged here and reported by the re-check's own leg, never an abort
            // that would cost the operator the whole report.
            if let Err(err) = reclaim_stale(&socket, false) {
                tracing::warn!(error = %err, "the control socket was left alone");
            }
        }
    }

    Ok(if notes.is_empty() {
        format!("{applied} repairs applied")
    } else {
        format!("{applied} repairs applied; {}", notes.join("; "))
    })
}
```

The sketch's `.try_collect::<Vec<RepairAction>>()` is `futures::TryStreamExt`'s, not an `Iterator` collect: `ActionStream<E>` is a `BoxStream<'static, Result<RepairAction, E>>` (`crates/tinio-core/src/cleanup.rs:45`), so the stream is consumed asynchronously and the trait has to be in scope — the same import the lifecycle plan's own repair call uses, and the one place this path meets `Error::Fs` (`FsCleanup::Error` is `tinio_fs::Error`, `crates/tinio-fs/src/cleanup.rs:627`, and `?` does one hop). **`--dry-run` and `--fix` are mutually exclusive**, and they are exclusive in the parser (Task 1): the pair is a usage error, exit 2, rather than one flag silently winning. In the code the private-state leg is one call one flag apart — `FsCleanup` with `dry_run: true` for the diagnostics pass, `dry_run: false` inside `repair` — and what separates a plain run from `--dry-run` is not the call but the rendering (Task 2's `Proposals`): the same "would …" rows are information in one and findings in the other, so the list a dry run prints is the list the fix applies (`contracts/cli.md`'s doctor section) and the two runs exit differently on purpose — a dry run's proposals are what the operator is about to commit to, a plain run's are a diagnosis. `--fix` then adds the three things the trait does not own: the plane's `state`/socket reclaim (api builds, unix for the socket), and the meta store's `check_integrity` and offline compact. Its re-check runs with `Proposals::Findings`, so anything a repair did not clear is reported as the finding it still is: a fix that cleared everything exits 0, and a finding no repair addresses exits 1.

**A live server makes `--fix` refuse through the report, never through an `Err`.** The refusal is `Finding::check("control channel", Severity::Error, "a server is running for {root}; stop it before --fix")` pushed into the report, and **no repair runs at all**: the process exits 1 because the report's worst finding is an error, `--json` on a refused run is still a document a consumer can parse, and the operator reads a check name instead of a bare message on stderr. The contract's "a live control-channel probe is an error (exit 1)" (`contracts/cli.md`'s doctor section) is exactly this finding's severity and exit code. The probe is Task 2's `liveness`, run by `state_dir_for_run` before anything else and then **carried on the `StateDir`**, so the refusal, the report's first row and the repair all speak about one directory: `Liveness::Live` refuses, and `Liveness::Unreadable` does not — a `state` file nobody can decode is not evidence of a running server, `State::remove` needs no parse to clear it, and the check reports "not inspected" rather than guessing (Task 2's table). (**Correction, 2026-09-23, code-review round: the second half of that reasoning does not hold, and the repair path no longer acts on it.** "Nobody can decode it" is not "nobody is behind it": one of `State::read`'s three failure arms is a **version** this build does not read, which is exactly what a *newer* server writes — so an `Unreadable` file can be a live server's. The refusal is unchanged (`Unreadable` still does not refuse a whole `--fix`), but the **removal** is now gated: `repair` re-takes the dial immediately before it clears anything — `liveness` for the file's own verdict, and for an `Unreadable` verdict the channel's own probe, `client::probe` at the name the **document recorded** — and when the channel answers it pushes a note and clears nothing, leaving the `state` file and the socket alone. The gap that made this reachable was the window between `state_dir_for_run`'s probe and the removal, which spans every store stage: seconds of work on a large store, and long enough for a server to start. (**Correction, 2026-09-23, second pass — the first fix was incomplete on both platforms.** It dialled `reclaim_stale`, which is `#[cfg(unix)]`, so a Windows run took the `_ => false` arm and cleared the file unconditionally; and it dialled `socket_path_of`'s path, which falls back to the *configured-or-derived* name, so on unix it too missed a server that had recorded an override. Both come from one root cause: `State::read` decoded the whole document and then threw it away on the version check, so the recorded `control_name` — the only route to the channel a newer server bound — never reached the dial. `tinio-api` now also exposes `State::read_any_version` (the same decode without the version check, with `read` rebuilt on it so its behaviour is unchanged), `liveness` uses it and carries the recorded name out on an `Unreadable` verdict, and the dial is `checks::channel_answers` — `client::probe` at that name, one call for both platforms. A file that does not decode at all still carries no name and is still reclaimed: the verdict is unchanged, only the removal decision is now made with the name in hand.) (**Correction, 2026-09-23, third pass.** An answering foreign-version channel refuses the whole `--fix` before any store repair, the same error as `Liveness::Live`. The dial is `Probe::channel_answers`, taken inside `liveness` for every decoded document; `checks::channel_answers` is gone. A file that does not decode still does not refuse and is still reclaimed.) The old `server_is_live(&resolution)` helper is gone with it: a second probe in this file would be the thing that could disagree with the report.

**`--fix` and `--dry-run` exist only in `api` builds**, and so does the state/socket leg (Task 2): both flags are `#[cfg(feature = "api")]` fields of plan B's `DoctorArgs` (Task 1), and the code they reach — `state_dir_for_run`'s candidate probe, `liveness`, `State::remove`, `reclaim_stale` — carries the same gate, with the socket half under `unix` inside it. A build without the plane has no control channel to stop the repair, no `state` file to clear and no socket to reclaim, so it offers neither flag and says nothing about the plane; the checks that do not need it run unchanged in both builds. The flag pair is read through a `#[cfg]`-split local (the first three statements of `run`), because `args.fix` anywhere in the body would be an unresolved field in exactly the build the acceptance block compiles: `docs/tests.md:28` runs the whole workspace with `--no-default-features`, and **Task 1's** `api` gate on those two fields is what turns them off — the workspace's `tinio-cli` row carries `default-features = false` (`Cargo.toml:81`), so the facade's `api` feature is the only thing that enables them, which is why `crates/tinio/tests/doctor.rs`'s own `#[cfg(feature = "api")]` gates line up with the compiled-out half instead of silently skipping tests.

**Both renderings leave the command through `println!`.** `render_text` and `render_json` each return their text with no trailing newline, and `run` prints it with `println!("{}", …)` — one line discipline for the two renderings, `--json` included, so a piped `tinio doctor --json` ends with exactly one newline rather than with whatever `print!` left in the buffer.

**The `--fix` summary finding is `repairs applied`.** `Finding::from_action` names every action it maps `"private state"` (Task 1), so the summary cannot reuse that name without making the report ambiguous about which row summarises and which rows are summarised: the summary is `Finding::check("repairs applied", Severity::Ok, <what ran>)`, one row with its own check name, and the action rows stay the engine's.

**`--fix` in read-only mode repairs the relocated state dir and nothing else.** There the state dir is `~/.tinio/roots/<sha256(canonical root)16>/` (or `TINIO_STATE_DIR`) and it is writable by construction, while the served root may be a genuinely read-only filesystem: `CleanupOptions::access` carries the mode's `AccessMode::ReadOnly`, and the fs implementation skips every stage that would write the root (FR-023). The stopped-server probe reads `state` in that same **chosen** state dir — the `StateDir.dir` that `open_store`, `State::remove`, `reclaim_stale` and the cleanup leg all receive — so the probe and the repair can never be aimed at different directories; the dual-candidate probe of the Global Constraints decides which candidate answered, and the report's first row names it. **Every write the flag performs is inside that directory**: the store the repair may create and the compact's rewrite of it, `State::remove`'s unlink, the socket's reclaim, and the cleanup stages — of which the two that write the served root are skipped outright by `AccessMode::ReadOnly` (`crates/tinio-fs/src/cleanup.rs:637-645`), which is why the flag can repair a read-only root at all. Nothing it writes is user data: buckets and objects are the cleanup trait's one prohibition (`crates/tinio-core/src/cleanup.rs:157-165`), and the `--fix` legs assert it by reading an object back.

`repair(RepairKind::Full)` is what makes meta-orphan reclamation land here rather than in a second inventory — the `--fix` task must **not** implement it by hand. The three things `repair` adds beside it are named at the call, because the trait does not own them: the meta store's `check_integrity` and its offline compact (`tinio_fs::database`'s own two steps, the same module and the same configured threshold the diagnostics leg read — `fs-backend.md:89`'s "`--fix` runs `check_integrity` plus an offline compact", both halves), and the plane's `state`/socket reclaim (api builds, plan A's `State::remove` and `reclaim_stale`). The home root-state-dir GC that `RepairKind::Full`'s doc names is **not** in this task either: the fs implementation defers it to US2 (`crates/tinio-fs/src/cleanup.rs:14-15`, which now reads **T144** — the re-point has landed in the tree, so the comment and this paragraph agree), and T076 is delivered. `--fix` inherits the stage the day T144 lands, and this plan promises nothing about it.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio --test doctor`
Expected: PASS — twenty-one tests on Windows (twenty-five defined: four are `#[cfg(all(feature = "api", unix))]` and one is `#[cfg(all(feature = "api", windows))]`).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli -p tinio --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 4: T057's remaining legs, the docs, and the check

**Files:**
- Create: `specs/001-s3-local-server/contracts/doctor-report.schema.json` (its content is spelled out in Step 3 below; the File Structure table carries the row)
- Modify: `crates/tinio/tests/doctor.rs`, `docs/tests.md`, `specs/001-s3-local-server/tasks.md`, `specs/001-s3-local-server/contracts/cli.md` (the doctor section's five edits: the exit-code rule, the `--dry-run`/`--fix` exclusivity, the `--dry-run` run-semantics sentence, `[--read-only]` in the synopsis, and the no-api-note frame that points at the schema). `docs/superpowers/specs/2026-09-20-cli-integration-design.md` is **read** in Step 3 and not modified — its §2 row already carries the correction this plan once promised

- [x] **Step 1: Add the legs Task 3 did not cover**

T057's list also names: a **stale unix socket** beside a stale state file (`crashed_root` writes `state` and no socket, so the fixture gains a sibling that also creates the socket that derivation names — `tinio_api::state::socket_path(&root.join(".tinio"), &root)`, the same call the socket-length check (Task 2) and `crashed_root`'s own `control_name` helper make rather than a second spelling of the name — and records that path as the state's `control_name`; `#[cfg(all(feature = "api", unix))]`, since the socket is the unix transport's and the leg is gated on both, and on Windows there is nothing here to assert — a pipe's live/stale state is the plane's bind, not a file); the leg asserts the probe finds no listener, the report says so, and `--fix` clears the file through plan A's `reclaim_stale(path, false)` — the socket is gone, rather than a second probe written by the test agreeing with the first). That sibling has a **second variant, and it is the one that proves the path rule**: the same fixture with the socket file and the `state` file's `control_name` at a **path that is not `socket_path`'s output** — the shape `[api.unix] path` produces (the documented remedy for a deep root) — where `--fix` must remove *that* file; the derived path is never created, so a `--fix` that re-derived would leave the stale socket behind and pass the first variant while failing this one; **orphaned meta-store entries** (rows in `meta.redb` whose bucket or object no longer exists — T057's `orphaned meta` words — with the integrity leg and the fragmentation leg each asserted); abandoned multipart uploads, orphaned bucket records, `meta.redb`'s integrity and fragmentation report, and `--json` on each of them. Each is one test in `crates/tinio/tests/doctor.rs` (the facade crate's target, `cargo test -p tinio --test doctor`) that seeds the defect, runs the binary with the flag under test, and asserts the severity, the exit code (`0` clean, `1` for a warn or an error, in every mode) and — for `--fix` — that the defect is gone while `bucket/object.txt` still reads back.

**Five legs this plan owes by name, because the fixes above are only as good as the tests behind them.** (1) **`--fix` on a store it cannot open**: `meta.redb` holding bytes that are not a database. The run must still produce a report, the meta finding must be `error`, and the summary must name the store — `fs-backend.md:89` requires `check_integrity` of `--fix`, and Task 3's repair answers a store it cannot open with a note rather than an abort. (2) **The redaction leg**: `--json` on a root whose `state` file carries `crashed_root`'s distinctive token, asserting that string appears in neither stdout nor stderr — the rule Task 2 states, and the only thing that keeps it true under later edits. (3) **The liveness answers**: a `state` file with nothing behind it (`Stopped`), no `state` file at all (`NoStateFile`, which still probes the derived socket path), a live server (`ok`, and `--fix` refusing), and a `state` file whose bytes are not a document (`Unreadable` → `warn "not inspected: …"`, and `--fix` clearing it rather than aborting on it). (4) **The store-less root**, which is the store rule's own leg and the one no fixture above can carry: a root holding a complete configuration and **no** `meta.redb` — no `state`, no `tmp/` — asserted to exit 0 with the meta and private-state rows `ok`, and then the same root re-checked for the file: `meta.redb` must not exist after the run, in either mode. `crashed_root` cannot be that fixture (it has a store on purpose, because a crashed run did), so this leg builds its root by hand. (5) **The overridden socket path**, from the socket bullet above: the recorded `control_name` is not the derived name, the socket file is where the record says, and `--fix` must reclaim *that* file — the leg that fails if `repair` ever goes back to calling `socket_path` instead of `socket_path_of`, in either the check or the fix.

Seed the defects through the same `crashed_root` helper's siblings rather than through `FsStorage` where the file shape is documented in `failure-handling.md §3`. Every one of these legs spawns the binary through Task 3's `spawn(dir)` helper — `TINIO_STATE_DIR` plus `HOME`/`USERPROFILE`, the same helper and the same reason (the Global Constraints rule: computing the state-dir candidate *creates* it, and these are exactly the legs that resolve it).

**The home root-state-dir GC is not a leg of this plan.** `RepairKind::Full`'s doc names it, but the fs implementation defers it to US2 (`crates/tinio-fs/src/cleanup.rs:14-15`, whose comment names **T144** — the re-point has landed in the tree) — not T076, which is delivered. So this plan neither tests the leg nor claims it: it lands with T144, which owns the leg and its tests, and the contract's `~/.tinio/roots/` bullet (`contracts/cli.md`'s doctor section) describes something no build has yet.

- [x] **Step 2: Run the suite**

Run: `cargo test -p tinio --test doctor`
Expected: PASS — every leg.

- [x] **Step 3: The docs and the ticks**

`docs/tests.md` gains the doctor legs under the CLI section plan B opened (one line per leg, in the existing compressed style). `specs/001-s3-local-server/tasks.md` ticks T057, T073 and T074; T075 was plan A's and T055 plan B's.

Five `contracts/cli.md` edits land here, each a correction this feature owes, beside one file this step creates and one verification that needs no edit at all:

- The parenthetical in `contracts/cli.md`'s doctor section — `1` = warnings/errors found (**with `--dry-run`**) **or remain (with `--fix`)** — is rewritten to say what Task 1 implements: **`0` when the report holds no warn and no error, `1` when it holds either, in every mode** — a plain run counts defects, `--dry-run` counts the proposals a fix would apply (findings there, information without the flag), and `--fix` counts what the repair left behind — with `2` for a usage error. As it stands the parenthetical attaches `1` to `--dry-run` alone, which would make a plain `doctor` answer `0` forever while FR-007 says "exit 0 clean / 1 problems" and `tasks.md`'s T057 agrees.
- The doctor section gains the pair's exclusivity: `--dry-run` and `--fix` may not be given together, and doing so is a usage error (exit 2) rather than a run in which one flag quietly wins.
- the `--dry-run` sentence in `contracts/cli.md`'s doctor section gains the distinction the flag now carries as a *run*: a plain `doctor` reports defects and lists what a fix would change as information, while `--dry-run` promotes every one of those proposals to a finding — so it exits 1 on a root whose only issue is a leftover temp file, and a plain run over the same root exits 0. That is what "lists exactly what a fix would change" means when the sentence is read as an exit code, and without it the two runs are the same run.
- The synopsis in `contracts/cli.md`'s doctor section gains `[--read-only]`: `tinio doctor [DIR] [--json] [--dry-run] [--fix] [--read-only]`. The flag is part of the command's surface (plan B's `DirArgs`, decision 4's precedence), and a section that spells out the other three flags cannot leave it out.
- The section says what a build without the plane looks like and where the JSON shape lives: `tinio doctor` deliberately carries **no** api-only note the way `status` and `stop` carry theirs, because the command exists in every build — only `--dry-run`, `--fix` and the state/socket check are compiled out, which is the qualifier `contracts/cli.md`'s doctor section already puts on that check — and the section names `contracts/doctor-report.schema.json` as the normative shape of `--json`.

No edit is owed to `docs/superpowers/specs/2026-09-20-cli-integration-design.md`: this plan once promised to correct §2's row for the command files, but the row **already carries** the corrected `src/report.rs` home for the report type and both renderings, so the check is to read it and confirm the two documents agree rather than to change it; the row now also names `src/commands/doctor/` as a module directory, which is the shape the delivery landed in (2026-09-23 correction, code-review round). The row is a verification, and the verification is the whole step's relation to that file.

And the schema file this step creates — **read the file, do not copy a block from here**: `specs/001-s3-local-server/contracts/doctor-report.schema.json` is the one home of that shape, and the enumerations it carries (`severity`, the per-finding keys, `additionalProperties: false` at both levels) have no second definer. (2026-09-23 correction, code-review round: this step used to embed the whole document so the implementer could copy it, which made the plan a copy of the file — `docs/superpowers/README.md`'s "an enumeration has one definer". Task 1's key-set assertion is what keeps the renderer true to the *file*, so the copy bought nothing and drifted on the first edit.)

The schema is the normative half of the shape and `the_json_rendering_is_the_same_report`'s key-set assertion (Task 1) is the implementation half: a field added on one side only fails the other, and `additionalProperties: false` at both levels is what makes that a contract rather than a style.

- [x] **Step 4: The workspace gate**

Run the `docs/tests.md` Standard acceptance block in full.
Expected: PASS. Report each command's output — including the `--no-default-features` run (`docs/tests.md:28`), which is the one that compiles `crates/tinio/tests/doctor.rs` and `tinio-cli` with the plane off: it is what proves the store rule's unconditional half, the `#[cfg(feature = "api")]` gates on the fixtures and the flags, and the `#[cfg]`-split flag pair in `run`.

- [x] **Step 5: The ownership check**

The check is the one `docs/superpowers/README.md` describes against this plan and its two siblings — the file is in this worktree now (it arrived with the sandbox P0 merge, `a091cce`). For each family below, name the single owner and search the other two plans for a second statement of the same fact; a hit is a defect to fix **now**, not in a follow-up:

- the check list (this plan's `checks`), the severity mapping (`Finding::from_action` and its informational twin, and Task 2's `Proposals`, which is the only place that chooses between them) and the report type (this plan's `report.rs`);
- the resolution (`resolve.rs`, plan B) and the `DoctorArgs` clap fields (plan B's `cli.rs`, `--read-only` included; this plan consumes them and owns only the `--dry-run`/`--fix` exclusivity and their `api` gate), and the two candidates (`resolve::state_dir_candidates`, plan B — called once, by the shared client resolution `commands::client_target`/`client_resolution`, whose pair `state_dir_for_run` consumes as an argument (2026-09-23 correction, code-review round); a second traversal of the pair anywhere is the defect this line is looking for);
- the `Resolution` → `FsOptions` mapping (`resolve::fs_options`, plan B's Task 2 — `server` calls it with `Some(&pipelines)`, this plan's repair path with `None`; a second mapping in any of the three plans is the defect this line is looking for);
- **the store access**: one probe (`open_store`'s `symlink_metadata`), one open per pass — never two handles at once — and that handle handed to `FsStorage::new_from_db`. `FsStorage::new` must not appear in this plan's **production** code (`commands/doctor/`, and plan B's `start.rs`), since it would open (and create) a second handle; the fixtures may and do call it (`crashed_root` builds the crashed run's store with it, and the held-store test holds one open on purpose to take the lock). Search those two production files for `FsStorage::new`, `new_from_db` and `database::open`, and check each call site against the store rule in the Global Constraints;
- the socket-name derivation (`tinio_api::state::socket_path`, plan A) and the stale-socket probe-and-reclaim (`tinio_api::bind::reclaim_stale`, plan A — `bind_unix` and this report are its two callers, never two implementations), plus the `sun_path` limit (`tinio_api::bind::SUN_PATH_MAX`, plan A's constant, which this plan's check must name instead of a number: re-spelling 108 is wrong on macOS and is exactly the second copy this check exists to catch);
- the meta-store legs: `tinio_fs::database`'s `check_integrity`, `open` and `compact_if_needed`, with `needs_compact` promoted **and re-exported** (`pub use compact::{…, needs_compact};` — the module is private, so the visibility alone publishes nothing) and `snapshot` left `pub(crate)`, so the ratio rule and its 64 MiB floor keep one home (`crates/tinio-fs/src/database/compact.rs:10`) without widening the public surface for a caller that does not exist; and the occupied-store predicate (`tinio_fs::database::Error::is_already_open`) living in the crate that owns the error rather than in a `Display` match here;
- the free-space floor (`DEFAULT_FLOOR_BYTES` and `below_floor` in this plan's `commands/doctor/` — no other document names a floor, and the percentage arm lives in the function because only the probe knows the total);
- `RepairKind::Full`'s contents (`tinio-core`, already written), the home root-state-dir GC's owner (T144, not this plan) and the exit-code rule (this plan's `Report::exit_code`).

- [x] **Step 5b: Final checkpoint**

Report every file pending commit across the three plans, and the acceptance output. Do not run any git write.
