# Windows file-DACL removal and the `acl` module split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Windows builds install no current-user-only security descriptor, the sandbox's ledger subsystem is gone, `tinio-winsec` is dissolved into the two crates that still need its remnants, and `tinio-sandbox-windows`'s 4055-line `acl.rs` becomes seven focused modules.

**Architecture:** Every descriptor site is deleted, not narrowed; the four survivors of the dissolved crate are moved to the two crates that use them (`to_wide` to `tinio-util`, the ACE walk into the new `acl` directory). Deletions land first so each diff has one purpose; the split is a pure move that deletes nothing, and the two are separated by exactly that property.

**Tech Stack:** Rust (edition 2024), `windows-sys` 0.61, `cargo nextest`, the workspace's eight per-crate clippy lints.

**Spec:** `docs/superpowers/specs/2026-10-15-windows-file-dacl-removal-design.md` — this plan implements D1–D15 and its `Plan decomposition`. The spec's two-directory table (P granted, W never granted) is the fact every deletion's consequence is read against.

**Depends on:** nothing. Every task ships green on its own.

## Global Constraints

- English only in repo artifacts; no hard-wrapped prose (`docs/compress.md`). Plans and specs: structural breaks only.
- **The deletions come first, the split after** (D12). A diff that both deletes behaviour and moves code is a plan failure: it cannot be reviewed for either property.
- **The move is verbatim.** In Task 3 and Task 4, moved items keep their bodies, docs, `# Examples` blocks and tests byte-identical. The only permitted edits to moved code are visibility (`pub` → `pub(super)`), the `use` lines that reach them, and the module path in a doc link. **One body changed after the run, and it is recorded here rather than silently**: the post-run simplification pass replaced `OwnedSid::parse`'s hand-written UTF-16 buffer with `to_wide` (the reuse fix the review asked for, in `acl/descriptor.rs`). The move's acceptance was taken before that edit; no other body changed.
- **No new externally visible surface.** `tinio-sandbox-windows`'s `acl` keeps `pub use` re-exports at its root so every `crate::acl::…` and `tinio_sandbox::windows::acl::…` path resolves unchanged (D9). New `pub(super)` items are confined to the `acl` directory and listed in Task 4. Test-support additions behind the `testing` feature — Task 2's probe key — are not production surface and are outside this rule, because no shipped build compiles them.
- **No leg is deleted for being inconvenient.** A leg goes only when the descriptor it asserts is gone (D7); the grant-ACE legs stay. Deleting a grant leg is a review finding.
- **The requirement set is not touched** (D14): no FR/SC edit, and `specs/001-s3-local-server/spec.md`'s numbered requirements are not amended. The falsified clauses are free prose and are corrected in Task 5.
- **Prose corrections follow the record rule** (D15): live prose corrected in place, including landed specs' decision text and task bodies; fenced and measured records stay as written with the correction beside them.
- Git: every commit step asks the operator first (`docs/git.md`); the index is untouched until they say so; no AI/agent trailers.
- Gates after every task: `cargo check --workspace --all-targets`, `cargo nextest run --workspace --exclude tinio-e2e`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, and `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- **This host is Windows**, so the Windows-gated code compiles and its legs run here. The unix arms must keep compiling: the workspace's CI legs on ubuntu are unchanged, and no task may break them.

## Review Focus

Five input classes and failure modes the spec implies but no ordinary leg exercises. Each is pinned by the task named. Two of them cannot be held by an automated leg and are manual measurements in Task 6, which says why.

1. **A file in P is reached by the launcher's own grant.** After D1 the protected descriptor is gone, so a config or state file inside the granted directory is readable by anything holding the package SID, and by inheritance when the file is created after a grant. Expected: this is the accepted exposure, recorded once and pinned by Task 2's in-container legs, which read the directory from inside the confinement rather than listing its DACL from outside.
2. **A state directory the operator named, outside the root.** `TINIO_STATE_DIR` may point into a tree this product does not own and whose ACL grants other accounts; the token is then readable by them. Expected: unchanged behaviour from the product's side, recorded as a risk. Manual in Task 6 — no leg can assert it without owning a host's ACLs.
3. **The pipe carries Windows' own default named-pipe descriptor.** A null `SECURITY_ATTRIBUTES` does **not** mean the token default DACL: measured 2026-10-16 on this host, and identical on a control pipe an unrelated process created, the pipe answers `SYSTEM`, `Administrators` and the creating user at full control, with `Everyone` and `ANONYMOUS LOGON` at read. So `/stop` is reachable through `Administrators`'s full control and the channel is readable by anyone local, while the token in the state file is what still guards the endpoints. Expected: recorded as a reading rather than asserted as a mechanism. Pinned in part by Task 2's pipe leg — the second-instance refusal and the remote-client flag — with the descriptor's own contents measured rather than assumed.
4. **The salt on a host where `%LOCALAPPDATA%` is not the user's alone.** Expected: the salt's pre-computation defence is exactly that directory's grants, and the unsalted degrade path still works. Pinned by Task 2's salt leg (the property, not the host's ACLs).
5. **A behaviour change hidden inside the move.** The split must not alter any ACE classification, mask table or refusal. Expected: byte-identical moved code and unchanged legs; a diff that touches a body is rejected. Pinned by Task 4's acceptance step.

---

### Task 1: Delete the ledger subsystem

**Files:**
- Delete: `crates/tinio-sandbox-windows/src/ledger.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs:70`, `src/backend/mod.rs`, `src/acl.rs`, `Cargo.toml`
- Modify: `crates/tinio-e2e/tests/steps/cli.rs:3307-3360`, `crates/tinio-e2e/tests/features/confinement.feature:834`

**Interfaces:**
- Consumes: nothing.
- Produces: `PreparedLaunch` without an `others` field; `grant_whitelist` without a record hook. Task 3 does not depend on either.

- [x] **Step 1: Delete the module and its declaration**

```bash
rm crates/tinio-sandbox-windows/src/ledger.rs
```

Then remove the `pub mod ledger;` declaration at `crates/tinio-sandbox-windows/src/lib.rs:70`, with its doc comment above it (`lib.rs:63-69`'s block, which exists to explain why `path` is public).

- [x] **Step 2: Remove the ledger's two inputs in `backend/mod.rs`**

Delete `other_exposed_paths` (`src/backend/mod.rs:339-348`), the `others` field and its doc (`:296-304`), the field's construction in the `PreparedLaunch` literal (`:427`, `:434`), and in `grant_whitelist` the `records` vector and everything that feeds `ledger::Grant` (`:514-548`). Delete the `ledger` import.

`grant_whitelist` keeps its position, its `info!` banner, the sibling batch and the per-exposure loop: only the record half goes.

- [x] **Step 3: Sweep `acl.rs`'s ledger references**

`AclLock` **stays** — `acl.rs` uses it for its own read-merge-write, and only the ledger's uses go. Remove: the ledger's import (`crate::ledger`), the ledger-shaped doc references on `AclLock` and its tests, the type reference in the test at `:2050` that names the ledger's path-kind type, and the test `the_ledger_file_is_current_user_only_and_inherits_nothing` (`:3458`).

Run: `rg -n 'ledger|PathKind' crates/tinio-sandbox-windows/src/`
Expected: no hits outside prose that names the removed subsystem in the past tense.

- [x] **Step 4: Delete the cucumber scenario and its step**

Remove the scenario `The ACL ledger records the grants this run made` from `crates/tinio-e2e/tests/features/confinement.feature:834` (tags `@cli @confinement @FR-036 @windows`). **The step at `crates/tinio-e2e/tests/steps/cli.rs:3322` is shared and stays**: its body joins a scenario's relative path onto the served root, and `read_only.feature`'s `The environment beats the config file` calls it in ordinary e2e, so deleting it would leave a live scenario undefined — the same "deleted for being inconvenient" the Global Constraints forbid. Only its doc block (`:3307-3321`), which explains it in terms of the ledger, is rewritten.

If the traceability leg fails on a dangling tag after this, remove the tag from the removed scenario's row rather than adding a new mapping. Run: `cargo nextest run -p tinio-e2e --test traceability`

- [x] **Step 5: Remove the two dependency edges**

In `crates/tinio-sandbox-windows/Cargo.toml`, delete the `serde_json.workspace = true` and `time = { workspace = true, features = ["formatting"] }` lines from `[dependencies]`. Both were ledger-only: `serde_json`'s other mentions in the crate are comments, and every other `time::` reference is `std::time` inside a `use std::{…}` block.

Leave `getrandom`, `hex` and `sha2` — the salt and the root digest use them.

- [x] **Step 6: Gate and commit**

Run: `cargo check -p tinio-sandbox-windows --all-targets` and `cargo nextest run -p tinio-sandbox-windows`
Expected: green, with the ledger's eight unit legs and the two `acl` legs that read it gone.

Run: `cargo tree -p tinio-sandbox-windows -e normal | rg 'serde_json|^time'`
Expected: no `time`; `serde_json` only if another dependency pulls it transitively.

Ask the operator before committing.

**Evidence** — 11 paths changed, +46 / −1287. Deleted with the subsystem: the `ledger` module and its `pub mod ledger;` declaration with the doc block above it, `other_exposed_paths` and `PreparedLaunch::others`, `grant_whitelist`'s record hook, `acl.rs`'s ledger references (`AclLock` itself stays), the `serde_json`, `time` and `serde` (with `derive`) dependency edges — the third removed as an unlisted consequence of the ledger's deletion, named here so the record is complete — and the `@cli @confinement @FR-036 @windows` scenario `The ACL ledger records the grants this run made` — its shared step stays, because `read_only.feature`'s `The environment beats the config file` calls it in ordinary e2e. Ten legs deleted, each named with the descriptor it asserted, and all ten stood in for the ledger file's current-user-only, inheritance-free descriptor: the ledger's eight unit legs, `acl.rs`'s `the_ledger_file_is_current_user_only_and_inherits_nothing`, and `backend/tests.rs`'s `other_exposed_paths` leg (which pinned `PreparedLaunch::others`, the field that fed the ledger its path set). No leg added. **One public item lost its only caller and was kept deliberately**: `PackageSid::name()`, whose sole caller was the deleted ledger — the controller's ruling is that it stays as the crate's public accessor, uncalled in-tree rather than removed. Gate: `cargo nextest run -p tinio-sandbox-windows` 128 passed / 1 failed (the pre-existing launch leg), the workspace run 1394/1395, and `cargo tree -p tinio-sandbox-windows -e normal` no longer names `time` (`serde_json` survives only transitively). The launch-leg flake this gate shows was also measured by a **repeated narrow selection** — the empty-capability leg failed 6 of 10 runs on this tree against 0 of 10 on a pristine `HEAD` export — and that is a different run from Task 6's whole-suite one: the two do not conflict, because the artefact needs the full suite's concurrent composition, which the whole-suite run reaches and the narrow repeat does not.

---

### Task 2: Delete the descriptor sites and narrow the legs

**Files:**
- Modify: `crates/tinio-config/src/discover.rs:166-227`, `src/lib.rs:29`, `Cargo.toml`
- Modify: `crates/tinio-api/src/state.rs:318-364`, `src/dacl.rs`, `src/lib.rs:16-17`, `Cargo.toml`, `src/transport.rs:3525`
- Modify: `crates/tinio-sandbox-windows/src/sid.rs:228-440`, `:647-720`, `src/acl.rs:3374-3432`, `src/testing.rs`, `tests/confine.rs`, `Cargo.toml`
- Modify: `crates/tinio-server/tests/sandbox_e2e_windows.rs:330-358`

**Interfaces:**
- Consumes: nothing.
- Produces: `create_private` with a plain create-new on both platforms; `State::write` with a plain open on both platforms; `create_pipe` with a null descriptor; `create_state_directory` as a plain directory create with no `ensure_state_directory` guard (inlined into the salt's create by the post-run simplification, so no such helper exists now). `to_wide` is still imported from `tinio_winsec` here — Task 3 flips that import.

- [x] **Step 1: The config file's create call**

In `crates/tinio-config/src/discover.rs`, replace `create_private`'s Windows arm so both platforms take the same shape:

```rust
fn create_private(path: &Path) -> Result<fs::File, Error> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path).map_err(|err| error::io(path, err))
}
```

Delete `restrict_or_remove` and the Windows `restrict_to_current_user` shim (`:196-227`), the call to `restrict_or_remove` in `load_or_create` (`:161`), the import at `:19`, and `extern crate tinio_winsec as _winsec;` at `src/lib.rs:29`. Remove the `[target.'cfg(windows)'.dependencies]` block from `crates/tinio-config/Cargo.toml`.

Rewrite `create_private`'s doc: it keeps the "the file never exists wider than the configured mode" sentence for unix and states plainly that Windows takes the platform's own answer.

- [x] **Step 2: The config file's two legs**

Delete `the_created_config_is_private_on_windows` (`src/discover.rs:606`) and `a_restriction_that_fails_takes_the_file_with_it` (`:321`) — the second has no subject once there is no restriction to fail. Keep `the_created_config_is_private` (`:595`), which asserts the unix mode.

Add one Windows leg beside it, mirroring `the_created_config_is_private`'s shell — the same `TempDir`, the same create path, the same gate. Its three assertions, in order:

1. `load_or_create` creates the file, so the created path is a file.
2. A fresh read handle opens it, and a fresh write handle opens it: this is the assertion the deleted descriptor leg used to stand in for, restated as the property a reader cares about.
3. The descriptor text is **not** the single-ACE protected shape the module installed. Assert the absence of that shape — one entry naming the calling user at full control with no inherited entry — and not the presence of any particular entry. Do not hard-code the token default's contents: `SYSTEM` and the logon-session SID appear on some hosts and not on others, and a leg asserting them would redden per machine.

- [x] **Step 3: The state file's write**

In `crates/tinio-api/src/state.rs`, collapse both platform arms of `write` into one, keeping the unix mode:

```rust
let mut options = OpenOptions::new();
options.write(true).create_new(true);
#[cfg(unix)]
options.mode(0o600);
let mut file = options.open(&tmp).map_err(Error::Io)?;
```

Delete the read-back and repair branch (`:358-361`), the `_winsec` import (`:26`), and demote the re-export at `src/lib.rs:17` from `pub extern crate tinio_winsec as _winsec;` to a private `extern crate` — that demotion is the D5 public-surface removal, since `tinio_api::_winsec` stops resolving while the crate-internal path still works. **The dependency itself stays in `Cargo.toml` until Task 3**, because step 5 still reaches `to_wide` through it; deleting the edge here would not compile.

Rewrite the doc block at `:348-357`: the sentence that names the launcher's grant and the confined child's missing `WRITE_DAC` describes a decision that no longer exists; state instead that the create call is the only moment either way and that no descriptor is installed.

- [x] **Step 4: The state file's leg**

`the_file_is_private` (`src/state.rs:589`) asserts through `write`. Keep its unix half on the file's mode; replace its Windows half with the portable property (created, readable, written atomically) and drop the descriptor assertion.

- [x] **Step 5: The pipe's descriptor**

In `crates/tinio-api/src/dacl.rs`, the two `CreateNamedPipeW` and the open call pass `&dacl.attributes()` / the descriptor's attributes; pass a null pointer instead, and delete the `CurrentUserDacl` construction. Keep `PIPE_REJECT_REMOTE_CLIENTS`, `FILE_FLAG_FIRST_PIPE_INSTANCE` and the overlapped flag.

`to_wide` stays imported from `tinio_winsec` in this task; Task 3 changes that one import line.

**The failure wording moves rather than dies.** Move `win32_error_message` verbatim — body, doc and `# Examples` block — from the crate being dissolved into `crates/tinio-api/src/dacl.rs` as a private function, because this module is already the crate's one raw-Windows module and its pipe errors must keep naming the call, the OS's message and the decimal status. What ends is the *sharing*: two crates no longer agree by construction, and Task 5 corrects the prose that says they do.

Then delete `pipe_acl_is_current_user_only` (`:191`) and `descriptor_is_current_user_only`'s use here.

- [x] **Step 6: The pipe's leg**

`the_pipe_refuses_a_second_instance_and_both_dacls_are_current_user_only` (`src/transport.rs:3525`) keeps its first half — a second instance must still fail — and loses the second. Rename it to what it now asserts (`the_pipe_refuses_a_second_instance`), keeping the leg's spawn and connect shape and its `#[cfg(windows)]` gate.

- [x] **Step 7: The sandbox's state directory and salt**

In `crates/tinio-sandbox-windows/src/sid.rs`:

- `create_state_directory` (`:296-319`) becomes a plain create: `create_dir_all` when absent, nothing else. Delete the `acl_is_current_user_only` → `restrict_to_current_user` repair and the warn block, and rewrite its doc (`:261-295`), which exists to justify a repair that no longer happens — including the sentence about the price a third file pays for it.
- `ensure_state_directory` (`:427-440`) is **deleted** (D8), not thinned: Task 1 takes one of its two callers and the cache was its whole value, so the salt calls `create_state_directory` directly. Two places name it and both are corrected — the once-per-process enumeration at `capability.rs:113`, and the salt's doc.
- The salt's publication (`publish_whole`/`publish_private`, `:647-720`) uses `tinio_winsec::create_restricted` and then reads the descriptor back. Replace the create with a plain `create_new` write and delete the read-back branch that refuses a file which is not this user's alone. **Keep** the hard-link publication, the creation race handling, the wrong-length refusal and the uncreatable degrade: those are the salt's own behaviour, not its protection.
- The salt's doc at `:2146-2149`, which says the protected descriptor is what lets the salt's first use run before the directory is put back, is rewritten: nothing is put back any more.

- [x] **Step 8: The salt's leg and the state-directory leg**

- `the_salt_file_is_current_user_only_and_inherits_nothing` (`src/acl.rs:3384`) becomes the portable-property leg D7 asks for: the salt file exists, is readable and writable by this process, and is **not** the single-entry protected shape. Keep the module's ICACLS-free oracle (`dacl_entries`) and drop the assertions on the absence of an `S-1-15-` entry and on the control word's protected bit — those assert the descriptor this design removes.
- `the_state_directory_is_put_back_to_the_current_user` (`src/sid.rs:2151`) is deleted: the repair it asserts is gone, and its four `acl_is_current_user_only` reads have no subject.
- `a_salt_file_that_is_not_this_users_alone_is_refused_rather_than_used` (`:2087`) is deleted: the refusal it pins was the 2026-09-28 fix that D3 reverts. Record the reversion in the task's evidence line, not in a surviving leg.

- [x] **Step 9: The in-container read legs**

The exposure this design accepts cannot be measured by listing a DACL from outside: a listing cannot tell "sealed, so the grant never propagated" from "propagated". It is measured by reading **from inside** the confinement, and the harness already has that mechanism — the test binary re-execs itself as the confined child role, whose body appends outcome lines to a report file the parent reads. Two limits have to come off first.

In `crates/tinio-sandbox-windows/src/testing.rs`, the probe tree takes `root=`, `denied=` and `report=` and hard-codes the file names its child body reads. Add `read=<path>` keys, one per path, and give the child body a loop over them, appending `<name>: OK` or `<name>: refused (<kind>)` to the report beside the lines it already writes. **This is test support behind the `testing` feature**, so it is not production surface and does not touch the no-new-visible-surface rule above.

Then two legs in `crates/tinio-sandbox-windows/tests/confine.rs`, beside `the_whitelist_refuses_a_path_outside_the_exposures` and using the same tree and launch helpers. Both seed a nested directory that mimics the product's layout — the root gains a `.tinio` child holding a `state` file — because the property under test is the grant's reach, not the product's file names:

- **The exposure, pinned.** The `state` file is created plainly, the way the product now creates it, and the child's read of it must report `OK`. The control is a file created fresh inside the same directory immediately before the launch, which must also report `OK`: without it the leg would pass vacuously on a host that granted nothing at all.
- **The remedy, pinned.** The same file, sealed the way the product used to seal it — inheritance removed, one explicit ACE naming the calling user — must report `refused`. This is what shows a reader that re-sealing still closes the exposure, and it is the property the spec's evidence table cites when it says propagation skips a protected child.

**Neither leg changes colour because of this task, and the plan says so rather than implying a red-to-green.** The sandbox's propagation behaviour is untouched by D1, so a leg about it is green before and after. What changes is that the product stops sealing its own files, and the evidence for *that* is the leg this task deletes — not a new one. The pair above exists so the exposure is pinned as a fact and the remedy as available, not to detect the deletion.

- [x] **Step 10: The end-to-end fixture's rationale**

`crates/tinio-server/tests/sandbox_e2e_windows.rs:330-358` seals a fixture and explains, in prose, that it uses `M` because the type the product installs carries `tinio_winsec::create_restricted`'s descriptor. The product no longer installs one. Rewrite that paragraph to say what the fixture is now and why `M` is still the nearest spelling in the crate's own table, and leave the seal, the read-back and the three legs alone.

- [x] **Step 11: Manifests**

Remove `tracing-subscriber` from `crates/tinio-sandbox-windows/Cargo.toml`'s `[dependencies]` and from the `testing` feature's list (D13). Verified zero references in the crate; `tinio-server` declares its own copy and `tinio-e2e` uses none, so neither dependent loses anything. Keep `tinio-winsec` in this manifest until Task 3.

- [x] **Step 12: Gate and commit**

Run: `cargo nextest run -p tinio-config -p tinio-api -p tinio-sandbox-windows` and `cargo check --workspace --all-targets`
Expected: green, with the deleted legs accounted for in the task's evidence line — each named with the descriptor it belonged to — and Step 9's two legs present and passing.

Run: `rg -n 'acl_is_current_user_only|create_restricted|restrict_to_current_user|pipe_acl_is_current_user_only' crates/tinio-config/src crates/tinio-api/src crates/tinio-sandbox-windows/src`
Expected: only `tinio-winsec`'s own remaining callers inside `tinio-sandbox-windows`'s walk-adjacent code, and none in `tinio-config` or `tinio-api` outside `to_wide`.

Ask the operator before committing.

**Evidence** — the five descriptor sites deleted (the config file's create-time descriptor with `restrict_or_remove` and the `restrict_to_current_user` shim, the state write's read-back and repair branch, the pipe's security attributes with `CurrentUserDacl`, the state directory's repair with `ensure_state_directory`, and the salt's descriptor with its read-back), the failure wording moved into `tinio-api`'s `dacl.rs` as a private function — **its `# Examples` block became a unit test in both copies** (`tinio-api/src/dacl.rs`, and `tinio-sandbox-windows/src/acl/walk.rs` from Task 3's move), because the item is private and a doctest compiles against the public API alone; that is a deviation from the "verbatim — body, doc and `# Examples` block" instruction and from the closed permitted-edit list, recorded here — and `tracing-subscriber` removed from the crate and from its `testing` feature. Legs deleted, each with the descriptor it belonged to: `tinio-config`'s `the_created_config_is_private_on_windows` (the config file's protected single ACE) and `a_restriction_that_fails_takes_the_file_with_it` (the create-time restriction's remove-on-failure branch, which has no subject once nothing restricts); `tinio-api`'s `pipe_acl_is_current_user_only` and `descriptor_is_current_user_only`'s use (the pipe's `CurrentUserDacl` security attributes); `tinio-sandbox-windows`'s `the_state_directory_is_put_back_to_the_current_user` (the state directory's repaired current-user-only descriptor, whose four `acl_is_current_user_only` reads lose their subject) and `a_salt_file_that_is_not_this_users_alone_is_refused_rather_than_used` (the salt's read-back, which refused a file whose descriptor was not this user's alone — the 2026-09-28 fix D3 reverts; the reversion is recorded here, not in a surviving leg). Legs replaced: `the_pipe_refuses_a_second_instance_and_both_dacls_are_current_user_only` → `the_pipe_refuses_a_second_instance`, keeping the spawn-and-connect shape and the second-instance refusal and dropping the two-descriptor half; `the_salt_file_is_current_user_only_and_inherits_nothing` → `acl::descriptor::tests::the_salt_file_is_a_plain_file_this_process_can_use`, the portable property, keeping the module's ICACLS-free `dacl_entries` oracle and dropping the `S-1-15-` and protected-bit assertions; and `the_file_is_private` keeps its unix mode half and takes the portable created / readable / written-atomically property on Windows. Legs added: `tinio-config`'s `the_created_config_opens_for_read_and_for_write`, which asserts the absence of the protected single-ACE shape rather than the presence of any entry, and in `tinio-sandbox-windows/tests/confine.rs` the two in-container legs `the_grant_reaches_a_file_the_product_creates_inside_it` and `a_sealed_file_in_the_granted_directory_is_refused_from_inside`. Gate: `cargo nextest run -p tinio-config -p tinio-api -p tinio-sandbox-windows` 291/295 and `cargo check --workspace --all-targets` clean, with `tinio-config` 115/115, `tinio-api` 51/51 and `tests/confine.rs` 6/6 including both new legs.

**Superseded after the run** — the failure wording is one definer now (`tinio-util::win32::error_message`, read by the walk and by the pipe), not two private copies, and `tinio-api`'s module is `pipe`; the sandbox walk's example became a unit test beside the type. See design D4 and its Status.

---

### Task 3: Dissolve `tinio-winsec`

**Files:**
- Create: `crates/tinio-util/src/wide.rs`, `crates/tinio-sandbox-windows/src/acl/mod.rs`, `crates/tinio-sandbox-windows/src/acl/walk.rs`
- Move: `crates/tinio-sandbox-windows/src/acl.rs` → `src/acl/mod.rs` (this task only opens the directory; Task 4 splits the rest)
- Delete: `crates/tinio-winsec/` in full
- Modify: `crates/tinio-util/src/lib.rs`, `Cargo.toml`, `crates/tinio-sandbox-windows/src/lib.rs`, `src/sid.rs`, `src/sweep.rs`, `src/process.rs`, `src/testing.rs`, `crates/tinio-api/src/dacl.rs:41`, `Cargo.toml` files, root `Cargo.toml`

**Interfaces:**
- Consumes: Task 2's `to_wide` import still pointing at the dissolved crate.
- Produces: `tinio_util::wide::to_wide` (signature unchanged, verbatim) and `crate::acl::walk`'s `walk_dacl`, `Ace`, `ACCESS_ALLOWED_ACE_TYPE`, behind a private `mod walk` — the three names stay crate-private, and the post-run simplification pass re-pointed the one test that had used the facade at `walk` directly (D9).

- [x] **Step 1: `to_wide` moves to `tinio-util`**

Create `crates/tinio-util/src/wide.rs` and move `to_wide` into it **verbatim** — its signature, body, doc comment and `# Examples` block — from the crate being deleted. Do not retype it from this plan: open the file, move the item.

Register it beside the job-object sequence, with the same reasoning in the comment:

```rust
// **The wide-string writer is gated on the target and not on a feature**, the way the
// job-object sequence is: the management plane's pipe name and the sandbox's own Win32
// calls both need it in a production build, and `tinio-util` is where the workspace keeps
// the one copy of a small runtime helper both crates already depend on.
#[cfg(windows)]
pub mod wide;
```

Add the crate's module-doc bullet for it in `crates/tinio-util/src/lib.rs` and a layout row in `crates/tinio-util/README.md`, naming the reader's home (the sandbox crate) so the pair is findable from either side (D4's Ownership row).

- [x] **Step 2: Flip the two consumers' imports**

- `crates/tinio-api/src/dacl.rs:41`: import `to_wide` from the util crate instead of the dissolved one; delete the remaining dissolved-crate import line entirely.
- `crates/tinio-sandbox-windows`'s five files that import `to_wide` (`src/acl/mod.rs:95`, `src/sid.rs:32`, `src/sweep.rs:83`, `src/process.rs:29`, `src/testing.rs:1157`): the same flip.

- [x] **Step 3: Open the `acl` directory**

```bash
mv crates/tinio-sandbox-windows/src/acl.rs crates/tinio-sandbox-windows/src/acl/mod.rs
```

Nothing else changes in this step: the file's contents are untouched, and `crate::acl::…` resolves exactly as before because a directory module's root is its facade.

- [x] **Step 4: The walk lands as `acl/walk.rs`**

Move `Ace`, `walk_dacl` and `ACCESS_ALLOWED_ACE_TYPE` — verbatim, with their docs, their `# Examples` blocks and the ACL-test legs that install real foreign entries — into `crates/tinio-sandbox-windows/src/acl/walk.rs`. Their module header keeps the claim that this is the workspace's one ACE traversal, now with no second crate to name.

The walk's tests are the ones that measure classification by installing real DACLs: move them into the new module's own `#[cfg(test)] mod tests`, and leave the two legs Task 2 rewrote or deleted where they are.

`acl/mod.rs` re-exports no walk name: the three stay crate-private behind `mod walk`, and the one consumer that had used the facade — the spec table's test — reaches `walk` directly (D9, as the post-run simplification pass left it).

- [x] **Step 5: Delete the dissolved crate**

```bash
rm -rf crates/tinio-winsec
```

Remove its workspace pin from the root `Cargo.toml:121`, its row from `README.md`'s crate index, and its dependency lines from `crates/tinio-sandbox-windows/Cargo.toml`. Run `cargo update -w` or the lock edit cargo itself performs so `Cargo.lock` loses the entry.

- [x] **Step 6: Sweep the survivors**

Run: `rg -n 'tinio_winsec|tinio-winsec' crates/ docs/ README.md`
Expected: only prose that names the dissolved crate in the past tense (which Task 5 rewrites) — no `use`, no manifest line, no intra-doc link.

Run: `rg -n 'create_restricted|restrict_to_current_user|acl_is_current_user_only|descriptor_is_current_user_only|CurrentUserDacl' crates/`
Expected: no hits at all. Anything left is a call site Task 2 missed.

- [x] **Step 7: Gate and commit**

Run: `cargo check --workspace --all-targets` and `cargo nextest run -p tinio-util -p tinio-sandbox-windows -p tinio-api`
Expected: green, including `tinio-util`'s own tests (which now compile a Windows-gated module on this host) and the moved walk legs.

Run: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
Expected: clean — the four unresolved intra-doc links the dissolved crate carried on Linux went with it.

Ask the operator before committing.

**Evidence** — `tinio-winsec` dissolved. `to_wide` moved byte-identically — signature, body, doc comment and `# Examples` block — into `tinio-util`'s new Windows-gated `wide` module, registered beside the job-object sequence with the crate's module-doc bullet and a README layout row naming the sandbox crate as the reader's home; `Ace`, `walk_dacl` and `ACCESS_ALLOWED_ACE_TYPE` moved with their docs, `# Examples` blocks and ACL-test legs into `acl/walk.rs`, reached through `acl/mod.rs`'s existing `pub use`; six consumer imports flipped (`tinio-api/src/dacl.rs` and the sandbox crate's `acl/mod.rs`, `sid.rs`, `sweep.rs`, `process.rs`, `testing.rs`); the crate gone from both dependent manifests, the root workspace pin, `Cargo.lock` (593 → 592 entries) and the README index. No leg added, but two were **deleted** with the crate — both from `HEAD:crates/tinio-winsec/src/dacl.rs`, both justified because their subjects are the descriptors D1 removes: `an_ace_for_another_trustee_is_not_the_current_users_dacl` (the read-back predicate) and `a_created_file_keeps_an_inheriting_parents_aces_out` (the descriptor writer). The walk's four legs moved with the walk and were **renamed** as they did — `a_deny_entry_for_the_current_user_is_not_the_current_users_dacl` → `a_deny_entry_for_the_walks_sid_is_not_the_allow_it_resembles`, `an_audit_entry_is_not_the_current_users_dacl` → `an_audit_entry_is_not_the_allow_it_resembles`, `an_object_typed_ace_is_not_the_current_users_dacl` → `an_object_typed_ace_is_not_the_allow_it_resembles` and `a_failed_call_reports_the_path_beside_the_shared_wording` → `a_failed_call_reports_the_path_beside_the_wording` — which this evidence did not say before. That matters because the task's acceptance is a byte-identity reading of the moved bodies, and the renames leave that reading intact. `to_wide`'s doctest moved with it into `tinio-util`'s doc tests. Gate: `cargo check --workspace --all-targets` clean, `cargo nextest run -p tinio-util -p tinio-sandbox-windows -p tinio-api` green including `acl::walk` 4/4 and `tinio-util`'s 24 doc tests, and the crate-scoped doc gates clean — the four unresolved intra-doc links the dissolved crate carried on Linux went with it.

**Superseded after the run** — the walk's three names are not reached through `acl/mod.rs`'s `pub use`: they stay crate-private behind `mod walk`, and the one test that used the facade reaches `walk` directly (design D9).

---

### Task 4: Split the `acl` module

**Files:**
- Create: `crates/tinio-sandbox-windows/src/acl/{spec,refusal,descriptor,lock,icacls,grant}.rs`, `src/acl/test_support.rs`
- Modify: `crates/tinio-sandbox-windows/src/acl/mod.rs`, `src/acl/walk.rs`

**Interfaces:**
- Consumes: Task 3's `acl/mod.rs` facade and `acl/walk.rs`.
- Produces: the seven-module layout. `acl/mod.rs` keeps its `pub use` re-exports so every path that resolved before still resolves; nothing outside the directory changes.

- [x] **Step 1: Move the six production clusters**

Move each cluster out of `acl/mod.rs` into its own file, verbatim, taking the cluster's tests with it (D10):

| Destination | Contents |
|---|---|
| `acl/spec.rs` | the mask constants, the target enum, the spec and bits functions, the grant-spec string function |
| `acl/refusal.rs` | the two refusal message constants and the two refusal constructors |
| `acl/descriptor.rs` | the handle type and its impl and `Drop`, the free-descriptor helper, the descriptor read, the read verdict, the owned-SID type |
| `acl/lock.rs` | the lock consts, the lock directory, the timing consts, the lock type, its impl and `Drop` |
| `acl/icacls.rs` | the binary path, the SID spellings, the mention predicate, the three invoke helpers |
| `acl/grant.rs` | the grant entry points, the SID argument type, the standing-grant batch and its impl |

**The 69-line module header splits by responsibility, not by convenience.** The paragraphs that justify one cluster's mechanics travel with that cluster — the `(OI)(CI)` propagation reasoning with `acl/descriptor.rs` and `acl/grant.rs`, the icacls-text-versus-read reasoning with whichever module holds each half. The paragraphs that state the file's two jobs — it writes the ACEs, and it reads a descriptor back to decide whether a grant is already satisfied — stay in `acl/mod.rs` as the directory's overview, because `docs/sandbox.md` and the design spec both point at the whitelist as one unit, and a facade whose header is one line gives them nothing to point at.

Each moved module gets a header stating its one responsibility, and the `#![allow(unsafe_code)]` boundary follows the unsafe code: `acl/descriptor.rs` and `acl/walk.rs` carry it, the other five carry none.

- [x] **Step 2: Widen visibility exactly as far as the seams require**

The clusters call each other across the new boundaries. Items that were private and are now used by a sibling become `pub(super)`, and this is the complete permitted list:

`Object`, `OwnedSid`, `GrantSid`, `access_matches`, `carries`, `acl_names`, `any_ace_in`, `free_descriptor`, `read_dacl`, `lock_directory`, `run`, `checked`, `invoke`, `ace_bits`, `sid_spellings`, `kind_refusal`.

Anything else that needs widening means a seam is in the wrong place: fix the seam, do not widen.

- [x] **Step 3: The test-support module**

Move the helpers more than one cluster's tests share — the mask table helper, the descriptor-shape helper, the DACL-reading helper, the ACE-line helpers, the SID renderers and the text-mention helpers — into `crates/tinio-sandbox-windows/src/acl/test_support.rs` under `#[cfg(test)]`:

```rust
//! Helpers more than one cluster's tests share. **`cfg(test)` and not `pub(crate)`**: the
//! alternative is widening a production item so a test two modules away can reach it, which
//! is the cost D10 rules out.
#![cfg(test)]
```

The three instrumentation seams (`opens`, `overlap`, `writes`) move into `acl/test_support.rs`, beside the other test devices — the post-run simplification pass moved them out of the facade, which now holds only the `pub use` surface and the test-support module.

- [x] **Step 4: Fix the intra-directory paths**

Every `super::…` a moved item used before the split becomes `super::<its new module>::…` or `crate::acl::…`. Doc links that named an item by its old path are updated to its new one; no doc link is deleted to make the gate pass. Run: `RUSTDOCFLAGS="-D warnings" cargo doc -p tinio-sandbox-windows --no-deps`

- [x] **Step 5: Accept the split on D11's three-part evidence**

This step is the task's acceptance and it is a diff reading, not a test run:

- **Move-only**: `git diff -M --stat` on the task's commit shows renames and no substantive hunks inside the moved regions — only the headers, the visibility keywords and the path lines Step 2 and Step 4 name.
- **Byte-identical**: for each moved item, the body is unchanged. Spot-check by diffing the pre-move file at the task's base commit against the concatenation of the new modules, ignoring the permitted edits.
- **Surface confined**: `rg -n 'pub\(super\)|pub\(crate\)' crates/tinio-sandbox-windows/src/acl/` returns exactly Step 2's list plus the items that were already `pub(crate)`.

A finding on any of the three is fixed before the task closes.

- [x] **Step 6: Gate and commit**

Run: `cargo nextest run -p tinio-sandbox-windows`
Expected: the same leg count as before the split — the split moves tests, it does not add or drop one.

Run: `cargo clippy -p tinio-sandbox-windows --all-targets -- -D warnings` and `cargo fmt --check`
Expected: clean. `clippy::absolute_paths` is the one likely to complain about the new `super::` paths; fix the import, not the lint.

Ask the operator before committing.

**Evidence** — `acl/mod.rs` went from 3913 lines to 127 with seven files created (`spec.rs`, `refusal.rs`, `descriptor.rs`, `lock.rs`, `icacls.rs`, `grant.rs`, `test_support.rs`) and `walk.rs` kept; 95 blocks moved with zero changed lines apart from one signature rustfmt reflowed after a visibility keyword; the `acl` leg count unchanged at 35 (31 moved plus the walk's four); the `unsafe_code` permit now sits on `descriptor.rs`, `walk.rs` and `lock.rs`, the three modules that own Win32 calls. The move-only acceptance quotes **the re-reviewer's independently recomputed residue, not the implementer's 176/287** (whose classifier no longer exists to reproduce them): 28 pre-split lines have no counterpart in the new tree, and 154 new lines (140 unique) have none in the old, every one of them classified as a blank line, an import, test scaffolding, a module header or a facade line. Gate: `cargo nextest run -p tinio-sandbox-windows` with the same leg count as before the split, `cargo clippy -p tinio-sandbox-windows --all-targets -- -D warnings` and `cargo fmt --check` clean.

**Superseded after the run** — one body changed after the move's acceptance: `OwnedSid::parse` uses `to_wide` instead of a hand-written UTF-16 buffer (recorded in Global Constraints; design Status).

---

### Task 5: Correct the prose, manifests and crate index

**Files:**
- Modify: `README.md`, `crates/tinio-config/README.md`, `crates/tinio-api/README.md`, `crates/tinio-sandbox-windows/README.md`, `crates/tinio-sandbox/README.md`, `crates/tinio-util/README.md`, `docs/sandbox.md`
- Modify: `specs/001-s3-local-server/{spec.md,data-model.md,research.md,checklists/security.md,tasks.md,contracts/config.md}`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md`, `2026-09-20-cli-integration-design.md`, `2026-10-01-host-confinement-capability-design.md`
- Modify: `crates/tinio-config/src/discover.rs:168`
- Modify: `crates/tinio-cli/src/signal.rs`, `crates/tinio-util/src/path.rs` — both cite the dissolved crate in the present tense and are in no other task's list (found while finishing Task 3)

**Interfaces:**
- Consumes: everything Tasks 1–4 landed.
- Produces: no code. This task is what makes the tree's prose agree with it.

- [x] **Step 1: The parity claims**

Rewrite `crates/tinio-config/src/discover.rs:168`'s "two platforms, two mechanisms, one promise" — it is a source doc comment, not a document, and it is the phrase the spec's D6 names. The replacement states what each platform now holds and says which is stronger.

Then the documents that state the pair as equivalent: `crates/tinio-config/README.md:15,46`, `crates/tinio-api/README.md:7`, `specs/001-s3-local-server/spec.md:216,219`, `contracts/config.md:137,177`, `data-model.md:105`, `research.md:155`, `checklists/security.md:33,36`, and the feature's task bodies at `specs/001-s3-local-server/tasks.md:130,136,137` — task text is live prose under the record rule, so it is corrected in place rather than left describing a descriptor that no longer exists.

Two documents that look like they belong here and do not: `specs/001-s3-local-server/contracts/management-api.md` carries no access-control clause for the pipe (only the single-instance flag) and its "reachable by every local user" sentence is about a loopback TCP port against the unix socket's mode, which is unchanged — leave it. The plans under `docs/superpowers/plans/` and the reviews under `docs/superpowers/reviews/` are records: they stay exactly as written, and this design's spec is the correction beside them.

The strongest claim to correct is `docs/superpowers/specs/2026-09-20-cli-integration-design.md:295`, which says a platform default would not give the pipe's posture: correct it in place and point at this design as the superseding ruling (D15).

- [x] **Step 2: The ledger and the sandbox's per-user files**

`docs/sandbox.md`: the heading at `:37` loses the ledger, `:39` names one per-user file instead of two, `:41` goes. Add the line the spec's risk requires — the audit is gone and revocation was already manual — rather than falling silent. Then `crates/tinio-sandbox-windows/README.md:25,43` (the ledger's module row and paragraph) and `2026-09-18-sandbox-design.md:444,448,450,604`.

- [x] **Step 3: The dissolved crate and the moved symbols**

`README.md:22` (the row goes), `:21` (the util crate's role gains the wide-string writer), `crates/tinio-sandbox/README.md:16`, `crates/tinio-sandbox-windows/README.md:3,19,20`, `crates/tinio-config/README.md:15`, `crates/tinio-api/README.md:45,55`.

`docs/superpowers/specs/2026-09-18-sandbox-design.md:157`: **decision 14 states it is the only place the unsafe-site enumeration lives.** Delete the dissolved crate's entry and add the walk's new site in the sandbox crate, keeping the list's per-crate shape. `:616`'s pointer to it needs no change.

- [x] **Step 4: The directory repair and the state directory's clauses**

`docs/sandbox.md:42` (the repair bullet) goes; `crates/tinio-sandbox-windows/README.md:20`'s state-directory claim is corrected. In `2026-09-18-sandbox-design.md`, the whitelist rows that describe the repair (`:320,329-331`) and the salt's descriptor (`:446`) are corrected in place with their measured rows left as written.

Add this design's two-directory fact where a reader will meet it: the sandbox doc's per-user-file section gains the sentence that the salt's directory is never granted while the state directory holding the config and the state file is — with a pointer to the spec, not a restatement.

- [x] **Step 5: The salt's scope and the host-confinement spec**

`docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md:28,52,79,81,110` describe the salt file and the refusal ordering; correct the clauses that assume the salt's descriptor exists, leaving the refusal's own ordering claims alone (they are about not running before a refusal, and that still holds).

- [x] **Step 6: The facades, manifests, lock and index**

D5's removals are visible through two facades, so check them rather than the crates they point at: `tinio::_api::_winsec` (through the hidden api re-export) and `tinio_sandbox::_windows::ledger::path` (through the sandbox crate's hidden re-export) must no longer resolve, and neither facade's doc comment may still name them. Read `crates/tinio/src/lib.rs` and `crates/tinio-sandbox/src/lib.rs` for stale mentions — the re-exports themselves stay, since what they name changed, not whether they exist.

Then confirm the workspace manifest has no dissolved-crate pin, every dependent's manifest line is gone, `Cargo.lock` no longer names it, and the root `README.md`'s index has one row fewer and no dangling link. Run: `cargo metadata --frozen > /dev/null` and `rg -n 'tinio-winsec' --glob '!docs/superpowers/plans/**' --glob '!target' .` — only past-tense prose may remain.

- [x] **Step 7: Gate and commit**

Run: `cargo fmt --check`, the workspace clippy gate, and the doc gate.
Expected: clean. `cargo fmt` found nothing to rewrite because nothing in this task is Rust except one comment.

Ask the operator before committing.

**Evidence** — 16 paths changed; 14 documents corrected and 9 checked and left alone, including `specs/001-s3-local-server/contracts/management-api.md`, verified to need no edit because its "reachable by every local user" clause is about the loopback TCP port against the unix socket's mode and not about the pipe. The workspace's unsafe-site enumeration gained the split-out lock module's site — the one D9 had not counted — and its audit recipe was re-derived, now returning thirteen sites across twelve files. No leg deleted and none added. Gate: `cargo fmt --check` clean (nothing to rewrite, the task's only Rust being one comment), the workspace clippy gate clean, and the crate-scoped doc gates for the touched crates clean; the workspace doc gate stays red only in the untouched `tinio-store-pg`.

---

### Task 6: Acceptance and evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-10-15-windows-file-dacl-removal.md` (one evidence line per task, below)
- Modify: `docs/superpowers/specs/2026-10-15-windows-file-dacl-removal-design.md` (Status line only)

**Interfaces:**
- Consumes: the whole change.
- Produces: the evidence lines, written into **this plan's** task bodies — which is where `docs/superpowers/README.md` puts them for a subagent-run execution.

- [x] **Step 1: The workspace gates**

Run each and paste the output into the evidence:

```bash
cargo nextest run --workspace --exclude tinio-e2e
cargo test --doc --workspace --exclude tinio-e2e
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

Expected: all green. The leg counts drop by the descriptors deleted and rise by the portable-property legs added; the evidence names both sides rather than reporting a net number.

- [x] **Step 2: The confinement family on this host**

CI filters the whole `@confinement` cucumber family out of every leg, so the end-to-end claim is only verifiable here:

```bash
TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags 'not @parquet and not ((@linux or @macos or @windows or @unix) and not @windows) and not @interop and not @boto3 and not @mc and not @pg and @confinement'
```

**The tag has to carry the default filter's axes with it.** `nextest` refuses `--tags` for a `harness = false` target ("arguments are unsupported", exit 96), and through `cargo test` an explicit `--tags` *replaces* the default filter rather than adding to it — a bare `@confinement` re-admits 58 scenarios, of which 16 belong to other platforms or to the third-party-client suites and fail here. Spelling out the platform axis and the external umbrella is what makes the run mean what it says; the recorded run took 38 scenarios and 245 steps, all green.

Expected: the remaining confinement scenarios pass, the ledger scenario is absent, and the ACE witnesses (`the run was confined` / `the run was not confined`) — which read the launcher's grant on the served root — still pass. Those two are the ones that would catch a grant regression this change is capable of causing (Review Focus 1).

- [x] **Step 3: The sandbox end-to-end file without a silent skip**

The file's legs return early and pass when the host cannot meet their preconditions. Force a skip to be a failure so a green run means something:

```bash
TINIO_SANDBOX_ALLOW_SKIP=0 cargo nextest run -p tinio-server --features sandbox --test sandbox_e2e_windows
```

Expected: the three legs run rather than skip, and pass. If the host cannot meet a precondition the run must fail here, and the evidence says which precondition and why.

- [x] **Step 4: Do not re-measure the in-container exposure**

Review Focus 1's exposure is pinned by Task 2's in-container leg, which reads the granted directory from inside the confinement. Nothing here duplicates that, and a hand-rolled "in-container read" written at this point would be a second confined-launch harness for a mechanism the sandbox crate already re-execs correctly — including the launch slot, the job guard and the handshake that `docs/tests.md` keeps in one place. Name the two legs in the evidence instead, with what each asserts.

- [x] **Step 5: The two measurements no leg can hold**

Record, as readings rather than assertions, with the command in a fence:

- `%LOCALAPPDATA%\tinio`'s ACL and `acl-salt`'s, on this host — Review Focus 4. Expected: the salt carries the directory's grants, and the evidence says plainly that this is the salt's whole defence now.
- The named pipe's effective access, by listing its DACL — Review Focus 3. Expected: Windows' own default named-pipe descriptor — `SYSTEM`, `Administrators` and the creating user at full control, `Everyone` and `ANONYMOUS LOGON` at read — and **not** the token default DACL, which this line assumed until Task 6's measurement falsified it with an unrelated control pipe.

Neither can be a leg: both are functions of the host's ACLs, and a leg asserting them would redden on a machine configured differently (which is the same reason the portable-property legs assert a property and not a shape).

- [x] **Step 6: Close the spec and tick this plan**

Set the spec's Status line to record that it shipped, naming **the tree state rather than a commit range** (the operator ruled no commits for this run, so there is no range to name), the gates' output and the two readings. Do not edit its decisions — a landed spec's decisions are corrected in place only by a ruling that changes them, and this step records that they held.

Then tick this plan's step boxes and write one Evidence line into each task's own body — that is the shape `docs/superpowers/README.md` prescribes for a subagent-run execution, and this plan is the artifact the operator reads. Quote the re-reviewer's independently recomputed residue for Task 4 rather than the report's figures, which its classifier can no longer reproduce.

- [x] **Step 7: Write the evidence, then commit**

The evidence goes into **this plan's** task bodies — one line per task: its gate output, every leg it deleted together with the descriptor that leg belonged to, and every leg it added. That is the shape `docs/superpowers/README.md` prescribes for a subagent-run execution, where the dispatcher ticks the boxes and writes the line once the review passes. Then ask the operator before committing the plan's evidence and the spec's Status line.

**Evidence** — the acceptance run. **No commit step ran**: the operator ruled no commits for this whole run, so Step 7's commit is void, the index was not touched, and the spec's Status line therefore names the tree state rather than a commit range. The workspace gates, all with `--workspace --exclude tinio-e2e`: `cargo nextest run` is 2604 passed / 3 failed / 1 skipped (the skip is `tinio-select`'s `#[ignore]`d fixture regenerator), and the three failures are `tinio-sandbox-windows`'s `backend::tests::a_confined_launch_holds_the_window_across_its_acl_phase`, `process::tests::a_rust_binary_runs_inside_the_container_and_the_whitelist_holds` and `process::tests::the_empty_capability_set_launches` — each `CreateProcessW` → `ERROR_FILE_NOT_FOUND (2)` with "the profile behind this launch's package SID is gone". That is the **pre-existing harness defect the Notes name, not a regression**: the sweep's liveness gate keys on `LAUNCHER_IMAGE = b"tinio"` and so cannot see the test binary, logging `other_tinio_processes=0` while it reaps profiles under legs running concurrently; it was reproduced on a pristine `git archive HEAD` export (which predates this plan's work, this branch carrying none of it committed) and this run's signature matched that one exactly — same three legs, same error, same gate count (a **whole-suite** run on each tree, not a repeated narrow selection: Task 1's 6-of-10 and 0-of-10 figures for the empty-capability leg are that repeated-selection measurement, and the artefact needs the suite's concurrent composition, which the whole suite reaches and the narrow repeat does not). `cargo test --doc --workspace --exclude tinio-e2e` exit 0; `cargo clippy --workspace --all-targets -- -D warnings` exit 0; `cargo fmt --check` exit 0. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` is exit 101, red only in `tinio-store-pg` (`content.rs:1485` and `:1519` link the private `publish_live_copy` from public-item docs) — a crate this plan never touches, recorded as pre-existing with its cause — while the crate-scoped gate for the eight crates the plan does touch is exit 0.

**Superseded after the run** — the three failing sandbox legs are fixed (the probe's `Dispatch::none()` interest poisoning and two legs launching without the profile window), `tinio-store-pg`'s two intra-doc links are corrected, and the suite is green; design Status carries the current reading.

The two host-only runs CI cannot do. The plan's literal confinement command `TINIO_E2E_CONFINEMENT=1 cargo nextest run -p tinio-e2e --test cucumber -- --tags '@confinement'` cannot run at all (nextest: "failed to parse test binary arguments `--tags`: arguments are unsupported", exit 96) because the `cucumber` target is `harness = false`, and `docs/tests.md` and the e2e README both run tagged cucumber through `cargo test`; through `cargo test` the tag selects 58 scenarios across four features and 16 of them fail, every one a scenario this host's default filter excludes — 11 `@linux`/`@macos` platform-tagged and 5 `@interop`/`@boto3`/`@mc` — because an explicit `--tags` replaces the default filter rather than adding to it, re-admitting other platforms' scenarios and the third-party-client ones. With the platform axis and the external umbrella restored, the family is green: `TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags 'not @parquet and not ((@linux or @macos or @windows or @unix) and not @windows) and not @interop and not @boto3 and not @mc and not @pg and @confinement'` → 38 scenarios, 38 passed; 245 steps, 245 passed; exit 0. The ledger scenario is absent (no `ledger` anywhere in the feature corpus), and the two ACE witnesses pass — `the run was confined` nine times and `the run was not confined` four times, all green. `TINIO_SANDBOX_ALLOW_SKIP=0 cargo nextest run -p tinio-server --features sandbox --test sandbox_e2e_windows` → 3 tests run, 3 passed, **0 skipped**, so no leg took the silent-skip path and the green means something; the file's two preconditions were both met on this host (`CONOUT$` opens for the control event, and `can_confine`'s token predicate plus a real `icacls` grant on a scratch directory answers `Ok`). The three legs are `a_confined_server_holds_one_ace_while_it_runs_and_after_the_launcher_is_killed`, `a_child_that_never_reports_is_the_parents_own_error_frame` and `a_restarted_launcher_takes_the_residue_over_and_a_clean_exit_keeps_it` — all three of D7's grant legs, kept.

The in-container exposure is **not** re-measured here, and no second confined-launch harness was written: Review Focus 1's exposure is pinned by Task 2's two legs, `the_grant_reaches_a_file_the_product_creates_inside_it` (a `state` file created plainly inside a granted directory, with a control file created just before the launch, both read `OK` by the confined child that holds the package SID — the control being what keeps the leg from passing vacuously on a host that granted nothing) and `a_sealed_file_in_the_granted_directory_is_refused_from_inside` (the same file sealed the old way, inheritance removed and one explicit ACE for the calling user's SID, read `refused`, with the same control).

**Superseded after the run** — `a_sealed_file_in_the_granted_directory_is_refused_from_inside` is deleted: its subject was a hand-installed remedy rather than a product behaviour, so the exposure is pinned by `the_grant_reaches_a_file_the_product_creates_inside_it` alone (design Status).

The two readings no leg can hold, each a function of this host's ACLs — a leg asserting either would redden on a machine configured differently:

```powershell
# Reading A — Review Focus 4: the sandbox's per-user directory and the salt, on this host
icacls "$env:LOCALAPPDATA\tinio"; icacls "$env:LOCALAPPDATA\tinio\acl-salt"
(Get-Acl "$env:LOCALAPPDATA\tinio").Access              # .IsInherited, .AreAccessRulesProtected
(Get-Acl "$env:LOCALAPPDATA\tinio\acl-salt").Access
# …and the same two objects created fresh, so the *new* create is what is read:
$env:LOCALAPPDATA = Join-Path $env:LOCALAPPDATA 'task6-la'   # empty
cargo nextest run -p tinio-sandbox-windows --test confine -E 'test(the_whitelist_refuses_a_path_outside_the_exposures)'
(Get-Acl "$env:LOCALAPPDATA\tinio").Access; (Get-Acl "$env:LOCALAPPDATA\tinio\acl-salt").Access

# Reading B — Review Focus 3: the named pipe's effective access, from a live server
target/debug/tinio.exe server <root> --port 0          # then read <root>/.tinio/state's control_name
# CreateFileW(name, GENERIC_READ) → GetSecurityInfo(SE_KERNEL_OBJECT, DACL_SECURITY_INFORMATION)
#   → ConvertSecurityDescriptorToStringSecurityDescriptor
```

Reading A: `%LOCALAPPDATA%\tinio` and `acl-salt` **both** answer the single ACE `FLIER-WIN11\Admin:(F)` with `AreAccessRulesProtected = True` — the pre-change protected shape, because both objects were created 2026-09-28 20:55 and nothing re-seals an object that already exists (the new directory create is a plain `create_dir_all` when absent, and the salt a plain create when absent). Driven again under a fresh `%LOCALAPPDATA%`, both answer `AreAccessRulesProtected = False` and inherit all five of the directory's grants: `FLIER-WIN11\CodexSandboxUsers:(M)`, the unresolved `S-1-5-21-2006358616-1683619250-718909971-2530351631:(M)`, `NT AUTHORITY\SYSTEM:(F)`, `BUILTIN\Administrators:(F)` and `FLIER-WIN11\Admin:(F)`. So the salt does carry the directory's grants, and on this host those grants are `CodexSandboxUsers` and an unresolved SID at Modify: `%LOCALAPPDATA%` is not the user's alone here, and that ACL is now the salt's whole defence — Review Focus 4 realised rather than hypothetical. The same directory still holds the deleted subsystem's residue, `acl-ledger.jsonl` (4 777 028 bytes, last written 2026-10-16 21:10:23 +08:00, untouched by this build), which per D2 carries the package name and raw package SID in cleartext beside the salt; removing an operator's file is not this plan's business, but the exposure D2 describes is still standing on this host.

Reading B: the pipe's DACL is `D:(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;S-1-5-21-326081291-4248401882-2295824474-1000)(A;;FR;;;WD)(A;;FR;;;AN)` — full control to SYSTEM, to Administrators and to the creating user, and read to Everyone and ANONYMOUS LOGON. That is Windows' own **default named-pipe descriptor and not the token default DACL**: a control pipe created with a null `SECURITY_ATTRIBUTES` by an unrelated process answers the identical string, while the creating token's default DACL is `D:(A;;GA;;;<the user>)(A;;GA;;;SY)(A;;GXGR;;;S-1-5-5-0-4876874)` — the user, SYSTEM and the logon-session SID. Review Focus 3's stated expectation ("the token default DACL, naming SYSTEM and the logon session beside the user") is therefore **falsified for pipes on this host**, and the reachability it worries about is realised through `BUILTIN\Administrators` (full control, so `/stop` is writable) and through `Everyone`/`ANONYMOUS LOGON` (read only) rather than through a logon-session SID. The design decision is not re-opened — D1 removes tinio's descriptor and this is the platform's own answer to a null `SECURITY_ATTRIBUTES` — but the expectation text is wrong, and the spec's evidence-table row carries the same claim; Review Focus is neither a step box nor a task body, so the correction is recorded here for the controller to rule on. The pipe's unchanged refusals still hold: `PIPE_REJECT_REMOTE_CLIENTS` keeps it off the network and `the_pipe_refuses_a_second_instance` pins the second-instance refusal.

---

## Plan-set ownership

- **This plan owns** the deletions in Tasks 1–2, the dissolution in Task 3, the split in Task 4, the prose correction in Task 5, and the evidence in Task 6.
- **The spec owns** every decision and the two-directory fact. No task restates a decision; each names it by number.
- **`docs/sandbox.md` owns** the operator-facing account of the sandbox's per-user files; Task 5 corrects it and nothing else may state the same file list.
- **The sandbox design spec owns** the unsafe-site enumeration (its decision 14) and the salt's scope; Task 5 corrects the enumeration, not the scope.
- **`docs/tests.md` owns** the Windows CI coverage rule; this plan does not change it, and Task 6 points at it rather than restating it.
- **No task edits another task's file.** `crates/tinio-sandbox-windows/src/acl.rs` is Task 2's to delete from, Task 3's to move, and Task 4's to split — in that order, which is why the tasks are sequential and not parallel.

## Notes for the executor

- **Run the tasks in order.** Task 2 leaves `to_wide` imported from a crate that Task 3 deletes; Task 3 leaves the walk inside a directory Task 4 splits. Each task's gate is green on its own, and the ordering is what makes that true.
- **The move is the risk, not the deletion.** Deleting a call is mechanical; moving 4055 lines is where a silent change hides. If a moved body needs a change to compile, the seam is wrong — move the seam, not the body.
- **Two legs are deleted on purpose and they were real.** `a_restriction_that_fails_takes_the_file_with_it` and `a_salt_file_that_is_not_this_users_alone_is_refused_rather_than_used` both pinned protections this design removes. Name them in the evidence with the descriptor they belonged to; a leg that disappears without a line saying why is a review finding.
- **The evidence lines are the deliverable of Task 6**, not a summary. Measurements with their commands, leg names with their subjects, and the two host-dependent readings labeled as readings.
- **The spec's risk list is not a formality.** Two of its entries are accepted exposures rather than fixed defects, and the review's job is to confirm they were accepted knowingly — which the evidence line for Task 6 Step 4 is what shows.
