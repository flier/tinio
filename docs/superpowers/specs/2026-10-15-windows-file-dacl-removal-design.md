# Design: Windows file-DACL removal, and the `acl` module split

**Date**: 2026-10-15
**Status**: shipped 2026-10-16, recorded as **a tree state and not a commit range** because the operator ruled no commits for this run: branch `dev` at HEAD `8996879f`, the whole change in the working tree. The acceptance evidence — every command and its output — is `docs/superpowers/plans/2026-10-15-windows-file-dacl-removal.md`'s Task 6 Evidence line. **A post-run simplification and fix pass changed these things that evidence does not cover**: the failure sentence and `system_directory` moved to `tinio-util::win32` (D4); the three failing sandbox legs were fixed (the capability probe's `Dispatch::none()` cached `Interest::never()` for every callsite it raised, and two legs launched without the profile window); `tinio-store-pg`'s two private-intra-doc links were corrected so the workspace doc gate is green; the walk's three names stay crate-private and the six test oracles ride the `testing` feature (D9); `tinio-api`'s `dacl` module is `pipe` now, since it installs no descriptor; and the confine re-seal leg, whose subject was a hand-installed remedy rather than the product, is deleted. The suite is green. The two host-only runs CI cannot do both pass: the `@confinement` family 38 scenarios / 38 passed with the ledger scenario absent and both ACE witnesses green, and the sandbox end-to-end file 3 legs / 3 passed with a skip forced to fail. No decision changed its ruling — D1–D15 and the two-directory fact held — and D9's four turn-ups are recorded in D9 itself.
**Scope**: the Windows arms of the config-file create (`tinio-config`), the state write and the named-pipe create (`tinio-api`), the sandbox state directory and the salt file (`tinio-sandbox-windows`); the ledger subsystem's deletion; the dissolution of `tinio-winsec`; the split of `tinio-sandbox-windows`'s `acl.rs` into modules; every README, contract, manifest, doc comment and landed-spec clause that names any of them. Unix arms unchanged. No wire-format, FR/SC, or success-path change.

## Goal

Windows builds install no current-user-only security descriptor. Every object the workspace creates takes the platform's own answer — the parent directory's inheritable ACEs, falling back to the creating token's default DACL only when it inherits nothing — and nothing installs or verifies a current-user-only descriptor afterwards. The ACE reads that decide whether a grant is already satisfied are untouched: they ask what a descriptor names, not whether it is the current user's alone.

## Non-goals

- Unix arms: `mode(0o600)` on the two secret files, 0700 on the state tree, the control socket's explicit 0600. The platforms' promises now differ, and the docs name them as different instead of claiming one promise.
- The confinement grant: the exposure set, `grant_whitelist`, the `(OI)(CI)` ACE shapes, the whitelist computation and the `icacls` fallback are unchanged. This design changes what happens to the *files inside* a granted directory, not what is granted.
- `PIPE_REJECT_REMOTE_CLIENTS`, the local-only transport rule, the access token and its generation.
- What a run grants, and the manualness of revocation — the ledger recorded and never revoked.
- Windows' own defaults: a plain create under `%LOCALAPPDATA%` still reaches the user, SYSTEM and Administrators. This design stops *narrowing*; it does not change what Windows grants on its own.

## Two directories the code calls "the state directory"

The distinction is load-bearing for every risk below, and nothing in the tree names it.

| | Path | Holds | Granted to the package SID? |
|---|---|---|---|
| **P** | `<root>/.tinio` (read-write), `TINIO_STATE_DIR`, or `~/.tinio/roots/<sha256(canonical root)16>` (read-only) | `state`, `.tinio.toml` | **Yes** — through the root's own exposure, or as its own read-write entry when it sits outside the root |
| **W** | `%LOCALAPPDATA%\tinio` | `acl-salt` (the ledger used to live here) | **Never** — no caller of the sandbox state directory feeds an exposure set |

P's grant carries `(OI)(CI)M`, so it is inheritable and reaches pre-existing children — that is measured and is design §6's intent, because the confined server must write `state` there. What keeps the grant off the sealed files today is `SE_DACL_PROTECTED`: a protected DACL is skipped both by inheritance at create and by later propagation.

## Evidence base

Measured 2026-10-15 on this host (Windows 11 Pro for Workstations 10.0.26300) by reading ACLs live, beside the workspace's own records and a code-path audit.

| Fact | Evidence |
|---|---|
| `%LOCALAPPDATA%` grants | explicit inheritable `CodexSandboxUsers:(OI)(CI)(M)` and an unresolved `S-1-5-21-…-2530351631:(OI)(CI)(M)`, with inherited User/SYSTEM/Administrators |
| A plain file directly under `%LOCALAPPDATA%` | inherits both `(I)(M)` entries — read from two pre-existing files, not constructed |
| A plain file inside P, today | token default DACL: user `(F)`, `SYSTEM:(F)`, `LogonSessionId_…:(RX)` — nothing inherited, because P's own descriptor is protected |
| P is in the granted set | the exposure builder adds the root always and the state directory as its own `ReadWrite` entry when it lies outside the root |
| P's grant shape | `(OI)(CI)M` for a read-write directory, `(OI)(CI)RX` for read-only — one table, spelled twice, for `icacls` text and for `EXPLICIT_ACCESS_W` |
| What keeps the grant off the sealed files | `SE_DACL_PROTECTED`, installed by the descriptor the salt, `state` and the config are created with; propagation skips a protected child |
| The repair's ordering | the state directory's repair runs strictly before any grant in the process, and it repairs W, which is never granted — so it cannot drop a package-SID ACE |
| Unix hardening on Windows | the 0700 helper is a plain directory create on Windows, so the read-only home tree already inherits normally — P's protection was already file-level only |
| A named pipe's DACL | no inheritance source at all, and its default is **not** the token default DACL: a null `SECURITY_ATTRIBUTES` yields Windows' own named-pipe descriptor — measured 2026-10-16, and identical on a control pipe an unrelated process created: `SYSTEM`, `Administrators` and the creating user at full control, `Everyone` and `ANONYMOUS LOGON` at read |
| The unix control channel | protected by an explicit `set_permissions` to 0600 after bind, on the unix arm alone |
| Dependency edges that were ledger-only | `serde_json` (its other mentions are comments) and `time` with `formatting`; `getrandom`, `hex` and `sha2` serve the salt and the root digest and stay |
| An unused optional dependency | `tracing-subscriber` has zero references in the crate; `tinio-server` declares its own and `tinio-e2e` uses none |
| The `acl` file's size | 4055 lines, of which the inline test module is 2012–4055 — half the file is tests; 32 test fns; no integration `acl` target exists |
| CI coverage on Windows | `windows-latest` runs the workspace suite minus the e2e crate; clippy for Windows is cross-compiled on ubuntu; no Windows rustdoc job; the whole `@confinement` cucumber family is filtered out of every leg |

## Decisions (ruled 2026-10-15)

- **D1 Removal scope** — every descriptor this workspace installs on Windows goes: the config file's create-time descriptor and its post-write re-assertion, the state file's create-time descriptor and its read-back repair, the named pipe's security attributes, the state directory's repair, and the salt file's descriptor with its read-back.
- **D2 Ledger deleted** — the module in full, its `PreparedLaunch::others` input with `other_exposed_paths`, `grant_whitelist`'s record hook, the lock keyed on its path, the legs that read an operator's real ledger, the cucumber scenario, and the `serde_json` + `time` edges that existed for it. **Why this is not only a reduction**: the ledger published the package name and the raw package SID in cleartext beside the salt, which is what made the salt's stated scope vacuous; deleting it is what lets D3 mean anything.
- **D3 Salt kept, unprotected** — the file is created plainly, with no descriptor and no read-back; the unsalted degrade path and the salt-landing re-grant of every served root stay. Its defence is now exactly what `%LOCALAPPDATA%` grants, because W is never granted by the launcher (see the two-directory table). Accepted by ruling; the exposure is a risk below, not an implication.
- **D4 `tinio-winsec` dissolves** — `to_wide` moves to `tinio-util` as a Windows-gated module beside the job-object sequence, because the pipe name still needs it and the alternative is a management-plane dependency on a confinement backend; `Ace`, `walk_dacl` and `ACCESS_ALLOWED_ACE_TYPE` move into the `acl` directory. The descriptor writer, both read-back predicates, the restrict path and the descriptor builder are **deleted, not moved** — nothing outside the walk survives. **The shared failure wording moved instead, in the post-run simplification pass**: it is `tinio-util::win32::error_message` now, read by the ACE walk and by the management plane's pipe — the two private copies the first pass left were exactly the drift the dissolved crate existed to prevent, so one definer replaced both. `system_directory` moved there with it, because the config crate's DACL test asked the kernel the same question the sandbox's `icacls` oracle did.
- **D5 Public surface removals are plain deletions** — the hidden re-export of the dissolved crate, the sandbox crate's ledger module with its one public `path`, and the facade path that reaches it. Every crate is `publish = false`, so there is no external contract to honour; no deprecation cycle and no stub, and the review checks the facades rather than a compatibility note.
- **D6 Divergence is stated** — the config file's "two platforms, two mechanisms, one promise" is rewritten, and the config and management-api contracts' clauses are corrected to say what each platform holds now. The unix arm keeps the stronger guarantee, and the docs name it as the stronger one rather than implying parity.
- **D7 Legs follow their subject, narrowed** — only legs asserting a descriptor this design deletes go with it. The grant ACE legs **stay**: the sandbox end-to-end file keeps all three of its legs, and the third one's fixture rationale is rewritten where it quotes the deleted descriptor as "what the product installs". The salt's descriptor leg becomes a leg asserting the portable property — the file exists and this process can read and write it, and it does **not** carry the old single-ACE protected shape (one entry naming the calling user at full control, nothing inherited) — so what the leg pins is the **absence** of that shape rather than the presence of any entry; the ledger's goes with the ledger.
- **D8 The state-directory cache collapses** — the once-per-process guard and its path vector existed to amortize the repair's token open, two `GetTokenInformation` calls and ACE walk. A plain directory create is idempotent and costs nothing, so the guard goes with the repair.
- **D9 The `acl` module split** — six production modules under an `acl/` directory with `acl/mod.rs` kept as a `pub use` facade, so every existing crate-internal `acl::…` path resolves unchanged and no production surface is added. The walk's three names, which the old file imported **privately**, stay crate-private behind a private `mod walk`: the first draft widened them because their examples were doctests, which compile as external crates and cannot reach a private item, and the post-run simplification pass turned those examples into unit tests beside the type instead — so the facade re-exports none of them, and the one test that used the facade reaches `walk` directly. **One exception, recorded beside it**: `crate::acl::AclLock` and `crate::acl::REPARSE_POINT`, which the old file reached at the module root, do not resolve at the facade after the split — they moved into `lock.rs` and `refusal.rs` and neither is re-exported, because both are `pub(crate)` with no caller in the workspace and an unused `pub(crate) use` is what clippy refuses. Nothing in the tree reaches either path. Every crate here is `publish = false`, so the paths that stop resolving carry no external contract. A closed list of items becomes `pub(super)`, all confined to the directory; the implementation found one the list had missed — the grant entry point its own tests cross a seam to reach — and that addition is recorded rather than re-derived. The walk lands as a seventh module beside the descriptor core, and the `unsafe` permit moves: `mod.rs` carries none, and **each module that owns unsafe Win32 calls carries its own** — `descriptor.rs`, `walk.rs`, and `lock.rs`, whose lock-file sequence this design had not counted — plus a test-only module for the shared test helpers. The boundary follows the unsafe code rather than the directory, and the workspace's unsafe-site enumeration gains the lock's site.
- **D10 Tests move with their subject** — each new module carries its own test module; the eleven helpers more than one cluster's tests share become a test-support module inside the directory under `cfg(test)`, so nothing has to become `pub(crate)` for a test's sake. The three instrumentation seams move with their subjects.
- **D11 Split acceptance is three-part** — move-only, the moved code and tests byte-identical, and the new `pub(super)` surface confined to the named list. The three together are what separates "split" from "changed behaviour in passing".
- **D12 Order** — the deletions land first, the split after. Each diff then has one purpose: the deletion is reviewable against today's file, and the split is a move nothing is deleted in.
- **D13 Manifest hygiene** — the unused optional `tracing-subscriber` goes from the crate and from its `testing` feature, verified as zero references and with neither of the two dependents relying on it transitively.
- **D14 The requirement set is untouched** — every clause this design falsifies is free prose, a checklist entry or task text. No FR or SC quantifies over a Windows ACL; the Windows guarantee was never numbered, and this change does not number its absence.
- **D15 Prose corrections follow the record rule** — live prose is corrected in place: the contracts, the assumptions, the data model, the research decision, the checklist, the task bodies and the landed design specs' decision text. Fenced and measured records stay as written, with the correction in live prose beside them. This design is the owner of the superseding fact.

## What is deleted, what survives

| Surface | After |
|---|---|
| Config-file create, Windows arm | plain create-new; the post-write re-assertion and its remove-on-failure branch deleted |
| State write, Windows arm | plain open; the read-back and repair branch deleted |
| Named-pipe create | null security attributes; the pipe flags and the launch-token rule unchanged |
| State directory | plain directory create; the repair, its check and its warning deleted |
| Salt file | plain create; descriptor and read-back deleted; degrade path and re-grant kept |
| Ledger | gone, with `others`, `other_exposed_paths` and the record hook |
| `to_wide` | `tinio-util`, Windows-gated, beside the job-object sequence |
| `Ace`, `walk_dacl`, `ACCESS_ALLOWED_ACE_TYPE` | the `acl` directory, beside the descriptor core |
| Descriptor writer, the descriptor builder, both read-back predicates, the restrict path, the shared failure wording | deleted |
| `tinio-winsec` crate | gone — manifest, README, lib.rs, tests |

## The `acl` split (D9–D12)

| Module | Owns |
|---|---|
| `acl/spec.rs` | the target/access table, its two spellings and the mask constants — pure, no `unsafe` |
| `acl/refusal.rs` | the reparse-point and wrong-kind refusals |
| `acl/descriptor.rs` | the handle, the descriptor read, the read verdict, the owned SID — the first `unsafe` owner |
| `acl/walk.rs` | the ACE walk moved in from the dissolved crate — the second `unsafe` owner |
| `acl/lock.rs` | the per-path lock, its directory and its timing |
| `acl/icacls.rs` | the binary, the SID spellings and the invoke helpers |
| `acl/grant.rs` | the orchestrator: grants, revoke, has-ace, the standing-grant batch — the only module that may depend on all siblings |
| `acl/mod.rs` | the facade and the test-support module (the instrumentation seams live in it) |

## Ownership (one definer per fact)

- The config file's protection rule — `specs/001-s3-local-server/contracts/config.md`; this design changes its Windows clause.
- The management-plane transport's access rule — `specs/001-s3-local-server/contracts/management-api.md`; the token rule is unchanged and is not restated here.
- The requirement set's environment assumption and the data model's permission row — `specs/001-s3-local-server/{spec.md,data-model.md}`; both carry a Windows half that this design falsifies.
- The salt's purpose and honest scope — the sandbox design spec's §6; this design changes the file's protection, not the scope statement.
- The unsafe-site enumeration — the same spec's decision 14, which states it is the only place the list lives; the dissolved crate's entry goes and the walk's new site is added.
- The Windows sandbox's per-user files and the ledger — `docs/sandbox.md`; it loses the directory-repair and ledger bullets, keeps the salt's, and must say the audit is gone rather than fall silent.
- The wide-string rule — split by D4; `tinio-util`'s module doc owns the writer and the sandbox crate's owns the reader, each naming the other.
- The crate index and each crate's README — `README.md` and the crate READMEs; the dissolved crate's row and file go, and the receiving crates' roles gain what they now hold.
- The CI coverage rule — `docs/tests.md`; this design does not change it, and the acceptance section points at it rather than restating it.

## Testing & acceptance

- **CI covers** the in-process Windows legs: the workspace suite on `windows-latest` minus the e2e crate — `tinio-config`, `tinio-api` and `tinio-sandbox-windows` unit legs, and the sandbox end-to-end file's default-feature leg. Deleting the descriptor legs removes that signal for the dissolved crate and for the three crates' descriptor assertions; that loss is the point, and the legs that replace them (D7) are what the suite gains.
- **CI does not cover** the end-to-end Windows behaviour: the whole `@confinement` cucumber family is filtered out of every leg, Windows clippy is cross-compiled on ubuntu, and no Windows rustdoc job exists. Verifying the end-to-end claim takes this host with the confinement variable set, and the plan says so where it records the run.
- **A vacuously green leg is not evidence**: the sandbox end-to-end file returns early and passes when the host cannot meet its preconditions, unless the skip is made to fail. The plan's evidence line names the variable that makes a skip visible.
- **The decisive end-to-end measurement is an in-container read, not a DACL reading.** A descriptor reading cannot tell "protected, so not propagated" from "propagated"; the leg that settles it is a process holding the package SID attempting to open and read `state` and the config and failing, while succeeding on a file created fresh in the granted directory.
- `cargo clippy --workspace --all-targets -- -D warnings` clean, and the rustdoc gate clean — the four unresolved intra-doc links the dissolved crate carried on Linux go with it.
- The split's three-part acceptance (D11) is checked as its own step, the moved code and tests byte-identical in a diff that deletes nothing.

## Risks

- **W loses its repair and inherits `%LOCALAPPDATA%`.** On this host that is `CodexSandboxUsers` and an unresolved SID at Modify. This is the root of the salt's exposure below. Accepted by ruling 2026-10-15.
- **The two files in P become reachable by anything holding the package SID.** `state` and `.tinio.toml` sit inside a directory the launcher grants `(OI)(CI)M`, and today the protected descriptor is the only thing keeping that grant off them; `SE_DACL_PROTECTED` blocks both the create-time parent ACEs and later propagation, and this design removes it from both. **On a new install they then carry no user-or-SYSTEM floor**: a create takes its directory's inheritable ACEs rather than the token's default DACL, so the two files' ACEs are exactly the served root's inheritance — whatever that directory's DACL names — and the user-and-SYSTEM entries the token default would have added are not there to bound the exposure. The chain is worth writing down because its first link is the risk above: read the salt in W, derive the package name, obtain a token carrying the package SID, then read the credential pair out of the config. Accepted by ruling 2026-10-15.
- **The table's "W is never granted" is a code-path fact, and an operator can falsify it.** No caller of the sandbox state directory feeds an exposure set, so the launcher's grant never names W — that is true of the two directories' code paths as written, not of every layout an operator can configure. Point `TINIO_STATE_DIR` at `%LOCALAPPDATA%\tinio` and the state directory and the sandbox's per-user directory become the same object, after which the launcher's grant propagates onto the salt. Recorded 2026-10-16.
- **The salt's pre-computation defence now rests on `%LOCALAPPDATA%` being the user's alone.** True on a default Windows install, false on this host. Accepted by ruling; recorded rather than left implied.
- **Two review findings are reverted, and this design says which.** The ledger's inherited descriptor was a MEDIUM finding and disappears with the ledger — that one the deletion resolves rather than reopens. The salt's read-back was the fix for "the salt was trusted as found" and is reverted by D3. The other two findings against that subsystem are moot once it is gone.
- **Removing the ledger removes the workspace's only record of the ACEs a run leaves standing.** Revocation was already manual and had no list, so nothing that worked stops working — but an operator loses the audit, and `docs/sandbox.md` must say so.
- **D1 stops installing descriptors; it does not strip the ones already in place.** Measured 2026-10-16: on an existing install the state directory and the salt still answer their pre-change single-ACE protected descriptor, because nothing re-seals an object that already exists — while a freshly created one, under the same parent, inherits every grant the parent carries. The exposures above are therefore prospective on an existing install and immediate on a new one.
- **The deleted subsystem's file is still on disk.** `acl-ledger.jsonl` sits beside the salt in that directory carrying the package name and the raw package SID in cleartext — exactly what D2 stopped writing. The code that wrote it is gone and removing an operator's file is not this plan's business, so the file stands until the operator removes it.
- **The pipe's reach is wider than the token default would be.** The measured descriptor grants `Administrators` full control, so `/stop` is writable by that group, and read to `Everyone` and `ANONYMOUS LOGON`. What still guards the endpoints is the token in the state file, which is the exposure the first risk above names.
- **The two platforms' guarantees diverge.** A reader who takes the config file's "one promise" line at its word today would be wrong after this change; D6 is the mitigation, and the review must confirm no document still implies parity.

- **The grant reaching the files is a change in what the confined program can reach, while the grant itself is unchanged.** A reviewer comparing exposure sets will see no difference; the difference is in the descriptors of the files inside the granted directory, which is why D7 keeps the grant legs and the acceptance measures an in-container read.

## Plan decomposition

One plan, six tasks, each shipping on its own.

1. Delete the ledger subsystem: module, inputs, hook, lock, legs, scenario, edges.
2. Delete the descriptor sites and their read-backs, and narrow the legs to D7's boundary.
3. Dissolve `tinio-winsec`: move the wide-string writer to `tinio-util`, open the `acl` directory with the facade and the walk, delete the rest.
4. Split the `acl` file into D9's modules with D10's test layout, under D11's acceptance.
5. Correct the prose, contracts, READMEs, manifests and lock, and remove the unused optional dependency.
6. Run the acceptance list, including the in-container read, and record the evidence.
