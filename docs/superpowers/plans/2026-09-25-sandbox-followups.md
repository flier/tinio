# Sandbox follow-ups (after the `feat/sandbox` merge)

**Status**: merge closed; this plan is not started · **Created**: 2026-09-25 · **Depends on**: the merged `feat/sandbox` tree (nothing here is needed for that merge to be correct)

The merge itself is closed by getting the tree to compile, pass its gates and be committable. Everything below is **separate, optional** work that the merge review, the two prior-art surveys (`docs/superpowers/sandbox-prior-art-research.md`) or the confined-run measurements turned up. Each item carries **Problem → Solution → Sites → Acceptance**, and the items that touch the same file are marked so they are done one at a time.

Ordering notes: **§1–§3 all edit `crates/tinio-sandbox-windows/src/acl.rs` and must be serial**; §5 needs a dev-dependency change and nothing else.

---

## 1. Read the DACL through Win32 APIs — **LANDED 2026-09-25**

- **Problem**: the skip that makes a standing grant free was silently dead. `icacls` lists a well-known SID **by name** (`APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES`), so a check for the literal `S-1-15-2-1` never fired and the program directory was rewritten every run — 10–30 s of `SetNamedSecurityInfoW` full-tree propagation each time. The same rendering appears as the SDDL alias `AC`, so text matching has three spellings to guess.
- **Solution (landed)**: `has_ace` → `acl_names` → `any_ace_in` reads `GetNamedSecurityInfoW` + `GetAclInformation`/`GetAce` + **`EqualSid`**. `already_granted` → `acl_grants` is the same walk plus the mask/flag test. SID text goes through `ConvertStringSidToSidW` once in an RAII `OwnedSid`; a `PackageSid` passes its own `raw()`. Mask constants are measured against the tool: `M = FILE_GENERIC_READ|WRITE|EXECUTE|DELETE` (`0x0013_01BF`), `RX = GENERIC_READ|GENERIC_EXECUTE` (`0x0012_00A9`), containment not equality, `INHERIT_ONLY_ACE` fails every target, `(OI)(CI)` required for `Target::Directory` only. `MapGenericMask` deliberately not called (user mode cannot store generic bits in an ACE).
- **Two findings**: (a) the API **cannot read the `*.dll` batch path** (`123 ERROR_INVALID_NAME`) → `already_granted` returns a 3-state `GrantOutcome { AlreadyGranted, Missing, Unanswerable }`, and a wildcard in the final component is `Unanswerable` ⇒ `grant_to` writes; the sibling skip still cannot fire with a pattern API, but that is ~8–16 ms/run over a few DLLs and the old text search never matched there either. (b) the well-known-SID read went from always-false to correct — one wasted re-grant measured **224 ms / 4 201 objects**.
- **Sites**: `crates/tinio-sandbox-windows/src/acl.rs`.
- **Acceptance (met)**: `tinio-sandbox-windows` suites green (lib 37 + 14 suites, 0 failed); clippy clean; `mentions_sid`/`sid_spellings` kept as the test oracle.
- **Still open from this item**: a one-line "reads are API, writes are `icacls`" note in `specs/2026-09-18-sandbox-design.md`, and the `MapGenericMask` precedent in `docs/superpowers/icacls-vs-win32-acl-research.md`.

## 2. Write the DACL through Win32 APIs — **OPEN** *(same file as §1 and §3)*

- **Problem**: every grant and revoke spawns `icacls` (≈7–10 ms of process launch plus SID name resolution per operation). With §1's read fixed, the spawn is the remaining per-operation cost, and `icacls`'s text output is what §1 had to work around.
- **Solution**: `GetNamedSecurityInfoW` → merge with `SetEntriesInAclW` → `SetNamedSecurityInfoW`. Do it **in two stages**: first the directory/file `grant`/`revoke` (the non-pattern shapes), then decide the batch. The `*.dll` pattern **cannot** go through the API (§1's finding) — either enumerate the directory's DLLs and grant each (a measurable change from one call to N) or keep `icacls` for that shape; the batch is ~8–16 ms/run, so keeping the subprocess is defensible and must be justified with a measurement rather than assumed.
- **Traps**: only the security descriptor may `LocalFree` (never the ACL pointer); a `NULL`-DACL directory can return as an empty deny-all DACL after a grant→revoke round trip; the merge must preserve other ACEs, the owner, the SACL and the control bits exactly as `/grant:r` does (decision 15).
- **Sites**: `crates/tinio-sandbox-windows/src/acl.rs` internals only (public surface unchanged: `grant`, `revoke`, `has_ace`, `Grants`).
- **Acceptance**: the four operations behave identically to the `icacls` implementation on the existing suites; `tests/acl.rs`'s skip/standing assertions keep their meaning; per-operation timing reported against the `icacls` baseline.

## 3. One lock per path around read-merge-write — **OPEN** *(same file as §1 and §2)*

- **Problem**: grants and revokes are paired against the same directory, so two confined runs sharing a root can lose or resurrect each other's ACE (`/grant:r` and `/remove:g` are separate operations).
- **Solution**: a per-path advisory lock around the whole read-merge-write, following DSH: lock file under the system temp dir named `tinio-acl-locks\<sha256(lowercase path)[:16]>.lock`, opened without sharing delete and held with `LockFileEx` (exclusive) for the duration. Take it in `Grants::apply`/`apply_standing`/`revoke` rather than in `icacls`-level helpers, so the write path (§2) inherits it for free.
- **Cost**: one file create plus a lock per operation; the lock files themselves must be documented as intentionally left behind (DSH's choice) or reaped on a best-effort basis.
- **Sites**: `crates/tinio-sandbox-windows/src/acl.rs` (`grant`, `revoke`, `Grants::apply`, `Grants::apply_standing`) and the grant stage in `backend/mod.rs`.
- **Acceptance**: a concurrent test grants two SIDs to one directory from two threads and asserts both ACEs survive; `tinio-sandbox-windows` suites unchanged otherwise.

## 4. Report enforcement and denial as data, not only as log lines — **OPEN**

- **Problem**: a caller cannot tell "the sandbox broke (the command never ran)" from "the command was denied (it ran and was refused)"; today both reach the caller as an `Error`/log line.
- **Design (from the 2026-09-25 read-only pass, no code written)**: new `crates/tinio-sandbox-core/src/enforcement.rs` holding `Enforcement { Full, Partial, Unimplemented, NotReported }`, `RunnerFailureRule { allowed_exit_codes, fatal_signatures, informational_lines }`, `Diagnostics { enforcement, denial_signatures, runner_failure_rules }` with `NOT_REPORTED` and `attribute(platform, status, stderr)`, plus `Refused { Path, Capability, Unnamed }`, `Denial { platform, refused, evidence }` and `Attribution { Broken { platform, detail }, Denied(Denial) }`.
- **Solution, in four steps**: (1) land the core types and re-export them through the facade (`crates/tinio-sandbox/src/lib.rs`) — the CLI only depends on the facade, so without the re-export it cannot see them; (2) give `Backend` a defaulted `fn diagnostics(&self) -> Diagnostics { Diagnostics::NOT_REPORTED }` so all three platforms keep compiling; (3) add the two CLI `Error` variants (`SandboxBroken`, `SandboxDenied`) while **keeping the existing refusal sentences verbatim** (the `serve_sandbox_channels` legs and `cli_lifecycle.feature` assert their wording), and expose `sandbox::diagnostics()` / `sandbox::attribute(status, stderr)` as the only entry points; (4) map the Windows backend's refusals and hand the child's status+stderr to `attribute` at the supervise site.
- **Sites**: new `crates/tinio-sandbox-core/src/enforcement.rs` plus the facade re-export in `crates/tinio-sandbox/src/lib.rs`; `Backend` in `crates/tinio-sandbox-core/src/backend.rs`; `crates/tinio-cli/src/error.rs`, `crates/tinio-cli/src/sandbox/mod.rs`, `crates/tinio-cli/src/commands/start/run.rs`; the refusal mapping in `crates/tinio-sandbox-windows/src/backend/mod.rs`.
- **Wiring caveat**: "which path was refused" lives in `crates/tinio-cli/src/commands/start/run.rs`, where `?` on `create_dir_all`/`FsStorage::new`/`bind` currently turns an io error into a path-less `Error::Io`; the classifier has to be called there, gated on `adopted.is_some()`.
- **Unverified**: the real Windows denial text, the io kind of an AppContainer refusal, and whether the launcher captures (rather than merely inherits) the child's stderr.
- **Acceptance**: three platforms can express it (the macOS stub says "unsupported", not "full"); a denial names the channel or path; fail-closed strength unchanged; nothing may run unconfined.

## 5. Pipe-pool extremes — **PARTLY LANDED; one test gap**

- **Problem**: (a) concurrent dials beyond the pool return `ERROR_PIPE_BUSY`, which used to be indistinguishable from a dead channel; (b) a client that never closes holds an instance under the drain; (c) whether `INSTANCES` should be configurable.
- **Solution (landed)**: (a) bounded retry (`BUSY_RETRIES = 5`, `BUSY_BACKOFF = 1 ms`, ~31 ms budget) and a spent busy dial now returns **`Error::Busy`** instead of a raw `io::Error`, so `stop` can tell "there is a server and it is full" from "nothing there" while `status` keeps reporting `running: false` with the state file intact (`contracts/cli.md:58`); (b) `wait_for_peer_close` is bounded by **`DRAIN_LIMIT = 250 ms`** and a forced reclaim emits `tracing::warn!(%identity, limit, waited, …)` — justified by measurement, not taste: across 201 sequential dials the drain ends in **80–181 µs** (p50 97 µs, p99 177 µs), i.e. the bound sits ~1 400× above the mean, and nothing is discarded at it (tokio's pipe write is `WriteFile` + `FlushFileBuffers`, which blocks until the client consumes, and `keep_alive(false)` means nothing follows the response); (c) `INSTANCES` **stays a constant** — configurability would create a second source of truth across the launcher handoff plus a matching `Plane::adopt` floor, for zero measured benefit (0 retries over 200 sequential dials).
- **Remaining gap (measured, not suspected)**: neither §5 scenario can be turned into a passing test in-tree, because `#[tokio::test]` runs **current-thread** here and (i) `ClientOptions::open` on an instance that has not reached `ConnectNamedPipe` **blocks the only thread** (spawning the dial does not help — the task inherits the thread), while (ii) a held `SendRequest` does **not** keep a pipe open (measured: the server released the instance ~100 µs after the exchange, because the handle lives in hyper's connection future).
- **Solution for the gap**: add `rt-multi-thread` to `tinio-api`'s dev-dependencies and write the two tests as `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`; the implementation needs no change for that. Keep the `Error::Busy` mapping pinned by the `error.rs` test that already exists.
- **Sites**: `crates/tinio-api/**` (`client.rs`, `error.rs`, `transport.rs`, `Cargo.toml`).
- **Acceptance**: the two scenarios become passing tests; `tinio-api` suites green; one real confined run (`status` ×2 then `stop`) with no forced-reclaim warning in the log.

## 6. Linux seccomp coverage review — **PARTLY LANDED**

- **Problem**: the filter blocked outbound `connect`/`sendto`/`sendmsg`/`sendmmsg` only; Codex blocks `io_uring_*` unconditionally because a ring can carry traffic without `socket()`.
- **Solution (landed)**: `io_uring_setup`/`enter`/`register` (425/426/427) are denied; `filter_program` is 19 instructions. The bypass was **measured**: before the change `io_uring_setup` returned a descriptor inside the confinement and an `IORING_OP_SENDMSG` SQE delivered a datagram while `sendto` was `EPERM`. `enter`/`register` are denied separately because tinio confines **in place**, so a ring opened before the filter lands would stay usable through `enter`.
- **Solution for what remains**: (a) `ptrace`/`process_vm_readv`/`process_vm_writev` — deny all three (Codex's companion to the user namespace: inject into an unfiltered same-uid process, or follow `/proc/<pid>/root` links across the mount view); the cost is tooling-visible (gdb/perf/strace), so record it and test it; (b) the **x32 ABI** gap — add one rule to the same program: `JSET 0x4000_0000` on the syscall number ⇒ `RET_ERRNO|EPERM` (the current program only compares `nr`, so this is the one addition that fits its shape); (c) keep allowed, deliberately: `recvfrom`/`recvmsg`/`recvmmsg`, `socketpair` (receiving cannot exfiltrate; denying breaks socketpair-based tooling), `sendfile`/`splice` (they copy into an already-connected fd, which no syscall filter can unmake); (d) `connect(AF_UNIX)` stays denied (the filter cannot see the family) and `socketpair` keeps local IPC working.
- **Documentation owed**: `docs/sandbox.md:11` and `plans/2026-09-18-sandbox-p1-linux.md:5247` list the deny set without `io_uring` — still true, no longer complete (see §18).
- **Sites**: `crates/tinio-sandbox-linux/src/seccomp.rs`.
- **Acceptance**: each item is either a rule with its reason in a comment or a documented "not needed"; a test or a measured before/after per addition; the "seccomp outbound denial ≠ netns isolation — loopback and inherited sockets still work" distinction recorded.

## 7. End-to-end coverage of the Linux overlay branch — **OPEN**

- **Problem**: after the remount fix re-asserts the source mount's `nosuid`/`nodev`/`noexec`, every leg takes the remount arm on this kernel, so the overlay arm is no longer exercised end to end (`read-only-arm: remount`, measured).
- **Solution**: reach `EPERM` through a **locked per-mount flag the request does not name** — `nosymfollow` on this kernel (the host's `/run/credentials/*` mounts carry it; `rustix::mount::MountFlags::NOSYMFOLLOW` exists). Constructing that state needs a **nested** user namespace: a wrapper creates the `nosymfollow` tmpfs in userns A, then execs `sandbox-child` so its own `unshare(CLONE_NEWUSER)` becomes B. Put that test behind an explicit opt-in variable (e.g. `TINIO_SANDBOX_TEST_NOSYMFOLLOW=1`) rather than the default gate, because it needs `mount -o nosymfollow` support and would otherwise redden hosts without it.
- **Sites**: `crates/tinio-sandbox-linux/tests/jail.rs` and the "one of the two" assertion.
- **Acceptance**: the arm stays; the gap is documented where the test asserts "one of the two"; the nested-userns test passes under the opt-in variable on this host.

## 8. Windows network isolation — **VERIFIED 2026-09-25, no work left**

- **Problem**: Linux confines outbound traffic with seccomp, while the Windows side had never been checked. An AppContainer *should* be unable to reach the network without a network capability — but "should" is not a measurement.
- **Solution (verified; nothing to implement)**: the capability vocabulary is closed and all three variants are network capabilities (`backend/capability.rs:37-45`; `S-1-15-3-1/2/3`), `Options` derives `Default` over an empty `Vec` (`backend/mod.rs:85-88`), the only operator channel is `TINIO_SANDBOX_CAPABILITIES` (`crates/tinio-cli/src/sandbox/windows.rs:152-154`; no config-file key exists), and an empty set reaches the kernel as a **null pointer with `CapabilityCount: 0`** (`process.rs:487-512`). WFP enforces the consequence against the token's capability SIDs, so tinio writes no filter, firewall rule or network namespace of its own.
- **Sites**: the threat model in `specs/2026-09-18-sandbox-design.md` §7, where the measurement is recorded.
- **Acceptance (met)**: unconfined the host reaches `1.1.1.1:53`; under `Options::default()` the child's outbound attempt fails with **`WSAEACCES` (10013)**, while the `internetClient` positive control succeeds on the same address with the same binary. Also recorded there: a confined child reaching the **unconfined launcher's** loopback listener is **silently dropped** (`TimedOut`, no raw code) with and without `internetClient` — AppContainer's loopback policy, not the capability filters.
- **Still unmeasured**: a LAN host reaching the inherited listener; non-53 external ports; the container's own loopback.

## 9. `journey.feature`: an observation point for the second server — **LANDED 2026-09-25**

- **Problem**: "an ephemeral `--port 0` start serves alongside the first server" started a second server and never talked to it; only `port() != 0` observed it — which still passed when the second process was already dead.
- **Solution (landed)**: `External` gained a `target_url` (what `aws_cmd` passes as `--endpoint-url`), a new step `the client targets the {word} server` switches it, and the scenario builds a bucket **on the second server**, sees it in the second server's own `s3 ls`, switches back and asserts the **first** server's listing does **not** contain it, then exercises the first server again.
- **Landing note**: the step must be its own sentence, not `I run aws against the second server …` — the existing `regex = r"I run aws (.*)"` is unanchored, so a new step starting with those words collides and cucumber-rs 0.23 raises `AmbiguousMatchError`.
- **Sites**: `crates/tinio-e2e/tests/features/interop/journey.feature`, `crates/tinio-e2e/tests/steps/clients.rs`, `docs/tests.md`.
- **Acceptance (met)**: the scenario runs 13 steps, all passing; `@cli and not @unix and not @confinement` 13/13; traceability 6/6.

## 10. Standing-grant hardening — **OPEN**

- **Problem**: `package_name` is a pure function of the canonical root, so any local user can compute the SID and create the same AppContainer profile; the grant is also **permanent** for every root ever served (standing, never revoked), and the program directory's `ALL APPLICATION PACKAGES` grant is readable by *any* AppContainer app on the machine.
- **Solution, in three layers** (any subset is useful): (a) **threat-model text** in `specs/2026-09-18-sandbox-design.md` stating both exposures plainly, so a reader does not discover them from the code; (b) **an optional persisted salt** mixed into `package_name`'s derivation (a random value kept in the state directory, one per user/machine) — this is the only thing in this plan that would justify a state file, and it invalidates existing grants once; (c) **an ACE ledger** recording which roots have been granted, so an operator can audit and revoke (Codex's `deny_read_state` structure is the reference), with the concurrency rule that a SID belonging to a run started after the ledger was read must not be removed.
- **Sites**: `crates/tinio-sandbox-windows/src/sid.rs` (salt), the design spec (threat model), and a new ledger module if (c) is taken.
- **Acceptance**: (a) done as text; (b) and (c) only if the exposure is judged worth closing, each with its migration cost stated.

## 11. Platform scripts as real gates — **OPEN**

- **Problem**: `@confinement` is host-gated (no CI leg runs it) and the two platform scripts carry the opt-in command as documentation only, so the strongest evidence the feature works is produced by hand.
- **Solution**: make the command a **step the script asserts** in `crates/tinio-sandbox-windows/scripts/verify-windows.ps1` and `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` (`TINIO_E2E_CONFINEMENT=1 … --tags '@cli and @FR-036' --concurrency 1`, with the exit status checked like the script's other steps). Leave CI out of it unless a runner with the prerequisites exists (console on Windows, unprivileged userns on Linux) — a permanently red leg is worse than an honest operator gate, which is why `.config/nextest.toml`'s `[profile.ci] default-filter` excludes the host-dependent suites.
- **Sites**: `crates/tinio-sandbox-windows/scripts/verify-windows.ps1`, `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`, and the gate list in `docs/tests.md`.
- **Note**: `wsl-chroot.sh` must stay **LF** — `.gitattributes` now pins `*.sh text eol=lf`, because a CRLF checkout made `bash -n` fail on Linux.
- **Acceptance**: on a host that can confine, the pair runs and its failure reddens the script.

## 12. macOS backend (P3) — **OPEN**

- **Problem**: `BACKEND_IS_STUB = true`; the platform plan is written and nothing landed, so `--sandbox` refuses on macOS.
- **Solution**: follow `plans/2026-09-18-sandbox-p3-macos.md`, using the skeleton in `sandbox-prior-art-research.md` §8 item 12: Codex's SBPL section order (which *is* the security semantics), `-p` + `-DKEY=path` parameterisation, the access-policy builder with `require-not` literal+subpath pairs, the writable-root anchor `deny file-write-unlink`, the glob→regex translator (a self-contained Rust function, portable nearly verbatim) with its ancestor denies, the network template, the unix-socket allowlist, the fcntl 80/110 closure, and the hard-coded `/usr/bin/sandbox-exec`.
- **Sites**: `crates/tinio-sandbox-macos/**` (the backend itself), `crates/tinio-sandbox-windows/src/backend/**` as the shape to mirror, and `plans/2026-09-18-sandbox-p3-macos.md` as the plan of record.
- **Traps**: `sandbox-exec` is deprecated and its policy engine is private — build it as a rung that can fail (probe with `sandbox-exec -p "(version 1)(allow default)" /usr/bin/true` and treat `sandbox_apply: Operation not permitted` as "skip, nested Seatbelt unavailable"), never resolve the binary through `PATH`, never inline a path into the policy text without `-D`, and decide the `ARG_MAX` fallback for a large policy (neither surveyed project guards it).
- **Acceptance**: P3's own step boxes, plus the probe-and-skip test pattern.

## 13. Test and script hygiene: server processes leak — **OPEN**

- **Problem**: one finished run left **19** `tinio server <temp root> --port 0` processes alive after their parent (PID 2588) had already died (a second batch had parent 58000). They held the image open, so `cargo build -p tinio` failed at the uplift step (`failed to remove ... target\debug\tinio.exe`: access denied) and every later measurement ran against a **stale binary**; several checks in that session briefly answered from the wrong artifact until a hash/marker comparison caught it.
- **Solution**: (a) find the spawner — the e2e harness, the cucumber steps or a measurement script — by having each spawn point record its child PIDs and reap them in a `Drop`/teardown (Windows: put the child in a job object so the tree dies with the test process); (b) add a suite-level assertion that `Get-Process tinio` is empty after the run, so a leak fails the suite instead of the next build; (c) any measurement that depends on a binary verifies the artifact first (mtime or a marker literal) — the practice that caught this.
- **Sites**: `crates/tinio-server/tests/support/windows_e2e_harness.rs`, `crates/tinio-e2e/tests/**`, the platform scripts.
- **Acceptance**: a suite that spawns servers leaves none behind, asserted by the suite; a stale binary cannot silently satisfy a measurement.

## 14. `capability.rs` documents a configuration key that does not exist — **OPEN**

- **Problem**: the module doc (`crates/tinio-sandbox-windows/src/backend/capability.rs:1-13`, and `:70`) says a capability name can be written "in the configuration file", but `tinio-config` has no such key.
- **Solution**: rewrite the doc to name the two channels that exist — `TINIO_SANDBOX_CAPABILITIES` (`crates/tinio-cli/src/sandbox/windows.rs:152-154`) and the library caller's `Options`. Adding a real config key is the alternative and is a **schema decision**, not a doc fix, so it belongs to whoever wants the key.
- **Sites**: that module doc, plus any CLI-side doc repeating the claim.
- **Acceptance**: the doc names only channels that exist.

## 15. `advanced.feature`'s 10 MiB multipart leg overflows the stack — **OPEN**

- **Problem**: observed while running `@interop`: the 10 MiB multipart leg aborts the test process with `STATUS_STACK_OVERFLOW` (`exit 0xc00000fd`) **on the main thread**, reproduced twice. That scenario never calls the journey step that changed the same day, and `aws_cmd` passes an identical URL when no target switch has happened, so it is unrelated to §9.
- **Solution**: locate the frame first (run the leg under `RUST_MIN_STACK` raised, which distinguishes "the test thread's stack is too small" from a genuinely unbounded recursion), then either drive that scenario on a worker thread with a larger stack or reduce what the leg holds on the stack. Record the measurement either way.
- **Sites**: `crates/tinio-e2e/tests/features/interop/advanced.feature` and whatever drives it.
- **Acceptance**: the leg passes on this host, or the stack requirement is raised explicitly with the measurement recorded.

## 16. `docs/tests.md` still states the old rollback contract — **OPEN**

- **Problem**: `docs/tests.md:166` says the `(OI)(CI)` ACE "is granted before `CreateProcessW` and **rolled back after the child exits**". Every grant this crate makes is standing, so the rollback half no longer exists, and a reader would expect a behaviour the tree does not have.
- **Solution**: rewrite that clause to "granted once and kept (standing); the launcher does not roll it back", keeping the measured grant-cost numbers that follow it (they are still the reason the readiness budget is what it is).
- **Sites**: `docs/tests.md:166` (the **Grant cost** bullet).
- **Acceptance**: no doc sentence claims an ACE rollback on a clean exit.

## 17. `docs/tests.md`'s ACE-latency causal paragraph is overtaken — **OPEN**

- **Problem**: `docs/tests.md:168` lists "the host's security/filesystem filter" as the **first candidate** for the 9–30 s descriptor write, on the evidence that new objects are unaffected. The later measurement explains the same facts by **eager full-tree propagation**: 82,184 objects × **0.136 ms/object** (and a directory created *inside* `target\debug` costs 0.06 s precisely because it has no descendants). The data and the "do not implement stale-ACE cleanup" ruling stay valid; only the causal paragraph is wrong.
- **Solution**: replace the candidate list with the propagation explanation, citing `specs/2026-09-18-sandbox-design.md:422`'s per-object rate and the standing-skip measurement (11 199 ms → 0 ms on the same root), and keep the eliminated-controls table as its evidence.
- **Sites**: `docs/tests.md:168-169`.
- **Acceptance**: the paragraph names propagation as the cause; no doc sentence lists a host filter as the leading explanation.

## 18. The seccomp deny list in two docs omits `io_uring` — **OPEN**

- **Problem**: `docs/sandbox.md:11` and `plans/2026-09-18-sandbox-p1-linux.md:5247` describe the filter as denying "`connect`, `sendto`, `sendmsg` and `sendmmsg` by syscall number". Still true, but since 2026-09-25 the set also carries the three `io_uring` ring calls, so a reader would underestimate what is denied.
- **Solution**: append "plus the `io_uring` ring calls (425/426/427), added 2026-09-25 because a ring can carry traffic past the filtered calls" to both. The historical fenced snippets in the p1 plan stay as written (they are that phase's record, and its prose already points at the landed shape).
- **Sites**: `docs/sandbox.md:11`, `plans/2026-09-18-sandbox-p1-linux.md:5247`.
- **Acceptance**: both lists name the ring calls.

## 19. The AppContainer profile is recreated every run, and its directory's DACL was never assessed — **OPEN**

- **Problem**: every run reports `taken_over=false`, because a clean `stop` deletes the profile, so each run creates its own directory under `%LOCALAPPDATA%\Packages`. That directory's DACL has never been measured or reasoned about: it sits outside the served root and the program directory, so no grant-side measurement covers it, and it is a per-run filesystem object created by the mechanism on the operator's machine.
- **Solution**: measure it once (create a profile, read the directory's DACL with the same API reader as §1, compare against a second run's directory) and then either record "nothing to do" in the design spec's residue section or add a cleanup step next to `PackageSid`'s `Drop` if the directory accumulates.
- **Sites**: `crates/tinio-sandbox-windows/src/sid.rs` (if cleanup is needed), `specs/2026-09-18-sandbox-design.md` (the residue section).
- **Acceptance**: the directory's DACL is measured and the outcome recorded as fact, with a cleanup only if the measurement asks for one.

## 20. `crates/tinio-api/Cargo.toml` is staged with an unused dependency — **OPEN, needs an index operation**

- **Problem**: the index carries `bytes.workspace = true` for `tinio-api` while the working tree does not, and nothing in the crate names `bytes::`. The file therefore reads `MM`, and **committing as staged would carry an unused dependency**.
- **Solution**: `git add crates/tinio-api/Cargo.toml` (staging the working-tree version replaces the index's extra line). This is an **index operation**, which no agent in this session may perform — the operator does it.
- **Sites**: `crates/tinio-api/Cargo.toml`.
- **Acceptance**: `git diff --cached` shows no `bytes` line for `tinio-api`, and `cargo metadata --frozen` stays at exit 0.

---

## Verified 2026-09-25: the standing skip works end to end

Same root, second confined run: **both** the program directory and the served root report `written=false`, the ACL phase measures **0 ms**, and readiness falls from **11287.9 ms to 65.2 ms** (~170×). Cross-checked without trusting the program's own `written` field — an NTFS `ChangeTime` probe (validated first: a descriptor write changes it, a read does not) shows the program directory and the root unchanged after the second run, and the ACE counts hold at 25/11. Reproduced on three binary revisions.

The first run's 11.19 s is **99.1%** one `SetNamedSecurityInfoW` on the program directory, whose subtree is 82,184 objects (76,034 files + 6,150 directories) — **0.136 ms/object**, the same order as the 0.16 ms/object the design notes carry. Because `target\debug` had never carried `S-1-15-2-1` on this host (its 13 residue ACEs are all per-root package SIDs), the first run must write it — the expectation that even the first run would skip is **disproved**. Afterwards a new root never rewrites the program directory (`written=false`) and costs 8–11 ms for its own grant.

Two DACL-touching operations still happen every run and are **not** on an exposure path, so neither counts as a grant: the launcher's own `state` file, and `CreateAppContainerProfile` (see §19).

## What this plan deliberately leaves alone

- **The merge's own decisions** (16–22) and the A/B/C/`--config` fixes: landed and verified, not reopened here.
- **The residue semantics**: standing ACEs are the design, not a defect; §10 is about auditing them, not reverting them.
- **The `examples/serve` mentions** (47 across `docs/superpowers/**`) and the fenced historical snippets in the plans: history, kept as written.
