# Sandbox prior art: Codex and DeepSeek Harness

**Date**: 2026-09-25 · **Scope**: `E:\GitHub\codex` (`codex-rs`) and `E:\GitHub\deepseek-harness` (TS monorepo) — both **read-only sources**, neither a dependency · **Nature**: research, no code changed here

> **Line-number currency**: every `path:line` into `E:\GitHub\codex` or `E:\GitHub\deepseek-harness` below is from those checkouts on 2026-09-25 — evidence for the decisions quoted, which rots independently of `tinio`. The few `path:line` pointing into **this** repo (`crates/tinio-sandbox-windows/src/**`) were read against this tree on 2026-09-27 and move with every edit to that file — one of them is already stale, which the note below records, so follow the symbol rather than the line. The design is cited by **section**, not by line. The one cost figure this document leans on is owned by `specs/2026-09-18-sandbox-design.md`'s §6 inheritance-and-cost paragraph; this document refers to it and does not restate it.

> **Note, 2026-09-27 — the `tinio today` column moved after this survey, and four of its cells and one of its citations describe the pre-change state.** The Windows DACL **read** moved onto the API (2026-09-25: `acl_names` → `any_ace_in`, `GetNamedSecurityInfoW` + `GetAce` + `EqualSid`), so §8's first transferable decision and its `acl.rs:333 has_ace` citation are wrong twice over: the line number sits inside `grant_to` today rather than beside the `has_ace` the citation names, and the **role** moved — what the launcher decides with is `apply_standing`/`apply` → `grant_to` → `already_granted` → `acl_grants` → `any_ace_in` (`GetNamedSecurityInfoW` and `EqualSid`), while `has_ace` remains, unchanged in signature and still the oracle its suites call (three of the crate's test files — `acl.rs`, `confine.rs` and `profile.rs` — plus its own doctest), reaching the same walk through `acl_names` — its own doc frames it as "**The oracle is the kernel's DACL and not the tool's rendering of it**". The launcher also no longer **revokes** anything, because every grant is standing (design decision 15, amended): the Windows table in §7 has three rollback-era cells — `Authorisation` ("`/remove:g` on exit"), `Cleanup` ("per-run rollback") and `Windows cost` ("every run rewrites each exposure's DACL") — §8's fifth decision states the same reading in prose ("`tinio` couples `/grant:r` with `/remove:g` on the same directory"), and the macOS table's `Mechanism` row still reads `BACKEND_IS_STUB = true`, `confine` returns `Error::Unsupported`, where `tinio_sandbox::backend_available()` answers `true` on all three targets since the macOS backend landed (that answer's home is the facade, `crates/tinio-sandbox/src/lib.rs`; the `BACKEND_IS_STUB` constant this note first named is deleted). **What this survey recommended has since landed**, which is why the comparison it drew has moved: its first three transferable decisions are decision 22's one-off program-directory grant and the launcher's exact-ACE skip, and the skip is what makes the standing form affordable (decision 15 carries the headline ratio and points at the measurement).

**Conclusion: borrow the *shape*, not the code — and expect neither project to validate tinio's platform choices.** Neither uses AppContainer (Codex has exactly one hit, an exclusion note; DSH has none). On Linux, neither has an in-process `unshare` + `pivot_root` launcher: Codex shells out to `bwrap` and demoted Landlock to legacy, DSH shells out to `bwrap` or a bundled Landlock launcher, and **neither restricts Linux network at all except through namespaces** (DSH has no seccomp anywhere). On macOS both use the deprecated `sandbox-exec`; Codex's SBPL pipeline is the only thing worth copying as a skeleton, and DSH's is a write-only minimum.

Three decisions transfer on Windows: **write a DACL only when the mask is missing** (6), **authorise once per root and never revoke** (2), and **the program directory as a one-off global grant** (3). Two transfer on Linux: **deny `io_uring_*`** (7) and **materialise a missing protected path instead of letting the child create it** (9). Nothing here replaces tinio's read-side isolation or its zero-external-binary Linux route.

---

## 1. Why this survey was run

`tinio` measured a confined launch spending **9–30 s** writing one DACL on the build directory, against 0.003 s for the same write on a fresh directory. Section 12 records what the cause turned out to be; the survey's job was to find whether either project had already solved it, and at what price.

## 2. Codex — Windows

Four mutually exclusive backends, dispatched at `codex-rs/sandboxing/src/manager.rs:376-526` and again at `codex-rs/core/src/exec.rs:692-728`:

| Backend | Selection | Mechanism |
|---|---|---|
| Windows, unelevated | `WindowsSandboxLevel::RestrictedToken` | `CreateRestrictedToken(WRITE_RESTRICTED)` + `CreateProcessAsUserW`. No identity, no ACL writes |
| Windows, elevated | `WindowsSandboxLevel::Elevated` | resident local group + two resident accounts + capability SIDs + persistent ACLs + firewall/WFP |
| Windows, MXC | `windows.sandbox="mxc"` | Microsoft MXC `BaseContainerRunner`; its AppContainer fallback is deliberately excluded (`mxc-sandbox/src/lib.rs:90`) |
| Linux / macOS | — | bwrap + seccomp / `sandbox-exec` seatbelt |

- **No AppContainer anywhere.** `AppContainer` has one hit in the whole tree and it is that exclusion note.
- **`CodexSandboxUsers` is real** — `windows-sandbox-rs/src/winutil.rs:29`, created or reused with `NetLocalGroupAdd` (tolerating `ERROR_ALIAS_EXISTS`/`NERR_GROUP_EXISTS`, `winutil.rs:223-249`). Members are two resident accounts, `CodexSandboxOffline`/`CodexSandboxOnline` (`setup.rs:53-54`), with 24-char random passwords DPAPI-sealed to `codex_home/.sandbox-secrets/sandbox_users.json` (`sandbox_users.rs:275-287`, `:314-378`).
- **Production never shells out to `icacls`** — it uses `SetEntriesInAclW` / `SetNamedSecurityInfoW` (`acl.rs:582-597`, `:1010-1019`); `icacls` appears only in tests.
- **Mask short-circuit, and standing ACEs.** Refresh runs per spawn but writes only when the target SID lacks the needed mask: `acl.rs:507-535` (`dacl_allow_mask_needs_refresh`), skipped at `acl.rs:563-565`; read-only roots first check the built-in principals and `continue` when they already suffice (`setup_provisioning.rs:247-270`). Write and read ACEs are **never revoked** on exit — stated as intentional, because descendants may outlive the launcher (`deny_read_state.rs:22-28`).
- **Capability SIDs are random but persisted** — `cap.rs:39-46` generates `S-1-5-21-…`, `cap.rs:35-37` stores them in `codex_home/cap_sid` keyed by canonical cwd/root (`cap.rs:89-114`). Same directory, same SID, across runs.
- **`TokenDefaultDacl`** is set on the restricted token (`token.rs:56-107`) precisely so sandboxed processes can create pipes and other IPC objects — the failure mode tinio's confined child hits with `CreateNamedPipeW`.
- Restricted token flags and restricting-SID order: `token.rs:463-521` (`WRITE_RESTRICTED|DISABLE_MAX_PRIVILEGE|LUA_TOKEN` at `:495`; order at `:476-492`). Process creation with job object and an explicit handle list: `process.rs:92-191` (`:118`, `:147`).
- **Network is layered off the ACL**: Windows Firewall COM rules per sandbox account SID (`setup_provisioning/firewall.rs:35-47`, `:96`, `:184`) plus persistent WFP filters (`wfp.rs:84-100`, `wfp/filter_specs.rs:25-124`); loopback is blocked except for the proxy ports (`firewall.rs:117-177`).
- **Fail-closed with reasons in the error**: seven "refusing to run unsandboxed" arms in `sandboxing/src/windows.rs`; the escape hatch is the explicit `danger-full-access` profile or `PermissionProfile::External`.
- **Price**: the Windows side is ~15k lines plus a service crate, and the elevated backend needs a one-time UAC install and a full uninstaller (`uninstall_windows.rs:92-176`).

## 3. Codex — Linux

Three stages: the host writes a `codex-linux-sandbox` helper invocation (`sandboxing/src/landlock.rs:26-67`), the helper execs `bwrap` (`linux-sandbox/src/linux_run_main.rs:289-341`), and bwrap re-execs **the same exe** as an inner stage that applies seccomp (`linux_run_main.rs:120-126`, `:191-274`). Filesystem is bwrap's job, syscalls are seccomp's.

- **Why two stages at all**: `bwrap` deployments may rely on setuid, while seccomp requires `no_new_privs`, and the two cannot hold in one process — so bwrap runs first and seccomp is applied after the filesystem view exists (`linux_run_main.rs:120-126`; `no_new_privs` comment at `linux-sandbox/src/landlock.rs:66-69`).
- **bwrap argv** (`bwrap.rs:321-402`): `--new-session --die-with-parent`, the filesystem section, conditional `--tmpfs /run/WSL`, always `--unshare-user` (never left to bwrap's auto-detect, which skips it for uid 0), `--unshare-pid`, `--unshare-ipc`, conditional `--unshare-net`, `--proc /proc`, conditional `--chdir`, `--cap-drop ALL`, `--`. There is no `--cap-add`.
- **Filesystem**: full read → `--ro-bind / / --dev /dev` (`bwrap.rs:506-518`); restricted read → `--tmpfs /` then one `--ro-bind` per readable root (`:519-575`) with platform defaults (`:50-59`); writable roots filter out non-existent paths, sort by depth, and `--bind` (`:436-440`, `:599`, `:636-640`).
- **Missing protected paths are materialised, never merely denied**: a missing directory becomes `--perms 555 --tmpfs <p> --remount-ro <p>` (`:1198-1205`), a missing file becomes **`--ro-bind-data <fd-of-/dev/null>`** (`:1187-1196`). Only the **first missing component** is materialised (`:1413-1438`), which is what stops a child from creating the whole protected hierarchy. Existing deny targets use `--perms 000 --tmpfs --remount-ro`, relaxed to `--perms 111` (execute-only) when an explicitly writable descendant must remain reachable (`:1297-1319`).
- **Cleanup**: synthetic mount targets are removed after bwrap exits, identified by **(dev, ino)** so a pre-existing empty file is never deleted, serialised by `flock` over a per-euid registry, with per-pid liveness markers (`linux_run_main.rs:1055-1139`, `:1302-1339`; `bwrap.rs:220-238`). A separate inotify monitor turns "the child created a protected path" into a non-zero exit even when the command succeeded (`linux_run_main.rs:1230-1233`, `:1399-1408`).
- **Unreadable globs are expanded with ripgrep** into concrete paths + canonical symlink targets, capped at 8192 matches, falling back to an internal `globset` walker when `rg` is absent (`bwrap.rs:805-1044`).
- **seccomp** (`linux-sandbox/src/landlock.rs:179-300`): default action `Allow`, hits return **`EPERM`** (not `SIGSYS`); three modes — `Restricted`, `ProxyRouted`, `VmSocketRestricted`.

| | Restricted | ProxyRouted | VmSocketRestricted |
|---|---|---|---|
| `connect`/`bind`/`listen`/`sendto`/`sendmmsg`/… | blocked | allowed | allowed |
| `socket`/`socketpair` | only `AF_UNIX` | `socket` only `AF_INET`/`AF_INET6` | block only `AF_VSOCK` |
| `io_uring_setup`/`enter`/`register` | **always blocked** | always blocked | always blocked |
| `ptrace` / `process_vm_*` | blocked | blocked | not blocked |
| `recvfrom` | **deliberately allowed** so socketpair-style tools (`cargo clippy`) work | allowed | allowed |

- **Landlock is demoted to legacy** — module header says filesystem restrictions are bwrap's job and the helpers are "legacy/backup utilities" (`linux-sandbox/src/landlock.rs:1-4`, `:145-146`); the stated reason is that Landlock **cannot isolate app-server Unix sockets** (`linux-sandbox/README.md:40-42`), enforced by `ensure_legacy_landlock_mode_supports_policy` (`linux_run_main.rs:405-412`). What remains only supports full-disk read (`landlock.rs:81-86`).
- **Network**: `--unshare-net` is added unless the mode is `FullAccess` (`linux_run_main.rs:446-457`; `bwrap.rs:114-118`), so `Isolated` and `ProxyOnly` both get it; in `ProxyOnly` the helper then builds a TCP→UDS→TCP bridge inside the netns and seccomp opens `AF_INET`/`AF_INET6` (`proxy_routing.rs:131-240`, `:355-390`). DNS goes through the bridge; there is no `*:53` rule on Linux.
- **Full-disk write + full network + no unreadable glob → the command runs with no bwrap at all** (`bwrap.rs:248-279`). There is **no** fallback from bwrap to Landlock (`linux_run_main.rs:289-292`), and a missing bwrap of both kinds panics (`launcher.rs:50-56`).

## 4. Codex — macOS

- Baseline is `(deny default)` plus a 116-line allowlist (`seatbelt_base_policy.sbpl:8-116`): process exec/fork/signal, sysctl reads, `RootDomainUserClient`, `com.apple.system.opendirectoryd.libinfo`, POSIX sem/shm, PTYs.
- **Section order is the security semantics** (`seatbelt.rs:1048-1090`): base, read policy, write policy, network policy, then platform defaults, daemon protection, **`(deny mach-lookup (xpc-service-name-prefix ""))`** (`:1070`), glob denies, and ancestor unlink denies **last** — "so no broader allowance can reopen the unlink operation used by rename" (`:1072-1074`).
- **Delivery**: `-p <full policy text>` plus `-DKEY=path` parameters and `--` (`seatbelt.rs:1100-1108`). Paths go through `-D`/`(param "…")` so quotes or newlines in a path cannot break the policy text. Nothing is written to a temp profile file. Neither repo documents why; the hard fact is that the policy never touches the filesystem.
- Executable is hard-coded `/usr/bin/sandbox-exec` (`seatbelt.rs:55-59`), with the comment that PATH injection is the threat and a tampered `/usr/bin/sandbox-exec` implies the attacker already has root.
- **Writable roots** get one `(allow file-write* (require-all (subpath …)))`, plus per-root `(deny file-write-unlink (require-all (literal <root>) (vnode-type DIRECTORY)))` — "a sandboxed process must not be able to replace an authority boundary that will be reused to build the next sandbox policy" (`:539-545`).
- **Exclusions are written twice** — `require-not (literal …)` **and** `require-not (subpath …)` — because `subpath` alone leaves a gap for first-time creation of the protected directory itself (`:558-591`).
- **glob deny** translates git-style globs to anchored regexes (`seatbelt.rs:724-832`: `**/`→`(.*/)?`, `*`→`[^/]*`, `?`→`[^/]`, `{a,b}`→`(a|b)`), emits read and write denies, and adds `deny file-write-unlink` for every ancestor of the pattern (`:670-681`).
- **Network**: default deny; enabling adds inbound/outbound; proxy mode allows only `localhost:<port>`; `allow_local_binding` adds bind/loopback and, only alongside proxy ports, DNS `*:53` (`:316-378`); a configuration that yields no legal endpoint returns an **empty policy** rather than widening access (`:354-364`). Unix sockets get an explicit allowlist (`:267-301`).
- **Back-door closures worth noting**: `(deny system-fcntl (fcntl-command 80 110))` because `F_MAKECOMPRESSED`/`F_TRANSFEREXTENTS` mutate files through read-only descriptors (`:1083-1088`); `(deny mach-lookup …)` because process-to-process authority is not a pathname capability.

## 5. DeepSeek Harness — Windows

Packages: `packages/sandbox/{sandbox,sandbox-local,sandbox-policy,sandbox-windows-acl}`, consumers in `packages/shell/{bash-sandbox,pwsh-sandbox}`, Win32 primitives in `packages/subprocess/win32-process`.

- **No new identity at all** — no group, no user, no AppContainer (`SandboxUsers`/`NetLocalGroup`/`LogonUser` have zero hits). The Windows back end is a **restricted token + capability SIDs + a Low integrity label**.
- Token: `CreateRestrictedToken(callerToken, WRITE_RESTRICTED|DISABLE_MAX_PRIVILEGE|LUA_TOKEN, …)` (`src/token.ts:216-243`); restricting list `[logonSid, Everyone]` read-only, `[…, workspaceSid, tempSid]` workspace-write (`token.ts:224-228`); the logon SID is copied out of `SE_GROUP_LOGON_ID` (`token.ts:52-77`).
- Capability SID = `S-1-4-<u30>-<u30>` derived from **sha256 of the canonical workspace path** (`src/workspace-sid.ts:35-40`). Deterministic, so the security-descriptor change materialises once per machine per workspace.
- **Two authorisation lifetimes**: the workspace ACE (plus deny and label) is **standing and deliberately never revoked** (`src/grant.ts:115-124`, `src/index.ts:408-420`), because revoking would force the next provision to re-propagate the whole tree (`workspace-sid.ts:5-14`); a per-session random temp directory is revocable (`grant.ts:118-124`).
- **Exact-ACE skip**: `grantWrite` reads the current DACL and label and returns **without calling `SetNamedSecurityInfoW`** when the allow ACE, the deny and the label are already exactly present (`acl.ts:286-327`, `:384-395`).
- **Per-path lock** around read-merge-write: lock file `GetTempPathW()\dsh-acl-locks\<sha256(lowercase path)[:16]>.lock` plus `LockFileEx` (`acl.ts:73-127`), with the stated motive that concurrent sandbox instances must not overwrite each other's ACEs (`acl.ts:17-20`).
- Three edits (allow, `FILE_DELETE_CHILD` deny for Everyone, integrity label) go out in **one** `SetNamedSecurityInfoW` (`acl.ts:229-266`, `:377-413`).
- Refusal is three-layered: the token's `WRITE_RESTRICTED` pass-2 check, the Low label written on each authorised directory (`token.ts:156-165`, `acl.ts:169-188`), and the Everyone `FILE_DELETE_CHILD` deny (`acl.ts:408`).
- **Reads and network are not confined at all** (self-reported `partial`; `src/README.md:117`, `:178`). The program directory needs **no ACE**, because the token keeps the caller's own SID: only `logonSid + Everyone` are kept as a "keep-alive" set, without which early DLL init fails `0xC0000142` and CNG crashes pwsh `0xE0434352` (`token.ts:184-206`).
- The control pipe is built by the **unconfined parent** and handed over by handle/inheritance (`runner.ts:186-188`) — the same origin as tinio's decision 16.

## 6. DeepSeek Harness — Linux and macOS

- **Chain**: `linux: ['bwrap','landlock']`, `darwin: ['seatbelt']`, `win32: ['windows-acl']` (`sandbox-local/src/index.ts:160-167`). A single-candidate chain is **not probed** (`:508-509`); multiple candidates are probed in order and the first non-`unusable` wins; nothing usable → `'unavailable'` → `SandboxUnavailableError` (`:497-515`).
- **bwrap is an unvalidated PATH dependency** (`['bwrap', …]`, `:341-349`); Landlock is the bundled one, resolved from a per-platform prebuilt package (`native/landlock-run/packages/entry/lib/index.js:45-58`) with a deliberately non-existent fallback path so cwd can never choose the binary.
- **Linux bwrap profile is eight arguments**: `--ro-bind / / --dev /dev --unshare-pid --proc /proc --die-with-parent`, plus `--tmpfs /tmp` and `--bind <workspace>` in workspace-write (`sandbox-local/src/profiles.ts:16-23`). **No `--unshare-user`, no `--unshare-ipc`, no `--unshare-net`, no `--cap-drop`.** Private PID namespace + procfs is deliberate: it closes the `/proc/<pid>/root|fd|cwd` magic links (`DSH/.agents/notes/implemented/bug-fix/2026-08-06-bwrap-private-pid-namespace.md:7-19`).
- **Landlock launcher** (`native/system/packages/entry/src/main.c`, the only C source): hand-rolled UAPI structs, ABI-negotiated `handled_access_fs` (ABI 5 adds `IOCTL_DEV`), one `landlock_add_rule(RULE_PATH_BENEATH)` per grant root with `--ro` = execute/read-file/read-dir and `--rw` = the handled set, `PR_SET_NO_NEW_PRIVS` **before** `landlock_restrict_self` (`:224-259`), fail-closed `exit 125` on an unopenable grant root or an unsupported ABI (`:196-202`, `:231-236`), `partial enforcement` reported on an older ABI but still applied (`:288-293`). No cleanup is needed — rulesets die with the process.
- **What it does not constrain**: network (no rules at all), Unix sockets, `/proc`, process visibility.
- **macOS profile is four forms**: `(version 1)`, `(allow default)`, `(deny file-write*)`, `(allow file-write* (literal "/dev/null"))`, plus one `(allow file-write* (subpath …))` per writable root (`profiles.ts:51-58`). Compared with Codex it has **no** read restriction, no `file-write-unlink`, no protected-metadata rules, no glob denies, no mach-lookup closure, no unix-socket allowlist, no network rules, no fcntl closure.
- **`sandbox-exec` is resolved through PATH with no validation** (`sandbox-local/src/index.ts:552-555`), and because darwin is a single-candidate chain it is never probed. DSH's own README records the dependency's status: "Seatbelt depends on deprecated `sandbox-exec` — macOS still ships it, but this provider cannot replace or probe that private policy engine if Apple removes it" (`sandbox-local/README.md:132`).
- **Failure is uniform and fail-closed**: no platform ever downgrades to unsandboxed, and the only escalation is the user-approved `danger-full-access` path. Observability is returned, not logged: `enforcement: full|partial`, `denialSignatures` (per-backend denial dialects), `runnerFailureRules` (`sandbox/src/index.ts:128-149`; `sandbox-local/src/index.ts:207-242`).

## 7. Comparison

Windows:

| Dimension | tinio today | Codex | DSH |
|---|---|---|---|
| Identity | per-root AppContainer profile → package SID | elevated: resident group + 2 accounts; unelevated: none | none — restricted token only |
| Authorisation | `icacls /grant:r` per run, `/remove:g` on exit | Win32 ACL API, mask-checked, never revoked | Win32 ACL API, exact-ACE skipped, standing per workspace |
| Windows cost | every run rewrites each exposure's DACL | steady state **0** writes | steady state **0** writes |
| Read isolation | **yes** (AppContainer denies by default) | unelevated: no; elevated: partial | **no** (documented `partial`) |
| Cleanup | per-run rollback; crash leaves ACEs | standing by design; uninstaller removes identity | standing by design; revocable only for temp |

Linux:

| Dimension | tinio today | Codex | DSH |
|---|---|---|---|
| Mechanism | in-place `unshare` user+mount ns, bind mount + `pivot_root`, seccomp, cap drop, `no_new_privs`; **no external binary** | two-stage: helper → `bwrap` → same exe as inner seccomp stage | one wrapper process: `bwrap` or bundled `landlock-run` |
| Filesystem | own bind-mount plan | bwrap's mount layout, incl. materialised missing paths | bwrap: `--ro-bind / /` + writable `--bind`; Landlock: path rules |
| Network | connect-only seccomp **inside the shared netns** | netns (`--unshare-net` unless full access) + seccomp family filter + TCP→UDS bridge for proxy mode | **none** |
| External dependency | none | `bwrap` (system or bundled+SHA256); `rg` optional | `bwrap` (PATH, unvalidated) or bundled prebuilt |
| Failure mode | fail-closed | never falls back to Landlock; missing bwrap panics | fail-closed, `exit 125` + `landlock-run:` line |

macOS:

| Dimension | tinio today | Codex | DSH |
|---|---|---|---|
| Mechanism | `BACKEND_IS_STUB = true`, `confine` returns `Error::Unsupported` | SBPL pipeline + `/usr/bin/sandbox-exec -p … -D…` | `sandbox-exec -p <4 forms>` |
| Baseline | — | `(deny default)` + 116-line allowlist | `(allow default)` + `(deny file-write*)` only |
| Read / IPC / network | — | read restrictions, mach-lookup closure, full network template, unix-socket allowlist, fcntl closure | none of them |
| Executable trust | — | hard-coded `/usr/bin/` | PATH-resolved, unvalidated |

## 8. Transferable decisions, in the order they are worth doing

1. **Exact-ACE skip** before any DACL write. `tinio` already owns the oracle (`crates/tinio-sandbox-windows/src/acl.rs:333` `has_ace`) and used it only in tests; without this step a stable SID changes nothing, because `icacls /grant:r` rewrites an already-present ACE and re-pays the full propagation.
2. **Once per root, not once per run.** `package_name` is already a pure function of the canonical root (`sid.rs:111-125`), and `CreateAppContainerProfile`'s SID equals `DeriveAppContainerSidFromAppContainerName(name)` — an existing test pins this (`sid.rs:693-761`). No state file and no seeded RNG is needed; the SID is already deterministic per root.
3. **The program directory as a one-off global grant** — `S-1-15-2-1` (ALL APPLICATION PACKAGES) `RX`, written once, because it does not vary by root. This must **not** be generalised to the served root or state dir: one global SID on two roots lets a child serving root A write root B.
4. **Direct ACL APIs instead of the `icacls` subprocess** (`acl.rs` internals; public surface unchanged). It removes two process spawns per read/write and makes (1) nearly free. DSH also measured that hand-run `icacls` cannot revoke these ACEs on that platform (`ERROR_NONE_MAPPED` 1332).
5. **A per-path lock** around read-merge-write, because `tinio` couples `/grant:r` with `/remove:g` on the same directory and concurrent confined runs sharing a root have a lost-ACE window.
6. **Enforcement facts in the API**, not only in logs: DSH returns `enforcement: full|partial` plus per-backend denial signatures and runner-failure rules, which separates "the sandbox broke" from "the command was denied".
7. **Deny `io_uring_setup`/`enter`/`register`** in the Linux filter. Codex blocks them unconditionally because **io_uring can create an `AF_VSOCK` socket without going through `socket()`** (`linux-sandbox/src/landlock.rs:195-199`), and it applies to every mode that restricts socket families. Cheapest item in this survey: three denies on an existing rule table, and `io_uring` is rare in ordinary toolchains.
8. **Consider `ptrace`/`process_vm_readv`/`process_vm_writev`** as the companion to the user namespace: Codex treats them as the same class of mount-view bypass as the procfs magic links (`bwrap.rs:369-373`). Risk: debuggers, profilers and some runtimes — evaluate before adding.
9. **Materialise a missing protected path instead of only denying it.** The insight is not bwrap's flag but the rule: for a protected path that does not exist on the host, a policy that only denies access lets the child create it. Codex binds `/dev/null` read-only onto the **first missing component** (`bwrap.rs:1187-1196`, `:1413-1438`). `tinio` can express the same thing in its bind-mount plan (a pre-opened `/dev/null` fd or a read-only empty inode) — but note that a `/dev/null` bind presents as a character device, not an empty file.
10. **Keep the netns/seccomp split as an idea, not as a port.** Codex uses netns for reachability and seccomp for the socket family inside it, and both `Isolated` and `ProxyOnly` unshare the network. `tinio`'s connect-only filter works **inside a shared netns**; adopting netns isolation would break loopback/proxy/DNS and require Codex's whole bridge (`proxy_routing.rs`). Document the distinction instead.
11. **Backend-neutral violation vocabulary.** Codex normalises reasons across Linux/Seatbelt/Windows (`sandboxing/src/violation.rs:11-99`: `OperationNotPermitted`, `PermissionDenied`, `ReadOnlyFileSystem`, `PolicyDenied`, `SignalSyscall`) and folds shell quick-reject codes in first. Pairs directly with item 6.
12. **If the macOS backend ever lands**, these are the reusable pieces: the section order of `seatbelt.rs:1048-1090`; `-p` + `-D` path parameterisation (`:1100-1108`); the access-policy builder with `require-not` literal+subpath pairs (`:507-611`); the writable-root anchor deny (`:539-545`); the glob→regex translator (`:724-832`, a self-contained pure function in Rust, portable nearly verbatim) with its ancestor denies (`:670-681`); the network template (`:316-378`); the unix-socket allowlist (`:267-301`); the hard-coded `/usr/bin/sandbox-exec` constant (`:55-59`); the fcntl closure (`:1083-1088`); and `seatbelt_policy_arg(args)` (`sandboxing/src/seatbelt_tests.rs:68-75`) as the unit-test shape (assert the policy text, never run the sandbox).

## 9. Not transferable

- **DSH confines writes only.** Its read and network behaviour is a deliberate scope choice for a read-hungry agent tool; copying its core mechanism wholesale would be a security regression for `tinio`, whose AppContainer default-deny read is a real advantage.
- **Codex's bwrap dependency and its CLI vocabulary.** `--ro-bind`/`--ro-bind-data`/`--perms`/`--remount-ro`/`--bind-try` are bubblewrap capabilities; `tinio`'s in-place `pivot_root` route has no place to put them, and adopting bwrap would import an external binary into a route that deliberately has none.
- **Codex's same-exe two-stage handoff.** It exists only because setuid bwrap and `no_new_privs` cannot coexist; `tinio` has no setuid bwrap, so the handoff would add complexity and a new argv0-forgery surface for nothing.
- **Bundled bwrap + SHA-256 + `/proc/self/fd` exec** is the engineering that comes with distributing a third-party sandbox; `tinio` has no such need.
- **Codex's synthetic-mount cleanup does not apply to `tinio`.** It exists because bwrap requires real mount targets on the host; `tinio`'s mounts live inside the new root, so the namespace exit removes them and there is no host residue to identify, lock and delete.
- **macOS `sandbox-exec` is deprecated.** DSH states it plainly (its provider cannot replace or probe that private policy engine if Apple removes it). A `tinio` macOS backend should be a rung that can fail (probe + execute-time classification), not the only path — and the Codex test pattern is the one to copy: probe with `sandbox-exec -p "(version 1)(allow default)" /usr/bin/true`, and treat `sandbox_apply: Operation not permitted` as "skip, nested Seatbelt unavailable" (`cli/tests/sandbox_tty.rs:43-55`), plus the `CODEX_SANDBOX=seatbelt` sentinel (`AGENTS.md:10`).
- **DSH's macOS choices are the negative examples**: `(allow default)` as the baseline, and `'sandbox-exec'` resolved through PATH.
- **Codex's own historical wart**: `(allow file-write* (regex #"^/"))`, annotated in-tree as "allegedly, this is more permissive than `(allow file-write*)`" (`seatbelt.rs:950`).
- **Glob expansion through `rg`** imports a second external process and a parsing surface for an edge feature; DSH simply not doing glob denies is a reasonable trade.
- **Neither repo guards against `ARG_MAX`** when the whole SBPL policy is passed as one argv element. That is a shared blank spot and `tinio` would have to decide for itself.

## 10. What this survey did not establish

- **The residue is not free in either project.** Both leave authorisation in place on purpose and document the consequences (stale ACEs after a rename, `deny` ACEs orphaned when the ledger is deleted). A `tinio` decision to go standing inherits that shape.
- **Neither repo documents why the seatbelt policy is inlined rather than written to a temp profile file.** The only hard fact is that `-p` keeps the policy text off the filesystem.
- **The measured Windows figures are from this host**; the per-object rate is owned by `specs/2026-09-18-sandbox-design.md` §6's inheritance-and-cost paragraph, and this survey did not re-measure it.

## 11. Why the Linux/macOS half of this survey matters to `tinio`

`tinio`'s Linux route is the odd one out: it is the only in-process, zero-external-binary launcher of the three, and it is the only one that restricts outbound network with seccomp rather than a network namespace. Codex's own demotion of Landlock (it cannot isolate Unix sockets) is the argument **for** the namespace route, not against it. The two concrete Linux actions that fall out of this survey are items 7 and 9; everything else on that platform is either already covered by the mount plan or would import a dependency.

## 12. The correction this survey produced

The 9–30 s cost is **not** a host security/filesystem filter driver (the earlier hypothesis). It is `SetNamedSecurityInfoW`'s **eager full-tree inheritance propagation**:

- Same SDDL, no-op write: `Set-Acl` on `target\debug` **9.208 s** vs a fresh directory **0.003 s**; a directory created **inside** `target\debug` costs 0.06 s (no descendants).
- Removing all 57 stale package ACEs changed nothing (19–30 s either way), which rules out ACE count.
- `specs/2026-09-18-sandbox-design.md`'s §6 already measured the per-object rate (NTFS ≈0.3–0.4 ms/object, ReFS ≈0.087) on a synthetic 200,200-object corpus, and DSH's own note records the same behaviour ("walks every descendant immediately — tens of seconds on large workspaces").

Consequence: the cost scales with the **descendant count** of the target, so the fix is to stop writing those DACLs at all (items 1–3), not to make the write faster.
