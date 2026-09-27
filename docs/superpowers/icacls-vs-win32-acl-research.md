# `tinio-sandbox-windows`: `icacls.exe` subprocess, or Win32 API directly?

**Date**: 2026-09-23 · **Scope**: DACL layer of `crates/tinio-sandbox-windows` (`src/acl.rs`) · **Nature**: research, no code changed

> **Line-number currency**: the `acl.rs` and `sid.rs` references in this report are from the day of writing, cross-checked line by line against the 2026-09-23 cleanup commit, and still exact. The two `backend.rs` references were updated after that module split into a directory module (`backend/mod.rs` + `capability.rs` / `exposures.rs` / `tests.rs`); splitting again or reordering lines will void the numbers — names and functions stay. **The measured figures are that day's too, not today's**: `src/acl.rs` is 675 lines today and `tests/acl.rs` 460 (§7 quotes 890 and 465), and §5's "about 0.067 ms/obj" is the appendix's 0.0675 rounded.

> **Note, 2026-09-27 — two of this report's premises changed after it was written, and its per-launch arithmetic is the pre-change one.** The DACL **read** moved onto the API (2026-09-25: `has_ace` → `acl_names` → `any_ace_in`, `GetNamedSecurityInfoW` + `GetAce` + `EqualSid`, with `ConvertStringSidToSidW` in an RAII `OwnedSid`), so §1's "read the DACL text rendered by `icacls`, then substring match" describes the state at the time of writing rather than the tree. And the launcher no longer **revokes** anything: every grant is standing (design decision 15, amended), so §5's "rollback → 1 per recorded path, i.e. about `2 × (N_caller + 1 + [siblings ? 1 : 0])` calls" is the rollback-era count, and `acl::revoke` — which this report prices — is a capability an operator or a ledger would call, not a step of the launch. That also settles the two clauses that argue from the revoke's *scope* (§2's "the rollback in this crate only revokes paths it granted itself" and §9's "rollback only ever revokes paths it granted itself"): nothing revokes at all now, so the silent scenarios they call unreachable are unreachable for that reason and more.

**Conclusion: not worth switching.** Switching saves the ~70–150 ms process spawn per launch, i.e. 0.2–0.5% of the 30 s readiness budget; switching does **not touch** the 32–38 s inheritance propagation that is what really bites in the design. The cost is re-proving the semantics of the entire DACL layer, plus a 7th `unsafe` site and changes to 5 docs.

---

## 1. The four operations that must be reproduced exactly

| Operation | Implementation today | Location |
|---|---|---|
| `grant(path, Target, AccessMode, sid)` | `icacls <path> /grant:r "*<sid>:<SPEC>" /q` | `acl.rs:300-316` (`grant`), `acl.rs:202-209` (`ace_spec`), `acl.rs:233-235` (`grant_spec`) |
| `revoke(path, sid)` | `icacls <path> /remove:g "*<sid>" /q` | `acl.rs:344-353` |
| `has_ace(path, sid)` | read the DACL text rendered by `icacls`, then substring match | `acl.rs:392-395` |
| `SiblingScan::pattern` → `*.dll` batch | one `icacls <dir>\*.dll /grant:r "*<sid>:RX" /q` | `acl.rs:543-552` (`apply_siblings`), `acl.rs:715-727` (`pattern`) |

The three shapes of `SPEC` (`acl.rs:202-209`): `Target::Directory` → `(OI)(CI)RX` / `(OI)(CI)M`; `Target::ProgramDirectory` and `Target::File` → `RX` / `M`. Mask check: `FILE_GENERIC_READ(1179785) | FILE_GENERIC_EXECUTE(1179808)` = `0x001200A9`, then `| FILE_GENERIC_WRITE(1179926) | DELETE(0x10000)` = `0x001301BF`, bit-for-bit consistent with the comment at `acl.rs:169-173`.

**Semantic constraints (each must hold)**: `/grant:r` replaces only that SID's ACE, leaving other ACEs / owner / SACL / control bits alone (`acl.rs:264-268`, decision 15, design line 146). Measured on this machine: for a directory whose `SE_DACL_PROTECTED` was set with `/inheritance:d`, after `/grant:r` and `/remove:g` the owner read back by `Get-Acl` and `AreAccessRulesProtected=True` are unchanged throughout, rule count 10 → 11 → 10.

---

## 2. API surface: what is needed, what the tool actually does

Required call chain (Microsoft's official example):

```
GetNamedSecurityInfoW(path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, …&pOldDacl, &pSd)
BuildTrusteeWithSidW(&trustee, psid)          // no allocation, no name lookup
ea = EXPLICIT_ACCESS_W { grfAccessPermissions: mask,
                         grfAccessMode: SET_ACCESS,   // /grant:r
                         grfInheritance: OI|CI,       // directory; files use NO_INHERITANCE
                         Trustee: trustee }
SetEntriesInAclW(1, &ea, pOldDacl, &pNewDacl)
SetNamedSecurityInfoW(path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, …pNewDacl…)
LocalFree(pNewDacl); LocalFree(pSd)
```

`/grant:r` = `SET_ACCESS`, `/remove:g` = `REVOKE_ACCESS`. Using `GRANT_ACCESS` gives `/grant` (merge), the behaviour this design explicitly does not want.

**The `icacls` internal algorithm is confirmed by reverse engineering and matches measurement.** The `SetEntriesInAclW` docs state a new allow ACE is inserted **before** existing allow ACEs; measured (granting one at a time through Users(545) → SYSTEM(18) → Admins(544)), the order read back is **Admins, SYSTEM, Users** — last written first, i.e. exactly front-insertion, not SID sorting or insertion order. **So `icacls` is a wrapper around this call chain, the FFI version is a transcription not a reimplementation, and canonical-order maintenance is free.** Conversely, hand-rolling the ACL buffer with your own `AddAce` does not get that order.

**`/remove:g` is a no-op for an inherited ACE, confirmed by measurement**: put one `(OI)(CI)R` (Users) on `D\`; file `D\S\F.txt` gets `BUILTIN\Users:(I)(R)`; running `/remove:g` on that **file** → exit 0, `Successfully processed 1 files`, ACE untouched. Only running it on the source directory clears the whole subtree.

**This is where the FFI version must decide for itself**: `REVOKE_ACCESS` deletes that trustee's ACE from the in-memory copy of the new ACL and writes the whole thing back — for an `(I)` ACE, the tool **does not write**, while the API **writes a DACL that does not contain it** (i.e. breaks the inherited projection on that object). This is a real behavioural divergence. The rollback in this crate only revokes paths it granted itself (`acl.rs:485-489`, `acl.rs:617-631`), so the production path never hits it; but `tests/acl.rs:222` does not cover such input.

**Control bits / SACL / owner**: passing only `DACL_SECURITY_INFORMATION` leaves owner/group/SACL untouched. Whether `SE_DACL_PROTECTED` is preserved has no explicit official statement, but the measurement above is circumstantial evidence for it (icacls goes through exactly this chain).

**SID / PSID lifetime**: `BuildTrusteeWithSidW` does not allocate and does not look up a name, it only stores the pointer in `TRUSTEE_W.ptstrName`; `SetEntriesInAclW` copies the SID into the new ACL, so the SID only has to outlive that one call. AppContainer package SIDs (`S-1-15-2-…`) have no account, so the `TRUSTEE_IS_SID` path is **mandatory**. This crate already satisfies that: `PackageSid` owns the raw `PSID` (`sid.rs:120-121`) and exposes `raw()` (`sid.rs:280`).

**No `windows-sys` feature has to be added at all** — all of it falls inside the set already enabled by `Cargo.toml:15-28` (checked one by one against 0.61.2 sources).

---

## 3. Existing crates: none usable

- **`windows-acl`** 0.3.0 (released 2021, last repo commit 2023-06). Closest, with in-place replacement of a single ACE and full inheritance-flag expressiveness. **But its replace matches only on `(SID, ACE type)` and does not compare inheritance flags** — with both `M` and `(OI)(CI)M` on the same trustee it replaces only the first. For `/grant:r` semantics that is a real bug. It does not use `SetEntriesInAclW` at all, rebuilding byte by byte from `AddAce`, and `remove_entry` does not skip inherited ACEs either.
- **`windows-permissions`** 0.2.4: the wrapper has `AddAce` but no `SetEntriesInAclW` and no `DeleteAce`, so replacement or revocation is structurally impossible.
- **`winsafe`** 0.0.29: no ACL / security-descriptor module at all.
- **`winacl` / `secdl` / `dacl`** do not exist on crates.io; **`windows-sddl`** is parse-only, can read but not write.

**The best precedent is not a crate, it is `openai/codex`'s `codex-rs/windows-sandbox-rs/src/acl.rs`** (`windows-sys`, 1097 lines): `GetNamedSecurityInfoW` → build `EXPLICIT_ACCESS_W{SET_ACCESS}` → `SetEntriesInAclW` → `SetNamedSecurityInfoW` → `LocalFree`×2, with `has_ace` using `GetAce` + `EqualSid` + `MapGenericMask` and skipping `INHERIT_ONLY_ACE`.

---

## 4. Batch `*.dll`: a reason to keep the subprocess, not a reason to switch

Measured on `D:\Temp` (NTFS) with 200 `.dll` + 200 `.bin`:

| Approach | Time | Amortized |
|---|---|---|
| `icacls <dir>\*.dll /grant:r …` (one call, 200 files) | **41.78 ms** | 0.21 ms/file |
| per-file `icacls <file> /grant:r …` (200 calls) | **4674.37 ms** | 23.37 ms/file |
| wildcard rollback `/remove:g` | 50.22 ms | — |

All 200 files got the ACE (count 200/200). So batching is worth about 4.6 s in the subprocess world (this workspace's `target/debug/deps` has 68 DLLs, worth about 1.6 s).

**After switching to FFI this optimization becomes meaningless**: in-process, 200 per-file calls are just 200 ACE writes themselves (order of 30 ms), same order as the 41.78 ms of the wildcard. What batching really kills is the combination "subprocess + per-file": 200 calls is 4.7 s, 200,000 calls is about 40 minutes. **Batch capability is the lifeline of the subprocess route, not its weakness.**

---

## 5. Performance: honest numbers

**Fixed cost of a single spawn is about 12 ms** (300 iterations minus loop overhead: `icacls <empty dir>` = 12.476 ms/call, `icacls <dir with one file>` = 21.7 ms/call; `icacls /?` is 15.3–26.6 ms). Rust's `Command::output()` should not be higher than this.

**Propagation per object within a single call is about 0.067 ms/obj** (101 obj → 14.20 ms, 1001 obj → 74.96 ms, delta 60.76 ms / 900 obj, intercept about 7.4 ms). The 12 ms spawn is only equivalent to the propagation of about 150 objects.

**How many `icacls` calls per launch**: `apply_siblings` at `src/backend/mod.rs:357` → 1 (when siblings exist); the loop at `:379` in the same file → 1 per non-file exposure; rollback → 1 per recorded path. I.e. about `2 × (N_caller + 1 + [siblings ? 1 : 0])` calls. Typical 1–2 exposures → **6–8 calls, about 72–150 ms**, i.e. **0.2–0.5%** of the 30 s budget. The "`2N` invocations" at design line 422 is the formula from before Ruling 78.

**Key: those 32–38 s save not one millisecond with the API either.** The `SetNamedSecurityInfoW` docs state plainly: when setting a DACL the system automatically propagates inheritable ACEs to existing child objects. The propagation is done by the **filesystem**; both implementations trigger the same kernel path. The design's numbers and the operational note in `README.md` are entirely unaffected by this switch. **If anyone wants to use FFI to fix the readiness budget, they are fixing the wrong layer.**

**How fast in-process actually is**: using .NET's `Get-Acl` + `SetAccessRule` as a proxy, 100 files in 519.2 ms (5.19 ms/file), against 13.2 ms/file for the same batch of files going through the subprocess `/remove:g`. Real Rust FFI would be far below 5.19 ms. So **being 10–20× faster per call is true, it is just that the call count is too low to turn that into value.**

---

## 6. What the tool does for you, and what the API makes you do yourself

| Item | Conclusion |
|---|---|
| SID string parsing | **Free** — this crate does not need it at all: `PackageSid::raw()` hands over the raw `PSID` directly, and `BuildTrusteeWithSidW` does no name lookup. Conversely the SID path is **mandatory**. |
| Inheritance flag semantics | **Write it yourself**: expressed by `grfInheritance`, three `Target`s → two sets of flags, about 10 lines. But "`(OI)(CI)` is a silent no-op on a file" **disappears** (the API writes it as told), i.e. the failure class pinned by `tests/acl.rs:342` **changes shape** rather than being eliminated. |
| ACE order | **Free, but only free if `SetEntriesInAclW` is used.** Hand-rolling the buffer and building the ACL yourself does not get it. |
| "successfully processed" semantics | **Lost, and that is a net gain.** At least two cases where the tool exits 0 having written nothing. |
| `\\?\` verbatim paths | **Unverified risk**. This crate canonicalizes every exposure into `\\?\` form and **has measured that `icacls` accepts it** (2026-09-19). Whether `SetNamedSecurityInfoW` accepts `\\?\` has **never been tested by anyone**. This is the most fragile link in the whole FFI route. |
| `TreeSetNamedSecurityInfoW` | **Do not use**: all-or-nothing over the whole tree, requires `READ_CONTROL`+`WRITE_DAC` on every object in the tree, and `TREE_SEC_INFO_RESET` wipes explicit ACEs along with the rest. |

---

## 7. If you really do switch: size and cost

**Approach**: go straight to `windows-sys`, no crate.

**LOC**: real code is about 150–200 lines (grant ~35, revoke ~30 shared helper, has_ace ~40, mask/flag mapping ~30, error mapping ~10, batch loop ~10). But at this repo's documentation density (of `acl.rs`'s 890 lines about 120 are code), the realistic size of the replacement module is **600–900 lines**.

**Tests / docs that must change**:

- `acl.rs:853` `the_icacls_binary_comes_from_the_system_directory_not_the_environment`, `acl.rs:869` `the_grant_argument_names_the_sid_literally` → replace with mask/flag mapping tests
- `tests/confine.rs:285-286` asserts `err.to_string().contains("icacls")` → change to Win32 error codes
- `tests/acl.rs` (465 lines) **mostly stays as is** — there `icacls` is an **independent oracle**, and production switching to FFI makes the oracle more independent, not less
- **`icacls` does not disappear from the repo** — three test suites still use it as oracle, and the gate's Windows section carries what it needs through design §6's ACE-shape pointer rather than at a line of its own
- design doc: decision 14 **explicitly enumerates the unsafe sites and, on the day this report read it, used `acl` specifically as the counterexample** ("`acl.rs` carries no allow and makes no raw call" — the sentence that decision has since corrected). Switching to FFI means editing that passage, the `allow(unsafe_code)` count, the one line in §6 and §11, plus the P2 plan's **81 occurrences** of `icacls`, `README.md`'s operator note, and `docs/tests.md`'s Windows section (which points at design §6)

**Failure modes newly introduced by FFI**:

1. The 7th `unsafe` site (`acl.rs` needs `#![allow(unsafe_code)]`), with invariants to write per decision 14(c) — in particular `PSID` lifetime and `LocalFree` ownership
2. **Error-code trap**: `SetEntriesInAclW` / `SetNamedSecurityInfoW` return `WIN32_ERROR` (DWORD) **directly**, without going through `GetLastError`. `handles::last_error()` (which reads `GetLastError()`) would report a **stale** error here
3. Two new `LocalFree` allocations (the new ACL, `GetNamedSecurityInfoW`'s `pSd`); `windows-sys` 0.61 has no RAII. The crate already has `LocalFree` precedent (`sid.rs:235,449,514,571`)
4. The `REVOKE_ACCESS` divergence on inherited ACEs described above
5. `\\?\` paths unverified

---

## 8. When the answer would flip (No → Yes)

1. **It must run on a Windows image without `icacls.exe`.** Today: `acl.rs:257-259` only does `system_directory().join("icacls.exe")`, **existence is never checked**, and its absence blows up into an `Error::Launch` at the first grant. This is the only condition that flips **by schedule** rather than by preference — once a deployment image is known to lack it, the subprocess route is dead. **Checked on this machine: both `System32` and `SysWOW64` have it (WinSxS holds 6 more copies); the condition does not hold.** Whether Server Core / Nano Server / Windows PE ship it cannot be confirmed from public material, and would have to be tested before deploying to those images.
2. **A measured spawn cost that exceeds the budget.** Today 12 ms × 6–8 calls ≈ 100 ms, impossible. Unless the exposure count becomes several hundred, or it changes to per-file grant (then 200,000 calls is about 40 minutes and the subprocess route surely dies).
3. **A batch the tool cannot express.** Checked: not the wildcard case; conversely the wildcard is the lifeline for the subprocess.
4. **A correctness requirement the tool's semantics cannot meet.** The most realistic one: if in future, to get around the 32–38 s inheritance propagation, the design changes to **writing non-inherited ACEs per file**. Then the call count goes from about 6 to the object count, the subprocess route's economics collapse; while in-process FFI batch writes (assuming ~0.1 ms/call, 200,000 objects ≈ 20 s) **might** beat NTFS's 32–38 s — but still lose to ReFS's 8.6 s. This is worth **measuring before deciding** when the readiness budget actually gets tight.

---

## 9. The strongest argument against this conclusion

**The "silent success" class is a real, uneliminated defect class on the subprocess route.** The only reason this crate has two regression tests (`tests/acl.rs:341`, `tests/acl.rs:222`) is `icacls` **exiting 0 having written nothing**. Add the fact that `has_ace` today does substring matching over **localized** DACL text (`acl.rs:355-364` records the trap of `S-1-5-32-545` being rendered as `BUILTIN\Users`), plus the line in §11 that has to admit "the error type must carry the tool's text as well". The FFI version eliminates all three at once, and per the docs and the codex precedent it is not a research project but a ~60-line recipe.

**Why that argument still does not flip it**: both silent scenarios are **unreachable** in this launcher's actual call patterns — `apply_siblings` always passes `Target::File` (`acl.rs:546`), and rollback only ever revokes paths it granted itself. That is: what would be replaced is **the boundary case already pinned by tests**, in exchange for "cleaner diagnostics", at the cost of one complete re-proof of the entire DACL layer. For a feature where 32–38 s is the real problem, that is a bad deal.

---

## Appendix: reproducible measurement record

Environment: Windows 11 Pro for Workstations 10.0.26200, `C:`/`D:` NTFS, `E:` ReFS.

| Measurement | Result |
|---|---|
| 300× `icacls <empty dir>` minus loop overhead | 12.476 ms/call |
| 100× `icacls /remove:g` on 100 files | 13.2 ms/file |
| grant delta from corpus 101 obj → 1001 obj | 0.0675 ms/obj, intercept about 7.4 ms |
| `icacls <dir>\*.dll` one call / 200 files | 41.78 ms (grant), 50.22 ms (rollback) |
| 200× per-file `icacls /grant:r` | 4674.37 ms (23.37 ms/file) |
| .NET `Get-Acl`+`SetAccessRule`+`Set-Acl` / 100 files | 5.192 ms/file (in-process proxy, including cmdlet overhead) |
| `/remove:g` on a file with only an `(I)` ACE | **exit 0, reports success, ACE unchanged** |
| `SE_DACL_PROTECTED` / owner after `/grant:r` | **both preserved** |
| ACE write order (insert 545→18→544) | read back 544, 18, 545 = **last written first**, confirming the `SetEntriesInAclW` docs |
