# `tinio-sandbox-windows`：`icacls.exe` 子进程，还是直接 Win32 API？

**日期**：2026-09-23 · **范围**：`crates/tinio-sandbox-windows` 的 DACL 层（`src/acl.rs`） · **性质**：调研，未改动任何代码

> **行号时效**：本报告的 `acl.rs` 与 `sid.rs` 引用取自撰写当日，并与 2026-09-23 的清理提交逐条复核过，仍然精确。`backend.rs` 的两处引用已因该模块拆分为目录模块（`backend/mod.rs` + `capability.rs` / `exposures.rs` / `tests.rs`）而更新；再拆分或调整行序会使数字失效——名字与函数不变。

**结论：不值得换。** 换掉能省的是每次 launch 约 70–150 ms 的进程 spawn，占 30 s readiness 预算的 0.2–0.5%；换掉**碰不到**设计里真正咬人的那 32–38 s 继承传播。代价是把整个 DACL 层的语义重新证明一遍，外加第 7 个 `unsafe` 站点与 5 份文档的改动。

---

## 1. 必须精确复现的四个操作

| 操作 | 今天的实现 | 位置 |
|---|---|---|
| `grant(path, Target, AccessMode, sid)` | `icacls <path> /grant:r "*<sid>:<SPEC>" /q` | `acl.rs:300-316`（`grant`）、`acl.rs:202-209`（`ace_spec`）、`acl.rs:233-235`（`grant_spec`） |
| `revoke(path, sid)` | `icacls <path> /remove:g "*<sid>" /q` | `acl.rs:344-353` |
| `has_ace(path, sid)` | 读 `icacls` 渲染的 DACL 文本后做子串匹配 | `acl.rs:392-395` |
| `SiblingScan::pattern` → `*.dll` 批量 | 一次 `icacls <dir>\*.dll /grant:r "*<sid>:RX" /q` | `acl.rs:543-552`（`apply_siblings`）、`acl.rs:715-727`（`pattern`） |

`SPEC` 的三个形状（`acl.rs:202-209`）：`Target::Directory` → `(OI)(CI)RX` / `(OI)(CI)M`；`Target::ProgramDirectory` 与 `Target::File` → `RX` / `M`。掩码核对：`FILE_GENERIC_READ(1179785) | FILE_GENERIC_EXECUTE(1179808)` = `0x001200A9`，再 `| FILE_GENERIC_WRITE(1179926) | DELETE(0x10000)` = `0x001301BF`，与 `acl.rs:169-173` 的注释逐位一致。

**语义约束（必须逐条保持）**：`/grant:r` 只替换该 SID 的 ACE，不动其它 ACE / owner / SACL / 控制位（`acl.rs:264-268`，decision 15，design 第 146 行）。本机实测确认：一个用 `/inheritance:d` 置了 `SE_DACL_PROTECTED` 的目录，经 `/grant:r` 与 `/remove:g` 之后 `Get-Acl` 读回 owner 与 `AreAccessRulesProtected=True` 全程不变，规则数 10 → 11 → 10。

---

## 2. API 面：需要什么，工具实际做了什么

所需调用链（Microsoft 官方示例）：

```
GetNamedSecurityInfoW(path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, …&pOldDacl, &pSd)
BuildTrusteeWithSidW(&trustee, psid)          // 不分配、不查名
ea = EXPLICIT_ACCESS_W { grfAccessPermissions: mask,
                         grfAccessMode: SET_ACCESS,   // /grant:r
                         grfInheritance: OI|CI,       // 目录；文件用 NO_INHERITANCE
                         Trustee: trustee }
SetEntriesInAclW(1, &ea, pOldDacl, &pNewDacl)
SetNamedSecurityInfoW(path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, …pNewDacl…)
LocalFree(pNewDacl); LocalFree(pSd)
```

`/grant:r` = `SET_ACCESS`，`/remove:g` = `REVOKE_ACCESS`。用 `GRANT_ACCESS` 就是 `/grant`（合并），是本设计明确不要的行为。

**`icacls` 内部算法已被反推证实，且与实测吻合。** `SetEntriesInAclW` 文档写明新 allow ACE 插在已有 allow ACE **之前**；实测（按 Users(545) → SYSTEM(18) → Admins(544) 逐条 `/grant`）读回顺序是 **Admins, SYSTEM, Users** —— 后写的最前，正是前插行为，而非 SID 排序或插入顺序。**所以 `icacls` 就是这条调用链的封装，FFI 版是誊写而非重实现，canonical order 的维护是免费的。** 反过来，若手搓 ACL 缓冲自己 `AddAce`，就得不到这个顺序。

**`/remove:g` 对继承来的 ACE 是空操作，实测确认**：在 `D\` 放一条 `(OI)(CI)R`（Users），文件 `D\S\F.txt` 得到 `BUILTIN\Users:(I)(R)`；对该**文件**执行 `/remove:g` → exit 0、`Successfully processed 1 files`、ACE 原封不动。对源目录执行才清掉整棵子树。

**这一点是 FFI 版必须自己决策的地方**：`REVOKE_ACCESS` 把该 trustee 的 ACE 从新 ACL 的内存副本里删掉再整体写回 —— 对一条 `(I)` ACE 而言，工具是**不写**，API 是**写一个不含它的 DACL**（即在该对象上打断继承投影）。这是真实的行为分歧。本 crate 的 rollback 只 revoke 自己 grant 过的路径（`acl.rs:485-489`、`acl.rs:617-631`），生产路径碰不到；但 `tests/acl.rs:222` 覆盖不了这种输入。

**控制位 / SACL / owner**：只传 `DACL_SECURITY_INFORMATION` 时 owner/group/SACL 均不动。`SE_DACL_PROTECTED` 是否保留官方无明文，但上文的实测旁证了它（icacls 走的就是这条链）。

**SID / PSID 生命周期**：`BuildTrusteeWithSidW` 不分配、不查名，只把指针存进 `TRUSTEE_W.ptstrName`；`SetEntriesInAclW` 把 SID 拷进新 ACL，所以 SID 只需活过那一次调用。AppContainer 包 SID（`S-1-15-2-…`）没有账户，**必须**走 `TRUSTEE_IS_SID` 路径。本 crate 已满足：`PackageSid` 拥有裸 `PSID`（`sid.rs:120-121`）并暴露 `raw()`（`sid.rs:280`）。

**`windows-sys` feature 一个都不用加**，全部落在 `Cargo.toml:15-28` 已启用的集合里（逐条核过 0.61.2 源码）。

---

## 3. 现有 crate：没有一个能用

- **`windows-acl`** 0.3.0（2021 发布，仓库最后提交 2023-06）。最接近，有就地替换单个 ACE 与完整的继承标志表达力。**但它的 replace 只按 `(SID, ACE type)` 匹配，不比较继承标志** —— 同一 trustee 同时有 `M` 与 `(OI)(CI)M` 时只替换第一条。对 `/grant:r` 语义而言这是真 bug。它完全不用 `SetEntriesInAclW`，全靠 `AddAce` 逐字节重建，`remove_entry` 也不跳过继承来的 ACE。
- **`windows-permissions`** 0.2.4：wrapper 里有 `AddAce` 而没有 `SetEntriesInAclW`、没有 `DeleteAce`，结构上做不到替换或撤销。
- **`winsafe`** 0.0.29：完全没有 ACL / 安全描述符模块。
- **`winacl` / `secdl` / `dacl`** 在 crates.io 上不存在；**`windows-sddl`** 是纯解析、能读不能写。

**最佳先例不是 crate，是 `openai/codex` 的 `codex-rs/windows-sandbox-rs/src/acl.rs`**（`windows-sys`，1097 行）：`GetNamedSecurityInfoW` → 组 `EXPLICIT_ACCESS_W{SET_ACCESS}` → `SetEntriesInAclW` → `SetNamedSecurityInfoW` → `LocalFree`×2，`has_ace` 用 `GetAce` + `EqualSid` + `MapGenericMask` 并跳过 `INHERIT_ONLY_ACE`。

---

## 4. 批量 `*.dll`：留在子进程的理由，不是换掉的理由

`D:\Temp`（NTFS）上 200 个 `.dll` + 200 个 `.bin` 实测：

| 做法 | 耗时 | 均摊 |
|---|---|---|
| `icacls <dir>\*.dll /grant:r …`（一次调用，200 文件） | **41.78 ms** | 0.21 ms/文件 |
| 逐文件 `icacls <file> /grant:r …`（200 次调用） | **4674.37 ms** | 23.37 ms/文件 |
| 通配符回滚 `/remove:g` | 50.22 ms | — |

200 个文件全部拿到 ACE（计数 200/200）。所以批量在子进程世界值约 4.6 s（本 workspace 的 `target/debug/deps` 有 68 个 DLL，约值 1.6 s）。

**换成 FFI 之后这个优化变得无意义**：进程内逐文件 200 次就是 200 次 ACE 写入本身（约 30 ms 量级），与通配符的 41.78 ms 同阶。真正会被批量杀死的是「子进程 + 逐文件」这条组合：200 次是 4.7 s，20 万次约 40 分钟。**批量能力是子进程路线的生命线，不是它的短板。**

---

## 5. 性能：诚实的数字

**单次 spawn 的固定成本约 12 ms**（300 次迭代减去循环开销：`icacls <空目录>` = 12.476 ms/次，`icacls <单文件目录>` = 21.7 ms/次；`icacls /?` 为 15.3–26.6 ms）。Rust 的 `Command::output()` 应不高于此。

**单次调用内按对象传播约 0.067 ms/obj**（101 obj → 14.20 ms，1001 obj → 74.96 ms，差值 60.76 ms / 900 obj，截距约 7.4 ms）。12 ms 的 spawn 只相当于约 150 个对象的传播量。

**每次 launch 发多少次 `icacls`**：`src/backend/mod.rs:357` 的 `apply_siblings` → 1 次（有 sibling 时）；同文件 `:379` 的循环 → 每个非文件暴露 1 次；rollback → 每个记录过的路径 1 次。即约 `2 × (N_caller + 1 + [有 sibling ? 1 : 0])` 次。典型 1–2 个 exposure → **6–8 次约 72–150 ms**，对 30 s 预算是 **0.2–0.5%**。design 第 422 行的 "`2N` invocations" 是 Ruling 78 之前的算式。

**关键：那 32–38 s 换了 API 也一毫秒省不掉。** `SetNamedSecurityInfoW` 文档明文：设置 DACL 时系统会自动把可继承 ACE 传播到既有子对象。传播是**文件系统**在做，两个实现触发同一个内核路径。design 的数字与 `README.md` 的运维提示完全不受这次替换影响。**若有人想用 FFI 解决 readiness 预算，那是修错了层。**

**进程内到底多快**：用 .NET 的 `Get-Acl` + `SetAccessRule` 做代理，100 个文件 519.2 ms（5.19 ms/文件），对比同批文件走子进程 `/remove:g` 的 13.2 ms/文件。真实 Rust FFI 会远低于 5.19 ms。所以**单次快 10–20 倍是真的，只是调用次数太少，放大不出价值。**

---

## 6. 工具帮你做了、API 必须自己做的事

| 事项 | 结论 |
|---|---|
| SID 字符串解析 | **免费** —— 本 crate 根本不需要：`PackageSid::raw()` 直接给裸 `PSID`，`BuildTrusteeWithSidW` 不查名。反倒是**必须**走 SID 路径。 |
| 继承标志语义 | **自己写**：由 `grfInheritance` 表达，三个 `Target` → 两套 flag，约 10 行。但「`(OI)(CI)` 在文件上是 silent no-op」这条**会消失**（API 会照写），也就是 `tests/acl.rs:342` 钉的失败类**换了形状**而非被消除。 |
| ACE 顺序 | **免费，但只有用 `SetEntriesInAclW` 才免费**。手搓缓冲自建 ACL 就得不到。 |
| "successfully processed" 语义 | **丢掉，这是净收益**。工具至少两种情况 exit 0 却什么都没写。 |
| `\\?\` verbatim 路径 | **未验证的风险**。本 crate 把每个暴露都 canonicalize 成 `\\?\` 形式，并**实测过 `icacls` 接受它**（2026-09-19）。`SetNamedSecurityInfoW` 是否接受 `\\?\` **没有任何人测过**。这是整条 FFI 路线最脆弱的一环。 |
| `TreeSetNamedSecurityInfoW` | **不要用**：整树全有或全无、需树上每个对象的 `READ_CONTROL`+`WRITE_DAC`，`TREE_SEC_INFO_RESET` 还会连显式 ACE 一起抹掉。 |

---

## 7. 如果真的要换：规模与代价

**方案**：直接上 `windows-sys`，不要任何 crate。

**LOC**：真实代码约 150–200 行（grant ~35、revoke ~30 共享 helper、has_ace ~40、掩码/标志映射 ~30、错误映射 ~10、批量循环 ~10）。但按本 repo 的文档密度（`acl.rs` 890 行里约 120 行是代码），替换模块现实体量是 **600–900 行**。

**必须改的测试 / 文档**：

- `acl.rs:853` `the_icacls_binary_comes_from_the_system_directory_not_the_environment`、`acl.rs:869` `the_grant_argument_names_the_sid_literally` → 换成掩码/标志映射的测试
- `tests/confine.rs:285-286` 断言 `err.to_string().contains("icacls")` → 改为 Win32 错误码
- `tests/acl.rs`（465 行）**大部分原样保留** —— 那里 `icacls` 是**独立 oracle**，production 换 FFI 反而让 oracle 更独立
- **`icacls` 不会从 repo 里消失** —— 三个测试套件仍把它当 oracle，`docs/tests.md:135` 仍要求 gate 机器上有它
- design 文档：decision 14（第 145 行）**明文枚举了 unsafe 站点，并专门用 `acl` 作为反例**（"`acl.rs` carries no allow and makes no raw call"）。换成 FFI 要改这段、改 `allow(unsafe_code)` 计数、改 §6 与 §11 的那一行，外加 P2 plan 的 **81 处** `icacls`、`README.md:42`、`docs/tests.md:135,137`

**FFI 新引入的失败模式**：

1. 第 7 个 `unsafe` 站点（`acl.rs` 需 `#![allow(unsafe_code)]`），要按 decision 14(c) 写不变量 —— 特别是 `PSID` 生命周期与 `LocalFree` 归属
2. **错误码陷阱**：`SetEntriesInAclW` / `SetNamedSecurityInfoW` 返回 `WIN32_ERROR`（DWORD）**直接返回**，不经 `GetLastError`。`handles::last_error()`（读 `GetLastError()`）用在这里会报**陈旧**错误
3. `LocalFree` 两处新分配（新 ACL、`GetNamedSecurityInfoW` 的 `pSd`）；`windows-sys` 0.61 无 RAII。crate 里已有 `LocalFree` 先例（`sid.rs:235,449,514,571`）
4. 上文 `REVOKE_ACCESS` 对继承 ACE 的行为分歧
5. `\\?\` 路径未验证

---

## 8. 什么情况下答案会翻转（No → Yes）

1. **必须跑在没有 `icacls.exe` 的 Windows 镜像上。** 今天是：`acl.rs:257-259` 只是 `system_directory().join("icacls.exe")`，**存在性从未被检查**，缺失要到第一次 grant 才炸成一个 `Error::Launch`。这是唯一一个**按日程**而非按偏好翻转的条件 —— 一旦部署镜像确定没有它，子进程路线直接死。**本机已核：`System32` 与 `SysWOW64` 均在（WinSxS 另有 6 份备份），该条件不成立。** Server Core / Nano Server / Windows PE 是否带它，公开资料无法确认，若将来部署到这些镜像需先测。
2. **一次实测到的 spawn 成本压过预算。** 今天 12 ms × 6–8 次 ≈ 100 ms，不可能。除非 exposure 数变成几百个，或改成逐文件 grant（那时 20 万次调用约 40 分钟，子进程必死）。
3. **一个工具表达不了的批量。** 已核查：不是通配符这种情形；反过来，通配符对子进程是生命线。
4. **一条工具语义满足不了的正确性要求。** 最现实的一条：如果将来为绕开 32–38 s 的继承传播，设计改成**每文件写非继承 ACE**。那时调用数从约 6 涨到对象数，子进程路线经济性崩塌；而进程内 FFI 批量写（假设 ~0.1 ms/次，20 万对象约 20 s）**有可能**优于 NTFS 的 32–38 s —— 但对 ReFS 的 8.6 s 仍然输。这值得在 readiness 预算真正吃紧时**先测量再决定**。

---

## 9. 反对本结论的最强论点

**「静默成功」这一类，是留在子进程路线上真实存在的、未消除的缺陷类。** 本 crate 有两条回归测试（`tests/acl.rs:341`、`tests/acl.rs:222`）存在的唯一理由，就是 `icacls` **exit 0 却什么都没写**。加上 `has_ace` 今天是在**本地化过的** DACL 文本上做子串匹配（`acl.rs:355-364` 记录了 `S-1-5-32-545` 被渲染成 `BUILTIN\Users` 的陷阱），以及 §11 那一行不得不承认「错误类型要同时承载工具文本」。FFI 版把这三样一次性消掉，且按文档与 codex 先例，这不是研究项目而是约 60 行的配方。

**这条论点为什么仍不足以翻盘**：那两个静默场景在本 launcher 的实际调用模式里**都不可达** —— `apply_siblings` 永远传 `Target::File`（`acl.rs:546`），rollback 永远只 revoke 自己 grant 过的路径。也就是说，要换掉的是**已经被测试钉死的边界情况**，换来的是「更干净的诊断」，付出的是对整个 DACL 层的一次完整重证。对一个 32–38 s 才是真问题的功能，这笔账不划算。

---

## 附：可复现的测量记录

环境：Windows 11 Pro for Workstations 10.0.26200，`C:`/`D:` NTFS，`E:` ReFS。

| 测量 | 结果 |
|---|---|
| 300× `icacls <空目录>` 减循环开销 | 12.476 ms/次 |
| 100× `icacls /remove:g` 于 100 个文件 | 13.2 ms/文件 |
| 语料 101 obj → 1001 obj 的 grant 差值 | 0.0675 ms/obj，截距约 7.4 ms |
| `icacls <dir>\*.dll` 一次 / 200 文件 | 41.78 ms（grant），50.22 ms（rollback） |
| 200× 逐文件 `icacls /grant:r` | 4674.37 ms（23.37 ms/文件） |
| .NET `Get-Acl`+`SetAccessRule`+`Set-Acl` / 100 文件 | 5.192 ms/文件（进程内代理，含 cmdlet 开销） |
| `/remove:g` 于只有 `(I)` ACE 的文件 | **exit 0，报 success，ACE 未变** |
| `/grant:r` 后 `SE_DACL_PROTECTED` / owner | **均保留** |
| ACE 写入顺序（按 545→18→544 插入） | 读回 544, 18, 545 = **后写的最前**，印证 `SetEntriesInAclW` 文档 |
