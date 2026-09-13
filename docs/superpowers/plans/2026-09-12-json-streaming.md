# JSON 流式读取器实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `crates/tinio-select/src/json.rs` 重构为 LINES / DOCUMENT 两个流式处理器，共享 serde_json 流式内核与 `PathSeg` 编译状态机。内存有界性按帧化分档（spec 偏差③）：LINES ≤ 一行的匹配集（行跨度 ≤ 1 MB）；DOCUMENT 数组根 ≤ 一个元素的匹配集；DOCUMENT 单值根 = 一次拉取累积整份文档的匹配集，**但同样被 1 MB 记录预算兜底**（超限时拉取中途即报 cap，见偏差③与 Q15 修订）。`json::Reader::new` 签名不变，调用点零改动。

**Architecture:** serde_json `Deserializer::from_reader` + `DeserializeSeed` 逐值拉取；帧化过滤器（`Read` 适配器）使每次拉取恰好一个记录根并中途执行 cap；`PathSeg` 编译为 `CompiledPath`，以 serde `MapAccess`/`SeqAccess` 回调为事件流匹配，未命中子树用自写的校验型跳过 `Skip` 消费（不物化，且校验与今日 `from_slice` 逐点一致）；`json::Reader::new` façade 签名不变，调用点零改动。**cap 按"根拉取"计费**：处理器在每个根边界武装/解除 `Budget`，`BudgetedRead` 对这次拉取期间被供出的**每一个字节**计费（含未命中、被跳过的子树与注入的虚拟空白；framing 内部吞掉的顶层 `[`/`]`/`,`、`skip_ws` 的空白与 `\n` 由 framing 自行记账）——于是"一次拉取 = 一个根 ≤ 1 MB"是唯一需要维持的不变量。

**Tech Stack:** Rust 2024、serde_json 1.0.151（`arbitrary_precision` + `preserve_order`，crate 级 features，`crates/tinio-select/Cargo.toml:22-25`）、serde 1（本计划唯一的依赖改动，已 staged 未提交）、flate2（常规依赖，集成测试用）。

**Spec:** `docs/superpowers/specs/2026-09-12-json-streaming-design.md`（六项已确认偏差①–⑥的权威来源；执行时与本计划同读）

## Global Constraints

- **不做任何 git 写操作**（CLAUDE.md 覆盖 skill 的 commit 步骤）：每个任务以"检查点：报告待提交文件清单"收尾，**不执行** `git add/commit/push`。
- 代码、注释、测试、提交信息一律英文；本计划与 spec 中文（用户明示豁免）。
- `unsafe_code = "forbid"` 是**逐 crate** 设置（`docs/cargo.md:10`，`crates/tinio-select/Cargo.toml:51`），不存在 workspace 级设置；**唯一允许的依赖改动**是 `serde.workspace = true`（已 staged 于 `crates/tinio-select/Cargo.toml`，属 spec 范围内、预先批准）。
- 现有测试：除偏差④指定的两处填充输入外**一行不碰**；红了的既有断言 = 实现 bug，不是改测试的理由。
- 语义以 spec 偏差①–⑥为准：① DOCUMENT 总量解禁；② 非末步 Name 二义流内即时报；③ 单值根匹配集按文档累积（受 1 MB 记录预算兜底，见 Q15）；④ LINES 非法行可能先报解析错；⑤ DOCUMENT 错误流式暴露；⑥ DOCUMENT 元素级真实偏移 + ScanRange 过滤。
- **cap 按根拉取计费**（2026-09-13 评审修订，Q9(a)）：`Budget::reset()` 由**处理器**在每次根拉取开始时调用一次（**Amended 2026-09-13**：原为 `arm()`/`disarm()` 一对；`BudgetedRead` 只在拉取块内存活，`armed` 标志恒真已删），matcher 的两个 seed 不持有 `Budget`；`BudgetedRead` 计这一次拉取期间被供出的每一个字节（framing 内部吞掉的——顶层 `[`/`]`/`,`、`skip_ws` 吞掉的空白、LINES 的 `\n`——不经它，由 framing 的 span/偏移记账兜底）。由此 cap 的语义是"一个根 ≤ 1 MB"——数组模式即一个元素、单值模式即整份文档，未命中子树同样受约束（不再有"跳过区不受 cap"的缺口）。
- 文档风格（docs/style.md）：compressed prose、imports 3+ 段去限定、错误经 per-crate `Error`。
- **实现纪律**：接口、测试与语义以本计划与 spec 为准；内联实现体在编译器约束下可调整，但每处调整必须在执行报告里逐条回报（已知两处由 rustc 1.98.1 `--edition 2024` 实测裁决：Task 4 的 `serve`/`skip_ws` 借用重构、Task 5 的 seed 形状）。
- **内存闸**：spec 的计数 `#[global_allocator]` 不可用（`crates/tinio-select/Cargo.toml:51` 的 `unsafe_code = "forbid"` 覆盖本 crate 全部 target，含 `tests/`，且 `forbid` 不可被 `allow` 覆盖），改为 crate 内 `#[cfg(test)]` 的 `pending_high` 高水位 pin（Task 7 的两条单测：数组根有界 + 偏差③例外），`tests/stream_memory.rs` 取消。
- 已核实的 serde_json 1.0.151 事实：`arbitrary_precision` 下**非整数**数字（`1.5`、`1e309`、≥ 2^64 的整数）以单键 map 回调访问者，键为 `"$serde_json::private::Number"`（`src/number.rs:17-18`，`pub(crate)` 不可导入，硬编码 + 测试钉住），**普通整数走 `visit_u64/i64`**（`de.rs:117-129` 的 `ParserNumber::visit`）；`serde_json::Error` 有 `is_eof()/is_io()/io_error_kind()`，**没有** `into_io_error`；`impl Deserializer for &mut Deserializer` 存在（`de.rs:1389`）；`de::IoRead` **逐字节**读取（`read.rs:200-207` 用 std 的 `io::Bytes`——逐字节来自 std `Bytes` 的实现，每次 `read` 恰好 1 字节），并把 peek 到的字节存放在自己的 `ch` 里（`read.rs:286-298`）——**该字节已从 framing 消费，丢弃这个 Deserializer 就永久丢失**；`de::IgnoredAny` 只做结构校验，其 `ignore_str`（`read.rs:348-366`）不校验代理对与 UTF-8（故所有跳过一律用自写 `Skip`，见 Task 5）。

---

### Task 1: 模块骨架拆分（json.rs → json/mod.rs + json/value.rs）

**Files:**
- Create: `crates/tinio-select/src/json/mod.rs`（由 `json.rs` 整体迁入）
- Create: `crates/tinio-select/src/json/value.rs`
- Delete: `crates/tinio-select/src/json.rs`（由前两项替代）

**Interfaces:**
- Consumes: 现有 `json.rs` 全部内容（`Type`/`Params`/`to_value`/`json_lookup`/`Reader`/测试）。
- Produces: `crate::json::value::{to_value, json_lookup}`；`crate::json::{to_value, json_lookup}` 经 `pub(crate) use` 再导出，`engine.rs` 与既有测试路径不变。

- [ ] **Step 1: 纯移动拆分**

`json/value.rs`（从旧 `json.rs` 原样剪切 `to_value` 与 `json_lookup`，含全部文档注释）：

```rust
//! Row-value mapping and object key lookup — the engine's select side and
//! the traversal matcher share these (review 2026-09-06b R10: the two copies
//! used to drift).

use serde_json::Value;

use crate::{
    error::Error,
    row::{NameStyle, Value as RowValue},
};

pub(crate) fn to_value(j: &Value) -> RowValue { /* 原样 */ }
pub(crate) fn json_lookup<'a>(
    map: &'a serde_json::Map<String, Value>,
    name: &str,
    style: NameStyle,
) -> Result<Option<&'a Value>, Error> { /* 原样 */ }
```

`json/mod.rs` 顶部加：

```rust
pub(crate) mod value;
pub(crate) use value::{json_lookup, to_value};
```

并从 `mod.rs` 删除两个函数的本地定义（`use crate::row::{NameStyle, Value as RowValue}` 若仅剩测试用则移到测试模块）。`lib.rs` 的 `pub mod json;` 自动解析到 `json/mod.rs`，无需改动。

- [ ] **Step 2: 跑套件验证纯移动**

Run: `cargo test -p tinio-select`
Expected: 全绿（与拆前一致）；`cargo check --workspace --all-targets` 无警告。

- [ ] **Step 3: 检查点**

报告：`crates/tinio-select/src/json.rs`（删除）、`json/mod.rs`、`json/value.rs`、`crates/tinio-select/Cargo.toml`（serde 依赖）待提交。**不执行 commit。**

---

### Task 2: framing.rs — Budget + BudgetedRead

**Files:**
- Create: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: `crate::record::{CAP_MESSAGE, MAX_RECORD}`。
- Produces: `pub(crate) struct Budget { reset, fired }`——**reset 由处理器在每次根拉取开始时调用**（Task 6/7；**Amended 2026-09-13**：`arm`/`disarm` 合并为 `reset`），matcher 的 seed 不持有它；`pub(crate) struct BudgetedRead<R>::new(inner, Rc<Budget>)`，实现 `Read`；`fn ws(b: u8) -> bool`。后续 Task 3–7 全部依赖。

- [ ] **Step 1: 写失败测试**

`json/framing.rs` 尾部 `#[cfg(test)] mod tests`：

```rust
#[test]
fn budgeted_read_fires_mid_stream() {
    use std::io::Read;
    let budget = std::rc::Rc::new(Budget::default());
    budget.arm();
    let mut r = BudgetedRead::new(
        std::io::Cursor::new(vec![b'a'; (MAX_RECORD + 1) as usize]),
        budget.clone(),
    );
    let mut sink = Vec::new();
    let err = r.read_to_end(&mut sink).unwrap_err();
    assert_eq!(err.to_string(), CAP_MESSAGE);
    assert!(budget.fired());
}

#[test]
fn budgeted_read_exact_cap_allowed() {
    use std::io::Read;
    let budget = std::rc::Rc::new(Budget::default());
    let mut r = BudgetedRead::new(
        std::io::Cursor::new(vec![b'a'; MAX_RECORD as usize]),
        budget.clone(),
    );
    budget.arm();
    let mut sink = Vec::new();
    r.read_to_end(&mut sink).unwrap();
    assert_eq!(sink.len() as u64, MAX_RECORD);
    assert!(!budget.fired());
    budget.disarm();
    assert!(!budget.fired());
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select framing`
Expected: 编译失败（`Budget`/`BudgetedRead` 不存在）。

- [ ] **Step 3: 实现**

```rust
//! Framing filters: `Read` adapters between serde_json and the input stream.
//! They reshape the byte stream so each serde pull parses exactly one record
//! root, enforce the 1 MB input-record cap mid-read, and account offsets in
//! uncompressed bytes.

use std::{
    cell::Cell,
    io::{self, BufRead, BufReader, Read},
    rc::Rc,
};

use crate::record::{CAP_MESSAGE, MAX_RECORD};

/// JSON whitespace per RFC 8259.
pub(crate) fn ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Per-root-pull budget: the handler arms it before a root pull and disarms
/// it after, so every byte serde is served during that pull is charged —
/// matched, skipped and injected-delimiter alike (bytes the framing consumes
/// internally — top-level `[` `]` `,`, whitespace skipped by `skip_ws`, and
/// the LINES `\n` — never reach this layer). One pull is exactly one record
/// root, so the cap this enforces is "one root ≤ `MAX_RECORD`" (an array
/// element in DOCUMENT array mode, the whole document in single mode). The
/// handler maps `fired` to `Error::Format(CAP_MESSAGE)`.
#[derive(Default)]
pub(crate) struct Budget {
    armed: Cell<bool>,
    remaining: Cell<u64>,
    fired: Cell<bool>,
}

impl Budget {
    pub(crate) fn arm(&self) {
        self.armed.set(true);
        self.fired.set(false);
        self.remaining.set(MAX_RECORD);
    }

    pub(crate) fn disarm(&self) {
        self.armed.set(false);
    }

    pub(crate) fn fired(&self) -> bool {
        self.fired.get()
    }

    fn charge(&self, n: usize) -> io::Result<()> {
        if !self.armed.get() || n == 0 {
            return Ok(());
        }
        let r = self.remaining.get();
        if n as u64 > r {
            self.remaining.set(0);
            self.fired.set(true);
            return Err(io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE));
        }
        self.remaining.set(r - n as u64);
        Ok(())
    }
}

/// `Read` wrapper charging every served byte to the shared `Budget` — armed
/// by the handler around a whole root pull, so the charge covers everything
/// serde is served: matched values, skipped subtrees and the injected
/// delimiter alike. Bytes the framing consumes internally (top-level
/// `[` `]` `,`, whitespace skipped by `skip_ws`, the LINES `\n`) never pass
/// through here; they are bounded by the framing's own span accounting
/// instead.
pub(crate) struct BudgetedRead<R> {
    inner: R,
    budget: Rc<Budget>,
}

impl<R: Read> BudgetedRead<R> {
    pub(crate) fn new(inner: R, budget: Rc<Budget>) -> Self {
        Self { inner, budget }
    }
}

impl<R: Read> Read for BudgetedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.budget.charge(n)?;
        Ok(n)
    }
}
```

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select framing`
Expected: 两个测试 PASS。

- [ ] **Step 5: 检查点**

报告 `crates/tinio-select/src/json/framing.rs` 待提交（并在 `json/mod.rs` 加 `pub(crate) mod framing;`）。

---

### Task 3: framing.rs — LineFraming

**Files:**
- Modify: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: Task 2 的 `ws`/`MAX_RECORD`/`CAP_MESSAGE`。
- Produces: `pub(crate) struct LineFraming<R: Read>`：`new(BufReader<R>)`、`start_line() -> io::Result<LineStart>`、`capped: bool` 字段；`pub(crate) enum LineStart { Line(u64), Eof }`。Task 6 依赖。

- [ ] **Step 1: 写失败测试**

```rust
fn framing(bytes: &[u8]) -> LineFraming<std::io::Cursor<Vec<u8>>> {
    LineFraming::new(BufReader::new(std::io::Cursor::new(bytes.to_vec())))
}

#[test]
fn lines_serve_content_then_eof_until_next_line() {
    use std::io::Read;
    let mut f = framing(b"{\"a\":1}\n{\"b\":2}\n");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap(); // 行尽即 EOF
    assert_eq!(s, "{\"a\":1}");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(8)));
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    assert_eq!(s, "{\"b\":2}");
    assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
}

#[test]
fn lines_blank_line_serves_immediate_eof_but_exists() {
    use std::io::Read;
    let mut f = framing(b"\n");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut buf = [0u8; 4];
    assert_eq!(f.read(&mut buf).unwrap(), 0); // 空行 = 存在但无内容
    assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
}

#[test]
fn lines_cap_fires_mid_serve_and_exact_span_allowed() {
    use std::io::Read;
    let mut f = framing(&vec![b'a'; (MAX_RECORD + 1) as usize]); // 无终止符
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut sink = Vec::new();
    assert_eq!(f.read_to_end(&mut sink).unwrap_err().to_string(), CAP_MESSAGE);
    assert!(f.capped);

    let mut f = framing(&{
        let mut v = vec![b'a'; (MAX_RECORD - 1) as usize];
        v.push(b'\n');
        v
    });
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut sink = Vec::new();
    f.read_to_end(&mut sink).unwrap();
    assert_eq!(sink.len() as u64, MAX_RECORD - 1);
    assert!(!f.capped);
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select lines_`
Expected: 编译失败。

- [ ] **Step 3: 实现（追加到 framing.rs）**

```rust
/// What `start_line` found at the stream position.
pub(crate) enum LineStart {
    /// A line exists (blank included); carries its start offset.
    Line(u64),
    /// Clean end of input: no more lines.
    Eof,
}

/// Serves one LINES line at a time: the line's content bytes (without the
/// `\n`), then EOF until `start_line` advances. The span (content +
/// terminator) is counted while serving and errors mid-serve past
/// `MAX_RECORD` — a line is never buffered whole.
pub(crate) struct LineFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    span: u64,
    eol: bool,
    pub(crate) capped: bool,
}

impl<R: Read> LineFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self { inner, offset: 0, span: 0, eol: true, capped: false }
    }

    pub(crate) fn start_line(&mut self) -> io::Result<LineStart> {
        debug_assert!(self.eol, "start_line before the previous line drained");
        if self.inner.fill_buf()?.is_empty() {
            return Ok(LineStart::Eof);
        }
        self.eol = false;
        self.span = 0;
        Ok(LineStart::Line(self.offset))
    }

    fn cap(&mut self) -> io::Error {
        self.capped = true;
        io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE)
    }
}

impl<R: Read> Read for LineFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.eol || out.is_empty() {
            return Ok(0);
        }
        let avail = self.inner.fill_buf()?;
        if avail.is_empty() {
            // Unterminated final line: EOF ends its content.
            self.eol = true;
            return Ok(0);
        }
        let nl = avail.iter().position(|&b| b == b'\n');
        let content = nl.unwrap_or(avail.len());
        let n = content.min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.inner.consume(n);
        self.offset += n as u64;
        self.span += n as u64;
        if n == content && nl.is_some() {
            self.inner.consume(1);
            self.offset += 1;
            self.span += 1;
            self.eol = true;
        }
        if self.span > MAX_RECORD {
            return Err(self.cap());
        }
        Ok(n)
    }
}
```

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select lines_`
Expected: 三个测试 PASS；既有套件仍绿。

- [ ] **Step 5: 检查点**

报告 `json/framing.rs` 待提交。

---

### Task 4: framing.rs — DocFraming

**Files:**
- Modify: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: Task 2 的 `ws`。
- Produces: `pub(crate) struct DocFraming<R: Read>`：`new(BufReader<R>)`、`peek_mode() -> io::Result<DocMode>`（幂等，缓存且不消耗首字节；Task 7 在建立 `Deserializer` 之前用它取模式）、`closed() -> bool`（顶层 `]` 已消费；Task 7 用它把 `[]` 与"值中途 EOF"分开），pub(crate) 字段 `mode: DocMode`、`element_start: u64`、`framing_msg: Option<String>`；`pub(crate) enum DocMode { Unknown, Empty, WsOnly, Array, Single }`。Task 7 依赖。行为契约：数组模式下每次 serde 拉取恰好解析一个元素（顶层 `[`/`]`/`,` 被吞并校验，标量元素尾注入一个虚拟空格）；单值模式透传；`element_start` = 当前元素首字节偏移。

- [ ] **Step 1: 写失败测试（经真实 serde 拉取验证字节整形）**

```rust
fn pull_values(bytes: &str) -> (Vec<serde_json::Value>, Vec<u64>, DocMode) {
    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(bytes.as_bytes().to_vec())));
    let mut vals = Vec::new();
    let mut starts = Vec::new();
    loop {
        let r = {
            let mut de = serde_json::Deserializer::from_reader(&mut f);
            serde_json::Value::deserialize(&mut de)
        };
        match r {
            Ok(v) => {
                vals.push(v);
                starts.push(f.element_start);
            }
            Err(e) if e.is_eof() => break,
            Err(e) => panic!("{bytes}: {e}"),
        }
        if f.mode == DocMode::Single {
            break;
        }
    }
    (vals, starts, f.mode)
}

#[test]
fn doc_array_elements_pull_one_at_a_time_with_offsets() {
    let (vals, starts, mode) = pull_values("[{\"id\": 1}, {\"id\": 2}]");
    assert_eq!(vals, vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})]);
    assert_eq!(mode, DocMode::Array);
    assert_eq!(starts[0], 1);
    assert!(starts[1] > starts[0]);
}

#[test]
fn doc_scalar_and_nested_elements() {
    let (vals, ..) = pull_values("[1, \"two\", true, null, [1, 2], {\"a\": \"x]y\"}]");
    assert_eq!(
        vals,
        vec![
            serde_json::json!(1),
            serde_json::json!("two"),
            serde_json::json!(true),
            serde_json::Value::Null,
            serde_json::json!([1, 2]),
            serde_json::json!({"a": "x]y"}),
        ]
    );
}

#[test]
fn doc_single_object_pulls_once() {
    let (vals, starts, mode) = pull_values("{\n \"a\": 1\n}");
    assert_eq!(vals, vec![serde_json::json!({"a": 1})]);
    assert_eq!(mode, DocMode::Single);
    assert_eq!(starts, vec![0]);
}

#[test]
fn doc_separator_errors_are_framing_errors() {
    for (input, msg) in [
        ("[1,]", "trailing comma"),
        ("[1 2]", "expected `,` or `]`"),
        ("[1", "unexpected end of input"),
        ("[1] x", "trailing characters"),
    ] {
        let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(input.as_bytes().to_vec())));
        let mut errs = 0;
        loop {
            let r = {
                let mut de = serde_json::Deserializer::from_reader(&mut f);
                serde_json::Value::deserialize(&mut de)
            };
            match r {
                Ok(_) => {}
                Err(e) if e.is_eof() => break,
                Err(_) => {
                    errs += 1;
                    assert!(
                        f.framing_msg.as_deref().is_some_and(|m| m.contains(msg)),
                        "{input}: {:?}",
                        f.framing_msg
                    );
                    break;
                }
            }
        }
        assert_eq!(errs, 1, "{input}");
    }
}

#[test]
fn doc_empty_and_ws_only_modes() {
    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(Vec::<u8>::new())));
    let r = {
        let mut de = serde_json::Deserializer::from_reader(&mut f);
        serde_json::Value::deserialize(&mut de)
    };
    assert!(r.unwrap_err().is_eof());
    assert_eq!(f.mode, DocMode::Empty);

    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(b"  \n".to_vec())));
    let r = {
        let mut de = serde_json::Deserializer::from_reader(&mut f);
        serde_json::Value::deserialize(&mut de)
    };
    assert!(r.unwrap_err().is_eof());
    assert_eq!(f.mode, DocMode::WsOnly);
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select doc_`
Expected: 编译失败。

- [ ] **Step 3: 实现（追加到 framing.rs）**

```rust
/// The document's root shape, resolved lazily on the first read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocMode {
    Unknown,
    /// Zero bytes of input.
    Empty,
    /// Only whitespace before EOF.
    WsOnly,
    /// Root is an array: elements are served one per pull.
    Array,
    /// Root is any other value: served verbatim, one pull.
    Single,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    ExpectElement { after_comma: bool },
    ServingElement,
    InjectSpace,
    ExpectSep,
    AfterRoot,
    ServeSingle,
    Done,
}

/// Reshapes a DOCUMENT stream so each serde pull parses exactly one root
/// value. Array mode swallows (and validates) the top-level `[` `]` `,`,
/// injects one virtual space at each element end (a clean terminator for
/// number tokens), and records each element's first-byte offset. Single
/// mode passes the stream through verbatim.
pub(crate) struct DocFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    state: State,
    in_string: bool,
    escape: bool,
    depth: u64,
    pub(crate) mode: DocMode,
    pub(crate) element_start: u64,
    pub(crate) framing_msg: Option<String>,
}

impl<R: Read> DocFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self {
            inner,
            offset: 0,
            state: State::Start,
            in_string: false,
            escape: false,
            depth: 0,
            mode: DocMode::Unknown,
            element_start: 0,
            framing_msg: None,
        }
    }

    fn peek(&mut self) -> io::Result<Option<u8>> {
        Ok(self.inner.fill_buf()?.first().copied())
    }

    fn consume(&mut self, n: usize) {
        self.inner.consume(n);
        self.offset += n as u64;
    }

    fn skip_ws(&mut self) -> io::Result<()> {
        loop {
            // Borrow scope: `avail` borrows `self.inner`, so only scalars may
            // cross out of it — `self.consume` needs `&mut self`.
            let (n, done) = {
                let avail = self.inner.fill_buf()?;
                let n = avail.iter().position(|&b| !ws(b)).unwrap_or(avail.len());
                (n, avail.is_empty() || n < avail.len())
            };
            self.consume(n);
            if done {
                return Ok(());
            }
        }
    }

    fn framing_err(&mut self, msg: &str) -> io::Error {
        self.framing_msg = Some(msg.to_string());
        io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
    }

    /// Resolve the root shape and cache it without consuming the first
    /// significant byte. The handler asks *before* the pull — whether the
    /// trailing probe must run depends on the mode, and once the
    /// `Deserializer` exists it holds `&mut self.framing`, so the mode can no
    /// longer be read. Idempotent: the `Start` arm calls it too, so callers
    /// that never ask still get the mode resolved.
    pub(crate) fn peek_mode(&mut self) -> io::Result<DocMode> {
        if self.mode != DocMode::Unknown {
            return Ok(self.mode);
        }
        self.skip_ws()?;
        self.mode = match self.peek()? {
            None if self.offset == 0 => DocMode::Empty,
            None => DocMode::WsOnly,
            Some(b'[') => DocMode::Array,
            Some(_) => DocMode::Single,
        };
        Ok(self.mode)
    }

    /// Whether the root array has closed cleanly at the top level (the `]` was
    /// consumed). The handler needs this to tell `[]` — the one MISSING row of
    /// the `S3Object[*]` zero-match rule — from an EOF that struck *inside* a
    /// value, which is a malformed document and must error like today's
    /// whole-document `from_slice` did.
    pub(crate) fn closed(&self) -> bool {
        matches!(self.state, State::AfterRoot | State::Done)
    }

    /// Advance the machine after `n` served bytes have been copied out. The
    /// copy itself lives in the caller, inside the borrow scope of `avail`:
    /// `avail` borrows `self.inner`, so no `&mut self` method is callable
    /// while it is live (rustc 1.98.1 reports E0499 for the `serve(out, avail,
    /// …)` shape this replaces).
    fn advance(&mut self, n: usize, next: State) -> usize {
        self.consume(n);
        self.state = next;
        n
    }
}

impl<R: Read> Read for DocFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::InjectSpace => {
                    out[0] = b' ';
                    self.state = State::ExpectSep;
                    return Ok(1);
                }
                State::Start => match self.peek_mode()? {
                    DocMode::Array => {
                        self.consume(1);
                        self.state = State::ExpectElement { after_comma: false };
                    }
                    DocMode::Single => {
                        self.element_start = self.offset;
                        self.state = State::ServeSingle;
                    }
                    // Empty / WsOnly end the stream; `Unknown` cannot occur
                    // (peek_mode always resolves it).
                    _ => self.state = State::Done,
                },
                State::ExpectElement { after_comma } => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(self.framing_err("unexpected end of input inside the root array"))
                        }
                        Some(b']') if !after_comma => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(b']') => return Err(self.framing_err("trailing comma in the root array")),
                        Some(b',') => return Err(self.framing_err("expected an array element")),
                        Some(_) => {
                            self.element_start = self.offset;
                            self.depth = 0;
                            self.state = State::ServingElement;
                        }
                    }
                }
                State::ServingElement => {
                    // Scan and copy live in one borrow scope: `avail` borrows
                    // `self.inner`, and only scalars may cross out of it so the
                    // `&mut self` advance stays callable (E0499 otherwise).
                    // Field writes (`depth`/`in_string`/`escape`/`state`) are
                    // disjoint from `self.inner` and therefore fine in scope.
                    let (n, next) = {
                        let avail = self.inner.fill_buf()?;
                        if avail.is_empty() {
                            // EOF: depth > 0 → serde reports the truncated
                            // value; depth == 0 → the scalar ends and ExpectSep
                            // flags the unterminated array.
                            if self.depth == 0 {
                                self.state = State::ExpectSep;
                            }
                            return Ok(0);
                        }
                        let mut n = 0;
                        let mut next = State::ServingElement;
                        while n < avail.len() && n < out.len() {
                            let b = avail[n];
                            if self.in_string {
                                if self.escape {
                                    self.escape = false;
                                } else if b == b'\\' {
                                    self.escape = true;
                                } else if b == b'"' {
                                    self.in_string = false;
                                }
                                n += 1;
                                continue;
                            }
                            match b {
                                b'"' => {
                                    self.in_string = true;
                                    n += 1;
                                }
                                b'{' | b'[' => {
                                    self.depth += 1;
                                    n += 1;
                                }
                                b'}' | b']' if self.depth > 0 => {
                                    self.depth -= 1;
                                    n += 1;
                                    if self.depth == 0 {
                                        next = State::InjectSpace;
                                        break;
                                    }
                                }
                                b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r' if self.depth == 0 => {
                                    // A scalar element ends BEFORE this byte
                                    // (delimiter left in the stream for
                                    // ExpectSep). n == 0 only for a malformed
                                    // empty element (`[}]`): serving nothing
                                    // lets serde report the truncated value —
                                    // no debug_assert: it would panic debug
                                    // builds on untrusted input.
                                    next = State::InjectSpace;
                                    break;
                                }
                                _ => n += 1,
                            }
                        }
                        out[..n].copy_from_slice(&avail[..n]);
                        (n, next)
                    };
                    return Ok(self.advance(n, next));
                }
                State::ExpectSep => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(self.framing_err("unexpected end of input inside the root array"))
                        }
                        Some(b',') => {
                            self.consume(1);
                            self.state = State::ExpectElement { after_comma: true };
                        }
                        Some(b']') => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(_) => return Err(self.framing_err("expected `,` or `]` in the root array")),
                    }
                }
                State::AfterRoot => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => self.state = State::Done,
                        Some(_) => return Err(self.framing_err("trailing characters after the JSON document")),
                    }
                }
                State::ServeSingle => {
                    let n = self.inner.read(out)?;
                    self.offset += n as u64;
                    if n == 0 {
                        self.state = State::Done;
                    }
                    return Ok(n);
                }
            }
        }
    }
}
```

注意 `State::Start` 里 `Some(b'[')` 分支消耗 `[` 后 `element_start` 不设——元素偏移在 `ExpectElement` 进入 `ServingElement` 时记录。

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select doc_`
Expected: 五个测试 PASS；既有套件仍绿。

- [ ] **Step 5: 检查点**

报告 `json/framing.rs` 待提交。

---

### Task 5: matcher.rs — CompiledPath + Skip + CaptureSeed/StepSeed

**Files:**
- Create: `crates/tinio-select/src/json/matcher.rs`
- Modify: `crates/tinio-select/src/json/mod.rs`（加 `pub(crate) mod matcher;`）

**Interfaces:**
- Consumes: `crate::path::PathSeg`；`crate::error::Error`；`crate::json::Type`。
- Produces: `pub(crate) struct CompiledPath { from_segments(segments: &[PathSeg], _ty: Type) -> Self, steps() -> &[Step] }`（`ty` 是 spec 的签名参数，matcher 不分支于它，故下划线）；`pub(crate) enum Step { Name(String), Index(usize), Wild }`（**Amended 2026-09-13**：`Step` 已删除——它是 `PathSeg` 的逐字段克隆、无新增行为，`CompiledPath.steps` 直接用 `Vec<PathSeg>`，`steps()` 返回 `&[PathSeg]`）；`pub(crate) struct CaptureSeed<'a> { pending: &'a mut VecDeque<Option<Value>> }`（**Amended 2026-09-13**：原声明含 `failure: &'a mut Option<Error>` 字段；capture 只在步耗尽后运行、不可能记录二义，该字段在实现中始终未用，已删——处理器另经未计划的 `child_into` 进入 seed，见下文）、`pub(crate) struct StepSeed<'a> { steps: &'a [Step], pending, failure }`（**无 `Sink` 包装**，见 Task 5 注记；**无 `budget` 字段**——cap 由处理器按根拉取武装，见 Global Constraints），均实现 `serde::de::DeserializeSeed<'de, Value = ()>`；`pub(crate) struct Skip`（校验型跳过，取代 `IgnoredAny`），实现 `DeserializeSeed` + `Visitor`。Task 6/7 依赖。**（Amended 2026-09-13）**`from_segments` 的 `_ty: Type` 形参已删除，签名改为 `from_segments(segments: &[PathSeg])`——根规则由帧化承担，matcher 不分支于 `Type`，该形参始终未用；下方代码块及 Task 6/7/8 中所有 `Type::Lines`/`Type::Document` 实参同此作废，以实际代码为准。

- [ ] **Step 1: 写失败测试（驱动于 `Deserializer::from_slice`，不依赖 framing）**

```rust
fn match_rows(segments: &[PathSeg], input: &str) -> Vec<Option<Value>> {
    let path = CompiledPath::from_segments(segments, Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(input.as_bytes());
        if path.steps().is_empty() {
            CaptureSeed { pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        } else {
            StepSeed {
                steps: path.steps(),
                pending: &mut out,
                failure: &mut failure,
            }
            .deserialize(&mut de)
        }
    };
    assert!(r.is_ok(), "{input}: {:?}", r.err());
    assert!(failure.is_none(), "{input}: {failure:?}");
    out.into_iter().collect()
}

fn segs(sql_path: &[PathSeg]) -> Vec<PathSeg> { sql_path.to_vec() }

#[test]
fn wild_over_int_scalar_is_missing() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": 5}"
        ),
        vec![None]
    );
}

#[test]
fn wild_over_float_scalar_is_missing() {
    // arbitrary_precision：浮点数以 NUMBER_TOKEN 单键 map 回调——必须识别为标量。
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": 1.5}"
        ),
        vec![None]
    );
}

#[test]
fn index_over_number_map_is_missing() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Index(0)]),
            "{\"x\": 1e309}"
        ),
        vec![None]
    );
}

#[test]
fn number_token_first_key_object_errors_not_panics() {
    // A real object with the reserved first key is not a number — and must
    // never trip a debug assert (untrusted input). Today's
    // `from_slice::<Value>` rejects it (invalid number); so do we.
    let path =
        CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())], Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(
            br#"{"$serde_json::private::Number": 1, "a": 2}"#,
        );
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert!(out.is_empty());
}

#[test]
fn number_token_single_entry_is_a_scalar() {
    // serde_json's documented `arbitrary_precision` collision: the key is
    // reserved, and today's `from_slice::<Value>` turns this object into a
    // number too (value/de.rs:125-129) — a step over it is a zero-match
    // either way.
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": {\"$serde_json::private::Number\": \"1.5\"}}"
        ),
        vec![None]
    );
}

#[test]
fn escaped_key_matches_case_insensitively() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("rules".into()), PathSeg::Wild]),
            "{\"R\\u0075les\": [1]}"
        ),
        vec![Some(serde_json::json!(1))]
    );
}

#[test]
fn final_name_holds_value_until_object_close() {
    // 重复折叠键：暂缓产出的值不得先行入队，报错时 pending 仍为空。
    let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("id".into())], Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(b"{\"id\": 1, \"ID\": 2}");
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert_eq!(failure, Some(Error::Ambiguous("id".into())));
    assert!(out.is_empty());
}

#[test]
fn non_final_name_duplicate_fails_mid_stream() {
    let path = CompiledPath::from_segments(
        &[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild],
        Type::Lines,
    );
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(b"{\"a\": [1], \"A\": [2]}");
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert_eq!(failure, Some(Error::Ambiguous("a".into())));
    // 偏差②：报错前已产出的行保留。
    assert_eq!(out.into_iter().collect::<Vec<_>>(), vec![Some(serde_json::json!(1))]);
}

#[test]
fn zero_match_rules_per_step() {
    // Name miss / Index out of range / Wild over empty / 链式 MISSING 传播。
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Name("a".into())]), "{\"b\": 1}"),
        vec![None]
    );
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Index(9)]), "[1]"),
        vec![None]
    );
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Wild]), "[]"),
        vec![None]
    );
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild, PathSeg::Name("b".into())]),
            "{\"c\": 1}"
        ),
        vec![None]
    );
}

#[test]
fn nested_wild_order_matches_dfs() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild, PathSeg::Name("b".into())]),
            "{\"a\": [{\"b\": 1}, {}, {\"b\": 2}]}"
        ),
        vec![Some(serde_json::json!(1)), None, Some(serde_json::json!(2))]
    );
}

#[test]
fn skipped_subtree_still_validates_string_content() {
    // `Skip` replaces `IgnoredAny` precisely so a subtree the path never reads
    // is validated the way today's whole-line `from_slice` validates it: a lone
    // surrogate escape and a raw non-UTF-8 byte must both fail here, and both
    // would have been accepted under `IgnoredAny`.
    for input in [
        &b"{\"b\":\"\\uD800\",\"a\":1}"[..],
        &b"{\"b\":\"\xff\",\"a\":1}"[..],
    ] {
        let path =
            CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())], Type::Lines);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(input);
            StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
                .deserialize(&mut de)
        };
        assert!(r.is_err(), "{input:?} must not be accepted");
        assert!(out.is_empty(), "{input:?}");
    }
}

/// One root pull, with the budget armed and the framing wrapped in
/// `BudgetedRead` exactly as the handlers do it (Task 6/7) — the fix under
/// test is *where* `arm` is called, so the test has to reproduce the whole
/// charging path. An armed budget without `BudgetedRead` is never charged:
/// `fired` would be vacuously false and the boundary assertions below would
/// prove nothing.
fn pull_with_budget(framing: &mut DocFraming<std::io::Cursor<Vec<u8>>>) -> (Vec<Value>, bool) {
    use std::rc::Rc;

    use crate::json::framing::{Budget, BudgetedRead};
    let budget = Rc::new(Budget::default());
    let path = CompiledPath::from_segments(&[PathSeg::Wild], Type::Document);
    let mut out = VecDeque::new();
    let mut failure = None;
    budget.arm();
    {
        let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
            &mut *framing,
            Rc::clone(&budget),
        ));
        let r = if path.steps().is_empty() {
            CaptureSeed { pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        } else {
            StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        };
        let _ = r;
    }
    budget.disarm();
    (out.into_iter().collect(), budget.fired())
}

#[test]
fn element_barely_under_cap_is_accepted() {
    // The boundary that decides where `arm` lives: delivered bytes stay one
    // byte under `MAX_RECORD`, so the record passes only when the budget is
    // armed once for the whole pull. One byte of margin, not zero, because
    // the assertion relies on a serde fact: a string is self-delimiting and
    // nothing past the closing quote is ever read — keep the margin so that
    // fact is not on the critical path. The exact 1 MB line is pinned at
    // the handler level by `document_input_record_cap_exact_allowed`
    // (json.rs:691).
    let content = "a".repeat(MAX_RECORD as usize - 3);
    let (vals, fired) = pull_with_budget(&mut DocFraming::new(BufReader::new(
        std::io::Cursor::new(format!("[\"{content}\"]").into_bytes()),
    )));
    assert!(!fired, "a record of exactly MAX_RECORD delivered bytes must pass");
    assert_eq!(vals.len(), 1);
}

#[test]
fn budget_is_charged_per_root_not_accumulated() {
    // Each root is one pull and one arm: four 400 KB elements total ~1.6 MB,
    // yet no single pull exceeds the cap. A cumulative budget would fire on
    // the second element and reject a document the framing accepts.
    // One array of four string elements — four concatenated arrays would be
    // trailing characters after the first root, not a document.
    let elem = |c: char| format!("\"{}\"", c.to_string().repeat(400 * 1024));
    let doc = format!("[{}, {}, {}, {}]", elem('a'), elem('b'), elem('c'), elem('d'));
    let mut framing = DocFraming::new(BufReader::new(std::io::Cursor::new(doc.into_bytes())));
    let mut total = 0usize;
    loop {
        let (vals, fired) = pull_with_budget(&mut framing);
        assert!(!fired, "the cap must count one root, not the document");
        total += vals.len();
        if vals.is_empty() {
            break;
        }
    }
    assert_eq!(total, 4);
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select matcher`
Expected: 编译失败。

- [ ] **Step 3: 实现**

```rust
//! `PathSeg` compiled to a state machine, and the serde `DeserializeSeed`
//! visitors running it over the streaming event flow (`MapAccess` /
//! `SeqAccess` are the events). Unmatched subtrees go through `Skip`, a
//! validating skip that replaces `IgnoredAny`: nothing materializes, and
//! string contents are validated exactly as today's whole-line `from_slice`
//! validates them.

use std::collections::VecDeque;

use serde::{
    Deserialize,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;

use crate::{
    error::Error,
    json::Type,
    path::PathSeg,
};

/// serde_json 1.0.151 `src/number.rs:18` (pub(crate) there): with
/// `arbitrary_precision` a non-integer number reaches a custom visitor as a
/// one-key map under this key — detect it and treat the "map" as a scalar.
/// Pinned by `wild_over_float_scalar_is_missing`.
const NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// One traversal step after the root `S3Object[*]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Name(String),
    Index(usize),
    Wild,
}

/// The FROM path, compiled once per reader. The leading `Wild` (the
/// implicit `S3Object[*]` root step) is the framing's job and dropped here,
/// mirroring the old `expand`'s guard.
pub(crate) struct CompiledPath {
    steps: Vec<Step>,
}

impl CompiledPath {
    /// `ty` is the spec's second parameter (spec §PathSeg 编译状态机: `from_segments(segments, ty)`);
    /// the root rule it could encode lives in the framing, so the matcher never
    /// branches on it — hence the underscore.
    pub(crate) fn from_segments(segments: &[PathSeg], _ty: Type) -> Self {
        let rest = if segments.first().is_some_and(|s| matches!(s, PathSeg::Wild)) {
            &segments[1..]
        } else {
            &[]
        };
        let steps = rest
            .iter()
            .map(|s| match s {
                PathSeg::Name(n) => Step::Name(n.clone()),
                PathSeg::Index(i) => Step::Index(*i),
                PathSeg::Wild => Step::Wild,
            })
            .collect();
        Self { steps }
    }

    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }
}

/// Per-pull outputs, borrowed field-by-field by both seeds — no `Budget`
/// here: the cap is armed once per root pull by the handler (Global
/// Constraints), not once per matched value, so the seeds carry no
/// budget state at all.
///
/// There is deliberately no `Sink<'a>` wrapper: with a field of type
/// `&'a mut Sink<'a>` the `Child` enum becomes *invariant* over `'a`, so
/// `fn child(&mut self) -> Child<'_>` cannot return a reborrow of the shorter
/// `&mut self` lifetime (rustc 1.98.1: "lifetime may not live long enough");
/// `-> Child<'a>`, an explicit `'b: 'a` bound and the UFCS spelling all fail
/// as well — the bound variant merely moves the error to the caller. Inlining
/// the fields keeps `Child` covariant, which is what makes the recursion
/// compile.
pub(crate) struct CaptureSeed<'a> {
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
    pub(crate) failure: &'a mut Option<Error>,
}

/// The steps are exhausted: materialize the current value as the record.
impl<'de> DeserializeSeed<'de> for CaptureSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.pending.push_back(Some(Value::deserialize(d)?));
        Ok(())
    }
}

/// Zero-match rule: the container closed with no match — one MISSING row.
fn push_missing(pending: &mut VecDeque<Option<Value>>) {
    pending.push_back(None);
}

/// A second case-folded match inside one object: recorded for the handler and
/// surfaced as a serde error so the pull aborts; the handler's `map_err`
/// prefers the recorded `Error::Ambiguous`.
fn ambiguous<E: serde::de::Error>(failure: &mut Option<Error>, name: &str) -> E {
    *failure = Some(Error::Ambiguous(name.into()));
    E::custom("ambiguous field name")
}

/// A scope step (`steps[0]`) matched against the current value. A container
/// closing with zero matches emits exactly one MISSING row (the zero-match
/// rule); a step over a scalar is a zero-match.
pub(crate) struct StepSeed<'a> {
    pub(crate) steps: &'a [Step],
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
    pub(crate) failure: &'a mut Option<Error>,
}

enum Child<'a> {
    Capture(CaptureSeed<'a>),
    Step(StepSeed<'a>),
}

impl<'de> DeserializeSeed<'de> for Child<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self {
            Child::Capture(c) => c.deserialize(d),
            Child::Step(s) => s.deserialize(d),
        }
    }
}

impl<'a> StepSeed<'a> {
    /// The seed for the value selected by the current step: one level
    /// deeper, or the capture once the steps run out.
    fn child(&mut self) -> Child<'_> {
        let rest = &self.steps[1..];
        if rest.is_empty() {
            Child::Capture(CaptureSeed {
                pending: &mut *self.pending,
                failure: &mut *self.failure,
            })
        } else {
            Child::Step(StepSeed {
                steps: rest,
                pending: &mut *self.pending,
                failure: &mut *self.failure,
            })
        }
    }
}

impl<'de> DeserializeSeed<'de> for StepSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for StepSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut matched = false;
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(Step::Wild) => {
                while let Some(()) = seq.next_element_seed(self.child())? {
                    matched = true;
                }
            }
            Some(Step::Index(i)) => {
                let target = *i;
                let mut ord = 0usize;
                loop {
                    if ord == target {
                        match seq.next_element_seed(self.child())? {
                            Some(()) => {
                                matched = true;
                                ord += 1;
                            }
                            None => break,
                        }
                    } else {
                        match seq.next_element_seed(Skip)? {
                            Some(()) => ord += 1,
                            None => break,
                        }
                    }
                }
            }
            Some(Step::Name(_)) => {
                // A by-name step over an array is a zero-match — consume.
                while seq.next_element_seed(Skip)?.is_some() {}
            }
        }
        if !matched {
            push_missing(self.pending);
        }
        Ok(())
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // First key read doubles as the number-map check: with
        // arbitrary_precision a non-integer number is a scalar in disguise.
        let key0 = map.next_key::<String>()?;
        if key0.as_deref() == Some(NUMBER_TOKEN) {
            map.next_value_seed(Skip)?;
            // Genuine token maps have exactly one entry (`NumberDeserializer`
            // is synthetic, number.rs:653-677), but a *real* object with this
            // literal first key arrives through the raw `MapAccess` —
            // `deserialize_any` makes no token check at `{`
            // (de.rs:1444-1454) — and collides here. serde_json documents
            // that collision and today's `from_slice::<Value>` rejects such
            // objects (value/de.rs:125-129), so error like today instead of
            // asserting: the input is untrusted and a `debug_assert!` would
            // panic debug builds. A single-entry "token map" is still taken
            // as the number without re-validating the value (today's `Value`
            // re-parses and rejects non-numeric strings — an accepted
            // divergence on a key serde_json itself reserves).
            if map.next_key::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("invalid number-token object"));
            }
            push_missing(self.pending);
            return Ok(());
        }
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(Step::Wild) => {
                let mut matched = false;
                let mut has_key = key0.is_some();
                while has_key {
                    map.next_value_seed(self.child())?;
                    matched = true;
                    has_key = map.next_key_seed(Skip)?.is_some();
                }
                if !matched {
                    push_missing(self.pending);
                }
                Ok(())
            }
            Some(Step::Index(_)) => {
                // An index step over an object is a zero-match — consume.
                let mut has_key = key0.is_some();
                while has_key {
                    map.next_value_seed(Skip)?;
                    has_key = map.next_key_seed(Skip)?.is_some();
                }
                push_missing(self.pending);
                Ok(())
            }
            Some(Step::Name(name)) => {
                let mut matched = false;
                let mut held: Option<Value> = None;
                let mut key = key0;
                while let Some(k) = key {
                    if k.eq_ignore_ascii_case(name) {
                        if matched {
                            return Err(ambiguous(self.failure, name));
                        }
                        matched = true;
                        if self.steps.len() == 1 {
                            // Final Name: hold the value until the object
                            // closes — the ambiguity verdict needs the full
                            // member scan (R10 parity, bounded by one value,
                            // which the pull's own budget already caps).
                            held = Some(map.next_value::<Value>()?);
                        } else {
                            map.next_value_seed(self.child())?;
                        }
                    } else {
                        map.next_value_seed(Skip)?;
                    }
                    key = map.next_key::<String>()?;
                }
                if !matched {
                    push_missing(self.pending);
                } else if let Some(v) = held {
                    self.pending.push_back(Some(v));
                }
                Ok(())
            }
        }
    }

    // A step over any scalar is a zero-match — exactly one MISSING row.
    fn visit_bool<E>(mut self, _: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_i64<E>(mut self, _: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_u64<E>(mut self, _: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_f64<E>(mut self, _: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_str<E>(mut self, _: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_string<E>(mut self, _: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_borrowed_str<E>(mut self, _: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_unit<E>(mut self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }
}

/// Validating skip for every unmatched subtree, replacing
/// `serde::de::IgnoredAny`.
///
/// `IgnoredAny` materializes nothing, but it also does not fully validate:
/// serde_json's `IoRead::ignore_str` (`read.rs:348-366`) checks escape *shape*
/// and the four hex digits of `\uXXXX`, yet never runs the surrogate-pairing
/// check (`ignore_escape` does not reach `parse_unicode_escape`) and never runs
/// `str::from_utf8` over the raw bytes. A string `from_slice` rejects today — a
/// lone `\uD800`, a raw non-UTF-8 byte — would therefore pass silently inside a
/// subtree the query never reads, which is precisely the validation gap that
/// ruled out the hand-written scanner (spec: serde 全量校验优先 is the ruling).
///
/// `Skip` takes `deserialize_any` — never `deserialize_ignored_any`, which
/// consumes the value before any visitor runs — so strings arrive as `&str`,
/// get validated, and are dropped. Memory stays O(1): scalars are dropped in
/// place, children recurse with `Skip`, and `from_reader` hands strings over in
/// serde's reusable scratch buffer. Numbers, literals, structure and control
/// characters were already validated; this closes the string-content half.
pub(crate) struct Skip;

impl<'de> DeserializeSeed<'de> for Skip {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Skip {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Keys go through `Skip` too: today's whole-line `from_slice` validates
        // every key, and a skipped key must not become the one exception.
        while map.next_entry_seed(Skip, Skip)?.is_some() {}
        Ok(())
    }

    // Scalars: the visit *is* the validation; the value itself is dropped.
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }
}
```

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select matcher`
Expected: 十三个测试 PASS（含 `skipped_subtree_still_validates_string_content`、`element_barely_under_cap_is_accepted`、`budget_is_charged_per_root_not_accumulated`、`number_token_first_key_object_errors_not_panics`）。**此步不跑全量 clippy**：framing 与 matcher 在 Task 6/7 接线前只被 `#[cfg(test)]` 引用，`--all-targets` 中不带 cfg(test) 的 lib target 会把这些 `pub(crate)` 项全部报 dead_code，`-D warnings` 必红——全量 clippy gate 在 Task 8 Step 3。

- [ ] **Step 5: 检查点**

报告 `json/matcher.rs`、`json/mod.rs` 待提交。

---

### Task 6: lines.rs — Lines<R> 处理器 + LINES 测试迁移

**Files:**
- Create: `crates/tinio-select/src/json/lines.rs`
- Create: `crates/tinio-select/src/json/testing.rs`（跨模块共享的测试 helper）
- Modify: `crates/tinio-select/src/json/mod.rs`（加 `pub(crate) mod lines;`、`#[cfg(test)] pub(crate) mod testing;`；其测试模块迁出 LINES 用例）

**Interfaces:**
- Consumes: Task 3 `LineFraming`/`LineStart`、Task 2 `BudgetedRead`/`Budget`、Task 5 `CompiledPath`/`Skip`/`StepSeed`/`CaptureSeed`、`crate::record::RecordReader`、`crate::row::Record`。
- Produces: `pub struct Lines<R: Read>`：`pub fn new(reader: R, from: &FromClause) -> Self`；`impl RecordReader for Lines<R>`。Task 8 façade 依赖。

- [ ] **Step 0: 把共享测试 helper 移出 `mod tests`（先绿）**

迁出的 26 个用例（LINES 19 + DOCUMENT 7）里 20 个引用 `read`；留守的引擎组合用例引用 `run`/`run_err`/`s`/`raw`（`traversal_and_where_filters` 用 `run`/`s`，另有 16 个引擎用例用 `run`），所以三个模块必须共享它们。新建 `json/testing.rs`，把 `json/mod.rs` 的 `mod tests` 里这四个 helper **原样**搬入（签名与函数体一字不动，仅去掉 `mod tests` 作用域）：**（Amended 2026-09-13）**原文为"这五个 helper"；实际搬入四个（`run`/`run_err`/`s`/`raw`），第五个 `read` 改由两个 handler 测试模块各自定义本地薄封装（`read(sql, input)`，构造自己的 `Lines`/`Document` 并委托 `testing::drain`）——约 20 个迁出用例的调用形状因此由 `read(sql, Ty, input)` 变为 `read(sql, input)`，输入与断言逐字节不变，无用例丢失。

```rust
//! Helpers shared by the `json` test modules: `read` (façade-level records),
//! `run`/`run_err` (engine rows / the engine's error) and the `s`/`raw` field
//! constructors. Moved out of `mod tests` verbatim so the migrated cases keep
//! their original call shape.

use std::io::Cursor;

use crate::{
    engine::{Engine, OutRow},
    row::{Field, Record, Value},
    sql::parse,
};

use super::{Params, Reader, Type};

pub(crate) fn read(sql: &str, ty: Type, input: &str) -> Vec<Record> { /* 原样 */ }
pub(crate) fn run(sql: &str, ty: Type, input: &str) -> Vec<OutRow> { /* 原样 */ }
pub(crate) fn run_err(sql: &str, ty: Type, input: &str) -> crate::Error { /* 原样 */ }
pub(crate) fn s(v: &str) -> Field { /* 原样 */ }
pub(crate) fn raw(v: &str) -> Field { /* 原样 */ }
```

`json/mod.rs` 加 `#[cfg(test)] pub(crate) mod testing;`；`mod tests` 保留 `AWS_EXAMPLE`（只有留守用例用它）并 `use super::testing::{raw, run, run_err, s};`（四个都被留守用例引用——17 个引擎用例用 `run`、多个用 `s`，只导入子集会编译失败）；`json/lines.rs`、`json/document.rs` 的测试模块各自按需 `use crate::json::testing::{…};`。

结构调整声明（spec §总体架构把"共享测试辅助"记在 mod.rs）：helper 独立为 `json/testing.rs`（`#[cfg(test)]`），因为 20 个迁出用例跨 lines/document 两个测试模块引用同一组 helper；spec 侧已补 Amendment。

Run: `cargo test -p tinio-select`
Expected: 全绿——纯搬移，此时新模块尚未建立。

- [ ] **Step 1: 迁移测试 + 偏差④填充调整 + 新增行尾用例（先红）**

从 `json/mod.rs` 测试模块**逐字迁出**以下用例到 `json/lines.rs` 的测试模块（两处构造差异：`Reader::new` → `Lines::new(..., &plan.from)`，以及从 `crate::json::testing` 导入 helper）：`lines_two_objects`、read-based `traversal_*`（全部 13 个）、`lines_final_unterminated_line_over_cap`、`lines_input_record_cap`、`lines_input_record_cap_exact_span_allowed`、`lines_last_record_start`、`empty_line_is_a_format_error`。

计数口径（json.rs 实测 16 个 `traversal_` 前缀测试）：除这 13 个 read-based 用例外，`traversal_case_insensitive_duplicates_are_ambiguous` 与 `traversal_bracket_name_duplicates_are_ambiguous` 直接构造 `Reader::new` 并断言 `Err(Error::Ambiguous)`——façade 签名不变，**留守 mod.rs 原样通过**；`traversal_and_where_filters` 是引擎组合用例（用 `run`/`s`），同样留守。

偏差④仅改两处输入构造（断言原样）：

```rust
#[test]
fn lines_input_record_cap() {
    // Valid JSON, span (content + terminator) 1 MB + 1: the cap fires
    // mid-serve (deviation ④: invalid filler would now parse-error first).
    let filler = "a".repeat(1024 * 1024 - 8);
    let mut input = format!("{{\"x\":\"{filler}\"}}").into_bytes();
    assert_eq!(input.len() + 1, 1024 * 1024 + 1);
    input.push(b'\n');
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Lines::new(Cursor::new(input), &plan.from);
    match r.next() {
        Err(crate::Error::Format(msg)) => {
            assert_eq!(msg, "input record exceeds 1 MB");
            assert_eq!(
                crate::Error::Format(msg).to_string(),
                "S3 select: input error: input record exceeds 1 MB"
            );
        }
        other => panic!("expected format error, got {other:?}"),
    }
}

#[test]
fn lines_final_unterminated_line_over_cap() {
    let filler = "a".repeat(1024 * 1024 - 6);
    let input = format!("{{\"x\":\"{filler}\"}}").into_bytes(); // 无终止符
    assert_eq!(input.len(), 1024 * 1024 + 2);
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Lines::new(Cursor::new(input), &plan.from);
    match r.next() {
        Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
        other => panic!("expected format error, got {other:?}"),
    }
}
```

新增（帧化与行尾探测）：

```rust
#[test]
fn lines_trailing_garbage_errors() {
    for input in ["{\"a\":1} x\n", "{\"a\":1} {\"b\":2}\n", "123 456\n", "123x\n"] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{input}: {m}"),
            other => panic!("{input}: expected format error, got {other:?}"),
        }
    }
}

#[test]
fn lines_crlf_records_parse() {
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Lines::new(
        Cursor::new(b"{\"a\":1}\r\n{\"b\":2}\r\n".to_vec()),
        &plan.from,
    );
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    assert_eq!(r.next().unwrap(), None);
}

#[test]
fn lines_indented_line_reports_the_line_start() {
    // "Line start" means the line's first byte, not its first *value* byte:
    // the framing records the offset before it skips leading whitespace, so
    // an indented line reports the indentation. `lines_last_record_start`
    // (`{"a":1}\n{"b":2}\n` → 0, then 8) pins the unindented case.
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Lines::new(Cursor::new(b"  {\"a\":1}\n".to_vec()), &plan.from);
    assert!(r.next().unwrap().is_some());
    assert_eq!(r.last_record_start(), 0);
}

#[test]
fn lines_whitespace_only_line_is_a_format_error() {
    // jsonlines.org rule (G8): whitespace *around* a value is ignored, but a
    // record with no value at all is an error — the same class as a blank line.
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Lines::new(Cursor::new(b"  \t \n".to_vec()), &plan.from);
    match r.next() {
        Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
        other => panic!("expected format error, got {other:?}"),
    }
}

#[test]
fn lines_skipped_subtree_still_validates_strings() {
    // The reader must skip with `Skip`, never `IgnoredAny`: a lone surrogate
    // escape in a subtree the path never reads is rejected today by the
    // whole-line `from_slice`, and must stay rejected.
    let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
    let mut r = Lines::new(
        Cursor::new(b"{\"b\":\"\\uD800\",\"a\":1}\n".to_vec()),
        &plan.from,
    );
    match r.next() {
        Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
        other => panic!("expected format error, got {other:?}"),
    }
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select`
Expected: 编译失败（`json::lines` 不存在）。此时 `mod.rs` 旧实现仍在，除迁移走的用例外套件应仍绿。

- [ ] **Step 3: 实现**

```rust
//! LINES handler: one streaming pull per line — the line is never
//! materialized. Framing lives in `LineFraming`; matching in `matcher`.

use std::{
    collections::VecDeque,
    io::{BufReader, Read},
    rc::Rc,
};

use serde::de::DeserializeSeed;
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        Type,
        framing::{Budget, BudgetedRead, LineFraming, LineStart},
        matcher::{CaptureSeed, CompiledPath, Skip, StepSeed},
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// LINES record reader: each line is one root value.
pub struct Lines<R: Read> {
    framing: LineFraming<R>,
    budget: Rc<Budget>,
    path: CompiledPath,
    pending: VecDeque<Option<Value>>,
    failure: Option<Error>,
    last_record_start: u64,
    done: bool,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            // One buffer layer is enough: serde_json's `IoRead` issues one
            // `read` per byte no matter what, and this `BufReader` keeps
            // those calls memory-cheap. A second layer underneath never
            // engages — refills are capacity-sized, and `BufReader::read`
            // bypasses its buffer for requests >= capacity.
            framing: LineFraming::new(BufReader::new(reader)),
            budget: Rc::new(Budget::default()),
            path: CompiledPath::from_segments(&from.segments, Type::Lines),
            pending: VecDeque::new(),
            failure: None,
            last_record_start: 0,
            done: false,
        }
    }

    /// The budget is armed around the whole root pull (see `pull_line`), so
    /// `fired` means "this line delivered more than `MAX_RECORD`"; the
    /// framing's own span check (content + terminator) is the stricter of the
    /// two and usually fires first, so both are mapped to the same message.
    fn map_err(&mut self, e: serde_json::Error) -> Error {
        if self.budget.fired() || self.framing.capped {
            return Error::Format(CAP_MESSAGE.into());
        }
        if let Some(f) = self.failure.take() {
            return f;
        }
        if e.is_io() {
            let kind = e.io_error_kind().unwrap_or(std::io::ErrorKind::Other);
            return Error::from(std::io::Error::new(kind, e.to_string()));
        }
        Error::Format(format!("json input: {e}"))
    }

    /// Parse the next line, filling `pending`; sets `done` at input EOF.
    fn pull_line(&mut self) -> Result<(), Error> {
        match self.framing.start_line() {
            Err(io) => return Err(Error::from(io)),
            Ok(LineStart::Eof) => {
                self.done = true;
                return Ok(());
            }
            Ok(LineStart::Line(start)) => self.last_record_start = start,
        }
        // The cap is armed around the whole pull — one line is one root, so
        // every byte serde is served here (matched, skipped or trailing) is
        // charged, and `disarm` starts the next line from full budget.
        // The Deserializer borrows the framing only inside this block, so
        // the handler inspects it again after the pull (cap flags, offsets).
        self.budget.arm();
        let (root, probe) = {
            let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
                &mut self.framing,
                Rc::clone(&self.budget),
            ));
            let root = if self.path.steps().is_empty() {
                CaptureSeed {
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            } else {
                StepSeed {
                    steps: self.path.steps(),
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            };
            // Trailing probe on the SAME Deserializer: `IoRead` parks a peeked
            // byte inside itself — already consumed from the framing — so a
            // fresh Deserializer would silently swallow it. `Skip` validates
            // the trailing value the way `from_slice` + `end()` used to.
            let probe = root.is_ok().then(|| Skip.deserialize(&mut de));
            (root, probe)
        };
        self.budget.disarm();
        match root {
            Err(e) => Err(self.map_err(e)),
            Ok(()) => match probe.expect("probe runs on a clean root pull") {
                Ok(()) => Err(Error::Format("json input: trailing characters on line".into())),
                Err(e) if e.is_eof() => Ok(()),
                Err(e) => Err(self.map_err(e)),
            },
        }
    }
}

impl<R: Read> RecordReader for Lines<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            if let Some(v) = self.pending.pop_front() {
                return Ok(Some(Record::Json(v)));
            }
            if self.done {
                return Ok(None);
            }
            // An error is terminal: latch `done` so a caller that keeps
            // polling after `Err` sees a finished stream, not a retried pull
            // against a broken framing.
            if let Err(e) = self.pull_line() {
                self.done = true;
                return Err(e);
            }
        }
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}
```

注意：`deserialize(&mut de)` 走 `DeserializeSeed`，故需 `use serde::de::DeserializeSeed;`（`Skip` 只实现该 trait，`Value` 侧走 `serde::Deserialize`，导入列表按 style.md 合并）。

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select`
Expected: 全绿（含迁移与新增用例）。

- [ ] **Step 5: 检查点**

报告 `json/lines.rs`、`json/testing.rs`、`json/mod.rs` 待提交。

---

### Task 7: document.rs — Document<R> 处理器 + DOCUMENT 测试迁移与新增钉

**Files:**
- Create: `crates/tinio-select/src/json/document.rs`
- Modify: `crates/tinio-select/src/json/mod.rs`（加 `pub(crate) mod document;`；测试模块迁出 DOCUMENT 用例）、`crates/tinio-select/src/record.rs`（新增一个 ScanRange 用例）、`crates/tinio-select/src/events.rs`（新增两条事件层 DOCUMENT 用例）；**（Amended 2026-09-13）**events.rs 另有一处**生产侧**改动未列入原计划：`SelectIter::pump` 在 `outbox.fail` 之前补 flush 暂存帧——偏差⑤（客户端先收 Records 事件后收错误事件）要求如此，否则失败前已成功读出的元素会被丢弃。评审记为 scope creep，特此补记。

**Interfaces:**
- Consumes: Task 4 `DocFraming`/`DocMode`/`peek_mode`、Task 2 `Budget`/`BudgetedRead`、Task 5 全部（**seed 无 `budget` 字段**，cap 由本任务在 `pull_root` 首尾武装/解除）。
- Produces: `pub struct Document<R: Read>`：`pub fn new(reader: R, from: &FromClause) -> Self`；`impl RecordReader for Document<R>`（`last_record_start` = 元素级偏移，偏差⑥）；`#[cfg(test)] pending_high`（**字节**高水位）。Task 8 façade 依赖。

- [ ] **Step 1: 迁移测试 + 新增钉（先红）**

逐字迁出到 `json/document.rs` 测试模块（构造差异同 Task 6：`Reader::new` → `Document::new(..., &plan.from)`，helper 从 `crate::json::testing` 导入）：`document_single_multiline_object`、`document_root_array_yields_elements`、`empty_document_and_array_yield_nothing`、`document_object_root_is_one_row_object`、`document_input_record_cap`、`document_input_record_cap_exact_allowed`、`document_last_record_start_is_zero`。

`document_last_record_start_is_zero` **一字不动**（spec：除偏差④的两处填充输入外任何现有测试一行不碰）——其输入 `"{\n \"a\": 1\n}\n"` 是起于 byte 0 的单值根，元素级偏移规则下断言仍是 `0`（已逐字核对 `json.rs:719-729`）；元素偏移的钉由下面的新用例承担。新增：

```rust
#[test]
fn document_array_larger_than_1mb_streams() {
    // 偏差①主钉：总 > 1 MB、单元素 < 1 MB 的数组文档流式通过。
    let elem = |tag: &str| format!("\"{tag}{}\"", "a".repeat(400 * 1024));
    let doc = format!(
        "[{}, {}, {}, {}]",
        elem("a"), elem("b"), elem("c"), elem("d")
    );
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
    for expected in ["a", "b", "c", "d"] {
        match r.next().unwrap() {
            Some(Record::Json(Some(Value::String(s)))) => {
                assert!(s.starts_with(expected), "record tag {expected}")
            }
            other => panic!("expected string record, got {other:?}"),
        }
    }
    assert_eq!(r.next().unwrap(), None);
}

#[test]
fn document_element_over_cap_errors_after_prior_records() {
    // 偏差①+⑤：首元素产出，第二元素超 cap 流内报错。
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    match r.next() {
        Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
        other => panic!("expected format error, got {other:?}"),
    }
}

#[test]
fn document_record_starts_are_element_offsets_monotonic() {
    // 偏差⑥：元素起点真实偏移，含 MISSING 行（取其所派生元素起点），单调非递减。
    let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
    let mut r = Document::new(
        Cursor::new(b"[{\"a\": 1}, {\"b\": 2}, {\"a\": 3}]".to_vec()),
        &plan.from,
    );
    let mut starts = Vec::new();
    while let Some(rec) = r.next().unwrap() {
        let _ = rec;
        starts.push(r.last_record_start());
    }
    assert_eq!(starts.len(), 3);
    assert_eq!(starts[0], 1);
    assert!(starts.windows(2).all(|w| w[0] <= w[1]), "{starts:?}");
}

#[test]
fn document_empty_array_missing_start_is_zero() {
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Document::new(Cursor::new(b"[]".to_vec()), &plan.from);
    assert_eq!(r.next().unwrap(), Some(Record::Json(None)));
    assert_eq!(r.last_record_start(), 0);
    assert_eq!(r.next().unwrap(), None);
}

#[test]
fn document_single_root_trailing_garbage_errors() {
    // `5x` / `123#` are the cases that expose a lost peeked byte: `IoRead`
    // parks the byte after a bare number inside itself, so a *fresh*
    // Deserializer for the probe would read past it and accept the record.
    for input in ["{} x", "{} {}", "5 {}", "5x", "123#"] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{input}: {m}"),
            other => panic!("{input}: expected format error, got {other:?}"),
        }
    }
}

#[test]
fn document_scalar_roots() {
    for (input, expected) in [
        ("5", Some(Record::Json(Some(serde_json::json!(5))))),
        ("\"x\"", Some(Record::Json(Some(serde_json::json!("x"))))),
        ("null", Some(Record::Json(Some(serde_json::Value::Null)))),
    ] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        assert_eq!(r.next().unwrap(), expected, "{input}");
        assert_eq!(r.next().unwrap(), None, "{input}");
    }
}

#[test]
fn document_whitespace_only_is_a_format_error() {
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Document::new(Cursor::new(b"  \n".to_vec()), &plan.from);
    match r.next() {
        Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
        other => panic!("expected format error, got {other:?}"),
    }
}

#[test]
fn document_row_order_table() {
    // 行序表驱动：通配扇出与 MISSING 交错（apply 顺序等价的枚举式补强）。
    let cases: &[(&str, &str, Vec<Record>)] = &[
        (
            "SELECT * FROM S3Object[*].a[*]",
            "{\"a\": [1, 2]}\n",
            vec![
                Record::Json(Some(serde_json::json!(1))),
                Record::Json(Some(serde_json::json!(2))),
            ],
        ),
        (
            "SELECT * FROM S3Object[*].a[*].b",
            "{\"a\": [{\"b\": 1}, {}, {\"b\": 2}]}\n",
            vec![
                Record::Json(Some(serde_json::json!(1))),
                Record::Json(None),
                Record::Json(Some(serde_json::json!(2))),
            ],
        ),
    ];
    for (sql, input, expected) in cases {
        let plan = parse(sql).unwrap();
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        let mut got = Vec::new();
        while let Some(rec) = r.next().unwrap() {
            got.push(rec);
        }
        assert_eq!(&got, expected, "{sql}");
    }
}
```

```rust
#[test]
fn document_unterminated_input_is_a_format_error() {
    // The spec's failure-path list requires this at the handler level (only the
    // framing-level `[1` case exists today). An EOF *inside* a value is a
    // malformed document, never a clean end: `["abc` and `[{"a":` are the cases
    // that would otherwise surface as a clean empty array, and `[1, "abc`
    // pins the deviation-⑤ shape (the first record stands, then the error).
    for (input, rows_before) in [
        ("[1", 1usize),
        ("[\"abc", 0),
        ("[{\"a\":", 0),
        ("[1, \"abc", 1),
        ("{\"a\":", 0),
    ] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        let mut seen = 0usize;
        loop {
            match r.next() {
                Ok(Some(_)) => seen += 1,
                Ok(None) => panic!("{input}: ended cleanly after {seen} records"),
                Err(crate::Error::Format(m)) => {
                    assert!(m.contains("json input"), "{input}: {m}");
                    break;
                }
                Err(e) => panic!("{input}: expected a format error, got {e:?}"),
            }
        }
        assert_eq!(seen, rows_before, "{input}");
    }
}

#[test]
fn document_array_root_keeps_pending_bounded() {
    // The architecture's promise, pinned portably (the spec's counting
    // allocator is ruled out by `unsafe_code = "forbid"`): one *root* is in
    // flight, so the buffered bytes never approach a document's worth. The
    // yardstick is element size, not record count — 20 000 tiny objects and
    // one 1 MB object are the same count and nothing alike in memory.
    let mut doc = Vec::from(&b"["[..]);
    for i in 0..20_000 {
        if i > 0 {
            doc.push(b',');
        }
        doc.extend_from_slice(format!("{{\"a\": {i}}}").as_bytes());
    }
    doc.push(b']');
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = Document::new(Cursor::new(doc), &plan.from);
    let mut count = 0u64;
    while r.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 20_000);
    assert!(
        r.pending_high <= 64,
        "pending peaked at {} bytes — an array root buffers one element's matches",
        r.pending_high
    );
}

#[test]
fn document_single_root_over_cap_errors() {
    // Deviation ③'s tier under the per-root cap: the tier is *not* unbounded.
    // One pull accumulates the document's whole match set, and that pull is
    // charged like any other — so an over-1 MB single-value root errors
    // instead of growing without limit. This replaces the older assertion
    // that the tier was unbounded (`pending_high == 5_000`).
    let plan = parse("SELECT * FROM S3Object[*].a[*]").unwrap();
    let mut doc = Vec::from(&b"{\"a\": ["[..]);
    for i in 0..200_000 {
        if i > 0 {
            doc.push(b',');
        }
        doc.extend_from_slice(i.to_string().as_bytes());
    }
    doc.extend_from_slice(b"]}");
    assert!(doc.len() > 1024 * 1024, "the fixture must exceed the cap");
    let mut r = Document::new(Cursor::new(doc), &plan.from);
    loop {
        match r.next() {
            Ok(Some(_)) => continue,
            Err(crate::Error::Format(msg)) => {
                assert_eq!(msg, "input record exceeds 1 MB");
                break;
            }
            other => panic!("expected the cap error, got {other:?}"),
        }
    }
}

#[test]
fn document_single_root_under_cap_yields_all_matches() {
    // The other side of the same boundary: under the cap the tier behaves as
    // deviation ③ describes — one pull, every match, in order.
    let plan = parse("SELECT * FROM S3Object[*].a[*]").unwrap();
    let mut doc = Vec::from(&b"{\"a\": ["[..]);
    for i in 0..5_000 {
        if i > 0 {
            doc.push(b',');
        }
        doc.extend_from_slice(i.to_string().as_bytes());
    }
    doc.extend_from_slice(b"]}");
    let mut r = Document::new(Cursor::new(doc), &plan.from);
    let mut count = 0u64;
    while r.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 5_000);
    assert!(
        r.pending_high > 0 && r.pending_high <= 1024 * 1024,
        "a single-value root accumulates its match set in one pull (deviation ③), \
         bounded by the record cap: peak {} bytes",
        r.pending_high
    );
}
```

`record.rs` 测试模块新增（偏差⑥的 ScanRange 侧）：

```rust
#[test]
fn document_scan_range_filters_elements_by_start() {
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let reader = json::Reader::new(
        Cursor::new(b"[{\"a\": 1}, {\"b\": 2}]".to_vec()),
        json::Params { ty: json::Type::Document },
        &plan.from,
    );
    // 元素 2 起点 11：窗口 [11, ∞) 只留它。
    let mut r = RangeFilter::new(reader, ScanRange { start: 11, end: None });
    assert_eq!(
        r.next().unwrap(),
        Some(crate::row::Record::Json(Some(serde_json::json!({"b": 2}))))
    );
    assert_eq!(r.next().unwrap(), None);
}
```

`crates/tinio-select/src/events.rs` 测试模块新增两条（偏差①/⑤ 的本质是事件层行为，而事件层的 DOCUMENT 覆盖此前为零）；复用该模块既有的 `run(sql, config, input)` helper：

```rust
#[test]
fn json_document_streams_past_1mb_then_stats() {
    // Deviation ①: an over-1 MB DOCUMENT streams element by element.
    // Deviation ⑥: `bytes_processed` closes on the last element's start, so it
    // is `scanned - 1` for a `[...]`-rooted document — the leading `[` belongs
    // to no record. Pinned deliberately: no existing test covers DOCUMENT byte
    // counters at all, and this value moves with deviation ⑥.
    let elem = |tag: &str| format!("{{\"tag\":\"{tag}\",\"pad\":\"{}\"}}", "a".repeat(400 * 1024));
    let doc = format!("[{},{},{},{}]", elem("a"), elem("b"), elem("c"), elem("d"));
    let scanned = doc.len() as u64;
    assert!(scanned > 1024 * 1024, "the fixture must exceed the old whole-document cap");
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        output: OutputMode::Json(JsonOutputParams::default()),
        ..Default::default()
    };
    let events = run("SELECT * FROM S3Object", config, doc.as_bytes());
    let stats = events
        .iter()
        .find_map(|e| match e {
            Ok(SelectEvent::Stats(c)) => Some(*c),
            _ => None,
        })
        .expect("Stats before End");
    assert_eq!(stats.bytes_scanned, scanned);
    assert_eq!(stats.bytes_processed, scanned - 1);
    let rows: Vec<u8> = events
        .iter()
        .filter_map(|e| match e {
            Ok(SelectEvent::Records(b)) => Some(b.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(rows.iter().filter(|&&b| b == b'\n').count(), 4);
    assert!(matches!(events.last(), Some(Ok(SelectEvent::End))));
}

#[test]
fn json_document_reports_the_cap_after_prior_records() {
    // Deviation ⑤: a DOCUMENT malformation surfaces in-stream, after the records
    // that preceded it — the first element's record stands, then the over-cap
    // element errors the stream out.
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        output: OutputMode::Json(JsonOutputParams::default()),
        ..Default::default()
    };
    let events = run("SELECT * FROM S3Object", config, doc.as_bytes());
    let first_err = events.iter().position(Result::is_err).expect("a stream error");
    assert!(
        events[..first_err]
            .iter()
            .any(|e| matches!(e, Ok(SelectEvent::Records(_)))),
        "the first element's record must precede the error"
    );
    match &events[first_err] {
        Err(crate::Error::Format(m)) => assert_eq!(m, "input record exceeds 1 MB"),
        other => panic!("expected the reader cap error, got {other:?}"),
    }
}
```

- [ ] **Step 2: 确认红**

Run: `cargo test -p tinio-select`
Expected: 编译失败（`json::document` 不存在）。

（顺带记下两条与被删旧实现绑定的断言，实现后它们是**绿**的判据而非红：`document_input_record_cap`（`json.rs:680`，`"` + 1 MB 无闭合）与 `document_input_record_cap_exact_allowed`（`json.rs:691`，恰好 1 MB 的闭合字符串）都是**单值根**，只有当预算"每次根拉取恰好武装一次"时后者才以 1 048 576/1 048 576 压线通过——这正是 Step 3 里 `arm()` 位置要在 `Deserializer` 之外的原因。）

- [ ] **Step 3: 实现**

```rust
//! DOCUMENT handler: one streaming pull per root value (root-array element
//! or the whole single root). Framing lives in `DocFraming`; matching in
//! `matcher`. Record starts are element offsets (deviation ⑥).
//!
//! The 1 MB cap is charged per **root pull** (`Budget::arm` before the
//! `Deserializer` block, `disarm` after it), so the invariant is "one root
//! ≤ 1 MB": an array element, or the whole document in single mode. Because
//! the charge covers every byte serde is served, an *unmatched* oversized
//! element is capped exactly like a matched one — there is no skip path that
//! escapes the cap. Single mode therefore bounds deviation ③ too: a document
//! over 1 MB errors out instead of accumulating its whole match set.
//!
//! Boundary shift from the whole-document rule this replaces: a `[...]`-rooted
//! document that used to be rejected for exceeding 1 MB now streams whenever
//! every element is ≤ 1 MB (deviation ①); a scalar or object root keeps the
//! old rule, since that root *is* the record.

use std::{
    collections::VecDeque,
    io::{BufReader, Read},
    rc::Rc,
};

use serde::de::DeserializeSeed;
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        Type,
        framing::{Budget, BudgetedRead, DocFraming, DocMode},
        matcher::{CaptureSeed, CompiledPath, Skip, StepSeed},
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// DOCUMENT record reader: a root array yields one record per element; any
/// other root is a single record.
pub struct Document<R: Read> {
    framing: DocFraming<R>,
    budget: Rc<Budget>,
    path: CompiledPath,
    pending: VecDeque<Option<Value>>,
    failure: Option<Error>,
    roots: u64,
    last_record_start: u64,
    done: bool,
    /// Test-only high-water mark of `pending`, measured in **bytes** of the
    /// buffered `Value`s rather than in records: it is the memory tripwire
    /// that replaces the spec's counting allocator, which
    /// `unsafe_code = "forbid"` rules out. An item count would say nothing
    /// about memory — a single queued 1 MB value and 5 000 tiny ones are the
    /// same number and three orders of magnitude apart in size.
    #[cfg(test)]
    pending_high: std::cell::Cell<usize>,
}

impl<R: Read> Document<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            // One buffer layer — see `Lines::new` for why a second never
            // engages.
            framing: DocFraming::new(BufReader::new(reader)),
            budget: Rc::new(Budget::default()),
            path: CompiledPath::from_segments(&from.segments, Type::Document),
            pending: VecDeque::new(),
            failure: None,
            roots: 0,
            last_record_start: 0,
            done: false,
            #[cfg(test)]
            pending_high: std::cell::Cell::new(0),
        }
    }

    fn map_err(&mut self, e: serde_json::Error) -> Error {
        if self.budget.fired() {
            return Error::Format(CAP_MESSAGE.into());
        }
        if let Some(f) = self.failure.take() {
            return f;
        }
        if let Some(msg) = self.framing.framing_msg.take() {
            return Error::Format(format!("json input: {msg}"));
        }
        if e.is_io() {
            let kind = e.io_error_kind().unwrap_or(std::io::ErrorKind::Other);
            return Error::from(std::io::Error::new(kind, e.to_string()));
        }
        Error::Format(format!("json input: {e}"))
    }

    /// One root pull: StepSeed/CaptureSeed over the framing and — in single
    /// mode — the trailing probe on the **same** `Deserializer`. `IoRead` parks
    /// a peeked byte inside itself, already taken from the framing, so a fresh
    /// `Deserializer` for the probe would silently swallow one trailing byte
    /// (`5x` would be accepted as the record `5`).
    fn pull_root(&mut self) -> Result<(), Error> {
        // Resolved *before* the `Deserializer` exists: whether the probe runs
        // depends on the mode, and afterwards `de` holds `&mut self.framing`,
        // so the mode can no longer be read.
        let mode = match self.framing.peek_mode() {
            Ok(mode) => mode,
            Err(io) => return Err(Error::from(io)),
        };
        // Armed before serde sees the first byte and disarmed after the pull:
        // every byte this root delivers is charged, and the next root starts
        // from a full budget (Global Constraints — charging per matched value
        // would accumulate across a root's matches and reject a document whose
        // elements are each well under the cap).
        self.budget.arm();
        let (root, probe) = {
            let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
                &mut self.framing,
                Rc::clone(&self.budget),
            ));
            let root = if self.path.steps().is_empty() {
                CaptureSeed {
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            } else {
                StepSeed {
                    steps: self.path.steps(),
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            };
            let probe =
                (mode == DocMode::Single && root.is_ok()).then(|| Skip.deserialize(&mut de));
            (root, probe)
        };
        self.budget.disarm();
        match root {
            Ok(()) => {
                self.roots += 1;
                self.last_record_start = self.framing.element_start;
                if let Some(probe) = probe {
                    self.done = true;
                    match probe {
                        Err(e) if e.is_eof() => {}
                        Ok(()) => {
                            return Err(Error::Format(
                                "json input: trailing characters after the JSON document".into(),
                            ))
                        }
                        Err(e) => return Err(self.map_err(e)),
                    }
                }
                Ok(())
            }
            Err(e) if e.is_eof() => {
                self.done = true;
                match self.framing.mode {
                    // Empty input: zero records, no error (today's behaviour).
                    DocMode::Empty => {}
                    // A root array that closed cleanly: `[]` yields the
                    // S3Object[*] zero-match MISSING row when nothing was
                    // produced; a closed array after elements needs nothing.
                    DocMode::Array if self.framing.closed() => {
                        if self.roots == 0 {
                            self.pending.push_back(None);
                            self.last_record_start = 0;
                        }
                    }
                    // EOF *inside* a value — a truncated element, a truncated
                    // single root, or a whitespace-only document — is a
                    // malformed document. Without this arm `["abc` and
                    // `[{"a":` would be reported as a clean empty array (today
                    // `from_slice` rejects both).
                    _ => return Err(Error::Format(format!("json input: {e}"))),
                }
                Ok(())
            }
            Err(e) => Err(self.map_err(e)),
        }
    }
}

impl<R: Read> RecordReader for Document<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            if let Some(v) = self.pending.pop_front() {
                return Ok(Some(Record::Json(v)));
            }
            if self.done {
                return Ok(None);
            }
            // An error is terminal: latch `done`, same as in `Lines::next`.
            if let Err(e) = self.pull_root() {
                self.done = true;
                return Err(e);
            }
            #[cfg(test)]
            {
                // Bytes, not records: `pending`'s memory, not its length.
                let n: usize = self
                    .pending
                    .iter()
                    .flatten()
                    .map(|v| v.to_string().len())
                    .sum();
                if n > self.pending_high.get() {
                    self.pending_high.set(n);
                }
            }
        }
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}
```

- [ ] **Step 4: 确认绿**

Run: `cargo test -p tinio-select && cargo test -p tinio-select record`
Expected: 全绿（含新增钉、`pending_high` 字节口径的两条、以及 `document_single_root_over_cap_errors` 的 cap 边界；record.rs 新用例同跑）。

- [ ] **Step 5: 检查点**

报告 `json/document.rs`、`json/mod.rs`、`record.rs`、`events.rs` 待提交。

---

### Task 11′: benches/select_scan.rs — JSON DOCUMENT 吞吐用例（**先于 Task 8**）

**Files:**
- Modify: `crates/tinio-select/benches/select_scan.rs`

**Interfaces:**
- Consumes: 既有 `drain`/`WARM_UP`/`MEASURE`（`benches/select_scan.rs:37-38,55`）、`events::{InputFormat, SelectConfig}`、`json::{Params, Type}`。
- Produces: `json_document_scan` bench 组，**以及重构前后的可比数字**。

**为什么先于 Task 8**：Task 8 删掉旧实现后，任何"重构前"的数就再也取不到了。本任务的 Step 2 必须在旧实现仍在树上时执行；Task 8 之后再跑一次同一条命令，两个数一起写进 Task 8 的模块注释（那注释里有 `<old> → <new>` 占位等着）。

- [ ] **Step 1: 实现**

在文件内追加（模式对齐既有 `scan_group`；`SelectConfig` 的 `input_format` 字段用法参照 `events.rs:996` 的测试构造）：

```rust
/// Benchmark fixture rows (JSON). ~800 KB, deliberately: the doc must stay
/// *under* the 1 MB whole-document cap so the pre-refactor implementation can
/// read it too — a bigger fixture would make the before/after incomparable
/// rather than merely slow.
const JSON_ELEMENTS: usize = 10_000;

/// JSON DOCUMENT scan: an array of small objects, streamed (2026-09-12).
fn json_document_scan(c: &mut Criterion) {
    let mut doc = String::from("[");
    for i in 0..JSON_ELEMENTS {
        doc.push_str(&format!(
            "{}{{\"id\": {i}, \"name\": \"record-{i:08}\", \"price\": {}, \"tags\": [\"alpha\", \"beta-{i:08}\"]}}",
            if i == 0 { "" } else { "," },
            i % 997,
        ));
    }
    doc.push(']');
    let doc = doc.into_bytes();
    let plan = parse("SELECT * FROM S3Object").unwrap();
    // Struct-update form, matching `events.rs:995-1001` — also what keeps
    // `clippy::field_reassign_with_default` quiet under `-D warnings`.
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        ..Default::default()
    };
    let mut group = c.benchmark_group("json_document_scan");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(doc.len() as u64));
    group.bench_function("select_star", |b| {
        b.iter_batched(
            || Box::new(Cursor::new(doc.clone())) as Box<dyn Read + Send>,
            |input| drain(plan.clone(), config.clone(), input),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}
```

导入处追加 `events::InputFormat`、`json`（按 style.md 归并）；`criterion_group!(benches, full_scan, filtered_scan, json_document_scan);`。

- [ ] **Step 2: 旧实现的基线数（**必须在 Task 8 之前跑**）**

Run: `cargo bench -p tinio-select -- json_document_scan`
Expected: bench 编译通过（旧 `Reader::new` 签名未变，本文件无需改动即可运行）并产出基线数字。**把 `time` 与 `thrpt` 两列记下来**，Task 8 之后要与新实现同命令对比。

- [ ] **Step 3: 检查点**

报告 `benches/select_scan.rs` 待提交 + **基线数字**（写入 Task 8 模块注释用）。

---

### Task 8: mod.rs façade 切换 + 旧实现删除

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs`

**Interfaces:**
- Consumes: Task 6 `lines::Lines`、Task 7 `document::Document`。
- Produces: `pub struct Reader<R: Read>`（`pub fn new(reader: R, params: Params, from: &FromClause) -> Self`，签名不变）；`impl RecordReader for Reader<R>` 委托变体。`events.rs`/`record.rs`/server 调用点零改动。

- [ ] **Step 1: 切换 façade 并删除旧实现**

`json/mod.rs`：删除旧 `Reader` 结构体及其全部方法（`read_line`/`read_document`/`expand`/`apply`/`parse`）、`VecDeque`/`BufRead` 等不再用的导入；替换为：

```rust
//! JSON record readers: LINES / DOCUMENT framings over one streaming
//! kernel — serde_json pulls one record root per call through a framing
//! filter, and the FROM path runs as a compiled state machine on serde's
//! event flow. Memory is bounded by one *pull*, never by the input: a LINES
//! line (≤ 1 MB span) or one DOCUMENT array element; a single-value DOCUMENT
//! root accumulates its whole match set in that one pull, and that pull is
//! charged like any other, so it is bounded by the cap rather than unbounded
//! (deviation ③, amended).
//!
//! Throughput: serde_json's `IoRead` reads one byte per call, so each handler
//! hands its framing a `BufReader` (a second layer never engages — refills
//! are capacity-sized and `BufReader::read` bypasses its buffer). The gap
//! against the whole-record `from_slice` parse this replaces is the price of
//! bounded memory, and it is real: `json_document_scan` in
//! `benches/select_scan.rs` is the baseline, and the measured old→new
//! criterion numbers live in the plan (Task 8).
//!
//! Recorded deviations (spec 2026-09-12, docs/superpowers/specs/):
//! ① DOCUMENT total size is unbounded — the 1 MB cap binds the root pull.
//! ② A non-final Name step's duplicate key errors mid-stream: rows already
//!   yielded by earlier pulls of that object are not retracted, and the
//!   failing pull's own rows are dropped — `next` clears `pending` and sets
//!   `done`, so a caller polling after the error sees a finished stream.
//! ③ A single-value root accumulates its match set per pull — bounded by the
//!   record cap that pull is charged against, not unbounded.
//! ④ An invalid LINES line may parse-error before the span cap fires.
//! ⑤ A DOCUMENT malformation surfaces after the records before it.
//! ⑥ DOCUMENT records report element-start offsets and ScanRange filters
//!   them (AWS: LINES-only — an extension).

pub(crate) mod document;
pub(crate) mod framing;
pub(crate) mod lines;
pub(crate) mod matcher;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;

pub(crate) use value::{json_lookup, to_value};

use std::io::Read;

use crate::{
    record::RecordReader,
    row::Record,
    sql::FromClause,
};

// `Type`/`Params` 原样保留（wire 映射不变）。

/// LINES/DOCUMENT dispatching reader — the wire-`Params` entry point.
pub struct Reader<R: Read> {
    inner: Handler<R>,
}

enum Handler<R: Read> {
    Lines(lines::Lines<R>),
    Document(document::Document<R>),
}

impl<R: Read> Reader<R> {
    pub fn new(reader: R, params: Params, from: &FromClause) -> Self {
        let inner = match params.ty {
            Type::Lines => Handler::Lines(lines::Lines::new(reader, from)),
            Type::Document => Handler::Document(document::Document::new(reader, from)),
        };
        Self { inner }
    }
}

impl<R: Read> RecordReader for Reader<R> {
    fn next(&mut self) -> Result<Option<Record>, crate::error::Error> {
        match &mut self.inner {
            Handler::Lines(h) => h.next(),
            Handler::Document(h) => h.next(),
        }
    }

    fn last_record_start(&self) -> u64 {
        match &self.inner {
            Handler::Lines(h) => h.last_record_start(),
            Handler::Document(h) => h.last_record_start(),
        }
    }
}
```

mod.rs 测试模块：保留引擎组合用例（`run`/`run_err`/`s`/`raw`/`AWS_EXAMPLE` 及其全部测试）与两个直接构造 `Reader::new` 的二义钉（`traversal_case_insensitive_duplicates_are_ambiguous`、`traversal_bracket_name_duplicates_are_ambiguous`）——façade `Reader::new` 签名不变，全部零改动。`document_root_array_select_star` 名字像 DOCUMENT 迁移用例，实属引擎组合（用 `run`）——同样留守，勿按前缀迁出。

- [ ] **Step 2: 全套件 + 新实现的后测数**

Run: `cargo test -p tinio-select && cargo bench -p tinio-select -- json_document_scan`
Expected: 全绿；bench 产出**切换后**的数字。把它与 Task 11′ 的基线一起记入本节末尾的「Task 8 实测」块（`time`/`thrpt` 两列、`select_star` 组、`JSON_ELEMENTS` 条）——**不写进 `mod.rs` 的模块文档**（docs/style.md 的 compress-hard：模块文档只留方向性结论，原始 criterion 数字归计划）。缓冲层为**单层**（第二轮修订曾默认双层；第三轮经 std 源码核实第二层为透传——`BufReader::read` 对 ≥ 容量的请求整体旁路其缓冲——已删，构造与字段同为 `LineFraming<R>`/`DocFraming<R>`）。

- [ ] **Step 3: clippy + fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo +nightly fmt --all -- --check`
Expected: 干净。

- [ ] **Step 4: 检查点**

报告 `json/mod.rs` 待提交。

**Task 8 实测（criterion，2026-09-13 记录）**：`json_document_scan/select_star`，fixture 897 751 B（< 1 MB，旧整文档路径同样接受），旧整文档 façade → 新流式 façade：`time` 37.023 ms（CI 34.204–39.186 ms）→ 58.620 ms（CI 49.368–65.262 ms）；`thrpt` 23.125 MiB/s（CI 21.849–25.031 MiB/s）→ 14.605 MiB/s（CI 13.119–17.342 MiB/s）。Criterion change line：`time +44.064%` / `thrpt −30.586%`（p = 0.02）——真实回归，非基线 ±4.5% 的 run-to-run 波动。同实现重复跑估得 51.311 ms / 16.686 MiB/s（对首跑 `new` 估计 p = 0.99，"no change"），故读方向而非点估计。`mod.rs` 模块文档只保留方向性结论与本块的指针。

---

### Task 9: record.rs 注释 + 2026-09-04 文档 supersede 标注

**Files:**
- Modify: `crates/tinio-select/src/record.rs:100-106`、`crates/tinio-select/src/record.rs:116-124`、`crates/tinio-select/src/record.rs` 测试 `document_start_above_zero_yields_nothing`
- Modify: `docs/superpowers/specs/2026-09-04-select-object-content-design.md`
- Modify: `docs/superpowers/plans/2026-09-04-select-object-content.md`

**Interfaces:**
- Consumes: 无。
- Produces: 文档一致性（spec"同步更新"节全部条目）。

- [ ] **Step 1: record.rs 注释**

`MAX_RECORD` 注释（`record.rs:100-104`）替换为：

```rust
/// AWS input-record cap (design §1, review 2026-09-05 #3): a record over
/// 1 MB errors the stream out — bounds memory, defuses decompression bombs.
/// Measured on the reader's own span accounting: the record's first byte
/// through its terminator (a span that may also include comment lines
/// immediately preceding the record, which the `csv` parser consumes inline).
/// Amended 2026-09-12 (json-streaming): for JSON the cap binds each root
/// (a DOCUMENT array's elements count separately; a single-value root is
/// one record). Amended 2026-09-13 (Q9(a)/Q15): the cap is charged per
/// root PULL — every served byte counts (matched values, skipped subtrees
/// and delimiters alike) — and deviation ③'s tier is NOT outside it: a
/// single-value root's pull is charged like any other, so an over-1 MB
/// document errors instead of accumulating without bound. The memory
/// backstop stays "skip without materializing + one root ≤ 1 MB".
```

`RangeFilter` 文档末句（`record.rs:121-124`）"JSON DOCUMENT's whole object is one byte-0 record, so `start > 0` yields nothing — correct, not a bug (spec §Background)." 替换为：

```rust
/// JSON DOCUMENT records report their root element's start (2026-09-12
/// json-streaming, spec D6): the window filters them like any other
/// framing; a single-object root still starts at byte 0.
```

测试 `document_start_above_zero_yields_nothing`：断言不动，注释改为"the single object root starts at byte 0, so `start = 1` still drops it — element-level offsets keep this case (spec D6)"。

- [ ] **Step 2: 2026-09-04 设计文档标注（保留原文，追加标注行，不静默改写）**

`docs/superpowers/specs/2026-09-04-select-object-content-design.md` 三处：

1. "JsonReader: LINES (serde_json per line) and DOCUMENT (whole object is one record)…" 行末追加：`(superseded 2026-09-12: DOCUMENT streams root-array elements one record at a time — docs/superpowers/specs/2026-09-12-json-streaming-design.md)`。（spec 同步更新条目 1 的字面是"更新为流式逐记录…表述"；此处与条目 2/3 一致采用保留原文 + 追加标注，属已声明的形式偏离——spec 侧已补 Amendment。）
2. "**ScanRange**: …with JSON `Type=DOCUMENT` the whole object is one record starting at byte 0, so `start > 0` yields **zero records** — correct behavior…" 该句后追加：`**Superseded 2026-09-12** (json-streaming deviation ⑥): DOCUMENT records report element-start offsets and the window filters them.`
3. "**Both input and output records are capped at 1 MB**…" 该句后追加：`(input side amended 2026-09-12: the cap binds each record root)`。**（Amended 2026-09-13）**措辞由 `each yielded JSON record` 改为 `each record root`——与偏差①自身修正案（"每个根"）一致：按命中计费会漏掉未命中的超限元素。

`docs/superpowers/plans/2026-09-04-select-object-content.md`：`grep -nE "byte[- ]0" docs/superpowers/plans/2026-09-04-select-object-content.md`（**必须兼容无连字符写法**：Task 9 的 "DOCUMENT … byte-0-anchored but still capped at 1 MB"（line 376）与 Task 10 的 RangeFilter 注记（line 394，原文用词是 "byte 0"，只 grep "byte-0" 会漏掉）），两处各追加 `(superseded 2026-09-12 — see 2026-09-12-json-streaming-design.md)`。

- [ ] **Step 3: 验证**

Run: `cargo test -p tinio-select record && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
Expected: 绿；doc 无警告。

- [ ] **Step 4: 检查点**

报告 `record.rs` 与两份 2026-09-04 文档待提交。

---

### Task 10: git LFS + 大文档 fixture + CI 流式测试

**Files:**
- Create: `.gitattributes`
- Modify: `.github/workflows/ci.yml:63-64`（`&checkout` 锚点）
- Create: `crates/tinio-select/tests/fixtures/large/document.json.gz`（生成，LFS）
- Create: `crates/tinio-select/tests/stream_large.rs`（行为钉 + `#[ignore]` 生成器同一文件，故 `ELEMENTS` 只有一处定义）

**Interfaces:**
- Consumes: `crate::record::{decompressed, Compression}`、`json::{Reader, Params, Type}`、`sql::parse`、`flate2`（tinio-select 的常规依赖，integration test 可用）。
- Produces: CI 上的大文档流式行为钉 + cap 行为钉（spec 测试计划条目要求"断言行数、cap 行为、完成与偏移单调"）。

背景事实（已核实）：仓库此前无 LFS（无 `.gitattributes`、`git lfs ls-files` 为空、CI 未开 lfs）；本机已装 git-lfs 3.7.1，但**仓库未做 repo-local 初始化**。CI 的全部 16 个 job 共用 `ci.yml:63-64` 的 `&checkout` 锚点（只有 `actions/checkout@v7`，无 `with:`），因此 `lfs: true` 一处生效于所有腿——包括 windows/macos 的 `test-port`，不会有哪个腿拿到指针桩。**内存闸不在本任务**：spec 的计数 `#[global_allocator]` 被 `unsafe_code = "forbid"`（覆盖本 crate 全部 target，含 `tests/`）挡住，已改为 Task 7 的 crate 内 `pending_high` 高水位 pin。

**CI 成本注记（评审补记）**：`lfs: true` 经共享锚点对全部 16 个 job 生效——fmt/lint/docs 等不跑该测试的腿同样会拉取 fixture（gzip 后 ~1 MB 量级，可接受；若 LFS 带宽/配额成问题，退路是 test 系 job 单独写 checkout 开 lfs）。行为钉在每个平台腿全量跑 ~16 MB 逐字节解析（serde_json `IoRead` 是 `io::Bytes`）：吞吐基线由**前移的 Task 11′**先出数（各平台差异可从 CI 日志读，不必另设 bench），**默认仍全平台跑**——`#[cfg(target_os = "linux")]` 只在数出来确实不可接受时才启用，且启用这个降级是需要显式记录的决策（cap 钉数据量小，任何情况下各平台保留）。规模经用户裁定从 400k/~64 MB 下调至 100k/~16 MB（2026-09-13）：>1 MB 总量流式、行数与偏移单调的钉住不依赖更大尺寸，逐字节解析成本约降为 1/4。

- [ ] **Step 1: .gitattributes**

Create `.gitattributes`：

```
crates/tinio-select/tests/fixtures/large/** filter=lfs diff=lfs merge=lfs -text
```

- [ ] **Step 2: git lfs install（**必须早于 Step 5 的生成器**）**

```bash
git lfs install
git check-attr filter -- crates/tinio-select/tests/fixtures/large/document.json.gz
```

Expected: hooks 安装；`check-attr` 打印 `...: filter: lfs`。**这条检查是这道闸的全部**：`git lfs install` 未执行时 `.gitattributes` 是惰性的，此期间任何 `git add` 都会把二进制永久写进历史。`git check-attr` 与 `git add` 的 clean filter 都读**工作树**的 `.gitattributes`——与跟踪状态无关，未跟踪同样生效，**无需提前提交**（与"不做任何 git 写操作"一致）。若你不愿让 agent 代跑，请自行执行，并把输出贴回。

- [ ] **Step 3: CI 开 lfs**

`.github/workflows/ci.yml` 的 `&checkout` 锚点改为：

```yaml
      - &checkout
        uses: actions/checkout@v7
        with:
          lfs: true
```

- [ ] **Step 4: 创建完整的 `tests/stream_large.rs`（只建文件，不跑测试）**

完整文件（生成器照 `tests/parquet.rs` 的 `#[ignore]` 成例，与行为钉同文件，`ELEMENTS` 因此只有一处定义；行为钉此时不能跑——fixture 尚未生成）：

```rust
//! Streaming a large DOCUMENT end to end (spec 2026-09-12): row count,
//! completion, monotonic element-start offsets. The fixture is the
//! gzip-compressed ~16 MB synthetic document in git LFS; the read path is
//! the production one (`decompressed(Gzip)`).

use std::{fs::File, io::Cursor};

use tinio_select::{
    json,
    record::{Compression, RecordReader, decompressed},
    row::Record,
    sql::parse,
};

/// Fixture size, shared with `generate_large_fixture` below.
const ELEMENTS: u64 = 100_000;

fn fixture_reader() -> json::Reader<Box<dyn std::io::Read + Send>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/large/document.json.gz");
    let file = File::open(path).expect("LFS fixture missing — run `git lfs pull`");
    let input = decompressed(Some(Compression::Gzip), Box::new(file));
    let plan = parse("SELECT * FROM S3Object").unwrap();
    json::Reader::new(input, json::Params { ty: json::Type::Document }, &plan.from)
}

#[test]
fn large_document_streams_to_completion() {
    let mut r = fixture_reader();
    let mut count = 0u64;
    let mut last = 0u64;
    while let Some(rec) = r.next().unwrap() {
        assert!(matches!(rec, Record::Json(Some(_))));
        let start = r.last_record_start();
        assert!(start >= last, "record starts not monotonic: {start} < {last}");
        last = start;
        count += 1;
    }
    assert_eq!(count, ELEMENTS);
}

#[test]
fn over_cap_element_errors_after_prior_records() {
    // The spec's cap-behaviour pin on the CI path (inline data, not the LFS
    // fixture). `CAP_MESSAGE` is `pub(crate)`, so the literal is pinned here.
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = json::Reader::new(
        Box::new(Cursor::new(doc.into_bytes())) as Box<dyn std::io::Read + Send>,
        json::Params { ty: json::Type::Document },
        &plan.from,
    );
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    match r.next() {
        Err(tinio_select::Error::Format(m)) => assert_eq!(m, "input record exceeds 1 MB"),
        other => panic!("expected the reader cap error, got {other:?}"),
    }
}

/// Regenerate the fixture:
/// `cargo test -p tinio-select --test stream_large -- --ignored`.
/// Same provenance shape as `tests/parquet.rs`'s ignored generator.
#[test]
#[ignore = "regenerates the LFS fixture"]
fn generate_large_fixture() {
    use std::io::Write;

    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/large/document.json.gz");
    let file = File::create(path).expect("create fixture");
    let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    gz.write_all(b"[\n").unwrap();
    for i in 0..ELEMENTS {
        write!(
            gz,
            "  {{\n    \"id\": {i},\n    \"name\": \"record-{i:08}\",\n    \"price\": {}.5,\n    \"tags\": [\"alpha\", \"beta-{i:08}\"]\n  }}{}\n",
            i % 997,
            if i + 1 < ELEMENTS { "," } else { "" }
        )
        .unwrap();
    }
    gz.write_all(b"]\n").unwrap();
    gz.finish().unwrap();
}
```

注意：`json::Reader<Box<dyn Read + Send>>` 需要 `R: Read` 满足——`Box<dyn Read + Send>` 实现 `Read` ✓。导入必须含 `RecordReader`（`next`/`last_record_start` 是 trait 方法，不导入编译失败）；`Cursor` 由 cap 钉使用（空导入会在 Task 12 的 clippy `-D warnings` 下报错）。

- [ ] **Step 5: 跑生成器（**Step 2 验证通过后才跑**）**

Run: `cargo test -p tinio-select --test stream_large -- --ignored`
Expected: 生成 `document.json.gz`（解压后 ~16 MB，gzip 后 ~1 MB 量级）。

- [ ] **Step 6: 确认 fixture 已归 LFS 管辖**

```bash
git status --short crates/tinio-select/tests/fixtures/large/
git check-attr filter -- crates/tinio-select/tests/fixtures/large/document.json.gz
```

Expected: fixture 显示为未跟踪新文件；`check-attr` 仍返回 `filter: lfs`。**不 add/commit**——由用户决定入库时机；add 之前先确认这两条输出，否则二进制永久进 git 历史。

- [ ] **Step 7: 跑测试**

Run: `cargo test -p tinio-select --test stream_large`
Expected: 全绿（`generate_large_fixture` 被 `#[ignore]` 跳过）。内存闸不在此文件——它是 Task 7 的 `document_array_root_keeps_pending_bounded`（字节口径）/ `document_single_root_under_cap_yields_all_matches` / `document_single_root_over_cap_errors` 单测，随 `cargo test -p tinio-select` 一起跑。

- [ ] **Step 8: 检查点**

报告 `.gitattributes`、`ci.yml`、fixture（LFS）、`tests/stream_large.rs` 待提交；提醒用户 LFS 首次推送需配额。**顺序已在本任务的 Step 1→2→5 里固定**（`.gitattributes` → `git lfs install` + `check-attr` 验证 → 生成器），提交时机由用户定，但 `git add` 之前必须重跑一次 `git check-attr filter -- <fixture>` 确认为 `lfs`。

---

> **（Task 11 已按 2026-09-13 评审前移——见 Task 8 之前的同名任务。）**

---

### Task 12: 验收 gate

**Files:** 无（纯验证）。

- [ ] **Step 1: 编译图**

Run: `cargo check --workspace --all-targets && cargo hack check --package tinio --each-feature --no-dev-deps && cargo hack check --package tinio-server --each-feature --no-dev-deps`
Expected: 干净。

- [ ] **Step 2: lint**

Run: `cargo +nightly fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: 干净。

- [ ] **Step 3: 单测（双图 + doctest）**

Run: `cargo test --workspace --exclude tinio-e2e && cargo test --doc --workspace --exclude tinio-e2e && cargo test --workspace --exclude tinio-e2e --no-default-features && cargo test --doc --workspace --exclude tinio-e2e --no-default-features`
Expected: 全绿。

- [ ] **Step 4: feature 腿**

Run: `cargo test -p tinio-select --features parquet && cargo test -p tinio-server --features select-parquet`
Expected: 全绿。

- [ ] **Step 5: doc + bench 冒烟**

Run: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps && cargo bench -p tinio-select -- --test`
Expected: 干净（`json_document_scan` 已在前移的 Task 11′ 中落地并两面都测过）。

- [ ] **Step 6: e2e select 相关腿（平台允许时）**

Run: `cargo test -p tinio-e2e --test cucumber -- --retry 2`（或 docs/tests.md 的平台限定变体）
Expected: 全绿。

- [ ] **Step 7: 最终检查点**

汇总全部待提交文件清单报告用户：**不做任何 git 写操作。**

---

## Self-Review 记录

- Spec 覆盖：模块拆分（T1）、解析内核/framing（T2–T4）、状态机（T5）、两处理器（T6/T7）、façade（T8）、偏差①（T7 `document_array_larger_than_1mb_streams` + events 层 `json_document_streams_past_1mb_then_stats`）、②（T5 `non_final_name_duplicate_fails_mid_stream`）、③（T7 `document_single_root_under_cap_yields_all_matches` 钉其可观察语义，**并按 Q15 修订**为"受 1 MB 记录预算兜底"，另加 `document_single_root_over_cap_errors` 钉上界）、④（T6 两处填充）、⑤（T7 `document_element_over_cap_errors_after_prior_records` + events 层 `json_document_reports_the_cap_after_prior_records`）、⑥（T7 偏移钉 + record.rs ScanRange 用例 + T9）、测试计划全部条目（T6/T7 + T10，含补入的纯空白行、缩进行与未闭合文档）、内存闸（T7 `pending_high` 两例，**字节口径**，替代 spec 的计数分配器）、cap 归属（T5 两条 + T7 的 `arm()` 位置，见下）、bench（T11′，**前移**）、同步更新（T9）、交付与 LFS（T10）、验收命令（T12）。无缺口。
- 占位符扫描：无 TBD/TODO；所有代码步骤含实际代码（`json/testing.rs` 的五个 helper 是既有代码原样搬移，故只列签名与模块头）。
- 类型一致性：`Budget::{reset, fired}`（**reset 由处理器调用**，matcher 不持有；**Amended 2026-09-13**：原 `{arm, disarm, fired}`）、`BudgetedRead::new(inner, Rc<Budget>)`、`LineFraming::{new, start_line, capped}`/`LineStart::{Line(u64), Eof}`、`DocFraming::{new, peek_mode, closed, mode, element_start, framing_msg}`/`DocMode::{Unknown, Empty, WsOnly, Array, Single}`、`CompiledPath::from_segments(segments, ty)/steps()`、`Skip`、`CaptureSeed`/`StepSeed`（**两/三字段内联，无 `Sink`、无 `budget`**）、`Lines::new(reader, from)`/`Document::new(reader, from)`、`Document::pending_high`（`#[cfg(test)]`，**字节**口径）——跨任务引用一致。
- 已知实现风险（执行时按 Global Constraints 的 serde_json 事实核对）：NUMBER_TOKEN 硬编码；**每次拉取新建的 Deserializer 会丢掉 `IoRead.ch` 里那个已从 framing 消费的字节**（故单值模式的探测必须复用同一个，见 T7）；`Skip` 的键路径依赖 `MapKey::deserialize_any`（若字符串键不走 `visit_str`、或非字符串键不报错，退路是键侧改回 `next_key::<String>`）；`is_eof`/`is_io`/`io_error_kind` 为唯一错误分类手段。
- **复审修订（2026-09-13 计划评审，grilling 三轮，全部落实为文档修订）**：① **cap 归属**——`Budget::arm()` 原在 `CaptureSeed`/末步 Name 内逐值调用（F1：跨捕获累积，会把"每个元素 400 KB、共 1.6 MB"的文档在第二个元素上误报 cap），且未命中的子树完全不武装、framing 的中途 cap 也无从计费；改为**处理器在每个根拉取首尾各调用一次**（Task 6/7 的 `pull_line`/`pull_root`），cap 语义变为"一个根 ≤ 1 MB"，两个 seed 随之删掉 `budget` 字段与全部 arm/disarm（Task 5）。这同时让 `document_input_record_cap_exact_allowed`（恰好 1 MB 的单值根，`json.rs:692`）以 1 048 576/1 048 576 压线通过。② **偏差③不再"无上界"**（Q15）——单值根的一次拉取同样被计费，故超 1 MB 即 cap 错；Task 8 模块注释、Task 7 测试（`document_single_root_over_cap_errors` / `document_single_root_under_cap_yields_all_matches` 取代原 `document_single_root_fanout_yields_all_matches`）与 **spec 偏差③正文**同步修订。③ **`pending_high` 改字节口径**（Q2/Q16）——条数在 20 000 个微对象上恒为小值，与内存无关；改为累计入队 `Value` 的序列化字节数，数组根断言 ≤ 64 字节量级。④ **双层 BufReader**（Q3/Q17）——serde 侧再包一层，免除 `IoRead` 逐字节驱动；bench 前移后用实测数字决定去留。⑤ **Task 11 前移为 Task 11′**（Q3/Q17）——bench 必须在 Task 8 删除旧实现**之前**落地并跑出基线，数据集取 ~800 KB（< 1 MB，旧实现也读得动）；Task 8 模块注释留 `<old> → <new>` 占位。⑥ **LFS 步骤顺序**（Q19）——Task 10 重排为 `.gitattributes` → `git lfs install` + `git check-attr` 验证 → 生成器 → 提交，把"二进制永久进历史"这个唯一不可逆动作挡在验证之后。⑦ **事实更正**：`serde` 是 staged 未提交（非"已就位"）、`unsafe_code = "forbid"` 是逐 crate（无 workspace 级设置）、trailing 检查由 `from_slice` 内部调 `end()`（tinio 侧无显式调用）、大 fixture 默认全平台（`#[cfg(target_os = "linux")]` 仅作退路）。
- 复审修订（2026-09-12 计划评审，全部落实为文档修订）：① 三处 BufReader 双层包装（Task 3 helper 返回类型、Task 6/7 处理器字段 → `LineFraming<R>`/`DocFraming<R>`——`new(BufReader<R>) -> Self` 返回 `…<R>`，原声明会 E0308）；② Task 5 全量 clippy gate 移至 Task 8（`--all-targets` 含无 cfg(test) 的 lib target，framing/matcher 接线前必报 dead_code）；③ Task 6 Step 0 留守导入 {raw, run_err} → {raw, run, run_err, s}，迁移计数 29 → 26，traversal 口径写清（16 个 = 13 迁 + 2 二义留守 + 1 引擎留守）；④ Task 10 补 spec 要求的 cap 行为钉，并补 `RecordReader` 导入与 `Cursor` 的实际使用点；⑤ Task 9 grep 改 `byte[- ]0`（line 394 是 "byte 0" 无连字符），条目 1 声明形式偏离；⑥ ServingElement 删 `debug_assert!(n > 0)`（`[}]` 类畸形输入在 debug 构建会 panic，release 本就返回 Ok(0) 走 Format 错误）；⑦ IoRead 事实校正（read.rs:200-207 / 286-298；逐字节系 std `io::Bytes` 行为，"out.len() == 1" 非 serde_json 源码原文）；⑧ Task 10 补 CI 成本注记（16 job 全拉 LFS、全平台跑 64 MB 钉的退路）。
- 复审修订（2026-09-13 计划评审·第二轮，全部落实为文档修订）：① Task 6/7 字段声明跟上双层 BufReader（`LineFraming<BufReader<R>>`/`DocFraming<BufReader<R>>`——修订④只改了构造函数，会 E0308）；Task 8 的"去层"提示补上字段类型同步。② Task 5 `pull_with_budget` 补 `BudgetedRead` 包装——原写法 armed budget 零计费、两条 `!fired` 断言恒真；`budget_is_charged_per_root_not_accumulated` fixture 改单数组四元素（原四数组串接必触发 trailing characters，total==1）。③ Task 9 的 `MAX_RECORD` 注释同步 Q9(a)/Q15（不再 "YIELDED record"，删 "One tier sits outside"）。④ Task 10 顺序修正：完整 `tests/stream_large.rs`（含 `ELEMENTS`/imports）先于生成器运行（原顺序生成器先于文件存在）；删 Step 2 的"`.gitattributes` 需已提交"错误断言（`check-attr`/`git add` 都读工作树属性，与跟踪状态无关）。⑤ spec §PathSeg 编译状态机补 Amendment：`StepSeed` 字段、逐值武装、`IgnoredAny` 三处与 2026-09-13 修正案对齐。⑥ 计数与占位修正：Task 5 测试数 12→11；Task 11′/Self-Review 占位串与 Task 8 实际的 `<old> → <new>` 统一。⑦ `BudgetedRead::new` 去掉 `impl Into<Rc<Budget>>`（注释理由不成立，调用点本就 `Rc::clone`）。⑧ `Lines::new` 缓冲层注释改为正确机制（`IoRead` 永远逐字节调 read，缓冲只降低单次成本；底层那层去留由 Task 8 bench 决定）。⑨ 检查点补漏：Task 6 加 `json/testing.rs`、Task 7 加 `events.rs`；Goal 行"拉取结束即报"改"拉取中途即报"。
- 复审修订（2026-09-13 计划评审·第三轮，外部评审记录 `.workbuddy/memory/2026-09-13.md`，全部落实为文档修订）：① **缓冲层回归单层**（推翻修订④的双层决策）——回 std 源码核实：`BufReader::read` 对 ≥ 容量的请求整体旁路自身缓冲，framing 自带缓冲的 refill 恰为容量大小，故贴着原始 reader 的第二层是透传、从不生效；`Lines`/`Document` 的构造与字段回到单层（`LineFraming<R>`/`DocFraming<R>`），Task 8 Step 2 与模块注释同步。（外部评审所列的 E0308、`pull_with_budget` 空断言、fixture 串接三项阻断针对第二轮修订前快照，均已在第二轮修复。）② `element_barely_under_cap_is_accepted` 余量 0 → 1 字节（content 改 `MAX_RECORD - 3`）：断言依赖"字符串自终止、闭合引号后不多读"，余量化后该事实不进关键路径；恰好 1 MB 边界归 handler 级 `document_input_record_cap_exact_allowed`（json.rs:691）。③ 错误路径收口：两个 `next()` 在 `pull_*` 报错时闩 `done = true`，`Err` 后再轮询看到终止流而非重试拉取。④ "每一个字节都计费"措辞精确化（Architecture/Global Constraints/Task 2 两处注释/spec §帧化）：framing 内部吞掉的顶层 `[`/`]`/`,` 与 LINES `\n` 不经 `BudgetedRead`，由 framing span/偏移记账。⑤ spec 补两条 Amendment：§解析内核 `next_key::<IgnoredAny>()` → `next_key_seed(Skip)`；测试计划 `document_element_over_cap_errors` 与实现对齐改名 `…_after_prior_records`。⑥ 引用行号更正：Task 7 注的 json.rs:681/692 → 680/691；外部评审所提 `docs/cargo.md:10 → :9` 经核**不成立**（逐 crate `unsafe_code` 条款确在 cargo.md:10），维持原引用。⑦ CI fixture 规模经用户裁定下调：400k 元素/~64 MB → 100k 元素/~16 MB（`ELEMENTS`、Step 5 Expected、CI 成本注记与 spec 测试计划/交付节同步）——行数、cap 行为、完成与偏移单调的钉住不依赖更大尺寸，逐字节解析成本约降为 1/4。
- 复审修订（2026-09-13 计划评审·第四轮，全部落实为文档修订）：① `visit_map` 的 NUMBER_TOKEN 分支删 `debug_assert!`——serde_json 只对真数字 token 走 `NumberDeserializer`（de.rs:127、938-953），`deserialize_any` 的 `{` 分支不做特判（de.rs:1444-1454），故首键为保留键的真实对象经原始 `MapAccess` 到达、多键时 assert 在 debug 构建 panic（不可信输入）；改为显式 `Error::custom`——与今日 `from_slice::<Value>` 对该类对象报错的语义一致（value/de.rs:125-129 先 `KeyClassifier` 后 `NumberFromString`，非字符串值/多键均报错）；单键“token map”仍按数字处理（serde_json 对 `arbitrary_precision` 的文档化碰撞，今日同样误判），值不回验数字字符串（今日会报错，属保留键上的已记录微偏差）。补两条测试：多键报错不 panic、单键按标量零命中；Task 5 测试数 11 → 13。② 计费措辞再精确：未计费字节含 `skip_ws` 吞掉的空白（不止顶层分隔符与 `\n`）——Architecture/Global Constraints/Task 2 两处注释/spec §帧化 Amendment 同步。③ Task 8 偏差②注释收敛：已产出行留存于 `pending`，事件层首个错误即停、不再 yield。④ Task 8 留守清单点名 `document_root_array_select_star`（名字像 DOCUMENT 用例，实属引擎组合）。
- 复审修正（2026-09-13 评审后，第五轮；审查固定点 HEAD `5e7ee2a`，diff = 全部已 staged 改动）：① **共享核**——两个处理器的孪生结构（字段、`new()`、`next()` 循环、拉取骨架，约 80 行）抽为 `json/core.rs` 的 `Core`：持 `budget`/`path`/`pending`/`failure`/`last_record_start`/`done`，共享 `pull()`（重置预算 + 建 `Deserializer` + `child_into` + 同源探测）与 `advance()`（`next()` 循环与终止错误规则）。`Lines`/`Document` 只留各自 framing 与 `pull_line`/`pull_root`——后者为自由函数（`&mut Core` + `&mut DocFraming` + `&mut produced`），因为闭包版与 `&mut self` 在 `core.advance` 上借用冲突。`pending_high` 随 `pending` 迁入 `Core`（`#[cfg(test)]`），测试引用改 `r.core.pending_high`。② **`Step` 删除**（详见 Task 5 条）。③ **二义谓词共享**——新增 `json::value::key_matches(key, name, style)`，`json_lookup` 与 `StepSeed` 的 Name 分支同用（后者传 `NameStyle::Bare`），兑现 R10"两处不得 drift"。④ **fixture 生成器共享**——新增 `tests/support/json_fixture.rs`（`#![allow(dead_code)]`，两个二进制各用其一）：`element_compact`/`element_pretty` 同源给出形状（`record-{i:08}`、`beta-{i:08}`、`i % 997`），布局与 `price` 小数位各自保留（bench 必须 < 1 MB、test 美化打印）。**已核验**：`cargo test --test stream_large -- --ignored` 重生成的 fixture 与提交版本 sha256 逐字节一致。⑤ **测试钉口放宽**——`document_single_root_trailing_garbage_errors` 不再钉 serde 的字面报文（`expected value at line 1 column 4` 等），改为断言"是 `json input` 错误、且是否为自有的 trailing-characters 报文"：保住原注释要的区分力，去掉 serde_json 升级即误伤的脆弱。⑥ **陈旧注释清理**——`document.rs` 模块文档、`matcher.rs` 两处注释里残留的 `Budget::arm`/`disarm`/`armed` 措辞改为 `reset`（协议已于 2026-09-13 简化为单一 `reset()`）。⑦ **实现期未授权改动的授权与补记**（2026-09-13 经用户确认）：(a) `Skip::visit_map` 的 NUMBER_TOKEN 探测（`TokenProbe`，`visit_str` 内比较、无分配）+ 一条新测试，补齐被跳过子树的保留键校验，使 Skip 与遍历路径一致；(b) 四条未计划测试 `lines_error_is_terminal_for_later_polls`、`lines_final_unterminated_line_under_cap_is_one_record`、`document_error_is_terminal_for_later_polls`、`document_wild_over_object_collapses_duplicate_keys`；(c) `mod.rs` 的 `pub(crate) fn json_error`（原计划此逻辑内联于两个 handler 的 `map_err`）；(d) `LineFraming::read` 的换行扫描窗口由整个 `avail` 收窄为 `out.len() + 1`——原实现每供出一字节即全缓冲扫描，行长 L 时为 O(L × min(L, 8 KiB))；收窄后逐字节等价（`out.len()` 处的 `\n` 仍终止本行），tinio-select lib 测试耗时 4.98 s → 0.61 s；(e) `ServingElement` 的 EOF/`n == 0` 路径相对计划代码块重构。⑧ **Wild-over-object 重复键折叠授权**：Wild 步落在对象上时按键分组缓冲、重复键 last-wins 且保留首现位置——复现被删旧管线 `from_slice::<Value>`（`preserve_order` 的 IndexMap）的折叠语义，属修偏差而非加特性，三条测试钉住。⑨ **偏差③措辞**对齐（见"同步更新"节）。
