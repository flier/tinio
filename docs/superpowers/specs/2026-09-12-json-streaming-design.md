# JSON 流式读取器设计：LINES / DOCUMENT 双处理器 + PathSeg 编译状态机

- 日期：2026-09-12
- 状态：已评审（brainstorming 流程，设计经用户逐节确认；grilling 两轮：偏差④⑤⑥、LFS 与测试形态已裁定）
- 范围：`crates/tinio-select/src/json.rs` 重构；`tinio-select/Cargo.toml` 新增一个已固定的依赖；git LFS 首次引入（测试 fixture）+ CI checkout 调整
- 语言说明：本文档应用户明确要求用中文撰写（CLAUDE.md 的 English-only 规范对本文件豁免）；实现中的代码、注释、提交信息仍为英文

## 背景与问题

S3 Select 的 JSON 输入有两种帧化（`InputSerialization.JSON.Type`）：LINES（每行一个 JSON 值）与 DOCUMENT（整个流是一个 JSON 文档，根为数组时逐元素为记录）。当前实现 `json.rs` 为单一 `Reader<R>`，在 `next()` 内按类型分发：

- LINES（`read_line`）：整行读入 `Vec<u8>`（≤ 1 MB 行宽上限）后 `serde_json::from_slice` 整体物化为一个 `Value`，再走 `apply` 候选集展开。
- DOCUMENT（`read_document`）：整文档读入 `Vec<u8>`（唯一防线是整文档 ≤ 1 MB 上限）→ 整体物化 → `pending` 队列一次性展开全部根元素。三重全量分配（字节、`Value`、`pending`），文档 > 1 MB 直接报错（`document_input_record_cap`）。

目标：两个模式各自流式解析与处理，任何时刻物化的只有当前记录，内存上界 = 一条记录（≤ 1 MB），与输入总大小无关；大 DOCUMENT 不再整体加载。引擎契约（`record.rs` 的 `RecordReader`：`next() -> Result<Option<Record>, Error>`，`Record::Json(Option<Value>)`）决定记录终须完整物化——流式的意义是把内存上界收敛到一条记录，而不是零。

非目标：CSV / Parquet 读取器的流式化；`RecordReader` 拉取契约的异步化（crate 保持同步拉取，`unsafe_code = "forbid"` 不变）；输出侧（事件打包）的任何改动；引擎 select 侧的按列克隆。

## 总体架构

`json.rs` 拆为 `json/` 目录：

- `json/mod.rs` — 模块文档（重写，含本设计的偏差记录）、`Type`、`Params`、分发入口 `Reader`（`enum Handler { Lines, Document }`）、`pub(crate) use value::{json_lookup, to_value};` 再导出（`engine.rs` 继续以 `crate::json::{to_value, json_lookup}` 解析，调用点不变）、共享测试辅助与引擎组合测试。（**Amended 2026-09-13**：共享测试辅助独立为 `json/testing.rs`（`#[cfg(test)]`）——20 个迁出用例跨 lines/document 两个测试模块引用同一组 helper；引擎组合测试仍留 mod.rs。）
- `json/value.rs` — `to_value`、`json_lookup` 原样迁入（无行为变化）。
- `json/framing.rs` — 帧化过滤器（`Read` 适配器）：`LineFraming`、`DocFraming`、`BudgetedRead`。
- `json/matcher.rs` — `CompiledPath`（PathSeg 编译产物）+ `StepSeed` 访问者状态机。
- `json/lines.rs` — `pub struct Lines<R>`：行帧化驱动的 LINES 处理器。
- `json/document.rs` — `pub struct Document<R>`：文档帧化驱动的 DOCUMENT 处理器。

接口保持不变：`json::Reader::new(reader, params, from)` 签名不变，`events.rs` 的 `build_reader`、`record.rs` 的 ScanRange 测试、`tinio-server` 的 `backend/select.rs` 参数映射全部零改动；`Record::Json(Option<Value>)` 记录模型不变。旧 `apply`/`expand`/`read_document`/按行整体 `parse` 删除。`tinio-select/Cargo.toml` 新增 `serde.workspace = true`（workspace 已固定 `serde = "1"`，`serde_json` 本就连带引入，传递闭包零增长）。

## 解析内核

统一走 serde_json 流式反序列化：`serde_json::Deserializer::from_reader`（serde_json 的 `StreamDeserializer` 同族机制）+ 逐值拉取。serde 的 `MapAccess`/`SeqAccess` 拉取回调即事件流，直接驱动编译后的 `StepSeed` 状态机：

- 未命中的子树用**自写的校验型跳过 `Skip`** 消费——取 `deserialize_any`（绝不用 `deserialize_ignored_any`：它在任何 visitor 运行前就把值消费掉）使字符串以 `&str` 到达、被校验后丢弃；`visit_seq`/`visit_map` 以自身递归，键同样经它。不物化，内存仍 O(1)。**Amended 2026-09-13**：原定用 `serde::de::IgnoredAny` 并称"serde 对全流做完整校验…无校验缺口"——经 serde_json 1.0.151 源码核对不成立：`IoRead::ignore_str`（`read.rs:348-366`）只校验转义形状与 `\uXXXX` 位数，**不校验代理对**（`ignore_escape` 不走 `parse_unicode_escape`）、**不校验 UTF-8**（跳过分支不过 `str::from_utf8`），于是 `{"a":1,"b":"\uD800"}` 这类今日 `from_slice` 拒绝的输入会被静默接受。`Skip` 逐点补齐，校验覆盖与今日一致。
- 只有末步命中的子树物化为 `serde_json::Value`；`arbitrary_precision` 数字保留原始 token（`1e309` → `1e+309` 的归一化由 serde 完成，`raw_number_passthrough_no_decimal_parse` 语义不变）。
- 键经 `next_key::<String>()` 由 serde 解码（`\uXXXX` 转义自动处理），跳过作用域用 `next_key::<IgnoredAny>()` 避免逐键分配。**（Amended 2026-09-13·二轮）**跳过作用域实际用 `next_key_seed(Skip)`——同样不分配，且键内容经 `Skip` 校验（与今日 `from_slice` 逐点一致），`IgnoredAny` 全面弃用（见本节 `Skip` 条的 Amendment）。

为什么不直接用 `StreamDeserializer::into_iter`：它只能产出完整顶层值（根数组会作为一个整体值返回），且无法在值中途挂起，做不到"每次拉取 = 一个记录根"。因此配一个帧化过滤器重塑字节流（见下节），用 from_reader + `DeserializeSeed` 逐值拉取。

已否决的备选：手写边界扫描器 + 可恢复显式栈状态机（该方案的可恢复栈与元素级偏移成立，但扫描器只判边界、不判标量，跳过区域的非法标量不再报错，与"serde 全量校验优先"的裁定冲突——grilling Q2 因此裁定只采纳其元素级偏移、测试与交付手法；其原稿已于 2026-09-13 按用户指示删除，不另行保留）；serde `SeqAccess` + 工作线程 + 通道（每查询一线程、`R: Send + 'static` 传染、跨线程错误传递，违背 crate 的同步拉取模型）。

## PathSeg 编译状态机

编译：`CompiledPath::from_segments(segments: &[PathSeg])`。**（Amended 2026-09-13）**原签名为 `from_segments(segments: &[PathSeg], ty: Type)`；根规则既定由帧化承担（见下条），matcher 从不分支于 `Type`，该形参始终未用——删除，调用点与 `Type` 导入同步去掉（评审发现：Speculative Generality）。

- 根规则（隐含的 `S3Object[*]`，与现状一致、与是否显式写 `[*]` 无关）由帧化承担：LINES 每行一根（空输入零根零行）；DOCUMENT 数组根逐元素、其余整体单根、空数组 `[]` → 处理器补一条 MISSING（零命中规则）。
- 步序列：段列表去掉开头的 `Wild` 后编译为 `Vec<Step>`，`Step = Name(String) | Index(usize) | Wild`。

运行：`StepSeed` 持 `{ steps, depth, pending: &mut VecDeque<Option<Value>>, budget }` 实现 `DeserializeSeed`，访问者按 `steps[depth]` 分发：

- `visit_map` + `Name(n)`：解码键 `eq_ignore_ascii_case` 比较；命中则以 `depth+1` 递归 `next_value_seed`；同对象内第二次大小写折叠命中 → `Error::Ambiguous`。未命中键的值用 `IgnoredAny` 跳过。
- `visit_seq` + `Index(i)`：序号计数，仅第 i 个元素递归，其余 `IgnoredAny`。
- `visit_seq`/`visit_map` + `Wild`：逐元素/逐成员值递归；空容器 → 零命中。
- 容器访问结束且命中计数为 0 → 入队一条 `None`（MISSING 步短路传播，等价于旧 `apply` 的 `None → None`）。
- 步作用于标量（`visit_bool`/`visit_i64`/`visit_u64`/`visit_f64`/`visit_str`/`visit_unit`）→ 零命中 → 一条 `None`。
- `depth == steps.len()` → 当前值即命中：武装 `BudgetedRead` 预算后 `next_value::<Value>()` / `Value::deserialize` 物化入队。
- **（Amended 2026-09-13，Q9(a)）本节三处过时，以 §帧化过滤器 与偏差①③ 的 Amendment 为准**：① `StepSeed` 字段实为 `{ steps, pending, failure }`——无 `budget`（cap 改由处理器按根拉取武装）、无 `depth`（按 `steps[0]` 分发、子 seed 切片递归），`failure` 记录 `Error::Ambiguous`；② 命中物化前不再逐值武装预算（逐值武装会跨命中累积误报）；③ 未命中子树一律用自写校验型 `Skip`（见 §解析内核 Amendment），不用 `IgnoredAny`。

两条语义支柱：

- 顺序等价：产出顺序与旧 `apply` 的逐候选保序展开逐点一致（字典序 DFS），现有遍历顺序测试全部保持。
- 二义性的流式处理：二义判定只有在对象关闭时才完整。**末步 Name 暂缓产出**——命中值先物化到暂存槽（一个值 ≤ 1 MB，有界），继续扫完本对象，关闭时无重复才入队、有重复报 `Ambiguous`；因此 `traversal_case_insensitive_duplicates_are_ambiguous` 与 `traversal_bracket_name_duplicates_are_ambiguous` 两个钉住的测试原样通过。非末步 Name（下方行数无界，无法缓冲）在流中发现重复键即报错，此前可能已产出该对象内的部分行——与解析错误"数据相关错误在流中途暴露"的既有契约一致（见"语义偏差"）。

## 帧化过滤器

两个 `Read` 适配器 + 一个配额包装器，使"每次 serde 拉取 = 一个记录根"，并在读取中途强制执行 1 MB 上限：

- `LineFraming`：每次只供应当前行的内容字节（不含 `\n`），行尽即 EOF、重置后供下一行——行不再整行入内存。行宽（内容 + 终止符）在供应中途计数，超 1 MB 即时报错（`lines_input_record_cap` / `lines_input_record_cap_exact_span_allowed` 语义不变）。空行 → 首字节即 EOF → serde 报 "EOF while parsing a value" → `Format`（`empty_line_is_a_format_error` 语义不变）。
- `DocFraming`：根形状探测——跳过前导空白后首字节为 `[` → 数组模式：顶层 `[`/`]`/`,` 不上供（吞掉），元素边界处注入一个虚拟空白字节（干净终止数字类 token），顶层分隔符校验（元素后非 `,`/`]` 报错；`[1,]`、`[1 2]` 报错；`]` 后非空白报错）——每次 `StepSeed` 拉取恰好解析一个元素。非 `[` → 单值模式：原样透传全流，一次拉取。字符串感知（`\` 跳一字节）保证顶层判定正确；EOF 时深度未归零 → serde 自然报 "EOF while parsing"。过滤器按自身字节记账给出每个元素的首字节偏移（供偏差⑥的记录偏移）。
- `BudgetedRead`：1 MB 预算（共享 `Rc<Cell>` 句柄）**按根拉取计费**——处理器在每次拉取开始时调用一次 `reset()`（**Amended 2026-09-13**：原为 `arm()`/`disarm()` 一对；`BudgetedRead` 只在拉取块内存活，`armed` 标志恒真、`disarm` 无事可做，已删），计时覆盖这次拉取期间被供出的**每一个字节**：命中值、被跳过的子树、分隔符一视同仁。读取超预算即时报 io 错并置标志位；处理器见标志位 → 精确返回 `Error::Format("input record exceeds 1 MB")`（CAP_MESSAGE 逐字不变）。**不变量："一次拉取 = 一个根 ≤ 1 MB"**（数组模式即一个元素、单值模式即整份文档），未命中子树不再有豁免缺口。LINES 侧另有行宽上限，二者同报文。**（Amended 2026-09-13）**原设计是"匹配器物化命中值前武装"（逐命中值计费）：实测下这会跨命中累积——若命中集合含多个各 400 KB 的元素，第二个元素即被误判超限并报 cap，而它的记录本身远小于 1 MB；且未命中子树完全不武装、框架中途的 cap 亦无从计费。改为按根拉取武装后，`document_input_record_cap_exact_allowed`（恰好 1 MB 的单值根）以 1 048 576/1 048 576 压线通过。**（Amended 2026-09-13·二轮）**上文"分隔符"指注入的虚拟空白等**被供出**的字节；顶层 `[`/`]`/`,`、`skip_ws` 吞掉的空白与 LINES 的 `\n` 由 framing 内部吞掉、不经 `BudgetedRead`，由 framing 自身的 span/偏移记账约束（LINES 行宽上限走的正是这条路）。

## 处理器

- `Lines<R>`：持有一个 `Deserializer::from_reader(LineFraming)`；每行一次 `StepSeed` 拉取（行 = 一根），随后二次拉取探测行尾：产出第二个值或报非 EOF 错误 → 行尾垃圾 `Format` 错（对齐 `from_slice` 的 trailing 行为）；`err.is_eof()` → 行干净结束。`last_record_start` 按行起点记账（`lines_last_record_start` 不变）。
- `Document<R>`：`Deserializer::from_reader(DocFraming)`；循环：过滤器报告有下一元素 → `StepSeed` 拉取一个元素。`[]` → 零元素 → 补一条 `None`（起点 0）；空输入（零字节）→ 零记录；纯空白 → 首拉取 `is_eof` → `Format`（与今日一致）；单值模式根值之后二次拉取探测 trailing（`{} x`、`5 {}` → `Format`）。`last_record_start()` = 当前根元素的首字节偏移（MISSING 行取其所派生元素的起点；单值根 = 根值首字节）——偏差⑥。

`pending` 有界性：LINES ≤ 一行的匹配集（行 ≤ 1 MB）；DOCUMENT 数组模式 ≤ 一个元素的匹配集；DOCUMENT 单值模式 = 整文档匹配集在一次拉取中累积，而该次拉取同样被 1 MB 预算计费，故超出即 cap 错（见"语义偏差"③，**Amended 2026-09-13**）。

## 错误处理

serde 错误与过滤器 io 错误统一映射为 `Error::Format(format!("json input: {e}"))`（`is_eof()` 用于区分干净的记录结束与格式错误，不做字符串匹配）；cap 经 `BudgetedRead` 标志位映射为逐字不变的 `"input record exceeds 1 MB"`；`Error::Ambiguous` 路径不变；引擎侧 `json_lookup` 不动（大小写规则与遍历侧一致性的单一实现仍在）。

## 语义保持与偏差（用户已确认）

不变项：`[]` → 恰一条 MISSING、LINES 空输入零行；空文档零记录、纯空白文档报错；行尾/文档尾垃圾报错（记录产出前严格探测）；空行报错；LINES 行宽 1 MB 上限与行起点记账；标量 serde 语义（含 `1e+309`）；跳过区域校验无缺口；字节级重复键 serde last-wins（grilling Q3 裁定不采纳 Ambiguous 扩张）；除偏差④所列两个测试的填充输入外，现有测试套件全部原样通过。

已确认的偏差：

1. DOCUMENT 总量解禁——1 MB 上限从"整文档"改为"**每个根**（数组模式即每个元素，单值模式即整份文档）"。元素各 ≤ 1 MB 的数组文档任意大均可流式处理；单对象/标量根 > 1 MB 仍报错（两个现有 cap 测试的文档本身是单条标量记录，原样通过）。**（Amended 2026-09-13）**措辞由"每条产出记录"改为"每个根"：cap 按根拉取计费，未命中的元素虽不产出记录，同样受 1 MB 约束（按命中计费会漏掉它）。
2. 非末步 Name 二义报错时机放宽——流中发现重复键即报错，此前可能已产出该对象内的部分行；末步 Name 仍严格（暂缓产出至对象关闭）。
3. 单值根匹配集按文档累积——单对象根 + 深通配的 DOCUMENT 一次拉取累积该文档全部匹配记录；数组根（主流大文档场景）不受影响，逐元素有界。**（Amended 2026-09-13）**该档**并非无上界**：那次拉取同样按根计费 1 MB 预算，文档整体超 1 MB 即返回 `Error::Format("input record exceeds 1 MB")`，而非无限累积。（2026-09-13 前本档被描述为"该档无上界"，与按根拉取计费的实现不符，按 Q15 修订。）
4. LINES 的 cap 与解析错误优先级——解析完全交给 serde_json，流式下边供边解析：非法内容的行可能在到达行宽上限前先报 serde 解析错误；cap 对格式良好的行仍中途强制执行（内存有界性不变）。`lines_input_record_cap`、`lines_final_unterminated_line_over_cap` 两个测试的填充输入改用合法 JSON 形态，断言不变（cap 报文逐字；恰好 1 MB 边界仍由本就合法的 `lines_input_record_cap_exact_span_allowed` 钉住）。
5. DOCUMENT 错误时机流式化——文档级格式错误从"全有或全无"（整文档解析，任何错误清零全部记录）变为逐元素暴露：出错元素之前的记录已产出，客户端先收 Records 事件后收错误事件。LINES 今日本为逐行流式语义，无变化；单值模式的根后垃圾经"拉取 + 探测"配对，仍在任何记录产出前报错。
6. DOCUMENT 元素级真实偏移与 ScanRange 过滤（extension）——记录（含 MISSING）报告其所派生根元素的首字节偏移（单值根 = 根值首字节；空数组零命中 MISSING = 0），`last_record_start` 在 DOCUMENT 下不再恒 0；ScanRange 窗口按记录起点过滤 DOCUMENT（AWS 文档将 JSON 的 ScanRange 限定为 LINES-only，记为 extension）；`BytesProcessed`（相邻起点差值）随扫描推进而非 EOF 一次性结清（实现上最终值 = `scanned − 首元素起点`，`[...]` 根即 −1：首字节 `[` 不属于任何记录；此前整文档记账下它恒等于 `scanned`）；（**Amended 2026-09-13**）EOF 落在某个值**内部**（截断的元素、截断的单值根）必须是 `Format` 错误——`[]` 与"值中途 EOF"由帧化的"顶层 `]` 是否已消费"区分，否则 `["abc`、`[{"a":` 会被当成干净的空数组；`document_start_above_zero_yields_nothing` 断言保留（其单对象根输入本就始于 byte 0，`start = 1` 仍丢弃），注释去掉 byte-0 理据；2026-09-04 的 byte-0 钉注按 supersede 标注取代（见"同步更新"）。

## 测试计划

迁移：LINES 各测试 → `lines.rs`；DOCUMENT 各测试 → `document.rs`；引擎组合测试（`run`/`run_err` 辅助与 AWS 示例）留 `mod.rs`。例外（偏差④）：`lines_input_record_cap` 与 `lines_final_unterminated_line_over_cap` 的填充输入改用合法 JSON 形态（`{"x":"<…>"}`），断言不变——除此之外任何现有测试一行不碰。

新增：

- `document_array_larger_than_1mb_streams`：4 × 400 KB 元素（总 > 1 MB）→ 四条记录无错——偏差①的主钉。
- `document_element_over_cap_errors_after_prior_records`：某元素 > 1 MB → 流内 `Error::Format("input record exceeds 1 MB")`，出错元素之前的记录已产出、后续元素不再产出（兼钉偏差⑤）。**（Amended 2026-09-13·二轮）**原名 `document_element_over_cap_errors`，与计划实现对齐改名。
- 匹配器矩阵：Name/Index/Wild 各步零命中 → 恰一条 MISSING；MISSING 短路传播；嵌套通配 DFS 顺序；Index 越界；末步 Name 暂缓产出 + 二义；非末步 Name 二义流内报错；转义键（`"R\u0075les"`）匹配。**（Amended 2026-09-13）**另加 cap 归属两条：`element_barely_under_cap_is_accepted`（交付字节恰好 `MAX_RECORD` 的记录必须通过——按命中值武装会少算开引号那一字节而误拒）与 `budget_is_charged_per_root_not_accumulated`（四个 400 KB 元素合计 ~1.6 MB 的文档不得在第 2 个元素上误报 cap）。
- 帧化：`[1,]`、`[1 2]`、`[1] x`、行尾垃圾、纯空白行、纯空白文档 → `Format` 含 `json input`；未闭合文档 EOF → `Format`；标量根（`5`/`"x"`/`null`）；嵌套数组元素（`[[1],[2]]`）。
- 行序表驱动用例：通配扇出与 MISSING 交错、嵌套通配、Index 未命中（`apply` 顺序等价的枚举式补强）；CRLF 行覆盖（今日零覆盖）。**（Amended 2026-09-13）**另加缩进行用例：`last_record_start` 报**行**的首字节（即缩进起点），不是值的首字节。
- 偏差⑥：root 数组 + `ScanRange` 窗口按元素起点过滤；记录起点单调非递减（含 MISSING 行）；空数组 MISSING 起点 0。
- CI 大文档流式测试（`tests/stream_large.rs`）：git LFS 中的 gzip 压缩 fixture（`tests/fixtures/large/document.json.gz`，解压后 ~64 MB 美化打印小对象数组），经生产同款 `record::decompressed(Some(Compression::Gzip), …)` 路径读取，断言行数、cap 行为、完成与偏移单调——测试本身不落地解压字节。**（Amended 2026-09-13·二轮，用户裁定）**规模 400k 元素/~64 MB → 100k 元素/~16 MB：四项断言不依赖更大尺寸，CI 逐字节解析成本约降为 1/4。
- 内存峰值闸（`tests/stream_memory.rs`，单文件单 `#[test]`）：计数 `#[global_allocator]`，流式读取同一 fixture 时断言宽松峰值上界（~32 MB）——回归绊线而非测量；单文件是为避免多线程 harness 把其他测试的分配计入。**Amended 2026-09-13**：`[lints.rust] unsafe_code = "forbid"`（`crates/tinio-select/Cargo.toml:51`，**逐 crate 设置**）覆盖本 crate 全部 target（含 `tests/`）且不可被 `allow` 覆盖，计数 `#[global_allocator]` 落不了地；改为 crate 内 `#[cfg(test)]` 的 `pending_high` 高水位 pin，`tests/stream_memory.rs` 取消。**该 pin 以字节计**（累计入队 `Value` 的序列化长度）：条数口径下"一个 1 MB 的值"与"5 000 个十几字节的值"是同一个数、内存差三个数量级，与它要守的内存承诺无关。数组根断言峰值在一元素量级（`document_array_root_keeps_pending_bounded`）；偏差③改由 `document_single_root_under_cap_yields_all_matches`（一次拉取产出全部匹配、峰值受 cap 兜底）与 `document_single_root_over_cap_errors`（超 1 MB 即 cap 错）钉住。该 pin 跨平台、无需 unsafe 与 fixture，且直接钉住设计承诺本身。

## 验收标准

- 除偏差④两个测试的填充输入外，现有测试套件全部原样通过（含 DOCUMENT 两个 cap 测试、两个二义测试、空行测试、恰好 1 MB 边界测试）；`document_start_above_zero_yields_nothing` 断言保留（偏差⑥）。
- `docs/tests.md` 标准验收的相关腿：`cargo test -p tinio-select`、`cargo test --workspace --exclude tinio-e2e`、`cargo test -p tinio-select --features parquet`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo +nightly fmt --all -- --check`。
- CI 大文档流式测试 + allocator 峰值闸（见"测试计划"）；更大尺寸（100 MB+）可本地临时生成跑，不进仓库。**Amended 2026-09-13**："allocator 峰值闸"以测试计划节的 Amendment 为准——计数 `#[global_allocator]` 被 `unsafe_code = "forbid"` 挡住，已改为 crate 内 `pending_high` 高水位 pin；本行文案保留原文未改写。
- `benches/select_scan.rs` 增加一个 JSON DOCUMENT 吞吐用例（同一确定性生成器，criterion 只管吞吐，内存归 allocator 测试）。**Amended 2026-09-13**："内存归 allocator 测试"同上行——归 `pending_high` pin。**该 bench 必须先于旧实现删除落地**（计划 Task 11′ 前移至 Task 8 之前），否则"serde 逐字节驱动的代价"只有论断没有数字；fixture 取 ~800 KB（< 1 MB），使旧实现（整文档加载）与新实现可同命令对比。比较数据出来后，再决定 serde 外层是否保留一层 `BufReader` 以减少逐字节驱动开销。

## 同步更新

- `docs/superpowers/specs/2026-09-04-select-object-content-design.md`："JsonReader: … DOCUMENT (whole object is one record)" 行更新为流式逐记录 + 按记录上限的表述；§ Background → ScanRange 的 byte-0 钉注按 **superseded 2026-09-12** 标注（保留原文，不静默改写，偏差⑥）；§ Background → Streaming 的输入侧 1 MB 表述按 **amended 2026-09-12** 标注（按产出记录计）。**Amended 2026-09-13**：JsonReader 行与下两处一致，采用保留原文 + 追加 `(superseded 2026-09-12: …)` 标注，不逐字改写。
- `docs/superpowers/plans/2026-09-04-select-object-content.md`：Task 9 "DOCUMENT … byte-0-anchored but still capped at 1 MB" 与 Task 10 RangeFilter 的 byte-0 注记，同法 supersede 标注。
- `crates/tinio-select/src/record.rs`：`RangeFilter` 文档末句（byte-0 一句）按偏差⑥改写；`MAX_RECORD` 注释补一句：内存防线现为"跳过不物化 + 单条记录 ≤ 1 MB"，cap 是记录形状规则而非内存兜底。

## 交付与 fixture

- 一次性落地：模块拆分、`CompiledPath`/状态机、过滤器与两个处理器作为一个整体交付（grilling 未采纳分阶段交付）；所有 git 写操作（含 commit）逐项请示，默认留在工作树。
- git LFS 首次引入：`.gitattributes` 增加 `crates/tinio-select/tests/fixtures/large/** filter=lfs diff=lfs merge=lfs -text`；CI 共享 checkout 锚点加 `lfs: true`；本地 `git lfs install` 由用户执行或另行授权。
- fixture 生成：/tmp 脚本确定性生成 ~64 MB 美化打印小对象数组（`{"id": N, "name": "…", "price": N, "tags": […]}`），gzip 压缩为 `crates/tinio-select/tests/fixtures/large/document.json.gz`（个位数 MB）后进 LFS；生成与提交过程逐项请示。**Amended 2026-09-13**：生成器改为与 fixture 同侧的 `#[ignore]` Rust 测试（`tests/stream_large.rs`，照 `tests/parquet.rs` 的成例，`cargo test -p tinio-select --test stream_large -- --ignored` 再生），`ELEMENTS` 只有一处定义，不再依赖 /tmp 脚本；`.gitattributes` + `git lfs install` 必须先于 fixture 的 `git add`，否则二进制永久进 git 历史。**（Amended 2026-09-13·二轮）**fixture 规模 ~64 MB → ~16 MB（100k 元素，同测试计划节的用户裁定）。
