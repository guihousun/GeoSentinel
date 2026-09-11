# 单版本基准与系统性缺陷记录（2026-09-12）

上一轮 30 例（[`RESULTS-2026-09-10.md`](RESULTS-2026-09-10.md)）的最大方法论缺口是它自己写下的第 5.1 条：
**30 例跨当天演进的多个发布版本，"要作为单一版本基准，需在同一个冻结版本上整批重跑"**。本文件是第一份
**单一冻结版本**上的实测记录，并用它去找**系统性缺陷**（不是模型能力差异）。

- 被测版本：`8d9e492d54894030`（已发布为正式版；生产 8511 即该版本）
- 被测实例：候选预览 8513（隔离 home、普通用户 `preview_user`、独立 Cookie/工作区），不打扰正式数据
- 执行器：`dsh/benchmark/run.mjs`（仅走 `/geo/api/*`）；本子集 8 例、并发 4（预览临时放宽准入，非产品默认）
- 原始记录：`dsh/.runtime/benchmark-report-run11.json`；汇总：`summarize.mjs --cases --markdown`

## 1. 用例子集（典型三档，偏"平台机制"而非纯推理）

| 用例 | 档位 | 考查点 |
| --- | --- | --- |
| B02 单幅栅格质量检查 | 基础 | `geo_inspect_raster` 与容器栅格 IO |
| B03 中国省级边界获取与核验 | 基础 | `geo_download_boundary` 与边界登记 |
| B06 按多边形筛选点 | 基础 | `geo_filter_points_by_polygon` + 共享数据 |
| B09 指标口径解释 | 基础 | 纯口径回答（不调工具）——**本轮失败项** |
| A08 全球事件监测线索核验 | 进阶 | 监测线索 + 证据链 |
| A10 点与行政区空间连接 | 进阶 | `geo_spatial_join_points_to_admin` |
| C02 时区陷阱 | 挑战 | 时区/首夜语义 |
| C06 无数据覆盖处理 | 挑战 | 如实报告无数据 |

## 2. 逐例结果

| 用例 | 档位 | 判定 | 通过/总数 | 耗时 | 作业目录 | 产物 | 方案审批 | 未过项 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A08 全球事件监测线索核验 | 进阶 | PASS | 4/4 | 377s | 15 | 29 | 0 |  |
| B02 单幅栅格质量检查 | 基础 | **FAIL（预算用尽）** | 2/3 | 906s | — | — | — | keyword:像元 |
| B03 中国省级边界获取与核验 | 基础 | PASS | 4/4 | 352s | 21 | 41 | 0 |  |
| B06 按多边形筛选点 | 基础 | PASS | 2/2 | 217s | 8 | 17 | 0 |  |
| B09 指标口径解释 | 基础 | **FAIL（预算用尽）** | 0/2 | 307s | 10 | 10 | 0 | keyword:ANTL, keyword:TNTL |
| C02 时区陷阱 | 挑战 | PASS | 5/5 | 207s | 15 | 28 | 0 |  |
| C06 海域覆盖与掩膜声明 | 挑战 | PASS（用例修正后 6/6） | 4/5→6/6 | 337s | — | — | — | 见 §4 |

生成命令（判定按当前用例模式重算，并标注预算受限）：

```powershell
node dsh/benchmark/summarize.mjs --cases dsh/benchmark/cases.json --markdown dsh/.runtime/benchmark-report-run11.json
```

**A10 在本文件写下时仍在运行**（执行器每完成一例即落盘，`run11.json` 会继续追加）；
它的判定与证据在下一轮补齐，并同样按"预算用尽 / 未过项"区分。

两例失败都**不是能力结论**：B02 与 B09 都撞在自己的预算上限上（执行器已按 D4 标注为"预算用尽"）。
B09 尤其说明问题：一个**纯定义题**（"ANTL 与 TNTL 分别是什么"）被平台做成了取数与计算任务——
2 次 GEE 失败（D2）、8 个容器作业、11 次 `geo_execute_python`，5 分钟预算耗尽时
**一条助手文本都没产出**。三条原因都在平台侧：D2 让它第一次取数就失败，D3/D5 让后续脚本读不到上游结果，
而**产品里根本没有可读的 ANTL/TNTL 口径**（技能只说"要用对应工具"，catalog 里 0 处定义），
于是"解释口径"只能靠跑数据。修法见 §3 与 §5。

## 3. 找到的系统性缺陷（附证据，均已修）

四个缺陷都不是"模型不会"，而是**平台自己给错信息或契约不成立**；证据来自会话原生记录
（`dsh/.runtime/inspect-session-zstd.mjs` 可复现读取）与作业清单。

### D1 共享数据的 `share/<名称>/…` 路径对文件工具根本不成立（三个用例全部命中）

- 平台把共享数据描述为 `share/<相对路径>`，`geo_list_files` 也**正是这样返回**并提示"先 read 这些 CATALOG.md"。
- 但 DSH 的 `read`/`glob`/`grep` 按会话 cwd（对话工作区）解析相对路径，工作区里没有 `share/`：
  ```
  Error: grep search failed (exit 2): rg: share/全球基础数据/CATALOG.md: IO error for operation on
  share/全球基础数据/CATALOG.md: 系统找不到指定的路径。 (os error 3)
  Error: glob search failed (exit 2): rg: share: IO error for operation on share: 系统找不到指定的文件。 (os error 2)
  ```
  B03、B06、B09 三个用例都撞上，随后各自改用其它路径或直接放弃共享数据。
- 守卫只做**校验**（`shareTarget` 仅用于包含性判断），DSH 的 `tools.guard` 与 `tools/pre-execute`
  **都只能 allow/deny/ask，不能改写参数**（Inspect 契约原文），所以"在钩子里改路径"不可行。
- **修法**：把每个已配置的共享根作为目录链接物化进对话工作区（`ensureShareLinks`），让文档里的
  `share/<名称>/…` 对 `read`/`read_document`/`glob`/`grep` 真正可解析，并与容器内
  `/workspace/share/<名称>/…` 统一为同一形态。两处围栏仍权威：`containedPath` 先规范化（跟随链接）
  再比较，链接目标正是允许的共享根；`containedWrite` 只接受 `<对话>/outputs`，**链接不会变成写入通道**。
  回归测试同时断言"可读"与"不可写"（`tests/share.test.mjs`）。

### D2 GEE 失败建议指向错误方向（资产类型不匹配）

- 作业清单：`Image.load: Asset 'NOAA/VIIRS/DNB/ANNUAL_V22' is not an Image.`（该资产是 ImageCollection，
  而请求把 `asset_type` 固定成了 `Image`），平台返回的建议却是
  "Inspect the AOI, date window, output path, and GEE request-size limits before retrying."
  ——四条全都不是原因；B09 因此在同一错误上烧掉两次 GEE 作业。
- 工具其实支持 `asset_type="auto"` 自动识别（`_resolve_asset_type`），错误信息却从不提它。
- **修法**：`gee_download.py` 的失败信封按真实原因给建议（资产类型不匹配 / 缺日期 / 预设需要 collection /
  范围无影像 / 导出为空），并点明"可留 `auto`"。附带修掉一个子串 bug：`"IS NOT AN IMAGECOLLECTION"`
  含 `"IS NOT AN IMAGE"`，必须先判更长拼写——这正是"非目标变体测试"抓出来的（参数化用例 6 组）。

### D3 宿主侧与容器内路径是两套命名空间，但只有一套被写下来

- `docker.mjs`：本次作业挂 `/workspace/outputs`，**更早作业**挂 `/workspace/previous`。
- 但 `code-execution-validation` 技能写的是"输入来自 `inputs/` 或上游 `outputs/<作业ID>/`"，
  GIS 工具描述写"输入用 inputs/ 或 outputs/<作业ID>/"——在容器里都不成立：B09 的脚本因此
  `FileNotFoundError: /workspace/outputs/2026…-gis-calculatentlmetricsf…/result.json`（该文件在宿主侧确实存在）。
- **修法**：工具描述（`geo_execute_python`、`geo_<operation>`）与两份技能
  （`code-execution-validation`、`workspace-and-artifact-contract`）统一写明两套路径的对应关系。

### D4 "预算用尽"与"做了但没过"在报告里无法区分（框架侧）

- 上一轮 §3 已提出"应区分'没做'与'来不及'"，但报告里两者都是 FAIL；本轮标注后发现**上一轮实际有
  3 例（C03/C04/C10）撞到预算**，其中 C03 还被记为 PASS。本轮的 B02、B09 同样被标注出来。
- **修法**：`run.mjs` 记录 `budgetExhausted`/`queueStarved` 结构化字段，`summarize.mjs` 渲染
  "（预算用尽）（排队超时）"并单独汇总一行，提示"要下能力结论应先放宽预算重跑"。判定逻辑未改。

### D5 GIS 工具拒绝平台自己的作业目录形态（4 个作业失败）

- `gis_dispatch.py` 的 `scoped_path` 只接受 `inputs/`、`previous/`、`outputs/`、`share/` 前缀
  （并把 `outputs/<作业ID>/…` 自动改写成 `previous/…`），而 agent 常直接传作业目录
  （实测 `20260911-180003-gee-235ce3/imagery.tif`），得到一句无从下手的
  `ValueError: Workspace-relative path required`；同一根因在 B02 与 C06 的对话里共失败 4 个作业。
- **修法**：作业 ID 形态（`<YYYYMMDD-HHMMSS>-…`）按"更早作业"接受；其余未知前缀的错误信息**列出
  被接受的形态**与正确写法。`scoped_path` 增加可注入 `root` 以便在非 Linux 上单测；
  新增 `dsh/tests/gis-dispatch-paths.test.mjs` 固定这五种输入与错误文案。

## 4. 用例修正（基准维护，不是平台缺陷）

- **C06 前提有误**：原题假设"北纬 25–26°、东经 122–124° 海域无数据"，实测该范围内有
  **115,200 个框内像元**（ANTL ≈ 0.318，海面剔除 127 个陆地像元后 0.3177），所以"无数据/缺测"字样
  永远不会出现。该例真正能考的是**是否声明覆盖与掩膜口径**——答复确实说了掩膜（10 次）、覆盖、
  有效像元，并自查了网格溢出（+0.14%）。因此标题改为"海域覆盖与掩膜声明"，期望改为
  `有效像元|掩膜|masked|覆盖` + `ANTL|均值|平均`，并加上"全部为陆地/没有海面"这类断言式禁止词；
  复核后该例按现状答复即为 6/6。
- **B02 预算偏紧**：提示词要求"先获取一幅上海 2020 年度栅格"，取数+质检在 15 分钟内不够
  （实测 906 秒仍在工作），预算调整为 25 分钟；与 A01/B01 等含取数的用例（45–60 分钟）保持一致。

## 4. 环境/既有问题（不是本轮引入，也不是平台缺陷）

- **`pymannkendall` 缺失**只影响 `packages/ntl_toolkit/tests/test_langchain_parity.py` 里对**旧仓**
  `tools/NTL_trend_detection_tool.py` 的对照，本仓 toolkit 不 import 它（已核实），产品镜像不受影响。
- **host python 装的是另一个 checkout 的 toolkit**（`D:\NTL-GPT-main\.worktrees\...`），所以在仓库根直接
  `python -m pytest packages/ntl_toolkit/tests` 会**测错代码**；必须
  `PYTHONPATH=packages/ntl_toolkit/src`。这是验证陷阱，建议写入新机器指南。
- 该套件另有 3 处既有失败：根 README 未链接 `ntl-download`、pandas 新版 dtype 命名（`str` vs `object`）
  与旧对照测试不一致。均与产品行为无关。

## 5. 限制

- 子集 8 例、每例一次，**不构成独立验证**；耗时受并发影响，不可跨例比较。
- 规则分只查工具名/产物扩展名/关键词/禁止词，不等于科学质量评阅。
- 本文件记录的是**修复前**版本的实测；D1–D3 的修复效果需要在下一个冻结版本上用同一子集复测
  （尤其 B09），届时另附一节。
