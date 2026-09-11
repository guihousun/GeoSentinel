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
| **A11 共享边界库取用与来源声明** | 进阶 | **本轮新增（可补充）**：只用共享数据取缅甸 ADM1、统计要素与面积、输出 CSV+Markdown，并报出文件路径、许可与年份——同时约束 D1/D2/D5 的修复面 |

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
| A10 点与行政区空间连接 | 进阶 | PASS（仅在修复版上跑通，见 §6） | 2/2 | 1121s | — | — | — | — |
| A11 共享边界库取用与来源声明 | 进阶 | PASS（新用例，在修复版上） | 6/6 | 1784s | — | — | — | — |

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
于是"解释口径"只能靠跑数据。修法见 §3 与 §6。

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

### D6 作业已完成却因等待 attach 事件而空耗整个预算（A10 实测 15+ 分钟）

- 现象：A10（点与行政区空间连接）的 `boundary-download`（provider `datav`）在 13.6 分钟后仍为
  `running`，作业目录里**只有 manifest、没有 execution.log**。
- 取证：`docker inspect` 显示容器 `Started=17:55:05`、**无 Finished**、`docker top` 无进程、
  `docker exec` 报 `cannot exec in a stopped state`，而**产物已经齐全**
  （`boundary.geojson` 148 KB、`result.json`、`boundary-source.json`），容器日志已打印
  `{"status":"completed","operation":"boundary-download"}`。也就是说**活早就干完了**。
- 机制：`docker.mjs` 只靠 `docker start --attach` 子进程的 `close` 事件判定结束
  （`execution.log` 正是在该 promise 结算**之后**才写，故其缺失即证明事件从未到达），
  Docker Desktop 偶尔会丢这个事件；此时只能等满 30 分钟超时，正好耗光 A10 的用例预算。
  现场验证：手动 `docker rm -f` 该容器后，管线**数秒内恢复**（立刻出现三个新作业）。
- **修法**：`--attach` 之外增加**容器状态看护**：每 5 秒 `docker inspect --format
  '{{.State.Running}} {{.State.ExitCode}}'`，一旦容器不再运行即按该退出码结算并杀掉残留 CLI 子进程；
  解析函数 `containerState` 单独导出并用单测固定（空输出/报错文本一律视为"未知"，绝不把垃圾输入
  当成"已结束"——那会把正在跑的作业误判为完成）。

### D7 容器停在"守护进程未回收"的中间态，`State.Running` 判据失效（D6 的盲区）

- 现场（修复版候选 `5b4651d9be7ede8e` 的预览上，A10 的 `gis spatial_join_points_to_admin`）：
  worker 早已抛错（容器日志 `ValueError: {'code': 'COLUMN_NOT_FOUND', …}`），但
  `docker inspect` 报 `Status=running Running=true pid=20313`、`docker top` **没有任何进程**、
  `docker exec` 答 `cannot exec in a stopped state`、作业 12 分钟仍为 `running`。
- 也就是说：容器其实已经没有进程，Docker 却没把它标记为 exited，于是 **D6 的 `State.Running` 看护
  也判定不了结束**，只能等满 30 分钟超时（再次吃掉用例预算）。D6 那例是"容器已停但 host 没收到
  close"，这一例是"容器既没进程也没被回收"——两种形态都会空耗预算。
- **修法**：看护除 `inspect` 之外再取 `docker top`，**连续两次没有进程**即按当前退出码结算；
  解析函数 `containerProcesses` 单独导出并单测（只有表头/空输出算 0，真实进程行必须 >0）。

### D8 空间连接工具的错误不可执行：没说该改哪个参数、图层里有什么字段

- 现场：`spatial_join_points_to_admin` 默认要求 `iso3`，而平台自己的共享边界库（geoBoundaries）
  用的是 `shapeISO`；错误只有一句 `Required column 'iso3' was not found.`、`suggestion: None`，
  agent 无从知道该设 `admin_iso_col` 还是换数据（A10 因此失败，并触发上面的 D7）。
- **修法**：`vector.py` 的 `_fail` 支持 `suggestion`，`_require_columns` 明确写出**要改哪个参数**
  以及**该图层实际有哪些字段**（`details` 同时带 `column`/`parameter`/`available`）；点表与行政区
  两处调用点都带上参数名。新增单测固定"geoBoundaries 用 `shapeISO`"的场景，并把原先只断言
  `details == {"column": "lon"}` 的旧用例更新到新契约。

### D9 D1 的修复引入回归：用量统计被平台自建的链接打断（本轮复查发现，已修）

- D1 把共享根以目录链接放进每个对话工作区，而 `directoryUsage()` 原本**遇到符号链接即 403**
  （"存储目录包含符号链接，拒绝操作"），`userUsage()` 又是从 `users/<账号>/…` 开始遍历的
  ——于是**任何开过对话的账号，用量接口都会直接失败**。实测预览实例：`users/` 遍历 403。
- 取证与验证：现场两次复现（真实实例 + 最小临时树）；修复后同一实例用量口径正常返回
  `{bytes: 19162464, files: 153}`（只含账号自身数据），清理口径仍按设计拒绝链接树。
- **修法收窄**：`directoryUsage` 增加 `skipLinks` 选项，用量口径跳过链接（链接指向管理员共享数据，
  不该计入账号用量），**清理口径保持默认严格**——清除时穿过链接可能删到围栏之外，
  `tests/storage-runtime.test.mjs` 的既有断言继续固定该行为；新增用例同时断言两种口径。

### D10 共享数据只在容器挂载里"能用一半"：检查与引用都够不着（全量跑批现场）

- 现场（候选 `1daa24e17c76df57`，用例 B01/B04）：agent 按平台自己的指引用共享库年度栅格
  `share/全球基础数据/NPP-VIIRS-LIKE-NTL/LongNTL_2020.tif`，连撞三堵墙：
  1. `geo_inspect_raster(path="share/…", source="inputs")` —— 工具的 `source` 只有 `inputs|outputs`，
     容器里 `inspect_raster()` 把路径**直接拼到 `/workspace/inputs/` 后面**，于是报
     `No such file or directory: /workspace/inputs/share/全球基础数据/…`——一个 agent 从未写过的路径；
  2. `geo_clip_raster(..., output_path="shanghai_ntl_2020_clip.tif")`（裸文件名）被 `scoped_path` 拒绝，
     且命中的是**没带值也没带可用形式**的那条旧提示"Workspace-relative path required"；
  3. `geo_write_report(source_paths=[… "share/…" …])` 与 `geo_write_evidence(evidence[].source="share/…")`
     直接 400："来源须为 inputs/ 或 outputs/ 文件"——**管理员共享数据无法作为证据来源**。
  结果：agent 改用手写脚本自己算分区统计，`geo_calculate_zonal_statistics` 一次没调用，
  B01 丢 1 项、B04 丢 1 项检查。
- **修法**（同一套路径策略，不再各写一份）：
  `worker.py` 的 `inspect_raster` 改为复用 `gis_dispatch.scoped_path`（`share/`、`inputs/`、`previous/`、
  `outputs/<作业ID>/`、裸作业目录都按同一规则解析，`source` 只对裸文件名生效）；
  `scoped_path` 接受**裸输出文件名**（输出只可能落在本次作业的 `outputs/`），并把每条拒绝信息改成
  "收到什么 + 可用形式"；`evidence.mjs` 新增 `resolveArtifactSource`，报告与证据链都接受
  `share/<根名>/<文件>`（只读、必须真实存在），错误信息点名具体值。
- 验证：容器内 3/3（`dsh/.runtime/verify-d10-in-container.mjs`：`share/` 路径、`source=share` 裸名、
  缺失文件报出真实路径）；单测 `gis-dispatch-paths.test.mjs`、`evidence.test.mjs` 固定新契约。

### D11 发布候选自己构建并验证的容器镜像，运行时从未使用（同批发现）

- 证据链：`release/manager.mjs` 为每个候选构建 `geosentinel-gis:release-<候选ID>` 并把**摘要**写进
  `validation.json`；`release/runtime.mjs` 把它导出为 `GEO_GIS_IMAGE`；但
  `plugins/research/index.mjs` 构造 `DockerRunner` 时**没有读这个变量**，一律落到默认标签
  `geosentinel-gis:0.1`（镜像 `79def542ccfb`，建于 2026-09-10 20:00，早于 D5 修复）。
- 现场指纹：B01 的 `geo_clip_raster` 失败回溯里 `gis_dispatch.py` 是
  **修复前**的行号布局（`raise ValueError("Workspace-relative path required")` 在第 21 行、调用点在第 38 行），
  而候选快照与候选镜像里同一句在第 35 行、调用点在第 62 行——即那次作业跑的不是候选自己的镜像。
- **影响**：任何容器侧修复（D5 的路径策略、worker/镜像依赖）都不会随候选发布生效，
  而验收与跑批都以为在测候选。
- **修法**：`plugins/research/index.mjs` 把 `process.env.GEO_GIS_IMAGE` 交给 `DockerRunner`（未设置时
  仍是开发用的通用标签），并在 `plugin-conventions.test.mjs` 里固定"发布实例导出的摘要必须被使用"这条接线。

### D12 容器被杀时只剩一句退出码：分不清"数据太大"还是"工具坏了"

- 现场（候选 `1daa24e17c76df57`，用例 C08）：全幅分区统计作业被 OOM 杀掉，工具返回的全文是
  `Docker exited 137: `——**后面什么都没有**（被 SIGKILL 的进程写不出日志）。
  agent 只能自己猜"内存上限"，再用"先裁小、再分区"绕开。
- **修法**：`docker.mjs` 新增 `exitExplanation(code, {memoryMiB, timeoutSeconds})`：
  137 → 说明可能是超出容器内存上限（带当前上限值）并给出"裁剪／减少一次处理的栅格数／分块，
  确需更大内存由管理员调 `GEO_DOCKER_MEMORY_MIB`"；143 → 说明可能是超过单次作业时限；
  其它退出码保持原样（日志才是读者需要的东西）。附加退出码说明的位置在 `finish()`，
  两个执行路径（普通 run 与 attach+watchdog）共用。
- 验证：单测固定在 `tests/docker-attach-watch.test.mjs`（137 含上限与 OOM 字样、143 含时限、
  其它码逐字不变）。**现场触发未复现**（该轮之后再没出现 OOM），如实标注为单测级证据。

## 4. 用例修正（基准维护，不是平台缺陷）

- **C06 前提有误**：原题假设"北纬 25–26°、东经 122–124° 海域无数据"，实测该范围内有
  **115,200 个框内像元**（ANTL ≈ 0.318，海面剔除 127 个陆地像元后 0.3177），所以"无数据/缺测"字样
  永远不会出现。该例真正能考的是**是否声明覆盖与掩膜口径**——答复确实说了掩膜（10 次）、覆盖、
  有效像元，并自查了网格溢出（+0.14%）。因此标题改为"海域覆盖与掩膜声明"，期望改为
  `有效像元|掩膜|masked|覆盖` + `ANTL|均值|平均`，并加上"全部为陆地/没有海面"这类断言式禁止词；
  复核后该例按现状答复即为 6/6。
- **B02 预算偏紧**：提示词要求"先获取一幅上海 2020 年度栅格"，取数+质检在 15 分钟内不够
  （实测 906 秒仍在工作），预算调整为 25 分钟；与 A01/B01 等含取数的用例（45–60 分钟）保持一致。
- **C01 禁止词过宽（全量跑批发现）**：原禁止词 `下载完成` 会命中**中间步骤的如实陈述**
  "20 瓦片全部下载完成"，而该例要抓的是"声称一次请求就完成了全国下载"。实测该轮 agent 确实
  分块取数成功，并在限制里写明"平台单次上限 48 MiB，没有单请求全国 500 m 的路径"——
  这正是该例想考的诚实边界。改为：关键词 `失败|上限|超出|限额|不支持`，禁止词
  `已下载成功|一次性下载完成|单次下载完成|一次请求下载完成`；按新规则复核同一份答复为 5/5。
- **C08 关键词过窄（全量跑批发现）**：原关键词 `464`（平台按球面近似算出的像元边长，√213,000 ≈ 462 m）
  要求答复复现**某一个具体数值**；该轮答复用 `pyproj.Geod` 逐纬度算了真实地面边长
  （"经向约 497.8 m、纬向 498.9 m 起，同一景内像元面积相差 1.67 倍"），比 `464` 更准确，
  却因为没写出 464 被判不过。关键词改为"必须给出米制地面尺寸或面积单位"：
  `(\d{3}(\.\d+)?\s*m\b|米|km²|km2|平方米|平方公里)`；按新规则复核为 5/5。
- **判分器不识别否定句（全量跑批发现，属框架缺陷）**：C09 的答复标题是"结论：**不构成**独立科学验证"，
  却被禁止词 `构成独立科学验证` 命中判为违规——用例文件自己写着"禁止词必须描述一个**主张**，
  而不是裸词"。修法集中在 `benchmark/score.mjs`：`claimsTerm()` 在匹配点前 8 个字符内
  检查否定词（不/未/非/无/没/不能/无法/并非/不会/未能/谈不上/算不上/不属于/不存在），
  命中否定则不算违规；`run.mjs` 与 `rescore.mjs` 共用这一个实现（此前各写一份）。
  自检 7 例（含否定、肯定、普通、带分组正则）全部符合；用同一份已捕获答复重算，C09 由 5/6 变 6/6。

## 5. 环境/既有问题（不是本轮引入，也不是平台缺陷）

- **`pymannkendall` 缺失**只影响 `packages/ntl_toolkit/tests/test_langchain_parity.py` 里对**旧仓**
  `tools/NTL_trend_detection_tool.py` 的对照，本仓 toolkit 不 import 它（已核实），产品镜像不受影响。
- **host python 装的是另一个 checkout 的 toolkit**（`D:\NTL-GPT-main\.worktrees\...`），所以在仓库根直接
  `python -m pytest packages/ntl_toolkit/tests` 会**测错代码**；必须
  `PYTHONPATH=packages/ntl_toolkit/src`。这是验证陷阱，建议写入新机器指南。
- 该套件另有 3 处既有失败：根 README 未链接 `ntl-download`、pandas 新版 dtype 命名（`str` vs `object`）
  与旧对照测试不一致。均与产品行为无关。

## 6. 修复验证（候选 `5b4651d9be7ede8e`，同一子集重跑）

D1–D6 修复后冻结候选 `5b4651d9be7ede8e`，在它的预览实例上重跑**曾受影响的 5 例**
（原始记录 `dsh/.runtime/benchmark-report-run12-fixed.json`）：

| 用例 | 修复前 | 修复后 | 耗时 | 说明 |
| --- | --- | --- | --- | --- |
| B02 单幅栅格质量检查 | 2/3 FAIL（预算用尽） | **3/3 PASS** | 661s | 预算调整为 25 分钟后在预算内完成 |
| B03 中国省级边界获取与核验 | 4/4 PASS | 4/4 PASS | 243s（前 352s） | 少了共享数据路径报错后的绕行 |
| B06 按多边形筛选点 | 2/2 PASS | 2/2 PASS（预算用尽） | 1209s（前 217s） | 并发与网络波动，耗时不可跨轮比较 |
| B09 指标口径解释 | 0/2 FAIL（预算用尽） | **2/2 PASS** | 309s | 见下 |
| C06 海域覆盖与掩膜声明 | 4/5 FAIL | **6/6 PASS** | 528s | 用例期望修正后按现状答复即通过 |

**D1 的直接前后对照**（同一用例 B03 的会话记录，`rg: share` 即文档路径无法解析的报错）：
| | `rg: share` 报错 | `os error 2/3` | 仍在使用共享数据 |
| --- | --- | --- | --- |
| 修复前 | 2 次 | 2 次 | 提及 `share/全球基础数据` 115 次 |
| 修复后 | **0 次** | **0 次** | 提及 35 次 |

**B09 的变化**最能说明问题：修复前它是"纯定义题被做成取数任务、5 分钟预算耗尽且零文本"；
修复后 309 秒内给出 2/2 命中的答复（仍做了少量实证，但不再卡在 GEE 资产类型与容器路径上）。
`share/<名称>/…` 的可用性也已在实例上核实：每个对话工作区都自动出现 `share/`（含两个已配置根）。

### 补跑 A10 与新增 A11（同一候选 `5b4651d9be7ede8e`）

| 用例 | 修复前 | 修复后 | 耗时 | 说明 |
| --- | --- | --- | --- | --- |
| A10 点与行政区空间连接 | 未在预算内收口（D6 卡死） | **2/2 PASS** | 1121s | 首次跑通；期间仍遇 D7 卡死，人工解除后才完成 |
| A11 共享边界库取用与来源声明（新） | —（新用例） | **6/6 PASS** | 1784s | 只用共享数据完成缅甸 ADM1 统计，主动用 CGAZ 交叉核对；同样遇 D7 卡死并人工解除 |

这两例把 D7 暴露得很清楚：**两次跑批各撞一次**——容器日志已打印
`{"status":"completed"}`、`docker top` 无进程，而平台已等 20+ 分钟（详见 §3 D7）。

### D7/D8 的现场验证（候选 `a01d2f98a7e9a206`）

在含 D7/D8 的候选上重跑同一对用例（`dsh/.runtime/benchmark-report-run14-d7.json`）：

| 用例 | 无 D7 时 | 含 D7 后 | 说明 |
| --- | --- | --- | --- |
| A10 点与行政区空间连接 | 2/2 PASS，1121s（含约 12 分钟卡死＋人工解除） | **2/2 PASS，277s** | 容器作业失败后立即收口，agent 继续修正 |
| A11 共享边界库取用与来源声明 | 6/6 PASS，1784s（含约 24 分钟卡死＋人工解除） | **6/6 PASS，327s** | 同上 |

**D7 的直接指纹证据**（脚本 `dsh/.runtime/find-watchdog-jobs.mjs`：看护收口的作业
`execution.log` 必为空——该文件只在 attach 承诺结算后才写）：

- 无 D7 的实例：`gis-spatialjoinpointstoa-3a3e83` **存活 869 秒**（就是人工解除的那次）。
- 含 D7 的实例：12 个同类失败作业**全部 1–8 秒收口**，没有任何多分钟残留。

**D8 只做到单测级验证，需如实说明**：本次 A10 的会话里 `admin_iso_col` 出现 42 次、
`COLUMN_NOT_FOUND` 出现 0 次——agent 一开始就传对了参数，所以那条改进后的错误提示**没有被触发**。
它的正确性由单测固定（含"geoBoundaries 用 `shapeISO`"的精确场景），现场触发留给后续跑批。

### 第二批用例：覆盖此前未跑过的路径（候选 `a01d2f98a7e9a206`）

第一批聚焦"平台机制"；第二批挑此前未跑过、且经过本轮改动路径的用例（`--concurrency 4`）：

| 用例 | 档位 | 考查点 | 结果 |
| --- | --- | --- | --- |
| B01 上海 2020 年分区 ANTL 统计 | 基础 | 边界获取 + 分区统计（D1/D5 路径） | **6/6 PASS，259s** |
| B04 逐年 ANTL 时间序列 | 基础 | 多期取数 + 时序一致性 | **3/3 PASS，436s** |
| A05 异常检测 | 进阶 | `geo_detect_ntl_anomaly` + 基线口径 | **3/3 PASS，734s** |
| A07 两套边界差异 | 进阶 | 两套边界的差异与登记（共享库相关） | **3/3 PASS，330s** |
| C01 超大范围下载失败语义 | 挑战 | 失败语义（D2 相关的 GEE 提示） | **3/3 PASS，993s** |
| C04 趋势显著性 | 挑战 | `geo_analyze_ntl_trend`（上轮唯一未调用的工具） | **6/6 PASS，772s** |

六例 24/24 检查全通过，且没有出现新的"工具报错但无人解释"的情况；观察窗口内也没有
再现 D6/D7 那类"作业没被回收"的中间态。

**口径提示**：`a01d2f98a7e9a206` 的冻结快照里**不含** D9（D9 修在冻结之后），
所以这一批验证的是 D1–D8 的行为。D9 只影响平台用量统计接口，不参与这些用例的判定。

原始记录 `dsh/.runtime/benchmark-report-run15-batch2.json`；跑批期间另用
`dsh/.runtime/watch-limbo.mjs` 观察容器中间态是否再现。

修复机制另有一处**平台外可复现**的验证：在临时目录里先复现 `rg --files share` 的
`os error 2`，再用 `ensureShareLinks` 建立别名后，同一命令命中
`share/全球基础数据/CATALOG.md` 并能 `rg -n` 搜到内容；D8 另有**容器级**验证
（`dsh/.runtime/verify-d8-in-container.mjs`：用平台真实的容器调用形态、对冻结快照里的
toolkit 触发列名错误，读到"用参数 admin_iso_col 指定…可用字段：shapeName、shapeISO、geometry"）。

**限制**：本轮只重跑了这几例、每例一次；B06 的耗时变化说明并发/网络波动足以改变单例耗时，
不能据此比较效率。D4（预算标注）与 D6（attach 看护）的**运行时**效果还需要在多轮跑批中出现
同类场景才能确认——D6 的判据是"作业完成即结束"，正常路径下与旧行为一致，故不会在顺利的作业上显形。

跑批期间对基准框架做了一处**诊断增强**（不属于产品源码，`dsh/benchmark/` 不在冻结范围内）：
`native-history` 会把失败工具的内容替换成固定句子，因此报告里原本只有一条无主的
`tool-error`。现在按 `tool/call` 的 `callId` 关联出**工具名**，并额外记录
`turns`/`steps` 与每次"催促"发生的 `seq`，使"模型提前停手、真实用户会说继续"与
"用例确实耗尽预算"在报告里可分辨。下一节的全量跑批即使用该版本框架。

## 7. 全量 31 例跑批（候选 `1daa24e17c76df57`）

D9 之后重新冻结候选 `1daa24e17c76df57`（候选内容 = D1–D9），在它的预览实例上跑**全部 31 例**
（`--concurrency 4`，账号 `preview_user` 普通用户，准入放宽到 4/4 + 8/8）。
原始记录 `dsh/.runtime/benchmark-report-full-1daa24e1.json`；本地墙钟 04:41→06:45（约 2 小时 4 分），
单例耗时之和 26,094s（并发 4，含排队等待）。

| 用例 | 档位 | 检查 | 耗时 | 说明 |
| --- | --- | --- | --- | --- |
| B01 上海 2020 年分区 ANTL 统计 | 基础 | 5/6 | 359s | 未调用 `geo_calculate_zonal_statistics`（见 D10） |
| B02 单幅栅格质量检查 | 基础 | 3/3 | 475s | |
| B03 中国省级边界获取与核验 | 基础 | 4/4 | 349s | |
| B04 逐年 ANTL 时间序列 | 基础 | 2/3 | 570s | 同上（D10） |
| B05 等距缓冲区 | 基础 | 4/4 | 318s | |
| B06 按多边形筛选点 | 基础 | 2/2 | 419s | |
| B07 栅格重投影 | 基础 | 2/2 | 579s | |
| B08 相邻栅格镶嵌 | 基础 | 2/2 | 595s | |
| B09 指标口径解释 | 基础 | 2/2 | 306s | |
| B10 中文可视化 | 基础 | 3/3 | 351s | |
| A01 分区统计 + 证据链 | 进阶 | 6/6 | 419s | |
| A02 首夜日期与 UTC 换算 | 进阶 | 2/2 | 952s | |
| A03 月度时间序列选型 | 进阶 | 3/3 | 867s | |
| A04 事件窗口对比 | 进阶 | 6/6 | 2708s | |
| A05 异常检测 | 进阶 | 3/3 | 399s | |
| A06 多源叠加分析 | 进阶 | 3/3 | 501s | |
| A07 两套边界差异 | 进阶 | 3/3 | 481s | |
| A08 全球事件监测线索核验 | 进阶 | 4/4 | 476s | |
| A09 多角色综合报告 | 进阶 | 5/5 | 1404s | |
| A10 点与行政区空间连接 | 进阶 | 2/2 | 346s | D8 的改进在本轮**仍未触发**（agent 一次就传对参数） |
| A11 共享边界库取用与来源声明 | 进阶 | 6/6 | 214s | |
| C01 超大范围下载的单次上限与失败语义 | 挑战 | 2/3→5/5 | 1910s | 原始判分未过，属**用例缺陷**（见 §4） |
| C02 时区陷阱 | 挑战 | 5/5 | 519s | |
| C03 跨传感器可比性 | 挑战 | 5/5 | 1507s | 上轮曾因"预算用尽"误判，本轮在预算内完成 |
| C04 趋势显著性 | 挑战 | 6/6 | 2346s | |
| C05 归因边界 | 挑战 | 5/5 | 1508s | |
| C06 海域覆盖与掩膜声明 | 挑战 | 6/6 | 660s | 上轮用例前提修正后稳定通过 |
| C07 来源冲突记录 | 挑战 | 3/3 | 382s | 上轮 FAIL（2/3），本轮通过 |
| C08 精度与空间支持声明 | 挑战 | 4/5→5/5 | 1203s | 原始判分未过，属**用例缺陷**（见 §4） |
| C09 重复运行不等于独立验证 | 挑战 | 5/6→6/6 | 557s | 原始判分未过，属**判分缺陷**（见 §4） |
| C10 端到端地缘事件分析 | 挑战 | 9/9 | 2414s | 上轮在 4508s 处耗尽预算，本轮完成 |

原始判分：**26/31 通过**（基础 8/10、进阶 11/11、挑战 7/10）。修正三处用例/判分缺陷后重算
（`node dsh/benchmark/rescore.mjs dsh/benchmark/cases.json dsh/.runtime/benchmark-report-full-1daa24e1.json`，
**不重跑**，只重放已捕获的答复）：**29/31 通过**（基础 8/10、进阶 11/11、挑战 10/10），
剩下的 B01/B04 由 D10/D11 解释。

跑批期间的正向证据：

- **D7 看护在真实跑批里持续生效**：`watch-limbo.mjs` 记录到多次"容器已在但无进程"的中间态，
  每次都在 **16–18 秒**内收口并结束（D7 之前是 12–24 分钟的空等）。
- D6/D7 的修复让长用例不再被 attach 卡死：C04 2346s、C10 2414s 都在各自预算内完成。
- 与单版本基线（`8d9e492d54894030`）相比，同一批用例从"4 例干净通过 + 2 例预算用尽 + 1 例前提有误"
  变为全量 29/31。

## 8. D10/D11 现场验证（候选 `9f829af5881d642f`）

D10/D11/D12 修完后重新冻结候选 `9f829af5881d642f`（内容 = D1–D12），在它的预览实例上重跑
**D10 的两个现场用例 + 一个共享数据用例**（`--concurrency 3`，原始记录
`dsh/.runtime/benchmark-report-d10-9f829af5.json`）：

| 用例 | 上一轮 | 本轮 | 耗时 |
| --- | --- | --- | --- |
| B01 上海 2020 年分区 ANTL 统计 | 5/6（缺 `geo_calculate_zonal_statistics`） | **6/6 PASS** | 564s |
| B04 逐年 ANTL 时间序列 | 2/3（同上） | **3/3 PASS** | 524s |
| A11 共享边界库取用与来源声明 | 6/6 | **6/6 PASS** | 197s |

**D10 的直接证据**（同一批用例里逐条核对工具调用与返回）：

- B01 第一次 `geo_inspect_raster` 就是
  `{"path": "share/全球基础数据/NPP-VIIRS-LIKE-NTL/LongNTL_2020.tif", "source": "share"}`，
  返回正常、**没有**再出现上一轮那句
  `No such file or directory: /workspace/inputs/share/…`。
- B04 的 `geo_write_report` 一次引用 3 个共享库文件
  （`share/全球基础数据/NPP-VIIRS-LIKE-NTL/LongNTL_2015.tif`、`LongNTL_2020.tif`、
  `share/全球基础数据/catalog.json`）加若干 `outputs/` 产物，**通过**；
  上一轮同类调用是被 400 拒绝的（"来源须为 inputs/ 或 outputs/ 文件"）。
- 两例都重新调用了平台自己的 `geo_calculate_zonal_statistics`，不再绕开自算。

**D11 的直接证据**：跑批期间每 5 秒采样一次作业容器的镜像（77 个样本，覆盖多个容器），
全部是 `sha256:28d2a7f36a2043709fccefbdbea304143a60a393a06f87a137bedab6a793fa52`，
与该候选 `validation.json` 里的镜像摘要**逐字相同**；修复前这里会显示通用标签
`geosentinel-gis:0.1`（`79def542ccfb`）。

**D12 仍是单测级证据**：该轮没有再现 OOM/超时，按限制如实标注。

本轮剩余的工具错误都是模型侧、且平台提示可执行：两次猜错技能名（`ntl-analysis`）、
三次猜错 GEE 资产 ID（`NOAA/VIIRS/001/VNP46A4` 等，平台回报
`GEE_DOWNLOAD_FAILED: ImageCollection asset … not found` 并给出建议）、
一次自造工具名 `geo_fetch_placeholder`（被守卫拒绝："该工具在本平台不可用"）、
若干次自写脚本语法/索引错误。

## 9. 结论与下一步

- 单版本（`8d9e492d54894030`）子集基线：8 例中 4 例干净通过、2 例预算用尽、1 示例前提有误、
  1 例遇到 D6 现场（A10 未在预算内收口）。
- 累计定位 **12 个系统性缺陷（D1–D12）**，全部与模型能力无关，全部已修并有测试/证据：
  D1–D6 在候选 `5b4651d9be7ede8e` 上完成 5 例复跑（全通过），D7/D8 在候选
  `a01d2f98a7e9a206` 上完成 A10/A11 复跑（A10 1121s→277s、A11 1784s→327s，D7 有直接指纹证据），
  D9 是本轮复查发现的 D1 回归；全量 31 例（候选 `1daa24e17c76df57`）又暴露了
  **D10（共享数据只在容器挂载里能用一半）、D11（候选验证过的镜像从未被运行实例使用）、
  D12（容器被杀只剩一句退出码）**，三者的修复已在源码与单测/容器内验证中固定，
  但**尚未在任何冻结候选的跑批里验证**——B01/B04 的失败正是 D10 的现场。
- 用例集从 30 例扩到 **31 例**（新增 A11）；累计修正 5 处用例/判分缺陷：C06 前提、B02 预算、
  C01 禁止词、C08 关键词、C09 否定句判分（后三处在 §4，判分器改动在 `benchmark/score.mjs`）。
- 下一步：① 全量 31 例在候选 `1daa24e17c76df57` 上已跑完（原始 26/31，修正用例/判分后 29/31）；
  ② D10–D12 已在候选 `9f829af5881d642f` 上重跑 B01/B04/A11 验证通过（§8），
  该候选可发布；③ 发布前需重新生成预览与验收（验收记录最长 1 小时），并由管理员明确授权；
  ④ 发布后在正式实例上抽验同一组用例。

## 10. 限制

- 全量与各批次均为每例一次，**不构成独立验证**；规则分不等于科学质量评阅。
- D10/D11 的修复只在候选 `9f829af5881d642f` 上重跑了 **3 例**确认（§8），不是全量复跑；
  B01/B04 也只是各一次，模型路径本身有波动（同一用例在不同轮次会走不同取数路线）。
- D12（容器被杀的可读原因）只有单测证据，现场触发需要再次出现 OOM/超时。
- `score.mjs` 的否定判据是**前缀启发式**（匹配点前 8 字符内的否定词），
  对"并非不能算作独立验证"这类双重否定仍会误判为通过；规则分本来就只覆盖词面。
- 环境既有问题（host python 指向另一个 checkout、pymannkendall 缺失等）见 §5。
