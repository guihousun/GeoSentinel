# 地缘环境智能计算基准（30 例）

## 来源与范围

工作区内没有 NTL-GPT 的 200 例原始集（`evaluations/` 只有 9 例回归清单，位于
`.ntl-gpt/skills/ntl-regression-evaluation/references/regression-checklist.json`）。
本基准以该清单中与地缘环境智能计算相关的题型为种子（GEE 路由、数据集选型、
首夜/时区、失败语义、文件管理、服务端统计），再补充平台典型场景，共 30 例：

| 档位 | 例数 | 考查重点 |
| --- | --- | --- |
| 基础 | 10 | 单步/两步；工具、产物、关键词明确 |
| 进阶 | 10 | 多步或多角色；真实获取 + 统计 + 证据链 |
| 挑战 | 10 | 口径、时区、归因、无数据、来源冲突；重点看是否如实报告限制 |

每例在 `cases.json` 中声明 `expect.tools / artifacts / keywords / forbidden`，
打分是规则式的（命中工具名、产物扩展名、关键词；禁止词出现即该项失败）。
规则分不等于科学质量评价：挑战档的“禁止词”用来抓最常见的夸大结论。

## 运行

只通过产品 HTTP 接口驱动**普通用户**账号，不调用内部插件：

```powershell
# 隔离预览实例（推荐：不打扰正式数据）
node dsh/benchmark/run.mjs --base http://127.0.0.1:8513 --preview-token <预览登录 token> `
  --concurrency 3 --out dsh/.runtime/benchmark-report.json

# 正式实例上的普通用户（需要邀请码或已有账号）
node dsh/benchmark/run.mjs --base http://127.0.0.1:8511 --username bench_user `
  --password '<密码>' --invite <邀请码> --concurrency 3
```

选项：

- `--only B01,C04`：只跑指定用例。
- `--timeout 20`：覆盖单例超时（分钟）。**谨慎使用**：用例自带 `timeoutMinutes`（挑战档 15–75 分钟），
  用小值覆盖会把长用例截断在超时上；被截断的结果是执行器限制，不是平台失败，必须重跑。
- `--no-approve`：不自动批准方案（默认以用户身份批准并计数，报告中记录 `approvals`）。
- `--project "名称"`：复用项目名。

执行器行为（2026-09-10 修正）：

- 一个待答请求最多回答一次。此前每次轮询都重发同一个 `requestId`，在“模型反复要求确认”的用例上累计出数百次失败尝试（报告中表现为 `approvals`/`errors` 异常偏大）。
- `errors` 去重且只扫描每轮新增事件；此前每次轮询重复记录同一个 `tool-error`。注意 `tool-error` 只说明某次工具调用失败（模型常常随后重试成功），判分不使用 `errors`。
- 建议 `--concurrency 2`：平台默认每账号同时最多 2 个活跃研究对话，第 3 例会排队；排队本身不是失败。

用例可以声明两类用户决策，两类都只回答"用户会怎么点"，不改变判分关键词：

- `answers: ["按默认口径"]`：按声明内容作答；平台给出多选项时优先选中包含该内容的选项。
- `clarify: "first"`：模型中途提出的方法澄清，按平台给出的第一个选项作答（与批准方案属于同一类 standing decision）。当前 A09、C02、C04–C10 声明了它。没有声明的用例遇到非方案问题就如实记为 `unanswered-question` 并等超时：凭空编造方法答案测的是执行器，不是平台。

并发受平台准入控制：普通用户默认同时最多 2 个研究对话、2 个 Docker 作业，
所以 `--concurrency 3` 时第 3 例会排队，这是平台的资源策略而不是执行器缺陷。

## 加速跑批（吞吐优先）

时间主要花在**模型步数**上，其次才是容器作业与下载（实测对照：B10 用了 2707 秒、产物 0、作业 0；
A01 3606 秒 / 22 个作业目录；真正密集跑容器的 A03 是 1195 秒 / 91 个作业）。因此能真正提速的只有并发与资源：

```powershell
# 1) 只给【预览实例】放宽每账号准入上限（不是产品默认值，见下方说明）
$env:GEO_USER_RESEARCH_CONCURRENCY = '4'
$env:GEO_USER_DOCKER_CONCURRENCY   = '4'
$env:GEO_RESEARCH_CONCURRENCY      = '8'
$env:GEO_DOCKER_CONCURRENCY        = '8'
node dsh/tools/release.mjs preview --id <id> --hold 14400

# 2) 执行器并发跟随上限
node dsh/benchmark/run.mjs --base http://127.0.0.1:8513 --preview-token <token> --concurrency 4 `
  --project "基准测试" --out dsh/.runtime/benchmark-report.json
```

- 预览子进程继承启动它的终端环境（`release/runtime.mjs` 的 `{ ...env }`），准入上限在实例启动时由
  `plugins/platform/runtime.mjs` 读取，所以必须在 `preview` 之前设置，改完要重启预览才生效。
- 代价：并发越高，单例耗时越长（争 CPU/网络），**耗时不可跨例比较**；判定（工具/产物/关键词）不受影响。
  内存按 4 × `GEO_DOCKER_MEMORY_MIB` 预留（默认 3 GiB → 至少 12 GiB 给 Docker）。
- 默认准入（每账号 2 个研究对话、2 个 Docker 作业）才是产品策略，已有单测覆盖（`tests/admission.test.mjs`）；
  放宽只用于跑批，不要写进正式 `.env`。
- 其他有效手段：给 Docker Desktop 更多 CPU/内存；不要重跑已通过的用例（报告按 id 合并、后写覆盖，`--only` 只跑缺的）；
  日常回归只跑 6–8 例 smoke 子集（三档各取 2–3 例），完整 30 例留给正式验收；GEE 走可用代理。
- **不要**用 `--timeout` 压缩预算换速度，也不要用更小模型或改写提示词：前者把长用例截断成假失败，后者换了被测对象。

## 输出

`report.json` 记录每例的工具调用、产物列表、最终回答、规则检查明细、
耗时与是否触发方案审批。脚本每完成一例就落盘，中途中断不会丢结果。

## 限制

- 规则分只验证“做了什么/说了什么”，不能替代人工评阅科学质量。
- 模型中途要求确认口径时，`clarify: "first"` 会替用户选第一个选项。这是可追溯的用户决策，不是平台结论；报告里把它记在 `approvals` 与用例声明的组合中，判分仍只看平台自己的答复与产物。
- 工具名来自会话工具事件；工具结果内容在原生接口中被有意裁剪，因此产物检查
  只依据工作区文件列表，不依据工具输出文本。
- 同一用例重复运行不构成独立验证。
- 用例提示词必须自足：缺少输入（栅格、点位、边界）的题目会退化成“澄清问题”，
  这不是能力差异。发现后已改为“先获取/先构造”。
- 普通用户准入是 2 个并发研究对话、2 个 Docker 作业；执行器默认 `--concurrency 2`，
  队列等待不计入单例预算，避免把平台资源策略误判为失败。
- Windows 上 Docker Desktop 的绑定挂载偶尔返回 `EIO`。执行器把它记为环境错误；
  产品侧已对容器启动失败与错误信封各做一次带退避的重试。
- 预览实例的 home 目录曾放在发布目录内部，深路径会让容器写入持续 `EIO`；
  现已改为发布目录旁的独立 `previews/` 短路径。用旧布局跑出的失败不能当作
  平台能力结论。

