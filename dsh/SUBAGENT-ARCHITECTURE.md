# 原生子智能体与 AgentTeams：调研结论

核对与实现日期：2026-09-08。已接通原生只读查看，保留 AgentTeams，不开放用户直接操纵子智能体。

## 结论

采用 AgentTeams 的任务编排，复用原生 DSH 的子会话目录、只读历史与运行状态展示。不能因为界面看不到子 Agent，就判断原生能力缺失或必须替换调度器。

GeoSentinel 使用 `@deepseek-ai/dsh@0.1.2-rc.1`；本次 npm 查询 `latest` 和 `next` 均为该版本，`alpha` 为 `0.1.3-alpha.2`。现有版本已具备需要的持久化子智能体能力，不必为了“能看见子智能体”升级到 alpha。

## 修复前为什么看不到

- `plugins/workbench/native/client.js` 的 `subagentsByParent` 始终为空，`subagentAddress` 返回 `undefined`，`refreshSubagents` 与 `setSubagentCatalogOpen` 为空实现，未实现 `openSubagent`。
- `plugins/platform/sidebar-adapter.mjs` 明确关闭 Better Sidebar 的 `subagent`、`sidechat` 入口。
- `plugins/platform/index.mjs` 已保存成员与主对话的归属、读取成员历史并监听成员进度，但原生页面的 `nativeHistory` 当前只提供主智能体事件。这是观测适配缺口，不是成员没有运行。

## 能力比较

| 能力 | 原生 DSH | 当前 AgentTeams fork |
| --- | --- | --- |
| 创建子智能体、角色提示与工具约束 | provider、persona、toolFilter、depth 等接口 | 调用原生接口，添加固定角色和团队规则 |
| 子会话持久化、后续消息 | `startContinuable`、`sendMessage`、会话记录 | 已使用这些原生能力，不是另一套子 Agent 存储 |
| 子树发现与 UI 导航 | `listChildren` / `listDescendants`、catalog、SubagentAddress、原生目录与会话渲染 | 团队成员映射可与原生目录对接 |
| 任务依赖、认领和重分配 | 子 Agent 基础设施本身不等于任务 DAG | 显式任务依赖、状态机、当前 attempt 身份与过期写入保护 |
| 批准后启动团队 | 需要产品层设计约束 | 已有 staged 方案、批准版本、成员启动和停止流程 |
| 任务级 Todo 事实来源 | 原生 Todo 负责展示/任务记录，不自动代表团队依赖执行 | 当前团队任务投影到原生 Todo |
| 多用户与 Docker 隔离 | 原生父子关系不是 GeoSentinel 用户权限 | 仍由平台/研究插件负责；替换 AgentTeams 也不能删掉这些边界 |

独立 fork 的 `src/members.ts` 已调用 `ctx.subagents.startContinuable(...)`；持久化子会话由 DSH 管理。AgentTeams 主要增加团队级任务编排，其附加状态位于 `.agent-teams`。

UI 展示的“运行”是逻辑 Agent 活动状态，不代表每个子 Agent 都对应独立操作系统进程。当前 provider 是 in-process spawn；Docker 容器属于地理计算作业，二者不能混同。

## 为什么暂不替换

当前“下载影像与获取区界并行，再统计和检查”的任务需要明确依赖。直接注册三个原生子 Agent 可以运行，但移除 AgentTeams 后仍需补回计划批准、任务依赖、失败重试、取消/恢复、成果汇总与 Todo 映射，不能只替换一个注册函数。

若以后确认产品仅需要简单的“主 Agent 委派、子 Agent 返回、主 Agent 汇总”，并愿意取消团队任务 DAG 和 attempt 级调度，则原生子 Agent 加小型 GeoSentinel 委派插件会更轻量。这属于独立迁移决策，需要相同研究案例、权限隔离和停止恢复回归，不在本轮直接切换。

## 已实现：原生只读可观测性

1. 后端提供受用户/主对话归属校验的子会话 catalog 与历史接口，使用 DSH 原生持久化目录，并与 `agent_sessions` 的成员归属交叉校验。
2. 前端填充原生 `subagentsByParent`，实现 `refreshSubagents`、`openSubagent` 和 `SubagentAddress` 导航，复用原生会话渲染而不是新造一套消息格式。
3. 目录准确显示“正在运行”或“当前未运行”，后者不等于完成。等待、失败或完成的任务细节以真实历史及主对话 Todo 为准。历史通过既有安全过滤器，只公开文本与工具成功/失败摘要，不公开模型请求、系统快照和原始工具参数。
4. 启用原生 `dsh-client-ui-subagent`，在原生 `conversation.composer` 插槽替换子会话输入区为只读状态与返回按钮。账号/主会话归属和原生父子关系必须同时满足；新增接口只有 GET，不接受发送、停止、重命名或批准。
5. Better Sidebar 继续关闭通用 subagent、sidechat 和终端功能。仅将其虚拟工作区信息和空能力状态通道兼容到子会话所属主研究，不开放主机路径或写入能力。

### 如何使用

- 打开已有多智能体研究，点击顶部会话名称旁的“若干个子智能体”，选择数据助手、分析助手或事件助手。目录按真实已创建成员显示，不填充虚构角色。
- 子会话复用原生消息、工具折叠项和会话层级导航；运行事件触发刷新，并每 4 秒补查一次。顶部可切换其他成员，底部“返回主对话”恢复正常提问入口。
- 指挥、停止整个研究和方案批准仍在主会话完成。打开子会话不会重跑其历史；浏览器刷新默认返回所属主对话。子会话的“资料与产出”读取所属主研究文件。
- `GET /geo/api/chats/:root/subagents` 返回过滤后的原生目录；`GET /geo/api/chats/:root/subagents/:child/history` 返回只读历史。每次请求验证账号、研究归属和成员归属，异步读取结束后再次校验。

### 验收边界

- 回归覆盖跨账号/错误主会话拒绝、子接口禁止写入、读取期间账号停用、客户端禁止发送/停止/重命名、原生 composer 参数以及返回主会话。
- 使用现有已停止研究的真实数据助手和分析助手历史，在 1366x768、1440x900 桌面进行目录切换、只读查看和返回测试；没有为验收重跑研究或修改其产物。
- 未在本轮执行新的长时间多智能体计算压力测试。模型历史保留原始语言，不做自动翻译或篡改旧结论。

## 核对来源

- [DeepSeek Harness 官方仓库](https://github.com/deepseek-ai/deepseek-harness)：版本和快速迭代边界。
- [原生 subagent 包](https://github.com/deepseek-ai/deepseek-harness/tree/master/packages/subagent/subagent)：本地安装版本的公共类型定义已核对 `startContinuable`、父子消息、持久化目录和 catalog。
- [AgentTeams 上游](https://github.com/NanmiCoder/dsh-agent-teams)：0.1.2 线的团队编排来源。0.1.5 线已改用 DSH 原生预设与原生 subagent 注册表，当年的可重建补丁随 `dsh/vendor/` 一起删除（需要时从 git 历史取回）。
- 当前原生 UI 适配：[client.js](plugins/workbench/native/client.js)；权限边界：[sidebar-adapter.mjs](plugins/platform/sidebar-adapter.mjs)。
