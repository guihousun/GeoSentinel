# 迁移到 GeoSentinel DSH 版

适用入口：本仓库 `dsh/`。依赖基线为 DSH `0.1.2-rc.1`，属于候选版；请按锁文件安装，不混用 alpha。本文不要求修改个人 DSH 或旧 Conda 环境。

## 1. 选择迁移方式

| 原系统 | 方式 | 历史处理 |
| --- | --- | --- |
| NTL-GPT Streamlit、旧 GeoSentinel FastAPI | 新目录部署，验收后切换公网 | 不导入旧账号、PostgreSQL 对话或 LangGraph checkpoint |
| 已运行的 GeoSentinel-DSH，同机升级 | 保持运行目录和数据路径，升级代码与 fork | 保留整套 DSH home，不能用空库替代 |
| GeoSentinel-DSH 换机器 | 完整停机备份，优先保持相同绝对路径 | 保留数据库、会话、项目文件与状态；不同路径不能保证直接兼容 |

**“旧对话不迁移”仅针对旧 Python 产品。已经由 DSH 版产生的对话应保留。** 仓库不包含运行数据库、测试账号或出厂密码。

新版已具备多用户项目、原生聊天/问答/方案审阅/Todo、固定四角色 AgentTeams、受控执行和公共监测。旧版完整 NTL/Earthdata/MCP 工具、国家专题、企业知识库、个人 GEE 登录和大型作业排队仍需逐项迁移。先验收关键业务，不要只测试“你好”就替换旧生产服务。

## 2. 盘点与备份

记录旧 Git 提交、服务端口、域名、数据库位置、用户文件目录、GEE 项目和凭据位置、代理、共享监测目录、外部模型接口及未完成任务。

旧 Python 产品至少备份：

- 原 `.env` 与服务配置，单独保管，不上传 GitHub。
- PostgreSQL 一致性备份。旧连接变量为 `NTL_HISTORY_DB_URL` / `NTL_LANGGRAPH_POSTGRES_URL`；按现场数据库使用 `pg_dump`，不要把连接串放进公开日志。
- `user_data` 或实际工作区、共享数据、RAG 资料、研究成果及来源记录。
- 可恢复的程序版本。此仓库 DSH 发布前基线为 `a97780b`，现场部署版本可能不同，以盘点记录为准。

已有 DSH 时，停止接收新任务，等待或明确取消未完成任务，停止应用后备份：

```text
<GEO_DSH_HOME>/
  profiles/geosentinel/                 配置和生成的插件链接
  sessions/                            DSH 原生会话记录
  geosentinel/platform.sqlite           用户、项目、对话归属与审计
  geosentinel/users/                    项目输入、对话输出与记忆
  geosentinel/jobs/                     作业记录
  monitor/                             默认公共监测目录
```

设置了 `GEO_DATA_DIR` 或 `GEO_MONITOR_DIR` 时，还要备份外置目录。AgentTeams 的 `.agent-teams` 状态也应随所在工作区保留。不要只复制 SQLite 主文件而遗漏写入中的 WAL，更不能只备份数据库而不备份文件。

Windows 停机后可复制到新目标，例如：

```powershell
$SourceHome = 'D:\GeoSentinel-data\home' # 替换为实际 GEO_DSH_HOME
$BackupRoot = Join-Path 'D:\GeoSentinel-backups' (Get-Date -Format 'yyyyMMdd-HHmmss')
New-Item -ItemType Directory -Path $BackupRoot
Copy-Item -LiteralPath $SourceHome -Destination (Join-Path $BackupRoot 'home') -Recurse
```

先检查目标路径、可用空间和备份工具对 junction 的行为。生成的插件目录包含链接；跨机传输时不要把链接指向的整个代码库当作用户数据重复复制。抽查关键文件哈希，并演练恢复。

## 3. 新目录安装

以 Windows PowerShell 为例：新代码为 `D:\GeoSentinel`，fork 为同级 `D:\GeoSentinel-AgentTeams`。目录已存在时先检查，不要覆盖。

```powershell
Set-Location D:\
git clone https://github.com/guihousun/GeoSentinel.git GeoSentinel
git -C D:\GeoSentinel status -sb
```

必须保留完整仓库。`dsh/` 引用根目录 `packages/ntl_toolkit/src` 和 `monitoring/sources.py`，不能单独搬运该子目录。

### AgentTeams fork

```powershell
Set-Location D:\
git clone --branch v0.1.15 https://github.com/NanmiCoder/dsh-agent-teams.git GeoSentinel-AgentTeams
Set-Location D:\GeoSentinel-AgentTeams
git rev-parse HEAD
# 应为 da2e2e49242c6ecd7e801a74dba0c8268a0a2f81
git switch -c codex/geosentinel-orchestrator
git apply --check D:\GeoSentinel\dsh\vendor\agentteams-geosentinel.patch
git apply D:\GeoSentinel\dsh\vendor\agentteams-geosentinel.patch
pnpm install --frozen-lockfile
pnpm build
node --test scripts/geosentinel-policy.test.mjs
```

补丁包含产品策略和版本锁。不要只复制 TypeScript 文件，也不要对已经应用补丁的 fork 再应用一次。升级已有 fork 时，保留原目录，在另一个干净目录重建后切换 `GEO_AGENT_TEAMS_DIR`。

### 产品依赖与镜像

准备 Node.js 24、pnpm 10.33.0 和 Docker Desktop/Linux Docker：

```powershell
Set-Location D:\GeoSentinel\dsh
pnpm install --frozen-lockfile
Copy-Item .env.example .env
docker build --provenance=false -t geosentinel-gis:0.1 docker
```

仅新部署复制 `.env.example`；已有 `.env` 不要覆盖。不要为了消除 pnpm 构建警告批准所有依赖脚本。普通用户终端未开放，`node-pty` 不是本产品用户执行入口。

## 4. 配置映射

配置位于 **`dsh/.env`**，不会自动加载根目录旧 `.env`。

| 旧配置/能力 | 新版处理 |
| --- | --- |
| `DeepSeek_API_KEY` | 推荐 `DEEPSEEK_API_KEY`，启动器兼容旧名 |
| `DeepSeek_Coding_URL` | `DEEPSEEK_BASE_URL`，填写供应商基础地址，不重复追加请求路径 |
| `GEE_DEFAULT_PROJECT_ID` | 同名保留，检查新运行账号的项目权限 |
| 旧 GEE 缓存 | `GEO_GEE_CREDENTIALS` 指向管理员授权文件，不放入仓库或用户 inputs |
| 本机代理 | `GEO_GEE_PROXY`；Docker Desktop 访问主机通常用 `host.docker.internal`，不是容器 localhost |
| DashScope/RAG | 不等于新版已接入企业知识库，按后续能力迁移，不是登录前提 |
| PostgreSQL | 新版使用独立 SQLite 与 DSH 会话，不转换旧 checkpoint |
| 主机 Conda/Python | 新版固定 Docker 镜像，不修改 NTL-GPT-Stable |
| 旧线程工作区 | 按资料归属重新上传，不直接挂载给新用户 |
| 个人 DSH 插件 | 不复制给全体用户，使用产品固定插件配置 |

新部署建议从一开始固定运行目录：

```env
GEO_DSH_HOME=D:/GeoSentinel-data/home
GEO_AGENT_TEAMS_DIR=D:/GeoSentinel-AgentTeams
DEEPSEEK_API_KEY=your-private-key
DEEPSEEK_BASE_URL=https://api.deepseek.com
GEE_DEFAULT_PROJECT_ID=your-authorized-project
GEO_GEE_CREDENTIALS=D:/GeoSentinel-secrets/earthengine-credentials
GEO_ALLOWED_HOSTS=127.0.0.1:8510,localhost:8510
GEO_SECURE_COOKIES=false
```

自行替换示例。凭据不得出现在公开截图、日志和 Git 提交中。普通用户个人 GEE 绑定尚未提供，不能把替换服务器凭据当作用户级授权。

运行 `node scripts/check-env.mjs` 检查版本、fork、凭据存在性与 Docker 镜像。它不证明网络和下载成功。缺少 GEE 凭据时能测试聊天，但不能宣称 GEE 已就绪。

## 5. 初始化账号与启动

仅在空的新账号库初始化管理员，使用自己的密码：

```powershell
Set-Location D:\GeoSentinel\dsh
$SecurePassword = Read-Host '新管理员密码' -AsSecureString
$env:GEO_BOOTSTRAP_PASSWORD = [System.Net.NetworkCredential]::new('', $SecurePassword).Password
node scripts/admin.mjs admin
Remove-Item Env:GEO_BOOTSTRAP_PASSWORD
pnpm start --port 8510 --no-open
```

已有 DSH 库不要重复初始化。仓库没有默认管理员、默认密码或固定邀请码。网页生成的邀请码默认一次性、一天有效；持久邀请仅由管理员本地管理，泄露后应撤销。

访问 `http://127.0.0.1:8510/`，自动进入 `/geo/native/`。不要用个人全局 `dsh web` 代替产品启动器。旧自建工作台和 `GEO_NATIVE_UI_PREVIEW` 开关已移除。

端口已被占用时，核对进程归属，或用另一个测试端口并同步修改 `GEO_ALLOWED_HOSTS`，不要停止不明服务。

## 6. 资料迁移

1. 在新版重新建立账号、研究项目和对话。
2. 由资料所有者选择需要的旧输入与成果，核对敏感性、许可、日期、坐标系及来源。
3. 通过“资料与产出”上传。旧成果再次作为输入时标为既有产物，不冒充新版生成结果。
4. 当前单文件上限 16 MiB、项目输入额度 512 MiB。超大数据应另行设计管理员导入/共享方案，不手工写入未知用户目录绕过权限。
5. 旧 checkpoint、账号主键、聊天记录、个人插件与认证缓存不导入新版。旧系统按原方式只读归档。

多智能体不再使用旧 Python 图。迁移工具时优先接入确定性能力和数据契约，不把旧调度器套进新的 AgentTeams。

## 7. 公共监测

监测是独立共享服务。新旧应用并行时只保留一个明确的采集服务；其他读者设 `GEO_MONITOR_EXTERNAL=true` 并指定 `GEO_MONITOR_DIR`。

旧 Python 快照没有通用转换器，不要直接当作 DSH 快照使用。格式不确定时保留旧档，让新服务重新采集；DSH 同版本快照可连同完整监测目录迁移。来源失败时如实显示降级，不填充虚构数据。详见 [公共监测说明](../dsh/MONITOR.md)。

## 8. 上线验收与公网切换

上线前逐项验证：

- 管理员邀请、普通用户注册；普通账号无管理入口。
- 两个账号各自创建项目，不能读取对方对话、文件或问答请求。
- 原生选项与自由回答可等待和继续，刷新不串会话。
- 讨论/拒绝不启动团队；确认当前版本才执行，过期或重复答案被拒绝。
- Todo 对应真实团队状态，失败不算完成；停止能终止当前运行。
- 上传资料、读取、真实生成并下载报告，内容与来源一致。
- 授权 GEE 数据小范围真实下载通过，核验波段、日期、CRS、范围与文件，而非只看成功提示。
- 地图、监测来源链接及私有证据导入正常。
- 1366×768 和 1440×900 首屏主要控制可见、文字清晰、无横向溢出。
- `pnpm test` 通过，`/geo/api/health` 正常。

切换前冻结旧应用新任务并完成备份，再改花生壳/反向代理。若公网映射仍是 `127.0.0.1:8502`，确认旧 8502 停止后再启动新版 8502，不与旧服务抢占端口。

配置公网 `GEO_ALLOWED_HOSTS`，HTTPS 入口设 `GEO_SECURE_COOKIES=true`。代理保留 Host 并支持 SSE/WebSocket。HTTPS 在代理终止时无需内部 HTTP 再装证书。验证登录、问答和侧栏，不只检查首页。不要公开个人 DSH 端口、Docker socket 或数据库。

DNS、花生壳映射、证书和应用端口是不同层，改端口不会自动改域名解析。

## 9. 已有 DSH 换机器或移动目录

DSH 会话保存绝对工作区路径，profile 包含代码和 fork 的链接。因此：

- 最稳妥是保持代码、fork、`GEO_DSH_HOME`、`GEO_DATA_DIR` 的绝对路径一致；目标机安装依赖和镜像后恢复停机备份。
- 同机代码升级保持数据路径不变。记录旧提交，用 `git pull --ff-only` 更新干净分支；存在修改时先处理，不用 `reset --hard` 覆盖。
- 跨机传输通常不能保留 Windows junction。不要复制 `node_modules`，按锁文件重装；生成的插件链接在目标机重建。
- `Managed profile link is stale` 表示链接仍指向旧代码。停机备份后，逐个核对 `profiles/geosentinel/node_modules` 的生成链接并修复目标；不要删除链接目标目录或清空 home。
- **不同数据绝对路径暂无自动迁移工具。** 保留原路径，或按第 6 节重建项目并重新上传资料。不要批量替换数据库/JSONL 字符串后声称历史已安全迁移。
- 两台服务不能同时写同一 SQLite/会话目录；不要用 Git 同步运行数据。

先离线恢复验收，再切换公网。应用代码、运行数据与模型/GEE 凭据分别备份和管理。

## 10. 回退

DSH 资源队列版本的同机升级、3/4 GiB 内存配置和停机清理教程见 [资源与队列管理](../dsh/RESOURCE-MANAGEMENT.md)。本次升级需要保留整个既有数据目录并重启服务，不需要重新注册账号或重建 GIS 镜像。

保留旧代码、配置、数据库和用户文件。新版关键业务失败时：停止接收新任务并保留新版新增资料，将代理改回旧端口，恢复旧服务。新旧库不合并覆盖。

回退不是删除新版数据。记录切换时间与失败案例，避免用户误以为两套系统共享一段对话。DSH 升级回退应同时匹配代码、fork、镜像和完整 home 备份，不只回退 `package.json`。

## 11. 常见故障

| 现象 | 检查 |
| --- | --- |
| `dsh web` 是另一套页面 | 使用本项目 `dsh/` 下 `pnpm start` |
| 找不到 fork 的 `lib/index.js` | 固定 commit、应用补丁、构建并检查 `GEO_AGENT_TEAMS_DIR` |
| 登录后项目为空 | 核对实际 home/数据库，可能误建新库，不要再次初始化 |
| 历史路径不匹配 | 核对绝对 cwd 和数据目录，不绕过路径校验 |
| GEE authentication is not configured | 配置凭据与项目，重启后做真实小范围验证 |
| 容器代理失败 | 容器 localhost 不等于宿主，检查 Docker 网络和代理监听 |
| 公网问答/侧栏失败 | Host 白名单、HTTPS Cookie、SSE/WebSocket 与代理超时 |
| 重启后问题消失 | 普通待答请求不跨进程恢复，旧 ID 已作废，重新发起 |
| 有监测记录但采集失败 | 检查来源状态、Docker、代理和独立 worker，不等于采集成功 |
| 旧功能找不到 | 查看 [当前实现](../dsh/IMPLEMENTATION.md)，按确定性工具逐项迁移验收 |
