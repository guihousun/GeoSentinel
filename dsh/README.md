# GeoSentinel on DSH

地缘环境智能计算平台的独立 DSH 运行入口：邀请制登录、多研究项目、多对话、固定角色协作、用户方案确认，以及 Docker 内的数据获取和分析。

换一台电脑从零安装并完整复盘（迁移清单、安装步骤、发布与回滚、数据迁移、验收清单、已知故障）：见 [新机器安装与完整复盘指南](../docs/new-machine-migration.md)。

本目录不替换旧 FastAPI/Streamlit 服务，也不读取或迁移旧对话。首次运行使用全新的账号与项目库。共享全球事件监测已接入；完整 NTL 工具迁移和专题地图尚未完成，不能据此宣称所有旧功能已经接入。

## 三个插件

管理员开发改动通过“验证 → 用户版预览 → 确认同步发布”进入正式快照，详见 [同步发布说明](RELEASES.md)。普通启动不再直接运行可变开发源码；首次建立快照会安装隔离依赖并构建固定镜像。

新接入的行政区获取和 14 项 GIS/NTL 原子工具见 [工具迁移](TOOL-MIGRATION.md)。管理员可通过平台内“开发模式”远程使用原生 DSH 设置、创造模式和主机工具，见 [管理员开发模式](ADMIN-DEVELOPMENT.md)；独立本机命令 `geosentinel admin` 继续兼容。

| 插件 | 职责 |
| --- | --- |
| `plugins/workbench` | 中文登录、项目与对话管理、方案确认、流式消息、上传和下载 |
| `plugins/platform` | 邀请、账号、SQLite 元数据、访问校验、工作区路径与 API |
| `plugins/research` | 来源材料、报告、固定 GEE 下载和隔离 GIS 计算 |

独立 AgentTeams fork 负责唯一的多智能体调度。保留地缘分析师、数据助手、分析助手、事件助手四个固定角色，按需启用成员；没有把旧 Python 多智能体图套在新调度器下面。

已有团队的主对话顶部提供原生“子智能体”目录，可查看各成员的持久化历史、当前运行活动与经过安全过滤的工具状态。子会话只读，指挥、停止和批准均回到主对话操作；不启用上游个人主机接口。实现与使用说明见 [子智能体架构](SUBAGENT-ARCHITECTURE.md)。

## 环境

- Node.js 24；pnpm 10.33.0。
- DSH 与全部 DSH 内部依赖锁定为 `0.1.2-rc.1`（2026-09-08 npm `latest`；版本名称仍为 RC，不是无预发布后缀的正式版）。不跟随 `alpha` 通道，不要用 npm 重新生成依赖树。
- AgentTeams 基线为 `v0.1.15`，已导出的独立 fork 差异见 [`vendor/agentteams-source.json`](vendor/agentteams-source.json)。
- Docker Desktop/Linux Docker，至少预留 GIS 镜像空间及容器内存。无需修改 NTL-GPT-Stable 或主机 Python 环境。
- DeepSeek API 凭据；已获授权的 GEE 项目与 Earth Engine 凭据文件。

### 1. 准备独立 fork

在 GeoSentinel 仓库旁建立兄弟目录，或通过 `GEO_AGENT_TEAMS_DIR` 指定其他位置：

```powershell
git clone --branch v0.1.15 https://github.com/NanmiCoder/dsh-agent-teams.git GeoSentinel-AgentTeams
cd GeoSentinel-AgentTeams
git switch -c codex/geosentinel-orchestrator
git apply --check ../GeoSentinel/dsh/vendor/agentteams-geosentinel.patch
git apply ../GeoSentinel/dsh/vendor/agentteams-geosentinel.patch
pnpm install --frozen-lockfile
pnpm build
node --test scripts/geosentinel-policy.test.mjs
```

将命令中的 `GeoSentinel` 改为实际克隆目录名。已经应用补丁的工作目录不要重复应用。上游 MIT 许可及变更来源均保留在 `vendor/`。

### 2. 安装、配置和构建

```powershell
cd <GeoSentinel仓库>\dsh
pnpm install --frozen-lockfile
Copy-Item .env.example .env
docker build --provenance=false -t geosentinel-gis:0.1 docker
```

在 `.env` 填写 `DEEPSEEK_API_KEY`、`GEE_DEFAULT_PROJECT_ID` 和 `GEO_GEE_CREDENTIALS`。凭据路径指向管理员已有的 Earth Engine 认证文件，不要把凭据复制进 Git 或项目上传区。兼容旧配置中的 `DeepSeek_API_KEY`、`DeepSeek_Coding_URL`，但新部署建议使用示例文件的名称。

`GEO_GEE_PROXY` 只传给固定 GEE 获取容器，不给模型生成的 Python。Docker 中的 `127.0.0.1` 是容器本身；Docker Desktop 访问主机代理时使用 `host.docker.internal`。代理与 Google 服务可达性应按部署机器实际验证。

如果此前用旧项目 `.env` 启动而缺少新版 GEE 参数，可运行 `node scripts/import-env.mjs --source <旧.env路径> --home <已有DSH-home路径> --credentials <Earth-Engine凭据路径> --port 8511` 生成独立 `dsh/.env`；需要主机代理时添加 `--proxy http://host.docker.internal:7897`，全球事件监测目录外置时添加 `--monitor-dir <目录>`。该命令拒绝覆盖已有 `.env`，只导入产品使用的配置，不修改源文件，也不会重新初始化账号。Windows 常见凭据位置为 `%USERPROFILE%\.config\earthengine\credentials`，以本机实际认证文件为准。

使用 `GEO_ENV_FILE` 时确认它指向新文件。启动器会先读取管理员配置，再从受控 profile 目录运行 DSH，让启动变量通过进程环境继承，避免 DSH 将 `DEEPSEEK_BASE_URL` 当作普通研究项目 `.env` 的禁止变量。不要绕过 `scripts/start.mjs` 直接在含该产品配置的目录运行裸 `dsh web`。

2026-09-08 修复了镜像 worker 仍残留 10 分钟闹钟的问题。此次更新需执行 `docker build -t geosentinel-gis:0.1 docker` 重建镜像；worker 默认 1800 秒，并接收宿主端传入的 `GEO_JOB_TIMEOUT_SECONDS`。验证命令：`node scripts/smoke-worker-timeout.mjs`。真实小范围 GEE 验证：`node scripts/smoke-gee.mjs`，在独立临时工作区执行，不修改已有研究。

```powershell
node scripts/check-env.mjs
```

此检查验证版本、镜像与凭据存在性，不打印密钥；网络与 GEE 权限仍需真实下载验收。

### 3. 初始化管理员并启动

在首次启动前设置 `GEO_BOOTSTRAP_PASSWORD` 环境变量为强密码（最低 8 位，管理员建议至少 12 位），再运行：

```powershell
node scripts/admin.mjs admin
Remove-Item Env:GEO_BOOTSTRAP_PASSWORD
pnpm start --port 8510 --no-open
```

访问 `http://127.0.0.1:8510/`。管理员初始化仅允许在空账号库执行；没有公开注册管理员接口。登录后可邀请用户，邀请码一次性使用、一天有效。用户可修改密码，修改后全部已有登录失效。

注册和修改密码最低 8 位，既有密码保持不变。管理员可在本机通过 `PlatformStore.persistentInvite(admin)` 生成固定、可重复使用且不过期的邀请码，通过 `revokeInvite(admin, code)` 撤销。此类型仅用于可信人员共享，不在普通用户界面开放创建；数据库仅保存邀请码哈希，不保存明文。固定邀请码泄露后任何持有者都能注册普通账号，应及时撤销。网页管理员邀请按钮仍生成一天有效的一次性邀请码。

本地后台启动或重启使用 `scripts/restart.ps1`，只处理该运行目录占用的端口，不接管其他服务。

### 日常启动与端口占用

推荐注册独立启动命令（每台电脑仅需一次，无需管理员权限）：

```powershell
cd <GeoSentinel仓库>\dsh\cli
npm link --ignore-scripts --package-lock=false
```

这是不含依赖的本地启动器包，不使用 npm 安装或修改 DSH 的 pnpm 依赖树。注册后可在任意目录运行：

```powershell
geosentinel
# 等价命令：geosentinel web
```

该快捷入口默认端口 **8511**，读取注册仓库的 `dsh/.env`，通过现有 `scripts/start.mjs` 加载独立配置与插件。`geosentinel --port 8512 --no-open` 可覆盖端口；与下方兼容入口默认 8510 的区别是有意保留。配置中的 `GEO_ALLOWED_HOSTS` 必须包含实际端口。如果终端提示找不到命令，确认 `npm prefix -g` 对应的 Windows 目录在 PATH 中，并重新打开终端。查看帮助：`geosentinel --help`。

移动/删除仓库前，在该 `cli` 目录执行 `npm unlink --global @geosentinel/launcher`；更换克隆目录后重新注册。已有 `GEO_ENV_FILE` 环境变量仍优先，请勿让它指向旧 NTL 项目。快捷命令不会改写或接管原来的 `dsh web`。

在本目录运行 `pnpm start --no-open` 即可，默认端口为 **8510**。不要使用全局 `dsh web`，它不会自动加载本项目的独立配置与插件。

- 同一地址已有健康的 GeoSentinel：显示访问链接并正常退出，不创建第二个实例，也不修改已有运行配置。
- 端口被其他程序占用、健康检查失败或服务仍在启动：给出简短提示并退出，不自动杀进程或更换端口。健康检查识别的是服务类型，不证明它属于本目录。
- 明确需要重启本项目：运行 `.\scripts\restart.ps1 -Port 8510`。正在进行的研究请求会被中断，请避开运行中的任务。
- 更换端口：例如 `pnpm start --port 8511 --no-open`，同时将 `.env` 中 `GEO_ALLOWED_HOSTS` 的本地地址改成 `127.0.0.1:8511,localhost:8511`，保留所需公网域名。未显式配置该变量时，启动器按所选端口生成本地白名单。

启动前确保 `.env` 已配置；临时设置的 `GEO_ENV_FILE` 不会自动保存到新终端。GEE、GIS 与监测采集需要 Docker Desktop 正常运行，无需切换 Conda 环境。端口预检查不是进程锁，极短时间内并发启动仍可能发生竞争。

## 使用流程

公共地图、事件队列、数据来源和独立运行方式见 [全球事件监测说明](MONITOR.md)。项目资料与产出收纳在左栏“资料与产出”，右栏用于共享监测。

1. 接受邀请并登录，创建研究项目，在项目中建立一个或多个对话。
2. 按需上传当前项目资料。其他项目和其他用户都不可见。
3. 提出研究问题。简单问答直接回答；多步骤任务呈现研究方案，用户点击“确认方案并开始”。
4. 在同一对话查看成员进度。点击“停止”取消成员及正在运行的 Docker 作业。
5. 在“资料与产出”中下载 GeoTIFF、统计结果或报告。输出路径包含唯一作业目录，防止相互覆盖。

当前研究工具：`geo_list_files`、`geo_read_evidence`、`geo_write_report`、`geo_download_gee`、`geo_inspect_raster`、`geo_execute_python`。未知数据集/波段不能靠模型猜测；首次选择新数据源仍须提供或查明来源元数据。

### 共享数据（只读）

管理员可以在 `.env` 里挂一个**共享数据库**：公开用例数据、已下载的全球矢量边界与影像、各国 GDP、参考书与数据包（例如《缅甸地理》）。这类数据往往远大于上传限制（单文件 16 MiB、单项目 512 MiB），而且是全体账号共用，不属于某个项目。

```ini
GEO_SHARE_DIR=E:/DSH/share                      # 一个根目录，按 share/<子路径> 访问
GEO_SHARE_DIRS="缅甸地理=E:/DSH/缅甸地理;GDP=E:/DSH/gdp"   # 多个命名根，按 share/<名称>/<子路径> 访问
```

- **同一批数据的三种入口**：宿主侧 `read`/`glob`/`grep`/`read_document` 用 `share/<…>`；左栏「文件」里有只读分组「共享数据（只读）」；分析容器里只读挂载在同一相对路径 `/workspace/share/<…>`，注册的 GIS 工具与模型生成的 Python 都能直接读，**不需要复制进项目资料**。
- **只读是硬约束**：宿主围栏只允许把模型写入落在对话自己的 `outputs/`，容器里的共享目录也以 `readonly` 挂载，因此产品无法修改共享数据。
- **容器的文件系统仍是只读根**。ESRI 个人地理数据库（`.mdb`）由 GDAL 的 PGeo 驱动读取，需要 MDB Tools ODBC 驱动；镜像已内置该驱动，并在启动时把注册表复制进可写的 `/tmp` 再指向它（`ODBCSYSINI`）。实测在该约束下可直接打开《缅甸地理》交通库：`pic1_road` 368,131 条线要素、`pic2_railway`、`pic3_airport`、国界/省邦界/区县界。
- 配置在启动时读取一次，改完需要重启实例（正式实例：`.\scripts\restart.ps1 -Port 8511`）。目录不存在时启动日志会明确报出该根，不会静默生效。

### 外部地理服务（远程 MCP，无本地安装）

产品 profile 挂了两台**远程** MCP 服务（`transport: streamable-http`）：**不安装任何依赖、不启动任何本地进程**，只是从产品进程发出站 HTTPS——与已有的 `web_search`/`web_fetch` 同一类调用。它们的工具以 `mcp__<服务器>__<工具>` 注册，因此同样受发布允许列表、角色表和平台守卫管辖。

| 服务器 | 工具（已放行） | 用途 |
| --- | --- | --- |
| NASA CMR（`mcp__cmr__*`，公开、无密钥） | `get_collections`、`get_granules`、`get_variables`、`get_keywords`、`get_citations`、`get_services`、`get_tools` | 官方数据集/颗粒/变量/引用目录：满足“不猜数据集与波段、先查明来源元数据”的规矩 |
| 高德 LBS（`mcp__amap__*`，需 `AMAP_API_KEY`） | `maps_geo`、`maps_regeocode`、`maps_text_search`、`maps_around_search`、`maps_search_detail`、`maps_distance`、`maps_direction_driving`/`_walking`/`_transit_integrated` | 地址↔坐标、POI、距离与路径规划 |

- 密钥只放管理员 `.env`（`AMAP_API_KEY=…`），由 profile 里的 `!!js` 表达式在加载时注入，**不进发布快照**；未配置时该服务器的工具不会出现，harness 照常启动（`failOnStartupError: false`）。
- **结果是外部来源**：可以引用，但必须写明服务名、查询内容与检索时间，且**不得当作平台观测**；`maps_schema_*` 这类只用于唤起高德客户端的工具刻意不放行。
- 角色分配：数据助手拿 CMR 目录查询 + 逆地理，分析助手拿地址解析/距离/路径规划，事件助手拿地理编码与周边检索。管理员可在「普通模式能力管理」里**不发版**逐项关闭。
- 想再挂别的服务器：在 `dsh/profile/cordis.patch.yml` 的 insert 区加一行 `@deepseek-ai/dsh-mcp-client`（只用远程 `streamable-http`；本客户端不支持 SSE，也刻意不使用需要 `npx`/`pip` 本地安装的 stdio 服务器），再把工具名加进 `plugins/platform/catalog.mjs` 与角色表。
- **数据目录**：`node dsh/tools/catalog-share.mjs --write --image geosentinel-gis:0.1` 从真实文件生成 `CATALOG.md`（相对路径、类型、大小、矢量字段/要素数/坐标系、表格工作表与列名、影像像元与波段、`.mdb` 图层清单）与机器可读的 `catalog.json`，写进每个共享根。随发布冻结的技能 `shared-data-library` 会让智能体**先读这份目录**，不再逐个文件试探；`geo_list_files` 也会直接返回 `share.catalogs` 路径。

原生入口已接入 DSH 的 `ask_user_question`、`UserQuestionService` 和原生问题/方案审阅组件。主智能体可暂停等待用户选择、自由回答或取消；子智能体不能直接向用户提问。方案整理完成后显示原生审阅卡，确认前仍不可执行；“去聊天里说”恢复输入框，修改后需要重新审阅。平台通过带登录校验的 `/geo/api/chats/:id/questions` 传输请求与答案，不启用原版个人主机 Remote API。回答绑定用户、对话、唯一请求 ID；方案确认额外绑定团队与版本。普通问题的回答不能绕过方案审批。刷新可恢复当前进程中的待答问题；服务重启后旧请求 ID 失效，待确认方案可重新生成审阅请求，执行中普通提问不跨进程恢复。

## 存储与运行边界

子智能体原生持久化、进程展示与 AgentTeams 的职责对比见 [架构调研](SUBAGENT-ARCHITECTURE.md)。当前保留调度器，已接通原生子会话只读查看。

```text
.runtime/home/
  profiles/geosentinel/       独立 DSH 配置
  sessions/                  DSH 原生会话记录
  geosentinel/
    platform.sqlite          账号、项目、对话归属与审计
    users/<user>/projects/<project>/
      inputs/                项目上传资料，只读挂载
      chats/<chat>/outputs/  当前对话产物
      chats/<chat>/memory/   当前对话工作空间
    jobs/<job>/              作业状态、执行请求与日志
```

- 普通用户无终端、主机目录选择器、模型、插件、MCP 或依赖安装权限。
- 方案批准绑定具体版本；方案未批准时不能执行 Python 或 GEE 下载。运行后的新范围/新成员必须建立新的待确认方案。
- 分析容器采用非 root 用户、只读根文件系统、禁网、能力移除、CPU/内存/进程数/时间限制；不挂载 Docker socket 或 GEE 凭据。
- GEE 获取容器使用固定代码，可联网并只读挂载管理员 GEE 凭据，不执行模型生成代码。个人 GEE 账号绑定保留为后续接入项，当前未提供普通用户切换凭据的界面。
- 研究对话与 Docker 计算均默认每账号同时 2 个、全平台同时 10 个，超出名额自动排队；管理员可通过 `.env` 调低实际部署上限。等待队列最多每账号 20 项、全平台 200 项；每账号每分钟仍限提交 20 次问题。
- 排队记录持久化到 SQLite；重启恢复未派发的研究请求，已执行的任务及其 Docker 等待调用标记中断，不自动重放。执行前再次校验账号、项目和方案版本。详见 [资源与队列管理](RESOURCE-MANAGEMENT.md)。
- 每 Docker 作业默认 1 CPU、3 GiB 内存、30 分钟、256 MiB 输出监测限额。可用 `GEO_DOCKER_MEMORY_MIB=4096` 改为 4 GiB。排队等待不计入运行时限。10 个容器的内存上限合计为 30 GiB（4 GiB 配置则为 40 GiB），应给系统、DSH 和监测另留余量。输出限额是轮询终止策略，并非文件系统硬配额。
- 上传暂限 16 MiB/文件、512 MiB/项目，项目写入串行核算；账号最多 100 个项目、每项目 200 个对话。默认可用磁盘低于 1 GiB 时阻止新写入或计算。
- 删除为软删除。管理员可停机运行 `node scripts/maintenance.mjs` 预览超过 30 天的已删除工作区，再显式加 `--apply` 清理。不会清理正常/仅归档项目、全球事件监测和 DSH 会话记录；不是磁盘安全擦除。生产备份须同时覆盖整个独立 DSH home 和平台数据目录。
- 此版本是单服务进程的受控试点，同一平台数据目录设有进程占用保护，不支持多机共享 SQLite。Docker 不等于无条件安全沙箱；公网运行仍需硬磁盘配额、备份、漏洞更新与运维监控。

## 公网

本地默认仅用于 HTTP 测试。经过花生壳或反向代理 HTTPS 发布时，在 `.env` 设置公开域名 `GEO_ALLOWED_HOSTS` 和 `GEO_SECURE_COOKIES=true`；代理需保留 Host 并支持 SSE 长连接。不要同时暴露 DSH 的个人配置服务或旧后端管理端口。最终切换到 8502 前先确认该端口原服务的迁移安排，本轮使用 8510，不抢占旧服务。

## 验收与维护

```powershell
pnpm test
node scripts/check-env.mjs
```

真实验收脚本位于 `scripts/smoke-*.mjs`，会调用真实模型/GEE、创建测试账号与项目，并产生 API 费用。只在隔离验收 home 执行，不在生产数据上运行。结果写入忽略的 `.runtime/*-acceptance.json`；具体已执行项目见 [`IMPLEMENTATION.md`](IMPLEMENTATION.md)。

开发独立 fork 后运行 `node scripts/export-fork.mjs` 更新可重建补丁；此命令使用临时 Git index，不改写 fork 的正常暂存区。新增能力先迁移确定性工具/数据契约，再做针对性验收，不重新启用旧调度器。

DSH 原生界面 + Better Sidebar 是本目录唯一的工作台入口。访问 `/` 或 `/geo/` 会转到 `/geo/native/`。旧自建工作台及其页面脚本、样式、专用资源路由已移除；不再使用 `GEO_NATIVE_UI_PREVIEW` 开关，旧配置中的该变量不会恢复旧页面。账号、项目、对话、文件及全球事件监测 API 保留。

原生入口为 `/geo/native/`。它使用项目依赖 `dsh-dream-skin@8.30.1` 的 Midnight 深色配色，通过 DSH 原生 `theme.register` 接入。登录、对话、Better Sidebar、全球事件监测和资料面板共享主题变量；次要文字与控件边框增强对比度。不启用上游共享换肤 API、壁纸上传、任意主题包导入或主机配置入口。主题由项目统一管理，与用户账号和研究任务无关。升级皮肤包后须重新运行主题数据与对比度测试。
