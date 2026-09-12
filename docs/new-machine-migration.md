# 新机器安装与完整复盘指南（GeoSentinel on DSH）

本文面向“换一台电脑，从零把 GeoSentinel 装起来并且能完整复盘”的场景：安装什么、填什么、
哪一步会慢、怎么验收、哪些东西必须带、哪些东西绝对不要带。

- 当前推荐部署面是 `dsh/`（基于 DSH 的独立产品运行时）。仓库里的 `web/`、`Streamlit.py`、
  `graph_factory_v2.py` 等是旧 Python 平台，保留作能力迁移参考，**不是**本文的安装对象。
- 本文的命令以 Windows + PowerShell 为准。路径示例用 `E:\GeoSentinel\project`（本机 2026-09-12 起的位置），
  换成实际的 clone 目录即可；旧路径若保留目录联接（junction），两种写法都能用。
- 文档中标注 **[实测]** 的步骤是在本机真实执行过的（2026-09-10 首轮 `D:\GeoSentinel-DSH`；
  2026-09-12 复核 + 换盘到 `E:\GeoSentinel\project`）；标注 **[文档步骤]** 的是按仓库既有文档
  （`dsh/README.md`、`dsh/RELEASES.md`、`dsh/ADMIN-DEVELOPMENT.md`、`dsh/MONITOR.md`）整理的操作。
  一条命令的安装路径（下节）已在同一台机器上反复跑通，换盘后再次验证。

相关文档：

- 产品运行与边界：`dsh/README.md`
- 管理员开发模式：`dsh/ADMIN-DEVELOPMENT.md`
- 同步发布流程：`dsh/RELEASES.md`
- 资源与队列：`dsh/RESOURCE-MANAGEMENT.md`
- 全球事件监测：`dsh/MONITOR.md`
- 已实现能力与限制：`dsh/IMPLEMENTATION.md`
- 工具迁移进度：`dsh/TOOL-MIGRATION.md`
- 插件契约与插件清单：`dsh/plugins/README.md`
- 基线基准（31 例）与缺陷记录：`dsh/benchmark/README.md`、`dsh/benchmark/RESULTS-2026-09-12-single-version.md`

---

## 最快路径：一条命令（**[实测]**，2026-09-12 复核）

```powershell
git clone https://github.com/guihousun/GeoSentinel.git GeoSentinel
cd GeoSentinel
node dsh/scripts/bootstrap.mjs            # 或 pnpm --dir dsh bootstrap
```

**默认分支 `main` 就是当前产品线**（2026-09-12 起与 `feat/dsh-0.1.5-native` 同一提交），所以普通 `git clone`
拿到的就是最新代码。想显式钉住也可以写 `--branch feat/dsh-0.1.5-native`，两者内容相同。

克隆后先确认版本，再往下走：

```powershell
git log --oneline -1            # 记下提交号，写进新机器的验收记录
git branch -r                   # 若 main 落后（产品在别的分支上），用 --branch 指定那个分支
```

它按顺序做六件事，每一步都先检查、已完成的跳过，可以反复运行：

1. **前置检查**：Node ≥ 24、pnpm 版本与 `packageManager` 一致、git、**Docker 引擎可用**。
2. `pnpm install --frozen-lockfile`（依赖树只按 lockfile 重建，不用 npm 重新解析）。
3. `.env`：不存在时从 `.env.example` 复制，然后**停下**并列出必填项（`DEEPSEEK_API_KEY`、
   `GEE_DEFAULT_PROJECT_ID`、`GEO_GEE_CREDENTIALS`）——填完重跑即可。脚本不写任何密钥。
4. 构建 GIS 镜像 `geosentinel-gis:0.1`（已存在则跳过）并执行 `tools/release.mjs prepare`
   冻结并校验首个版本（隔离离线安装 + 语法 + 测试 + 镜像）。
5. 跑 `scripts/check-env.mjs` 作为闸门（版本、镜像、凭据存在性，不打印密钥）。
6. 账号库为空时创建首个管理员并**只显示一次**初始口令（也可先设 `GEO_BOOTSTRAP_PASSWORD`）。

然后按脚本打印的命令启动：`pnpm --dir dsh start --port 8511 --no-open`，浏览器打开
`http://127.0.0.1:8511/`。同一台机器上想只体检不改动：`node dsh/scripts/bootstrap.mjs --check`
（JSON、非零退出码表示有阻塞项，可直接用在 CI 里）。可选加 `--with-share-data` 顺带拉取共享数据。

**不需要外部调度器仓库**：0.1.5 线已把委派、方案审批与成员目录移到 DSH 原生平面（见 §6）。
装完后 `node dsh/scripts/verify.mjs` 可再核对编码、模块语法、相对链接与“私密文件不会被提交”。

---

## 0. 先看这张表：迁移什么、不迁移什么

| 类别 | 具体内容 | 要不要带 | 说明 |
| --- | --- | --- | --- |
| 源码 | 整个仓库工作区（`dsh/`、`packages/`、`docs/`、`monitoring/`、旧 Python 平台…） | **必须** | 见 §3“工作区有未提交改动”的提醒 |
| 配置 | `dsh/.env` | **必须** | 含密钥，走安全通道传；模板见 `dsh/.env.example` |
| 凭据 | Earth Engine 凭据文件（Windows 常见位置 `%USERPROFILE%\.config\earthengine\credentials`） | **必须** | 只读挂载给固定 GEE 容器；不要放进仓库 |
| 调度器 | **不需要外部仓库**：0.1.5 原生预设提供委派工具，平台自带方案审阅与角色绑定 | 无 | 见 §6 |
| 依赖 | `dsh/node_modules`、pnpm store | 不要带 | 用 `pnpm install --frozen-lockfile` 重建；离线机器需先暖 pnpm 缓存 |
| 构建产物 | Docker 镜像 `geosentinel-gis:*` | 不要带（可选导出） | 4.06–4.24 GB/镜像，新机器 `docker build` 重建；也可 `docker save/load` |
| 平台数据 | `GEO_DSH_HOME`（本机 `E:\GeoSentinel\runtime\upgrade-rc1-home`）：`geosentinel/`（账号·项目·工作区）、`sessions/`、`storages/`、`releases/`（已发布版本与状态） | 仅当要沿用账号/产物/已发布版本 | 只带源码则等于全新开始，账号与产物为空 |
| 监测数据 | `GEO_MONITOR_DIR`（本机 `E:\GeoSentinel\runtime\home\monitor`） | 可选 | 不带则重新采集一轮即有数据 |
| 共享数据 | `GEO_SHARE_DIR` / `GEO_SHARE_DIRS` 指向的目录（本机 `E:\DSH\缅甸地理`、`E:\DSH\全球基础数据`） | 可选 | 不属于账号数据；新机器用 `pnpm --dir dsh share:data --root <目录>` 按需重新下载（geoBoundaries 全球边界 + WorldPop 等），或用 `docker save/load` 那种"搬数据"方式复制 |
| 临时/QA | `.runtime` 下的 `isolation-*`、`recovery-*`、`migrated-tools-*`、`*-qa-home`、`release-acceptance`、`previews/`、旧 `releases/versions/*`、`.playwright-cli/` | 不要带 | 都是验收与预览残留，体积大且无复用价值 |

原则：**代码靠 Git 与 `pnpm install` 重建，运行数据靠目录复制，凭据靠人工重新放置。**

---

## 1. 目标机器要求

| 项 | 要求 | 本机现状 |
| --- | --- | --- |
| 操作系统 | Windows 10/11 x64（Linux/macOS 需自行验证路径与 Docker 差异） | Windows x64 |
| Node.js | 24.x（文档基线 Node 24） | v24.15.0 |
| pnpm | 10.33.0 | 10.33.0 |
| Docker | Docker Desktop，**Linux 容器** | 29.2.1 |
| PowerShell | Windows PowerShell 5.1 可用；管理员开发建议 PowerShell 7 | 视机器 |
| Git | 任意近期版本（需要 `git apply`、`git switch`） | 可用 |
| 内存 | 单容器默认 3 GiB；研究对话与 Docker 各默认全平台并发 10 → 上限约 30 GiB | 按机器调整 `.env` 并发 |
| 磁盘 | 建议 ≥ 60 GB 空闲：GIS 镜像 4.2 GB + 依赖 + 每个发布版本 + 用户产物 | 清理前曾逼近上限，见 §15 |
| 端口 | 8510（兼容入口）、8511（正式启动器）、8513（候选预览）、8514（本机管理员入口）；平台内开发进程用动态回环端口 | 与本机一致 |
| 网络 | DeepSeek API、GEE、公开事件源；国内网络通常需要代理 | `.env` 的 `GEO_GEE_PROXY` |

---

## 2. 三分钟理解结构

```
E:\GeoSentinel\project\      （换盘前为 D:\GeoSentinel-DSH\，旧路径保留为目录联接）
  dsh\                        产品运行时（本文的主角）
    plugins\platform\         邀请/账号/SQLite/工作区/API/监测接入/发布服务
    plugins\research\         GEE 下载、隔离 Docker 计算、报告与证据
    plugins\workbench\        中文界面适配：侧栏、项目对话、全球事件监测、空间数据、简报
    profile\                  cordis.patch.yml（产品 profile）与 product.json（默认模型/角色工具）
    skills\                   随发布冻结的技能库
    monitoring\               监测 worker、快照归一化与关注等级规则
    docker\                   GIS 容器（Dockerfile、worker.py、requirements.txt）
    release\                  冻结/校验/预览/切换/回滚
    tools\release.mjs         本地运维 CLI（与界面同一套发布路径）
    scripts\                  start/admin/check-env/import-env/monitor/maintenance/smoke-*
    benchmark\                31 例基准、执行器与结果记录
    tests\                    node:test 套件
  packages\ntl_toolkit\src\   GIS 核心算法（冻结进发布）
  monitoring\sources.py       上游事件源采集（冻结进发布）
  docs\                      （本文件所在目录）
  web\ tools\ app_*.py …      旧 Python 平台，保留参考
```

发布冻结范围（`dsh/release/manager.mjs`）：`dsh/plugins`、`dsh/profile`、`dsh/skills`、`dsh/monitoring`、
`dsh/docker`、`dsh/release`、`dsh/development`、`dsh/scripts`、`dsh/cli`、`dsh/tests`、
`packages/ntl_toolkit/src`，外加 `dsh/package.json`、`dsh/pnpm-lock.yaml`、`monitoring/sources.py`。
改这些文件＝需要走一次发布；改 `dsh/benchmark`、`docs/` 等不在冻结范围内的文件不需要发布。

---

## 3. 取得源码

**[实测]** 当前源码状态（2026-09-12）：分支 `feat/dsh-0.1.5-native`（跟踪
`origin/feat/dsh-0.1.5-native`），工作区干净，`git status -sb` 无未提交改动。因此新机器**只用 Git 就够**：

```powershell
git clone --branch feat/dsh-0.1.5-native https://github.com/guihousun/GeoSentinel.git GeoSentinel
cd GeoSentinel
git log --oneline -1        # 记下这个提交号，写进新机器的验收记录
```

如果旧机器上确实还有未提交的改动（改代码的过程中迁移），再走下面任一种，**不要只 clone 就以为拿到了当前状态**：

```powershell
# 方式 A（推荐）：先在旧机器提交一个工作快照，再推分支
cd E:\GeoSentinel\project
git switch -c migration/<日期>
git add -A
git commit -m "chore: snapshot before new-machine migration"
git push -u origin migration/<日期>

# 方式 B：把整个仓库连历史打包（不含 node_modules 与 .runtime）
git bundle create ..\geosentinel.bundle --all
# .env 与凭据单独走安全通道，不要放进任何压缩包
```

复制到新机器后，`dsh/.runtime` 这类运行时目录不要一起带（见 §0 表格最后一行）；
`dsh/node_modules` 会在 §5 重建。**注意 `dsh/.env`、`dsh/.runtime/**` 都属于本地配置与数据，
不要提交进 Git，也不要放进任何会被分享的压缩包。**

新机器上放置目录时保持**同一层级结构**（`dsh/` 与 `packages/` 必须相邻），否则冻结与相对路径会失配。
仓库放在哪个盘都行，不必和旧机器一致；换盘迁移已有部署见 §19。

---

## 4. 安装步骤 A：基础环境

```powershell
node --version      # 期望 v24.x
pnpm --version      # 期望 10.33.0；可用 corepack enable 或 npm i -g pnpm@10.33.0
docker version      # 期望 Client/Server 均可用
docker info         # Server 段需为 Linux 容器
$PSVersionTable.PSVersion   # 建议 7.x（管理员开发模式在 Windows 上会探测 pwsh）
```

**[文档步骤]** 若 pnpm 缺失：

```powershell
corepack enable
corepack prepare pnpm@10.33.0 --activate
```

---

## 5. 安装步骤 B：依赖

```powershell
cd D:\GeoSentinel-DSH\dsh
pnpm install --frozen-lockfile
```

- **不要**用 npm/yarn 重装依赖，也不要改 `pnpm-lock.yaml`：发布验证按锁文件离线安装。
- 离线机器：先在联网机器执行一次 `pnpm install`（同版本 pnpm）以暖缓存，再整目录迁移或用 pnpm store 导出。
- 发布冻结会做**离线安装**，缺缓存会在“验证并生成版本”阶段失败。

---

## 6. 安装步骤 C：不需要外部调度器仓库

**0.1.5 线不再使用 AgentTeams fork，也不需要任何仓库外的调度器目录。** 之前那套（clone
`NanmiCoder/dsh-agent-teams`、应用 `dsh/vendor/agentteams-geosentinel.patch`、`pnpm build`）
随 0.1.2 线一起退役，补丁与许可文件也已从仓库删除（历史仍在 git）。

替代关系（都在 DSH 原生平面上）：

| 旧做法 | 现在 |
| --- | --- |
| fork 提供 `agent_teams_*` 工具与成员/任务表 | 原生预设（`agent-presets`，`default: standard`）提供 `subagent` / `send_message` / `list_agents` / `interrupt_agent` |
| fork 暂存方案、等用户批准 | 原生 `plan-mode`（可选）或平台自己的 `ask_user_question` 审阅流程 |
| fork 按 `roleTools` 收窄成员工具 | 平台按子代理目录里的角色名绑定 `agent_sessions.role`，并对该子会话 `restrict` 到 `product.json` 的角色表 |
| `GEO_AGENT_TEAMS_DIR` 环境变量 | 不再需要（保留在旧 `.env` 里也无害） |

因此这一步**无需任何操作**，直接进入下一步。验收要点：主管能用 `subagent` 派出专家，
且专家在成员列表里带角色名（详见 §13）。

---

## 7. 安装步骤 D：配置 `dsh/.env`

```powershell
cd D:\GeoSentinel-DSH\dsh
Copy-Item .env.example .env
notepad .env
```

必填与常用项（完整列表见 `dsh/.env.example`）：

| 变量 | 用途 |
| --- | --- |
| `DEEPSEEK_API_KEY` | 研究智能体模型；监测中文整理也用 |
| `DEEPSEEK_BASE_URL` | 默认 `https://api.deepseek.com` |
| `GEE_DEFAULT_PROJECT_ID` | 已授权的 GEE 项目 |
| `GEO_GEE_CREDENTIALS` | Earth Engine 凭据文件绝对路径（只读挂载给获取容器） |
| `GEO_GEE_PROXY` | 仅 GEE 获取容器使用；Docker 访问主机代理写 `host.docker.internal` |
| `GEO_DSH_HOME` | 产品 home（账号、会话、发布版本、预览）；本机 `E:/GeoSentinel/runtime/upgrade-rc1-home`（换盘前是 `D:/GeoSentinel-DSH/dsh/.runtime/upgrade-rc1-home`） |
| `GEO_MONITOR_DIR` | 监测快照目录；本机 `E:/GeoSentinel/runtime/home/monitor` |
| `GEO_MONITOR_ENABLED` / `GEO_MONITOR_TRANSLATE` / `GEO_MONITOR_MODEL` | 监测采集与中文整理 |
| `GEO_ALLOWED_HOSTS` | 必须包含实际端口，例如 `127.0.0.1:8511,localhost:8511` |
| `GEO_SECURE_COOKIES` | 公网 HTTPS 时设 `true` |
| `GEO_RESEARCH_CONCURRENCY` / `GEO_USER_RESEARCH_CONCURRENCY` | 研究与每账号并发上限 |
| `GEO_DOCKER_CONCURRENCY` / `GEO_USER_DOCKER_CONCURRENCY` / `GEO_DOCKER_MEMORY_MIB` | 容器并发与单容器内存（3072/4096） |
| `GEO_MIN_FREE_DISK_MIB` | 磁盘余量保护（默认 1024） |
| `GEO_GIS_IMAGE` | 由发布实例自动注入：候选验证时构建的 `geosentinel-gis:release-<版本ID>` **摘要**。手工启动/开发时留空即用通用标签 `geosentinel-gis:0.1`；不要手工指向别的标签，否则跑批测的容器和自己以为的不是同一个（见 `dsh/plugins/README.md` 与基准记录 D11） |
| `AMAP_API_KEY` | 可选：高德开放平台密钥，供外部地理服务 MCP（地址↔坐标、POI、距离、路径规划）使用；不配置则这些工具不出现 |
| `GEO_SHARE_DIR` | 共享数据库的单一根目录；按 `share/<子路径>` 访问，只读 |
| `GEO_SHARE_DIRS` | 共享数据库的多个命名根（`名称=路径`，`;` 分隔）；按 `share/<名称>/<子路径>` 访问，只读 |
| `GEO_ADMIN_HOME` / `GEO_ADMIN_PWSH_PATH` | 可选：本机管理员入口 home 与 pwsh 路径 |

从旧项目 `.env` 导入（不会覆盖已有 `.env`，不修改源文件）：

```powershell
node scripts/import-env.mjs --source <旧.env路径> --home <已有DSH-home路径> `
  --credentials <Earth-Engine凭据路径> --port 8511 `
  --proxy http://host.docker.internal:7897 --monitor-dir D:\GeoSentinel-data\monitor
```

环境自检：

```powershell
node scripts/check-env.mjs      # 校验版本、镜像与凭据存在性；不打印密钥
```

> 只检查“存在性”，网络与 GEE 权限仍需真实下载验收（§13）。

---

## 8. 安装步骤 E：GIS 容器镜像

```powershell
cd D:\GeoSentinel-DSH\dsh
docker build --provenance=false -t geosentinel-gis:0.1 docker
node scripts/smoke-worker-timeout.mjs      # worker 超时行为验收
```

- 镜像约 4.2 GB；发布流程会另建 `geosentinel-gis:release-<版本ID>` 标签（同一 Dockerfile）。
- 可选：用 `docker save geosentinel-gis:0.1 -o gis.tar` / `docker load -i gis.tar` 代替重建。
- 模型生成的 Python 只在**禁网、非 root、只读根文件系统**的容器里执行；GEE 下载走固定入口并可联网。
  不要把 Docker 当成无条件沙箱，公网部署仍需硬配额、备份与漏洞更新。

---

## 9. 安装步骤 F：管理员与首次启动

```powershell
cd D:\GeoSentinel-DSH\dsh
$env:GEO_BOOTSTRAP_PASSWORD = '<强密码，至少 8 位，管理员建议 ≥12 位>'
node scripts/admin.mjs admin
Remove-Item Env:GEO_BOOTSTRAP_PASSWORD
pnpm start --port 8510 --no-open
```

- 管理员初始化**只允许在空账号库执行**，没有公开注册管理员的接口。
- 首次启动会生成、验证并启动初始发布快照（冻结、离线安装、测试、构建镜像），**明显偏慢**；后续启动直接读已发布版本。
- 浏览器访问 `http://127.0.0.1:8510/`（或启动器默认的 8511）。
- 启动器行为：探测到同址已有健康实例会提示并退出，不会起第二个实例；端口被别的程序占用时给提示后退出，
  不自动杀进程；要重启本项目用 `.\scripts\restart.ps1 -Port 8510`（会中断进行中的研究）。

注册常用启动命令（每台机器一次，不需要管理员权限）：

```powershell
cd D:\GeoSentinel-DSH\dsh\cli
npm link --ignore-scripts --package-lock=false
geosentinel                 # 等价 geosentinel web，默认 8511，读取注册仓库的 dsh/.env
geosentinel --port 8512 --no-open
geosentinel admin           # 本机原生管理员入口，8514
```

移动/删除仓库前先 `npm unlink --global @geosentinel/launcher`，换目录后重新 `npm link`。
不要用全局 `dsh web` 启动本项目：它不加载本产品的 profile 与插件。

---

## 10. 使用与邀请

1. 管理员登录后生成邀请码（界面按钮：一次性、一天有效；`PlatformStore.persistentInvite(admin)`
   可生成固定可复用邀请码，`revokeInvite(admin, code)` 撤销——数据库只存哈希）。
2. 新用户接受邀请 → 创建研究项目 → 在项目中建对话 → 上传项目资料（≤16 MiB/文件、512 MiB/项目）。
3. 提问：简单问答直接回答；多步骤研究先出现方案，必须点“确认方案并开始”才会执行。
4. 结果在侧栏“资料与产出 / 分析结果”里下载；每个产物落在唯一作业目录。
5. 邀请码泄露风险：固定邀请码任何持有者都能注册普通账号，及时撤销。

---

## 11. 管理员开发模式与同步发布

### 11.1 两条管理员入口

| | 平台内开发模式（推荐） | 本机独立入口 |
| --- | --- | --- |
| 启动 | 正式平台登录管理员 → “创造任务/开发模式” | `geosentinel admin`（8514） |
| profile / home | `geosentinel-development` / `GEO_DSH_HOME/development/<管理员ID>` | `geosentinel-admin` / `GEO_ADMIN_HOME`（默认 `dsh/.runtime/admin-home`） |
| 授权 | 30 分钟密码再确认，绑定当前平台登录 | 仅回环端口绑定 |

两条入口都加载**源码**，都不改写已发布的普通用户快照；发布必须回到正式平台实例完成。

### 11.2 发布流程（界面）

产品配置页 → 保存设置 → 查看待发布差异 → “验证并生成版本” → “预览用户版”（8513）→ “确认同步发布”。

- 验证＝冻结源码/插件产物/依赖锁 → 离线安装 → JS 语法 → `node --test tests/*.test.mjs` → 构建专属镜像。
- 预览使用独立普通账号、Cookie、工作区，**预览的监测采集默认关闭**（`GEO_MONITOR_ENABLED=false`）。
- 登录链接 **10 分钟内有效**；不要分享。
- 发布请求存在期间不接受新的研究任务；已有研究完成后整实例切换（有短暂不可用），失败会尝试回退旧版本。

### 11.3 发布流程（本机 CLI，**[实测]**）

界面与 CLI 走同一套 `ReleaseManager`：

```powershell
cd D:\GeoSentinel-DSH
$env:GEO_DSH_HOME = 'D:\GeoSentinel-DSH\dsh\.runtime\upgrade-rc1-home'   # 或在当前终端加载 .env

node dsh/tools/release.mjs status
node dsh/tools/release.mjs prepare                        # 冻结+校验，打印 candidate id
node dsh/tools/release.mjs preview --id <id> --hold 900   # hold 单位是【秒】；900 = 15 分钟
node dsh/tools/release.mjs publish --id <id> --by "<操作者>"
node dsh/tools/release.mjs rollback --id <id> --by "<操作者>"
node dsh/tools/release.mjs cancel
```

实测注意事项：

- 本轮实际执行过 `status`、`prepare`、`preview`、`publish`；`rollback` 与 `cancel` 属同一 CLI 的其余子命令，
  **本轮没有真的回滚过**，第一次使用请先在候选版本上演练。
- `preview` 输出 `preview-login?token=...` 与 `holdingSeconds`；**`--hold` 是秒**，写 30 就是 30 秒后关预览（踩过）。
- `publish` 只是“登记请求”，**平台空闲时**才切换并重启正式实例；管理员开发会话在忙时不会立刻切换。
- 该请求存在期间**不能开新预览**，实测提示：`release: 已有版本等待切换，不能启动新预览`。
- 验证之后又改了源码，`publish` 会被拒（`开发源码在验证后发生变化`），需重新 `prepare`。
- 后台运行 `preview` 时注意进程归属：`release.mjs preview` 会一直持有预览**直到 `--hold` 到期**，
  只杀掉启动它的包装进程（例如终端作业）并不等于关掉预览，8513 仍被占用；要立刻释放就结束
  `release.mjs` 进程本身，见 §14.2。
- 每次发布都会新建 `geosentinel-gis:release-<id>` 标签；镜像与旧版本不自动清理，见 §15。

### 11.4 普通用户能力收窄（无需发布）

管理员的“设置 → 普通模式能力管理”读写部署 home 下的 `capability-policy.json`，可在**已发布**的能力目录里
把工具/技能切到“禁用”。普通实例每 5 秒读取该文件：工具在守卫处被拒（提示“该工具已被管理员在能力管理中关闭”），
技能重新注册后从目录消失。该层**只能关，不能开**；新增能力仍需源码 + 发布。

---

## 12. 数据与状态目录（备份/迁移用）

本机 `dsh/.env` 中的实际取值（**[实测]**，2026-09-12 换盘后，非密钥项）：

```
GEO_DSH_HOME   = E:/GeoSentinel/runtime/upgrade-rc1-home
GEO_MONITOR_DIR= E:/GeoSentinel/runtime/home/monitor
GEO_ALLOWED_HOSTS = 127.0.0.1:8511,localhost:8511
GEO_SHARE_DIRS = 缅甸地理=E:/DSH/缅甸地理;全球基础数据=E:/DSH/全球基础数据
```

```
GEO_DSH_HOME/
  geosentinel/
    platform.sqlite            账号、项目、对话归属、审计、队列（+ -wal / -shm）
    users/<user>/projects/<project>/
      inputs/                  项目上传资料（只读挂载给容器）
      chats/<chat>/outputs/   对话产物
      chats/<chat>/memory/    对话工作空间
      chats/<chat>/.dsh-uploads/<chatId>/   本对话上传件
    jobs/<job>/                作业状态、请求与日志
  sessions/  storages/         DSH 原生会话与投影缓存
  releases/state.json          发布状态（活动/候选/待切换/历史）
  releases/versions/<id>/app   正式快照（独立依赖，不链接开发源码）
  previews/<id8>-<token6>/     候选预览的隔离 home
  development/<adminId>/       平台内开发进程的独立 home
  profiles/                    DSH 侧配置
```

**完整复盘（沿用账号与已发布版本）**：停机后复制 `GEO_DSH_HOME` 与 `GEO_MONITOR_DIR`；

**全新开始（只看代码与能力）**：只带源码 + `.env` + 凭据 + 共享数据库，其余由首次启动生成。

复制 SQLite 的注意事项：最好停机复制；若必须热复制，把 `platform.sqlite`、`platform.sqlite-wal`、
`platform.sqlite-shm` 一起带走，或先让 SQLite 做一次 checkpoint（正常关停即可）。复制后按新机器的实际路径
改写 `.env` 里的 `GEO_DSH_HOME`、`GEO_MONITOR_DIR`、`GEO_GEE_CREDENTIALS`，以及共享数据的
`GEO_SHARE_DIR` / `GEO_SHARE_DIRS`（数据本身也要一起复制，或在新机器重新下载）。

### 外观与背景图（不随发布快照走）

当前界面的背景图是仓库根目录的 `BG1.jpg`（1090×595 蓝色点阵世界地图）。它**不是**通过文件路径引用的：

- 导入后，图片以 base64 data URL 存进**管理员开发 home** 的 `dream-skin.json`
  （`<GEO_DSH_HOME>/development/<管理员ID>/dream-skin.json`，约 200 KiB；导入时会重新编码，
  所以内嵌字节数与源文件不同）。
- `plugins/workbench/skin-theme.mjs` 的优先级是：**最近改动的开发 home 外观** → 已发布产品的
  `product.json.appearance` → 内置主题。也就是说当前这张背景属于**运行实例设置**，不随发布快照迁移。
- 新机器复现：把 `BG1.jpg` 通过「管理员设置 → 外观/皮肤」重新导入一次；若希望普通版用户也一直有它，
  需要把外观导入到草稿并**发布**（`product.json` 的 `appearance`）。只复制源码不会有这张背景图。
- 仓库根目录曾有一张候选图 `BG2.jpg`（2500×1731，仪表盘风格），从未启用，已删除且未入库；不要按旧文档去找它。

---

## 13. 验收

```powershell
cd E:\GeoSentinel\project\dsh
pnpm test                    # node --test tests/*.test.mjs，本机 154 项通过 [实测 2026-09-12]
node scripts/check-env.mjs   # 版本/镜像/凭据存在性
node scripts/bootstrap.mjs --check   # 同一件事的 JSON 闸门：ok=true 才算装好，可直接进 CI
```

真实 smoke（`scripts/smoke-*.mjs`）会调用真实模型与 GEE、创建测试账号与项目并产生 API 费用，
**只在隔离验收 home 执行，不要在正式数据上跑**：`smoke-gee`、`smoke-docker`、`smoke-research`、
`smoke-multiuser`、`smoke-recovery`、`smoke-migrated-tools`、`smoke-upgrade`、`smoke-worker-timeout` 等，
结果写入被忽略的 `.runtime/*-acceptance.json`。

桌面验收（本机用 `@playwright/cli`，需先 `npm i -g @playwright/cli`，本机 0.1.7，**[实测]**）：

```powershell
# 1) 开候选预览（记下 token）
node dsh/tools/release.mjs preview --id <id> --hold 900
# 2) 浏览器
playwright-cli -s=verify open "http://127.0.0.1:8513/geo/api/preview-login?token=<token>"
playwright-cli -s=verify resize 1366 768
playwright-cli -s=verify snapshot              # 取 ref 后再点，中文名直接 click 匹配不到
playwright-cli -s=verify eval "() => ({...})"  # 读 DOM/计算结果作为证据
playwright-cli -s=verify screenshot
playwright-cli -s=verify resize 1440 900
playwright-cli -s=verify console error         # 期望 0 条
```

> 预览实例的监测采集是关闭的。要验证监测界面，把正式实例的真实快照
> （`GEO_MONITOR_DIR/snapshot.json`）复制进预览 home 的 `monitor/` 目录即可；这属于**标注清楚的验证夹具**，
> 验证完删除，正式数据不动。此时界面会显示“数据待更新”，因为预览没有心跳。

### 基准（31 例）

```powershell
cd E:\GeoSentinel\project
node dsh\benchmark\run.mjs --base http://127.0.0.1:8513 --preview-token <token> `
  --concurrency 4 --project "基准测试" --out dsh\.runtime\benchmark-report.json

# 正式实例上的普通账号（需要邀请码或已有账号）
node dsh\benchmark\run.mjs --base http://127.0.0.1:8511 --username <u> --password <p> --concurrency 2
```

用例集与逐例结果见 `dsh/benchmark/README.md` 与 `dsh/benchmark/RESULTS-2026-09-12-single-version.md`
（本机 2026-09-12 全量 31 例：28/31 通过，基础档 10/10；三个未过项分别是预算与模型选路，非平台缺陷）。

- 选项：`--only A09,C04`、`--timeout <分钟>`、`--no-approve`、`--project`。
- 并发受平台准入控制：普通账号默认同时最多 2 个研究对话、2 个 Docker 作业，第 3 例会排队（平台策略，不是执行器缺陷）。
- 评分是规则式的（工具名、产物扩展名、关键词正则、声明式“禁止词”）；规则分不等于科学质量评阅。
- 用例可声明两类“用户会怎么点”：`answers: [...]`（按声明作答）与 `clarify: "first"`（模型中途要求确认口径时选第一个选项）。
  没有声明时，非方案类提问如实记为 `unanswered-question`，不替用户编造方法答案。
- 每例完成即落盘，中断不丢已完成结果；同一用例重复运行**不构成独立科学验证**。

---

## 14. 常见故障与处置（含本机真实踩坑）

1. **端口被占（8510/8511/8513/8514）**：先 `Get-NetTCPConnection -LocalPort 8511 | Select-Object OwningProcess`，
   确认是本项目实例再处理；重启用 `.\scripts\restart.ps1 -Port <端口>`，不要盲杀未知进程。
2. **残留进程占住 8513** [实测]：后台跑的 `release.mjs preview` 或 `benchmark/run.mjs` 在包装进程被杀后
   仍可能存活（`preview` 的存活时间由 `--hold` 决定）。查：

   ```powershell
   Get-CimInstance Win32_Process -Filter "Name='node.exe'" |
     Where-Object { $_.CommandLine -match 'release\.mjs|benchmark' } |
     Select-Object ProcessId, CommandLine
   taskkill /PID <pid> /T /F
   ```

   本机实测清理了 3 个残留（两个旧 `preview --hold`、一个旧 `benchmark/run.mjs`），清理后 8513 才释放。
3. **`--hold` 单位理解错误**：是秒。写成 30 会 30 秒后关闭预览，导致后续浏览器/基准操作对着死端口超时。
4. **容器启动挂载 `Input/output error`（Windows 9p 挂载）**：预览 home 已改为短路径 `previews/<id8>-<token6>`；
   产品对容器启动失败和错误信封各做一次带退避的重试。仍失败就重启 Docker Desktop，并确认磁盘与 WSL 正常。
5. **GEE 相关**：`check-env.mjs` 只查存在性；真实下载失败先看凭据路径、项目 ID、`GEO_GEE_PROXY`。
   容器里的 `127.0.0.1` 是容器自身，访问主机代理必须用 `host.docker.internal`。
6. **`AMAP_API_KEY` 缺失**：中国行政区改用六位 adcode 下载即可，不需要地名解析。
7. **角色成员派发被拦**：曾出现 `unknown member model "<模型名>"`，原因是该模型未登记进产品 profile 的模型目录；
   新增模型要同时更新 profile 与产品配置。
8. **离线安装失败**：本机无 pnpm 缓存；先在联网环境 `pnpm install` 暖缓存。
9. **测试抖动** [实测]：`tests/geo-preview.test.mjs` 在同时跑 Docker 基准、机器高负载时失败过一次，立即重跑通过。
   报告时按抖动记录，不要当成已修复的缺陷。
10. **发布被拒**：`开发源码在验证后发生变化` → 重新 `prepare`；`已有版本等待切换，不能启动新预览` → 先让待切换版本完成或被 `cancel`。
11. **磁盘不足**：`GEO_MIN_FREE_DISK_MIB` 默认 1 GiB，低于该值阻止新写入与计算。先清旧发布版本与旧镜像（§15）。
12. **`$home` 是 PowerShell 保留变量**：写脚本不要用 `$home` 作变量名（本机踩过）。

---

## 15. 维护与清理

```powershell
cd D:\GeoSentinel-DSH\dsh
node scripts/maintenance.mjs                 # 干跑：预览超过 30 天的已删除工作区
node scripts/maintenance.mjs --apply         # 显式执行；不清理正常/归档项目、监测与 DSH 会话
node scripts/monitor.mjs --stop              # 只停 PID 与本项目采集脚本匹配的进程
node scripts/monitor.mjs                     # 前台启动监测
node scripts/monitor.mjs --translate-cache --once   # 仅重跑中文整理缓存
```

- **发布版本**：`GEO_RELEASE_DIR`（默认 `GEO_DSH_HOME/releases`）下的 `versions/*` 不会自动清理。
  保留“活动 + 候选 + 少量可回滚历史”，确认不再需要回滚后再删。
- **Docker 镜像**：每次发布新增 `geosentinel-gis:release-<id>`。本机一度累积 58 个 release 标签
  （共 3 个底层镜像 ID：4.06/4.24 GB）。清理示例：

  ```powershell
  docker images --format '{{.Repository}}:{{.Tag}}' | Select-String 'geosentinel-gis:release-' |
    Where-Object { $_ -notmatch '<要保留的版本ID>' } | ForEach-Object { docker rmi $_ }
  docker image prune -f
  ```

- **备份**：`GEO_DSH_HOME` 全量 + `GEO_MONITOR_DIR` + `dsh/.env` + Earth Engine 凭据（分开保管）。
  删除是软删除，`maintenance.mjs --apply` 不是安全擦除。
- **公网**：`GEO_ALLOWED_HOSTS` 填公开域名、`GEO_SECURE_COOKIES=true`；代理需保留 Host 并支持 SSE 长连接；
  不要把内部开发进程、预览端口或原生 DSH 配置服务直接映射到公网。

---

## 16. 完整复盘清单（按顺序勾）

- [ ] A 基础环境：Node 24.x、pnpm 10.33.0、Docker（Linux 容器）可用
- [ ] B `dsh` 目录 `pnpm install --frozen-lockfile` 成功
- [ ] C 无需外部调度器仓库；确认主管能用 `subagent` 派出专家并在成员列表看到角色名
- [ ] D `dsh/.env` 填写完成，`node scripts/check-env.mjs` 通过
- [ ] E `docker build -t geosentinel-gis:0.1 docker` 成功，`smoke-worker-timeout.mjs` 通过
- [ ] F 管理员初始化成功，首次启动生成初始快照并能打开工作台
- [ ] G `geosentinel` 启动器可用（默认 8511）
- [ ] H 用普通账号走通：邀请 → 项目 → 对话 → 方案确认 → 产物下载 [文档步骤]
- [ ] I `pnpm test` 全绿（本机 154 项）；`node dsh/scripts/bootstrap.mjs --check` 返回 `ok: true`
- [ ] J 至少跑一次 `node scripts/smoke-gee.mjs` 验证真实 GEE
- [ ] K 走一次发布：`prepare → preview(8513) → publish`，浏览器按 1366×768 / 1440×900 验收
- [ ] L 跑通基准若干例（`dsh/benchmark/run.mjs`），确认工具、产物、关键词与“禁止词”评分工作
- [ ] M 监测：`node scripts/monitor.mjs` 采集一轮，`/geo/api/monitor/events` 返回带 `level` 的快照
- [ ] N 记录本次安装的实际版本号、镜像 ID、发布 ID，便于下次对比
- [ ] O 外观：把 `BG1.jpg` 重新导入「管理员设置 → 外观」，确认背景图生效（§12 外观与背景图）
- [ ] P 共享数据：`GEO_SHARE_DIR` / `GEO_SHARE_DIRS` 指向存在的目录，左栏「文件」出现只读分组「共享数据（只读）」，并且容器内 `ogrinfo /workspace/share/<…>` 能打开数据
- [ ] Q 生成数据目录：`node dsh/tools/catalog-share.mjs --write --image geosentinel-gis:0.1` 在每个共享根写出 `CATALOG.md` + `catalog.json`（智能体读这一份就知道有什么数据、字段与解析方式）
- [ ] R 外部地理服务：`.env` 配置 `AMAP_API_KEY` 后重启实例，确认智能体工具表里出现 `mcp__cmr__*`（无需密钥）与 `mcp__amap__*`；未配置密钥时 CMR 仍应可用
- [ ] S 如果这次是**换盘/搬迁已有部署**：按 §19 核对联接、`.env` 规范路径、`restart.ps1 -CheckOnly` 通过与平台数据计数不变

---

## 17. 建议的复盘顺序（半天 ~ 一天）

按“先跑起来 → 再看见能力 → 最后动源码”的顺序，避免一上来就卡在 Docker 或凭据上：

| 阶段 | 做什么 | 时间（本机量级） | 完成标志 |
| --- | --- | --- | --- |
| 1 | §4–§5 环境与依赖 | 20–40 分钟 | `node --version`、`pnpm install --frozen-lockfile` 通过 |
| 2 | §6–§8 调度器说明、`.env`、GIS 镜像 | 20–70 分钟（镜像构建最慢） | `check-env.mjs` 通过、`smoke-worker-timeout.mjs` 通过 |
| 3 | §9 管理员 + 首次启动 | 15–40 分钟（首次生成快照慢） | 浏览器能登录、能建项目对话 |
| 4 | §10 + §13 普通账号走通一轮研究 | 30–60 分钟 | 出现方案 → 确认 → 产物可下载；`pnpm test` 全绿 |
| 5 | §13 桌面验收 + 基准若干例 | 1–2 小时 | 截图/`eval` 证据留档；基准报告落盘 |
| 6 | §11 改一处源码并走一次 `prepare → preview → publish` | 30–60 分钟 | 版本切换成功，浏览器提示刷新 |
| 7 | §12/§15 数据迁移与清理演练 | 按数据量 | 停机复制、回滚点、清理后磁盘余量 |

复盘时请遵守两条硬规则：**不要把验收脚本跑在正式用户数据上**；**不要把重复运行说成独立科学验证**。

---

## 18. 本机现状快照（2026-09-12，会随时间变化）

- 位置：仓库 `E:\GeoSentinel\project`，运行时数据 `E:\GeoSentinel\runtime`；
  `D:\GeoSentinel-DSH` 与 `…\dsh\.runtime` 都是目录联接（见 §19）。
- 源码：分支 `feat/dsh-0.1.5-native`（跟踪 `origin/feat/dsh-0.1.5-native`），工作区干净，HEAD `d16f13f`。
- 正式实例：`http://127.0.0.1:8511`，健康检查 `{"status":"ok","service":"geosentinel-dsh","release":"9f829af5881d642f","preview":false}`；
  `releases/state.json` 里 `active=9f829af5881d642f`、`pending` 空、无 `lastError`。
- Docker：`geosentinel-gis:0.1`（`79def542ccfb`）与候选镜像 `geosentinel-gis:release-9f829af5881d642f`
  （`sha256:28d2a7f3…`，正式实例通过 `GEO_GIS_IMAGE` 使用它）；Docker 磁盘在 E: 盘。
- 平台数据：11 账号 / 7 项目 / 18 对话 / 1291 条审计；`geosentinel` 503 个文件、sessions 31、storages 31。
- 测试：`pnpm test` 154 项通过；`node dsh/scripts/bootstrap.mjs --check` → `ok: true`。
- 基准：31 例，全量结果与缺陷记录见 `dsh/benchmark/RESULTS-2026-09-12-single-version.md`
  （最终候选 28/31，基础档 10/10；D1–D13 十三项系统性缺陷均已修，其中 D13 是换盘暴露的运维缺陷）。
- 监测快照：200 条线索量级（重点/中/低关注随采集变化），渠道 GDACS、NASA EONET、EMSC、国际新闻可用；
  ACLED 未启用，GDELT 常受限流。
- 已知未完成项：Earthdata/VNP46 官方 HDF5 获取、专题地图、长技能继续精简、发布 smoke 脚本化；
  基准里 A04 的预算调整（45→60 分钟）与 D13 尚未进入已发布快照。

这些数字只用于对照，不作为承诺；以 `node dsh/tools/release.mjs status`、`docker images` 与测试输出为准。

---

## 19. 换盘 / 搬迁已有部署（**[实测]** 2026-09-12）

把部署从一块盘搬到另一块盘（本机从 `D:` 到 `E:`）时，**不需要改配置**也能先跑起来：

1. 停机：确认没有排队/运行中的任务（`runtime_jobs` 里没有 `queued/running/cancelling`），
   然后停正式实例与其控制器（`restart.ps1` 的停止部分，或手动结束 supervisor 与实例进程）。
2. 搬运：项目目录与运行时目录整体复制到新盘（`robocopy /E /COPY:DAT /R:2 /W:2 /MT:16` 支持长路径；
   1.2M 个小文件的项目树要几分钟到十几分钟）。
3. 留联接：在旧路径建**目录联接**指向新位置，旧路径继续可用：

   ```powershell
   cmd /c mklink /J "D:\GeoSentinel-DSH" "E:\GeoSentinel\project"
   cmd /c mklink /J "E:\GeoSentinel\project\dsh\.runtime" "E:\GeoSentinel\runtime"
   ```

4. 规范配置：把 `.env` 的 `GEO_DSH_HOME`、`GEO_MONITOR_DIR` 改成新盘的**真实路径**
   （备份原 `.env`；旧盘路径仍可用，但配置不该依赖联接）。
5. 启动与核对：`pwsh -NoProfile -File "<新路径>\dsh\scripts\restart.ps1" -Port 8511`，
   然后逐项对：健康检查里的 `release` 与迁移前一致、`state.json` 的 `active/pending` 未变、
   `platform.sqlite` 计数（账号/项目/对话）与迁移前一致、`docker image inspect <候选镜像摘要>` 仍在、
   共享数据根可解析、容器内 `python -c "import rasterio"` 可用。

两个**已经踩过**的坑：

- **重启入口的路径别名**：`restart.ps1` 曾经用字符串包含比较进程归属，换盘后会把旧拼写启动的正式实例
  判成"别的服务"而拒绝停止。现已按解析后的真实路径判断（`dsh/scripts/path-alias.ps1`，
  单测 `dsh/tests/restart-alias.test.mjs`）；如果你的部署版本较旧，先更新到含该修复的提交。
- **磁盘余量**：平台在可用空间低于 `GEO_MIN_FREE_DISK_MIB`（默认 1 GiB）时拒绝新写入与计算
  （上传会返回 507），迁移前先清掉旧 `releases/versions/*`（未被 `state.json` 引用的）
  与旧 `previews/*`，别等验收步骤上再发现。
