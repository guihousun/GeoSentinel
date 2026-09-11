# 管理员开发与同步发布

## 两类设置

- 原生管理员 DSH 的模型、权限和插件设置属于管理员开发实例；完全权限和创造模式不自动交给普通用户。
- 原生“设置 → 通用设置 → GeoSentinel 产品配置 → 配置与同步发布”打开受管理员账号保护的产品发布面板。正式平台侧栏也有“产品配置与发布”。平台内开发入口支持远程管理员，详见 [管理员开发模式](ADMIN-DEVELOPMENT.md)。
- 产品面板可以调整普通用户默认模型、全球事件监测开关、各研究角色可调用的产品工具。可以查看三个产品插件、AgentTeams、Better Sidebar 和 Dream Skin 的版本。
- UI、工具实现、提示词或插件版本在 GeoSentinel 源码/产品 profile 中修改。开发修改不会改写已经运行的正式发布快照；必须通过发布流程。

## 使用流程

1. 从平台的“开发模式”进入原生 DSH，或直接编辑受 Git 管理的源码；独立本机命令 geosentinel admin 继续兼容。开发与发布写操作需要再次确认管理员密码。
2. 在产品配置页保存设置，检查待发布文件差异。
3. 点击“验证并生成版本”。系统冻结源码、插件产物和依赖锁文件，安装隔离依赖，检查 JavaScript 语法，运行测试并构建专属 Docker 镜像。TypeScript 等开发源需要先构建成支持的运行产物；不会自动执行任意安装脚本。
4. 点击“预览用户版”。预览在本机 8513 使用独立普通账号、独立 Cookie 和工作区，没有管理员按钮或正式用户历史。登录链接十分钟内有效，不应分享。预览监测采集默认关闭，避免重复采集。
5. 检查预览后，点击“确认同步发布”。候选版本必须验证通过且已打开预览；源码在验证后再改动会要求重新生成版本。
6. 发布请求存在期间不接收新的研究任务。已有研究和等待队列继续完成，之后切换整个服务版本。不是对正在执行的 Agent 热换模型或工具。
7. 新版健康检查通过才记录正式版本；失败则尝试恢复旧代码版本。发布记录可手动回滚。已有浏览器收到版本变化后提示刷新，避免直接丢弃未发送的草稿。

保存产品配置只改开发配置，不等于发布。验证通过也不等于已经上线。

管理员可在统一工作台的原生设置中调整模型与外观，再用“应用开发设置到草稿”导入受支持字段；当前支持范围和限制见 [管理员开发模式](ADMIN-DEVELOPMENT.md)。

## 升级 DSH 内核依赖（跨内核版本的依赖切换）

- **何时需要**：产品 pin 与已安装依赖树不是同一内核。`node scripts/check-env.mjs` 会给出两条失败：`DSH pin` 与 `Resolved DSH dependency graph`（实测主树 pin `0.1.5-rc.1`、安装树仍是 `0.1.2-rc.1`，217 个包全部不匹配）。此时不要直接 `prepare`：离线安装会以版本不一致或 `ERR_PNPM_OUTDATED_LOCKFILE` 失败，勉强产出的外壳也会以 `Native UI dependency mismatch` 拒绝打包客户端。
- **影响面（先讲清再动手）**：`pnpm install` 替换的是 `dsh/node_modules`。正在运行的正式实例**不受影响**（它读冻结快照里的独立安装），但同一源码树里的**管理员开发实例与管理员当前会话的 DSH 运行时会被替换**。因此升级应在没有进行中的开发会话时执行，或明确接受该会话重启。
- **步骤**（在 `dsh/` 目录）：
  1. 记录现状：`node scripts/check-env.mjs`、`git status -sb`、`node tools/release.mjs status`。
  2. 用升级工具完成切换：先 `node scripts/upgrade-deps.mjs` 看它要做什么（干跑，不改动任何东西），再 `node scripts/upgrade-deps.mjs --apply`。它会先把 `node_modules` 改名为 `node_modules.bak-<时间戳>`（同盘改名，秒级），再装 pin 版本，然后跑 `check-env.mjs` **并**核对安装后的 `@deepseek-ai/dsh-web-app` 版本等于 pin；任一步失败（安装非零退出、校验不过、版本不一致）都会把新树改名到 `node_modules.failed-<时间戳>` 并把备份原样放回 `node_modules`，退出码非零。成功后备份保留，确认无误再手动删除。
     - 需要临时改用手工步骤时：`pnpm install --offline`（能联网时去掉 `--offline`）。pin 与锁文件一致时加 `--frozen-lockfile`；**pin 有变动时先在副本用 `pnpm install --lockfile-only` 正规重生成锁文件再回灌，不要手改 `pnpm-lock.yaml`**。
  3. 复核：`node scripts/check-env.mjs` 两条失败消失。
  4. `pnpm test`，然后按上面「使用流程」走 `prepare → preview → publish`。
- **回滚**：把 `node_modules.bak-<日期>` 改回 `node_modules`（或 `git checkout` 回旧的 pin 与锁文件后重装）。依赖回滚不等于版本回滚：已经发布的快照用 `node tools/release.mjs rollback --id <旧版本>` 处理，用户数据与产物都不随代码回滚。
- **不要**：把新内核的包手工拷进旧树的 `node_modules`，或长期混用两代客户端包——外壳会拒绝打包，而运行中的实例会以难以定位的槽位/服务缺失告终。

## 文件与进程

- 开发源：当前 Git 工作目录。发布范围由 release/manager.mjs 显式列出，包括产品插件、profile、GIS 核心、监测代码、Docker 构建文件、校验脚本及 AgentTeams 已构建产物。
- 正式快照：默认位于 GEO_DSH_HOME/releases/versions/版本/app。每个版本有源码校验和、独立依赖和固定镜像摘要，运行时不链接开发源码。
- 发布状态：GEO_DSH_HOME/releases/state.json。可用 GEO_RELEASE_DIR 指定独立位置。
- 账号及用户工作区：保留显式 GEO_DATA_DIR；未设置时为原来的 GEO_DSH_HOME/geosentinel，不随代码回滚删除或替换。DSH 原生历史保留在同一个产品 home。
- 预览工作区：发布目录下的 preview，与正式账号、文件及 Cookie 分离。预览关闭时取消其自身作业。
- 管理员 DSH：独立 GEO_ADMIN_HOME，默认 8514；正式启动命令仍是 geosentinel，默认 8511。

第一次使用新启动器会生成、验证并启动初始快照，启动较慢。后续常规启动直接读取已发布版本。准备候选版本不会停止当前正式服务。

## 边界

- 当前模型设置面向已部署的 deepseek-official 提供方。新增其他供应商需要显式产品配置、凭据引用和验收，不复制管理员的凭据文件或 .env。
- 配置检查拦截将原生主机接口、创造模式或管理员权限带进普通 profile 的改动。源码仍由受信任管理员审查，静态检查不能证明任意插件无漏洞。
- 同步不包含 .env、开发账号、聊天记录、本机凭据、用户文件或临时测试数据。安装脚本默认不执行，依赖从本机 pnpm 缓存离线安装；缺缓存时验证失败，应先准备依赖。
- 存储核心模块变更阻止自动发布，需要数据库迁移评审。代码回滚不是数据库回滚，也不回滚已经生成的用户产物。
- 产品 profile（`dsh/profile/cordis.patch.yml`）是 Cordis 条目列表，方言是 `js-yaml` 的 JSON schema 加上 `!!js` 表达式标签（与 `@deepseek-ai/dsh-app-boot` 启动时解析的完全一致）。`!!js` 在插件激活时求值，`process.env` 在作用域内——远端 MCP 行就是靠它从环境读密钥而不把密钥写进快照。**解析与重写 profile 必须使用 `dsh/release/profile-schema.mjs`**：改用普通 YAML 解析会把标签退化成字面量字符串，那一行随后会带着不可用的地址启动，并因 `failOnStartupError: false` 静默消失（`dsh/tests/profile-dialect.test.mjs` 盯着这个行为）。
- 远端 MCP 行的凭据只存在于部署环境的 `dsh/.env`（正式）或启动预览时导出的环境变量（预览），不进入发布快照，也不写入启动 profile。
- 首期等待队列清空后整实例切换，切换期间有短暂不可用，不宣称零停机或逐会话跨版本并存。
- 发布控制器自身升级、数据迁移、Docker/Node 环境升级需要维护操作，不应当作普通 UI 配置热更新。
- 不要把内部原生工作进程或预览端口直接映射到公网。远程管理员通过正式域名的鉴权网关进入开发模式；独立预览仍是本机入口，远程预览代理尚未接入。
- 快照和镜像占用磁盘，当前不自动清理历史版本。确认不再需要回滚后再做管理员清理。
- 异常终止验证可能遗留 operation.lock；确认没有验证进程后再由管理员处理，不要盲目删除锁。

## 验收

独立 8512 实例验证了源码冻结、产品设置保存、验证生成、普通账号预览、预览账号访问发布接口返回 403、未结束任务阻止切换、等待期间新研究返回 503、任务结束后自动切换、监测开关随版本生效及手动回滚恢复旧配置。等待任务为标记的验收队列夹具，未拿正式用户研究做破坏性测试。

回归覆盖敏感配置拒绝、profile 权限边界、候选预览门槛、验证后源码漂移、快照篡改和验证失败不改变活动版本。桌面验收使用 1366x768 和 1440x900。


## 两步发布与 Agent 授权（2026-09-11）

管理员页面点击「生成预览」后，后台依次执行环境检查、冻结构建与测试、8513 隔离预览、随机上传口令读取验收。只有验收通过才可点击「发布此版本」。自动验收以 preview_user 密码登录，保留预览登录票给管理员；不对8511创建验收数据。

Agent 使用同一验收和发布门槛：

```powershell
node tools/release.mjs generate-preview --by agent --hold 600
node tools/release.mjs publish --id <候选ID> --by agent --authorization "用户明确授权发布这个候选"
# 用户明确说过“本次修改验收通过后直接发布”时：
node tools/release.mjs generate-preview --publish --by agent --authorization "本次修改验收通过后直接发布" --hold 600
```

授权说明必须来自本次真实用户指令，Agent 不得自行编造授权。CLI 属于已有主机权限的可信运维入口；该字段是审计记录，不是对自然语言授权的独立身份认证。网页发布仍要求管理员登录与密码再确认。授权记录与最终候选 ID 一起存入 pending 和发布历史。

发布在写 pending 之前校验：同一候选的验收记录（最长1小时）、冻结文件与当前源码、构建成功、安装版本、控制器15秒内心跳及其版本兼容、旧版回滚快照可校验。控制器收到切换通知后再检查验收与授权。重新验收失败会作废旧验收证据。普通操作无法用 previewed 标记绕过门槛。

使用旧发布控制器时会在停服前拒绝发布，提示维护升级；新代码不会自行替换正在运行的旧控制器。当前已有8511实例仍需单独维护发布后才能显示新按钮。计算机重启或源码编辑不会自动发布。升级控制器、停止正式服务仍遵循运维授权，不属于普通预览操作。


### 正式重启入口

```powershell
pwsh -NoProfile -File "D:\GeoSentinel-DSH\dsh\scripts\restart.ps1" -Port 8511
# 只预检，不停止进程
pwsh -NoProfile -File "D:\GeoSentinel-DSH\dsh\scripts\restart.ps1" -Port 8511 -CheckOnly
```

默认端口8511。核对冻结版本和进程归属，停止本实例及其控制器，然后独立后台启动并检查健康状态中的版本ID。发布切换期间拒绝重启；重启会中断正在执行的任务，请等任务结束。历史与用户数据保留。兼容回滚逻辑位于release/compatible-runtime.mjs，只允许启动曾发布的旧版，并先验证其快照。

.runtime仅保存日志与运行数据，不再存放日常运维所依赖的实现。旧restart-published.ps1只作为正式脚本的兼容转发入口。日志为.runtime/server.log和.runtime/server-error.log。
