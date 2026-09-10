# Changelog

## 2026-09-11 - 内核对齐 DSH 0.1.5：原生界面复用与封闭平面下的缺口处置

- **依赖与 profile 跟随 0.1.5**（`0.1.5-rc.1`）：230 个 pin 对齐、2 个已下架包移除、227 条 `pnpm.overrides`，锁文件按 0.1.5 闭包重生成；`agent-presets` 与 `plan-mode` 打开（委派工具随 agent 预设平面移动），`ui-agent-preset` 关闭。0.1.2 时代"关掉原生认证栈"的做法在 0.1.5 不可行：`connection` 与 `web-runtime` 必须保持开启，否则 7 个条目（含 `fileUpload`、`sessionController`）永久 pending。
- **客户端外壳改为引导 0.1.5 原生界面**：`ui-sidebar` 家族、`file-upload`、`attachment`、`approval`、`deliverables`、`resources` 等。引导必须精确——只要有一个条目 pending，原生加载器就把**整个** bundle 判为失败并白屏；因此 `api-session-controller`/`api-gateway`/`ui-sidebar-files`/`ui-sidebar-documentpreview` 只打包不引导（它们等的是产品刻意关闭的浏览器 API 平面），`open-in-app`（轮询 401）与 `ui-plan`（需 `remote.commands`）直接不打包。
- **`ui-workspace` 不打包**：它的激活等待 `workspaces` 与 `remote.directoryPicker`，后者在封闭平面内，且没有别的模块引用它（实测引导它会让整包失效）。`uiWorkspace` 服务面由产品叠加层实现——`connectWorkspace`/`openSession`/`openWorkspace`/`startSession`/`forkSession`/`archiveSession`/`pickDirectory`/`listDirectory`/`createDirectory`，把"一个 workspace"映射为"一个研究项目"；主机目录选择、会话归档与分支复刻在本平面没有产品等价物，改为明确失败并给出中文提示，不做静默模拟。
- **0.1.5 服务面变更的三处兼容**：`layout` 不再有 `closeDetails`（回退到 `closeRightbar`，四处调用统一走受保护的 `layoutCall`，方法缺失或抛错只告警）；提交回声改用 0.1.5 的 `attachments`（同时保留旧 `images`）并补 `placement`；`assistant/message` 事件补 `stream: []`——0.1.5 的 token meter 读 `event.data.usage ?? streamUsage(event.data.stream)`，`undefined` 会直接中断整条事件流，而产品不转发原始流记录，因此如实给出空数组、不显示用量，也不编造 token 数。
- **浏览器实测**（候选 `0b98c5955ac0e478`，预览实例 8513，普通用户）：登录 → 新建研究项目 → 自动打开会话 → 发送提问 → 用户气泡、助手答复、"用时 1秒"、"1 轮 1 步"完整渲染；控制台错误 0、失败请求 0。真实答复由会话库中的 `assistant/message` 事件独立核对。研究容器、委派链路与 MCP 远程服务已在同一候选线的前序候选上分别验证。
- **侧栏导航与产品入口在 0.1.5 上恢复**（候选 `b855817f18abe1ee`）：原生侧栏是一层槽位外壳，`sidebar.workspaces`（single，会话列表区域）的原生填充者是 `ui-workspace`，而它需要封闭通道的主机目录选择器，因此该区域此前是空的——用户打开一个会话后就无法再切换项目或会话。改为把产品自己的 `ProjectTree` / `ProductEntries` 注册进 `sidebar.workspaces` 与 `sidebar.footer.action`（这两个位置在本 bundle 内没有其他注册者，`sidebar.settings` 与 `sidebar.footer.action` 的原生占用者都未打包）。实测：侧栏列出项目「侧栏导航验收」及其会话行、点击项目即打开会话，底部四个入口（全球事件监测 / 监测简报 / 空间数据 / 账号）齐全，三块面板都能打开并渲染真实内容（监测简报显示线索计数，空间数据渲染 Natural Earth 底图预览）；控制台错误 0、失败请求 0。旧内核上仍由产品自己的侧栏承载这两块内容，注册位置互斥。
- **管理员开发实例的两处订正**（候选 `f4cec4cb35fd2b40`）：① 真实缺陷——worker 用 `require("yaml")` 写 `settings.yaml` 与 profile 种子，而 0.1.5 的安装把 `yaml@2.9.0` 只放在内容寻址存储里、根目录不链接（实测 `dsh/node_modules/yaml` 不存在），升级后开发实例会以 `MODULE_NOT_FOUND` 启动失败；序列化改用发布链路已有的 `release/profile-schema.mjs`（`js-yaml`，含 `.pnpm` 兜底），profile / overlay / settings 三份文档与 DSH 真正启动的方言一致，`settings.yaml` 输出与既有 home 逐字节相同，并把开发实例的组合抽成可测的 `development/profile.mjs`。② 撤回一个错误判断：`dsh-better-sidebar` 并不占用 `sidebar` 槽位（它只提供 `betterSidebar` 服务并新增 `conversation.chat.turnTail` / `settings.section` 两个子槽），0.1.2 的原生侧栏行本就启用、两者长期共存；`ui-workspace` 与 `remote.workspace` 在管理员实例里本来就是可用的（该实例保持 API 平面开启），因此这里不需要任何禁用逻辑。
- **侧栏文件浏览与文档预览在 0.1.5 上恢复（复用原生右侧栏）**：上一轮把 `ui-sidebar-files` / `-documentpreview` 排除在外，是因为它们要 `remote.workspaceFiles`（正常经浏览器 API 平面下发）。该命名空间改由产品从**自己的** explorer 接口回答：`/sidebar/api/fs.tree` 与 `/sidebar/file`（均带归属校验），也就是产品原有的虚拟工作区视图（上传的文件 / 分析结果 / 共享数据（只读）/ 过程记录，显示名在宿主侧反解回真实文件）。落地要点：① 三个包声明为直接依赖并在引导清单里引导，解析器新增「产品根」兜底（产品自己声明的客户端依赖不在 web-app 闭包里）；② 会话摘要补 `cwd`（宿主下发的虚拟根 `/工作区/<标题>`），否则文件标签直接报「这个会话没有工作区目录」且不发请求；③ `dsh-api-workspace-files` 必须引导——文档预览把文件表示为 `dsh-resource://file/…` 地址并经 `ctx.resources` 解析，缺它则报「文件资源服务不可用。」（其惰性变更流仍无产品等价物，`read` 只提供文本页、`changes` 明确失败）。实测（候选 `4573497233b7160e`，1600×950）：右栏「工作区文件」标签列出上述分组，展开「上传的文件」看到上传件，点击后原生 Markdown 预览渲染出文件内容，网络事实为 `session.cwd → fs.tree ×2 → GET /sidebar/file`，控制台错误 0、失败请求 0。顺带修掉上传件的存储前缀外泄：`dsh-file-upload` 用 16 位十六进制前缀、平台自己的上传用 UUID，视图现在两种都按用户选定的文件名显示。
- **管理员开发实例在 0.1.5 上实测通过，并修掉一处会拖垮其 GUI 的缺陷**：用真实的 `development/worker.mjs`（副本的 0.1.5 安装、独立 home）启动管理员开发实例，原生 GUI 直接报 `Failed to load plugins … failed to import loader entry (@geosentinel/dsh-workbench): require("@geosentinel/dsh-theme") missed the module table`。原因是 `@geosentinel/dsh-theme` 只由产品外壳注入模块表，而管理员实例把 `geosentinel-workbench` 当作自己的客户端入口加载；`__GEOSENTINEL_DEVELOPMENT__` 守卫只覆盖「被网关内嵌」的页面，直接打开实例地址时守卫不生效，require 抛错就带走整个入口。修法是把主题模块的 require 包进 try/catch：取不到即判定「不是产品外壳」，返回惰性插件、不进入其余 require 与槽位注册。修后同法复测：无加载失败，原生 `ui-workspace` 的「选择工作区 / 选择一个工作区开始」工作流就位（说明管理员平面的 `workspaces` 与 `remote.directoryPicker` 在该实例里确实可用），控制台错误 0、失败请求 0。这也解释了为什么该缺陷在生产里没暴露：管理员日常是通过网关内嵌页面进入开发模式的。
- **仍存的缺口**：① 原生会话列表的分组交互（重命名/归档/拖拽排序/搜索）由 `ui-workspace` 提供，产品用自己的列表替代后这些交互仍是产品自己的实现（管理入口在行的齿轮按钮里）；② 文件面板没有实时变更流（`workspaceFiles.changes` 明示不可用），新增文件需要手动刷新标签；③ 正式发布尚未执行，生产仍是 `9e061099f3ccf519`，且主树依赖树仍是 0.1.2（`check-env` 报 `DSH pin` 与 `Resolved DSH dependency graph` 两条失败、217 个包不匹配），升级流程见 [dsh/RELEASES.md](dsh/RELEASES.md) 的「升级 DSH 内核依赖」。
- 单元测试 100 项通过（主树与 0.1.5 副本各一遍）；`prepare → preview` 在副本内通过；候选 `f4cec4cb35fd2b40` 的客户端 bundle 与已验证的 `b855817f18abe1ee` 逐字节相同（`sha256=9a49221d…`，9265336 字节），因此其上的侧栏与面板复验结论同样成立；生产版本未改动。

## 2026-09-10 - 远端 MCP（高德 / NASA CMR）真正可用：两处缺陷修复

- **发布链路丢掉了 profile 的 `!!js` 标签。** 产品 profile 是 Cordis 条目列表，方言为 `js-yaml` 的 JSON schema 加 `!!js` 表达式标签（`@deepseek-ai/dsh-app-boot` 启动时按此解析，`!!js` 在插件激活时求值，`process.env` 在作用域内）。原发布链路改用另一个 YAML 库解析后再 `YAML.stringify` 重写，标签退化成字面量字符串：实测候选版本真正用于启动的 profile 里写成 `url: "`https://mcp.amap.com/mcp?key=${process.env.AMAP_API_KEY}`"`，高德行随后带着不可用地址启动，并因 `failOnStartupError: false` 静默消失。新增 `release/profile-schema.mjs`（与启动器同一方言，含 `.pnpm` 兜底解析），`manager.validateProfile` 与 `runtime.launchRelease` 改用它；回归测试 `tests/profile-dialect.test.mjs` 固定该行为。
- **普通用户拿不到 `mcp__*` 工具。** `plugins/platform/index.mjs` 的平台白名单集合漏了 `MCP_TOOLS`，`tools.restrict()` 会把 `mcp__*` 从模型工具清单里剔除，守卫也会以「该工具在本平台不可用」拒绝执行——即使插件行已经加载。白名单补上 `MCP_TOOLS`。
- 普通用户实测（预览实例、非管理员账号）：`mcp__amap__maps_geo("上海外滩")` 返回 121.493167, 31.245385（GCJ-02，adcode 310109）；`mcp__cmr__get_collections("VIIRS nighttime lights")` 返回 total_hits=23，含 VNP46A2 / C3365931269-LAADS；两次调用均无错误。
- 技能 `external-geo-services` 增加「工具可用性」一节：高德缺 `AMAP_API_KEY` 时工具不会出现在清单里，必须如实说明不可用，不得凭记忆编造坐标或数据集编号；`RELEASES.md` 写明 profile 方言以及"不许用普通 YAML 解析/重写 profile"的约束。

## 2026-09-09 - 空间数据预览支持 GeoTIFF 与 Shapefile

- 「空间数据」标签新增两种格式：`.tif/.tiff` 解析 GeoTIFF 标签（BigTIFF、条带与瓦片、未压缩与 DEFLATE、单波段浮点/整型、GeoKey 与 ModelTiepoint），按 2%–98% 分位拉伸在服务端渲染 PNG，并给出波段、数据类型、压缩、CRS、取值范围与范围框；实测上海 2020 NTL 裁剪栅格 311×267 在 69 ms 内出图。
- `.shp` 支持几何（点/线/面/多点，含 Z/M 变体）与同名 `.dbf` 属性读取；`.shx/.dbf/.prj` 等配套文件默认不单独列出，下载 `.shp` 时用内置 zip 写入器打包为同名 zip（无外部依赖）。
- 无法渲染的情况如实降级：LZW/JPEG 压缩、多波段、超过 400 万像元只返回元数据与范围并写明原因；畸形几何仍被丢弃计数。
- 新增 `plugins/platform/tiff.mjs`、`shapefile.mjs`、`zip.mjs` 与 6 项测试；全量测试 85 项通过。

## 2026-09-09 - 侧栏文件改为贴近日常的视图

- 侧栏文件面板不再展示 `inputs/`、`outputs/<时间戳-作业ID>/` 这类实现细节，改为三个日常分组：**上传的文件**（项目资料、对话资料与上传件，去掉会话前缀）、**分析结果**（按 图表 / 表格 / 报告 / 空间数据 / 其他文件 归类，隐藏 `result.json` 机器信封，重名自动加 `(2)`）、**过程记录**（保留原始作业目录，供追溯）。
- 虚拟路径全部可以反解回真实文件；解析仍受所有权与工作区边界校验，`memory/` 继续拒绝，`..`、反斜杠与越出会话根目录的绝对路径分别返回 400/403。`fs.tree`、`fs.read`、`fs.search`、`/sidebar/file` 都走同一套解析。
- 新增 `dsh/plugins/platform/workspace-view.mjs` 与对应测试；在真实对话（45 个作业目录、41 个产物文件）上实测：主视图只显示 3 个分组，产物按类型归入 5 类且文件名保持可读。

## 2026-09-09 - 基准实测暴露的四处修复

- 预览实例的 home 目录改到发布目录旁的独立 `previews/` 短路径：旧布局下容器对深路径工作区的绑定挂载会持续返回 `EIO`，实测同一份代码换短路径后 B02/B03 从 909/206 秒且失败变为 90/60 秒通过。
- 研究容器绑定挂载出现 `EIO` 时，除容器启动失败外，worker 返回含 `Errno 5` 的错误信封也会重建容器并按 2 秒、6 秒退避重试；产物目录在容器启动前先在宿主机落一个探针文件，降低 9p 缓存未命中的概率；运行期产物扫描从 1 秒改为 3 秒，减少宿主机与容器争用同一挂载。
- `ntl_toolkit` 路径校验放行 POSIX 绝对路径（`/workspace/previous/...`）：Linux worker 与 Docker 挂载使用该形式，原校验按 Windows 半限定路径一律拒绝，导致 `geo_inspect_vector` 等工具在上游产物上失败。
- 模型目录补上 `deepseek-v4.1-flash-expires-on-0910`：聊天路由接受该模型，但团队派发按目录校验，未登记会让成员启动失败；目录同时列出 flash、pro 与 vision-exp，多模态模型标注 `inputModalities: [text, image]`。

## 2026-09-09 - 会话命名、简报与空间数据标签、国际新闻源与默认模型

- 研究对话复用原生 DSH 会话命名：侧栏显示由首条消息派生的标题（原生 session/title，含确定性回退），不再全是“新研究对话”；在产品内重命名会同时钉住原生标题。
- 侧栏新增「监测简报」标签集中查看结构化简报（不再只靠下载）；公共监测卡片保留来源与带入研究对话。
- 侧栏新增「空间数据」标签：列出当前对话工作区内的 GeoJSON / JSON / CSV，点击后在 Natural Earth 世界底图上绘制点线面或经纬度点，给出要素数、几何类型、范围与属性表；畸形几何丢弃并计数，不含几何的 JSON 只列字段。
- 监测新增国际新闻渠道（联合国新闻、半岛电视台、BBC World、RFE/RL、DW 公开 RSS），按冲突/制裁/军事/人道/灾害词表筛选，无坐标只作线索；UCDP 与 ACLED 需要令牌，如实记录未启用。
- 默认模型切换为 `deepseek-v4.1-flash-expires-on-0910`（原生多模态，实测可用）；不可用时备选 `deepseek-v4-flash-vision-exp`。
- 提示词与技能库精简：放宽“必须/只有”类硬约束（优先用现成工具、重要结论落证据链即可），产品技能库移除面向维护者的 `ntl-regression-evaluation`（保留在 `.ntl-gpt`），能力表补充公共监测可用性。
- 研究容器遇到 Docker Desktop 9p 挂载瞬时 EIO 时重建容器重试一次，不再把环境故障当任务失败上报。

## 2026-09-09 - 对话可视化要求与监测简报补齐

- 系统提示词明确要求对话重视可视化：能用图表、表格、指标卡或结构化面板说清楚的结果不只给文字和文件路径；需要交互式面板时用 dsh-ui 围栏调用 genui 组件，并保留单位、来源、范围与限制，不为了好看编造数据。
- 监测整理改为先合并快照再整理整份保留集合：上一轮遗留、尚未生成简报的记录也会补上，实测同一份 64 条快照从 36 条有简报补到 67/67。
- 采集轮失败后 2 分钟重试（不超过正常间隔），避免一次瞬时失败让共享地图陈旧整个周期。

## 2026-09-09 - 修复对话内图片与产物链接无法加载

- 产物接口把 `outputs/<作业ID>/<文件>` 当成相对 `outputs` 的路径再次拼接，导致研究工具返回的内联图片与下载链接全部 404，浏览器显示「图片无法加载」。
- 现同时接受 `outputs/<作业ID>/<文件>` 与研究目录列表的相对形式，并仍在当前对话自己的 `outputs` 目录内解析；目录穿越与跨目录访问继续被拒绝。
- 增加回归测试：按研究插件真实生成的链接形式断言 200 与 `image/png`，并覆盖两种路径形式的越权与穿越用例。

## 2026-09-09 - 公共监测结构化简报与来源渠道

- 采集渠道改为 GDACS、NASA EONET 与 EMSC 三源实采；USGS 在本机网络只解析到 IPv6 且 TLS 中断、ReliefWeb v1 已下线且 v2 需申请 appname，两者不纳入并在 `dsh/MONITOR.md` 记录原因。
- 每条记录生成结构化简报（概要、已记录要点、关注点、待核实），整理按 40 条分批并支持一次拆半重试，失败时保留来源字段并如实标注状态。
- 监测页增加按类型着色的可点击图例（含数量与筛选）、关注级别、数据源渠道状态与说明、事件卡片结构化简报，以及登录后可下载的 Markdown 简报接口。
- 修复发布冻结遗漏 `monitoring/sources.py` 的问题：采集容器按路径挂载该适配器，此前仅存在于活动版本目录，现纳入快照并校验。

## 2026-09-09 - 统一研究与创造工作台

- 管理员默认进入研究任务；创造任务嵌入同一工作台，保留统一导航、账号和开发历史。新建创造任务默认绑定 GeoSentinel 源码和原生 cordis 预设。
- 研究与创造视图切换保留开发草稿；原生设置在主区域打开，发布仍由平台显式确认。
- 增加开发模型、内置主题、字号和合规图片背景导入产品草稿，不复制凭据与主机权限。

## 2026-09-09 - 同源管理员开发模式

- 取消管理员必须本机访问的限制，通过平台账号和再次确认密码进入原生开发界面；内部工作进程仍只绑定回环端口。
- HTTP、原生 RPC、流式连接与 Better Sidebar 通道经过管理员鉴权；授权绑定登录，过期、退出或撤销角色时拒绝访问。
- 每管理员独立 DSH home，加载 GeoSentinel 开发插件，保留原生标准/创造模式、模型、插件、主题与文件管理。目录选择使用浏览器内选择器。
- Windows 开发启动增加可执行 PowerShell 探测，跳过损坏别名，不修改系统权限或已有 DSH 安装。
- 普通用户继续使用冻结产品版本与独立配额；不将原生管理员设置和密钥整体复制到普通版。

## 2026-09-09 - 管理员产品配置与同步发布

- 增加产品配置、角色工具分配、插件清单和源码差异面板；通过原生管理员设置扩展进入，不复制个人权限或凭据。
- 引入冻结源码快照、隔离依赖、固定 Docker 镜像摘要、用户视角预览、显式同步发布和回滚。
- 等待既有研究及队列清空，新研究暂缓提交；健康检查通过后记录新版，失败尝试恢复旧代码。原账号、历史与文件不随版本覆盖。
- 完成独立实例的发布等待、自动切换、监测配置生效、权限拒绝与回滚验收；首期是整实例切换，不宣称零停机，不自动进行数据库迁移。

## 2026-09-09 - NTL 工具注册与本机原生管理员开发

- 研究插件新增 DataV/geoBoundaries/GEE 矢量获取接口，并注册 14 项已有 NTL-GPT 栅格、矢量、统计核心工具；普通用户仍通过任务权限检查和 Docker 固定入口执行。
- 实测上海、武汉及缅甸区界获取，并以合成数据验证上海分区均值；记录几何修复、来源和验收边界，不宣称全部旧工具已经迁移。
- 新增 `geosentinel admin` 独立本机 DSH profile，保留原生设置、插件管理和创造模式。开发与多用户实例分开，端口不对公网开放。

## 2026-09-08 - 独立快捷启动命令

- 新增可本地注册的 `geosentinel` / `geosentinel web` 命令，默认端口 8511，在任意目录调用现有受控启动器；不修改全局 DSH，也不变更其依赖树。
- README 补充注册、端口覆盖、配置优先级与卸载说明；原 `pnpm start` 默认 8510 的兼容入口保留。

## 2026-09-08 - 原生子智能体只读运行记录

- 复用 DSH 原生子智能体目录、会话层级和历史渲染；新增受账号及父子归属双重校验的 GET 接口。
- 子会话不开放提问、独立停止、重命名或批准；底部可返回主对话。AgentTeams 仍是唯一团队调度器，原生 Todo 与方案审批流程保持不变。
- 兼容 Better Sidebar 的只读工作区信息与空能力状态通道，不开放终端、主机文件或通用子会话控制。补充权限和原生交互回归测试，以及两种桌面尺寸的真实历史查看验收。

## 2026-09-08 — GEE 启动配置与子智能体调研

- 修复旧项目 `.env` 未提供 `GEO_GEE_CREDENTIALS` 导致新版 GEE 获取不可用的配置路径，增加受限配置导入工具，保留既有 DSH home/账号和工作区。授权错误提前报告具体配置项，不再误导为数据集问题。
- 受控启动器从 profile 目录运行 DSH，避免产品 `.env` 的启动变量被上游普通项目环境层拒绝。
- 修复此前只调整宿主端为 30 分钟、镜像 worker 仍为 10 分钟的遗漏。worker 默认 1800 秒并接收宿主传入时限，需重建镜像；增加镜像内时限验证。
- 实测上海小范围 SRTM 与 2020 VIIRS ANNUAL_V21 影像获取、GeoTIFF 检查及跨容器读取成功；不代表完整上海 16 区统计案例已验收。
- 完成 [原生子智能体与 AgentTeams 对比](dsh/SUBAGENT-ARCHITECTURE.md)：现有 AgentTeams 已使用原生持久化子会话。当前看不到子 Agent 的原因是观测适配未接通，本轮只记录原生只读查看方案，未移除 AgentTeams。

## 2026-09-08 — DSH 多用户资源管理

- 研究对话和 Docker 作业均支持持久化自动排队，默认每账号同时 2 个、全平台同时 10 个，可通过管理员配置调整；提问频率保持每账号每分钟 20 次。
- 增加同一数据目录的进程占用保护、运行状态持久化、派发前归属/方案版本复核与取消联动。重启恢复未派发的研究请求，已执行任务标记中断，不自动重放。
- Docker 单作业默认运行上限延长至 30 分钟，内存改为 3 GiB；`GEO_DOCKER_MEMORY_MIB=4096` 可配置为 4 GiB。运行容器不随代码更新自动改变限制。
- 增加用量统计、同项目上传配额串行核算、低磁盘保护，以及管理员停机使用的软删除工作区清理工具（默认只预览）。
- 复用原生 DSH 扩展位置展示等待任务、取消操作和资源用量；继续保留多用户账号、项目、对话与文件权限边界。
- 新增 [资源与队列管理及升级教程](dsh/RESOURCE-MANAGEMENT.md)，补充环境示例与检查。46 项回归测试通过，并完成真实 Docker 隔离、内存限制、排队、取消和遗留容器恢复验收；未进行 10 容器满载压测。
- 已记录但未实施：只读取明确依赖且完整的前序结果，以及统一的文件级来源/完整性准入校验。现有 `previous/` 挂载机制暂不改变。

## 2026-09-03

- 消息气泡改版：去除外边框，改为无边框圆角（助手 16px / 用户 14px）+ 对比背景 + 轻投影，与提问栏风格统一。
- 对话头像升级：研究者与 AI 头像从占位文字（HY/AI）替换为 lucide 图标（user-round / bot），圆形头像内居中渲染，历史与新消息统一。
- 修复用户消息气泡文本挤块：头像+文本改用 flex row-reverse 镜像布局（原 grid 的 minmax(0,max-content) 在窄容器下塌缩导致文本竖排），气泡靠右、文本按内容自适应换行。
- 排版可读性二次升级：字号再放大一级（11px 起跳至 12px，正文 14-15px、标题 16-18px），低对比度文字再提亮（灰蓝系 +12%），次要注释/时间戳提亮；用户消息气泡改为右对齐镜像布局（头像在右、内容右对齐），与主流 Chat 交互一致。
- 排版可读性升级：全局字号整体放大一级并抬高最小字号下限（8-10px 提升至 11-12px，12px 起提升至 13px），同时提亮低对比度灰蓝文字色（约 +18% 亮度），桌面与移动端均无布局溢出。
- 界面布局调整：平台标题移入左侧导航顶部（保留完整展示），移除顶部工具栏与"导出简报"按钮，主窗口整体上移，桌面与移动端均保持折叠/展开正常。
- 右侧工作区新增"地图"Tab：任务生成的地理空间数据（tif/shp/geojson 等）自动加载为叠加图层；支持图层定位到范围、显隐切换、自动缩放至全部图层范围；复用 GDAL 预览管线（PNG + WGS84 envelope），与"预览"单文件查看互补。
- 修复全球监测地图背景不显示：放弃无国界渲染的 Esri Dark Gray Canvas，改用 Esri World_Street_Map + 暗化滤镜（去饱和 + 亮度 0.16-0.62），保留国界、地名与地形，适配深色工作台；修复 raster-brightness-max 超界（>1）导致的地图样式加载失败。
- 项目文件预览新增地理空间预览：GeoTIFF / Shapefile / GeoJSON / KML / KMZ 通过 GDAL 渲染为轻量地图预览（OSM 底图 + WGS84 配准叠加），并支持百分位增强显示；右栏"预览"标签页直接呈现，无需下载。
- 任务文件列表区分"成果文件"与"过程文件"：脚本、manifest、运行日志、审计 JSON（evidence_report / route_state / contract 等）收敛到次级"过程文件"分组，降低干扰；分类规则后端下发、前端兜底。

## 2026-08-31

- 修复正式 Web 链路中 GEE 数据获取任务卡死：为每轮模型输出设置上限（max_tokens=12000），并增加运行墙钟时限（NTL_RUN_WALL_CLOCK_TIMEOUT_S，默认 1500s）与 run_timeout 稳定错误码，超时任务自动释放运行锁并保留已生成文件。
- GEE/官方渠道下载链路实测打通：GEE_request_plan_tool → NTL_download_tool（逐日）→ NTL_Mean_Composite（归档合成）→ geodata_inspector_tool 复核。
- 修复 GEE_raster_download_tool 两处公网环境故障：① billing project 未解析时默认回退 GEE_DEFAULT_PROJECT_ID（环境变量→.env），避免 ee.Initialize 无项目导致的 serviceusage.services.use 权限错误；② 请求波段名自动按资产真实波段归一化（如 DNB_BRDF-Corrected_NTL → DNB_BRDF_Corrected_NTL），消除 Image.select 波段不匹配失败。

## 2026-08-30

- 正式 Web 工作台将监测、当前任务文件和文件预览整合为右侧标签工作区；默认显示监测面板，桌面端支持拖拽、键盘调整、折叠恢复和宽度持久化。
- 当前任务的研究资料与分析产出支持统一搜索，并可在右侧预览图片、表格、HTML、Markdown、文本和 PDF；HTML 预览使用受限沙箱与内容安全策略。

## 2026-08-25

- 正式智能体运行时升级到 Deep Agents 0.7.5、LangChain 1.3.15 和 LangGraph 1.2.11。
- 引入地缘分析师、数据助手、分析助手、事件助手四角色架构及角色级工具、技能和权限。
- 引入类型化任务/事件/观测/分析/证据包、产物身份、路由状态和转交记录。
- 将 GeoSentinel 定位扩展为地缘环境智能计算平台，夜间灯光调整为可选证据层。
- 明确所有校正操作默认关闭，仅接受用户明确授权或用户指定方法的必要步骤。
- GEE 角度校正工具改为调用时初始化，避免导入工具时触发认证。
- 增加 Windows 损坏证书条目的进程内 CA 回退，并为 NASA EONET 增加受限 HTTP/1.1 只读备用通道。
- 正式 Web UI 更新为四个短角色名称，并移除禁用传感器工具的用户入口。
- 增加单次任务 50 次模型调用预算、手动停止状态闭环、异常线程运行锁自愈和分类错误兜底回复。
- 正式 Web 事件流支持序号续传、重复事件过滤、指数退避重连和服务重启后的持久化会话恢复。
- 监测案件入口增加前后端双层短时去重，并改为“事件理解—补充核验—方向建议”的主 Agent 研判流程。
- 监测案件入口改为确认式草稿流程：只向聊天框填入包含时间、地点、坐标、来源和背景的完整问题，保留已有草稿，由用户检查或修改后主动发送。
