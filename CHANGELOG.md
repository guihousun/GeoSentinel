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
- **外壳采集器的两处修正与清单固化**（候选 `b0a22f9eccdbf0cc`）：① 传递依赖解析失败时不再冒泡——`collect` 的 `required=false` 分支原本会中断该包其余依赖的采集，外层再把整包误报成「跳过未安装的原生界面包」（0.1.5 副本里的 `ui-sidebar-right` 正是被这样误报，其实它已打包且已引导）；② `nativePlugins` 注释写实：这份核心清单与迁移前（0.1.2 时代）逐项一致，后附 0.1.5 增量，其余原生面板（trajectory/jobs/goal/skill/settings/模型与权限预设等）属产品长期以来的窄曲面而非迁移遗漏；`tests/native-ui.test.mjs` 新增结构性用例，把声明清单与「已打包 + 引导状态 == noBoot」绑定，静默丢包或静默引导都会失败。
- **签名流程在 0.1.5 上实测**（同一候选，预览实例）：「建立待确认的分步方案」→ 智能体给出 AOI/时间范围/来源/方法/产物/不确定性并**通过产品的提问机制**抛出入参确认 → 原生提问面板渲染（三选项、1/3 分页、跳过/下一题）→ 逐题作答后弹窗关闭、智能体继续执行（事件 43→72、无待回答问题、控制台错误 0、失败请求 0）。委派链路（子智能体→成员→完成）此前已在同线的候选上单独验证。顺带核实一次：该会话里 `web_search` **成功**（记录里有来源列表），失败的是 `web_fetch` 的 `TypeError: fetch failed`——管理员会话对同一 URL 复现完全相同的错误，属本机直连外站的网络限制，不是产品缺陷，也不是迁移回归。
- **界面上传在 0.1.5 上从「发不出去」修到「可用」**（候选 `1459dbed2c3c59c1`，阻断级缺陷）：实测用户真实路径（点附件按钮选文件）时，上传坞显示「上传失败，点击重试」而网络里**一个上传请求都没有**。两层原因：① 原生上传客户端用 `new Worker(blobURL)` 跑后台传输，产品外壳的 CSP 没有 `worker-src`，被 `default-src 'self'` 拦下——CSP 增加 `worker-src 'self' blob:`（blob 由本页自己创建，`script-src` 不放宽）；② 请求发出后立刻 401，因为原生路由 `/api/session/uploadFileBinary` 在 DSH 的单用户令牌认证之后，普通用户没有该令牌。处置：把该客户端唯一的上传常量改写到产品自己的桥 `/api/upload/native`（改写带守卫，源码里找不到原常量即报错），桥复用产品既有校验与存储——产品 Cookie 认证、按 `sessionId` 反查并复核会话归属、16 MiB/单文件与 128 MiB/对话上限、转发到同一个存储处理器，再按原生客户端解析的信封回答；`receiptId`/`attachmentId` 用存储后的工作区相对路径而非编造句柄。修后实测：坞里显示 `upload-sample.md / MD 63B`，请求为 `POST /api/upload/native?…`，工作区「上传的文件」列出该文件，控制台错误 0、失败请求 0。
- **上传链路的端到端复验与 PDF 预览**（候选 `1459dbed2c3c59c1`）：① 用界面控件一次上传 `preview-sample.pdf` 与 `read-note.md`，坞里分别显示 `PDF 604B` / `MD 95B`，文件面板「上传的文件」列出两者；② 点开 PDF，原生文档预览取字节（`GET /sidebar/file?…preview-sample.pdf`）并把页面渲染出来（截图可见白色页与文本），这条路径依赖上一轮的 `worker-src 'self' blob:`——PDF.js 同样用 Worker；③ 问智能体「读取我刚上传的文件并把口令原样回答」，8 秒内答出文件中的口令，工具轨迹为 `geo_list_files`（列出 `.dsh-uploads/<chatId>/…-read-note.md`）→ `read_document` → 口令，证明上传件真的进入了研究流程而不只是落在磁盘上；④ 顺带核实发送按钮本身正常（真实键盘输入 + 点击 → `/geo/api/chats/<id>/prompt` 发出、草稿清空、回复「收到」），此前一次「点了没反应」是测试脚本用 `fill()` 未触发受控输入状态更新，不是产品缺陷。
- **与在跑的正式版本做了一次机械化的客户端对比，并据此修掉一处流量成本**：取两边模块加载器注册的 id（而不是文本里像包名的子串，那会把 CSS 类名也算进来）逐项比对——在跑的 0.1.2 版本打包 17 个客户端模块、bundle 2.7 MB；0.1.5 候选 26 个模块（引导 25 个）、bundle 9.2 MB。**唯一被移除的模块是 `dsh-better-sidebar`**（其功能已用原生侧栏的 `sidebar.workspaces` / `sidebar.footer.action` 与原生右侧栏文件树重新表达，并在浏览器里逐个验过），新增的 10 个模块都是 0.1.5 原生界面（侧栏族、上传、附件、审批、交付物、资源、文件资源提供者），即能力上是超集而非缺失。对比同时暴露了代价：外壳此前用 `cache-control: no-store`，9 MB 的 bundle 每次进页面都要重下，而公网走既有 HTTPS 代理。改为 ETag + `no-cache`（校验子 = 构建版本 | 外观戳 | 路径 | 正文长度），实测首屏 200 → 同页复验 304、刷新后仍 304，外壳渲染正常。
- **依赖升级已工程化为一条带自动回滚的命令**：新增 `dsh/scripts/upgrade-deps.mjs`（默认干跑，`--apply` 才动手）：先把 `node_modules` 改名为 `node_modules.bak-<时间戳>`（同盘改名、秒级可逆），再装 pin 版本，然后三重校验（安装零退出、`check-env.mjs` 通过、安装后的 `dsh-web-app` 版本等于 pin）；任一步失败即把新树改名到 `node_modules.failed-<时间戳>` 并把备份原样放回。四个用例覆盖干跑不改动、成功保留备份、装完版本不一致按失败处理、安装中途失败后旧树原样放回；`dsh/RELEASES.md` 的升级章节改为引用该工具。
- **实测发现并部分修复「专家角色边界在 0.1.5 上失效」**（候选 `23b4385c696b802b`）：派给「数据助手」的一次性子代理，其**首个请求拿到了主管的全集 74 个工具**（角色表只有 21 个）。三个具体原因：① 绑定只靠轮询原生子代理目录，而目录行要经实时投影解析，一次性专家几秒跑完就再也列不出来；② 原生 `subagent/start` 事件存在，但事件只传载荷、`emit` 的第二个参数（父代理）不会到达监听器，且同级 ctx 之间不冒泡，必须监听根上下文；③ 派发瞬间既没有 label 也没有可读会话，运行时会往子会话追加 `subagent/descriptor`，其中的 `label` 就是主管写的委派描述。处置：新增可测的 `plugins/platform/role-binding.mjs`（角色解析顺序：标签 → 描述符 label → 首条用户消息，带界重试；按角色表收紧并在创建窗口内重复施加），`subagent/start` 监听挂在根上下文并从子会话 header 的 `parentSession` 取父会话，目录轮询保留为回退。修后实测：派发瞬间完成绑定、成员列表立即出现该专家（`mode=one-shot, label=数据助手`）、其第二次请求从 74 个工具收紧到 22 个。**仍未关闭**：一次性子代理的首个请求仍带主管全集（绑定天然晚一步）；子会话最终是「角色表 + `subagent`」而非角色表本身（agent-preset 层每次准备都把委派行加回来）。两者要一次解决只能走运行时的派生时组合——`create/resume` 接受 `composition: { persona, toolFilter }`，而原生 `subagent` 工具不暴露这两个参数，因此需要产品自己派发专家（已列为下一步任务，代码注释同步写明）。
- **专家角色边界改为派生时组合，首个请求就带自己的工具表**（候选 `f12ccaed2c9098d8`）：上一轮把根治路径写成「需要产品自己派发」，本轮找到了更省的做法——委派工具插件本身就是可配置的 Cordis 行，`persona` / `toolFilter` / `toolName` 都是行配置项，运行时会用它做**派生时组合**，在子代理的创建窗口内就装好 persona 与工具表，不存在竞态。产品因此新增三行 `geo-delegate-data` / `geo-delegate-analysis` / `geo-delegate-event`（provider `spawn`、`backgroundMode continuable`、各角色的职责 persona 与工具表），`TEAM_TOOLS` 改为这三个角色工具加三个只作用于既有子代理的控制工具，并把通用的 `subagent` / `subagent_fork` 从主管清单里去掉；主管 persona 改为按角色调用对应工具。实测：主管调用 `delegate_data` 完成派发，子会话**首个也是唯一一个请求**的工具集从 74 个降到 22 个（数据助手 21 个工具表 + 1）。**剩余一个**：`subagent` 仍多出，因为它来自 agent-preset 层自己挂的通用委派行（不是被产品禁用的 bundle 行），preset 每次组合都会带上它；去掉需要产品自带 preset，用三角色工具替换通用派生行（已列为下一步，原因写在 role-binding.mjs 注释里）。测试：新增 `tests/role-delegation.test.mjs`（三行存在、persona 点明角色、工具表与 `product.json` 逐项一致、主管清单无通用派生工具），`release.test.mjs` 同步断言；两种内核各 114 项通过。
- **角色委派切到 agent 预设平面，并由平台在启动时交付**：上一轮把三行 `geo-delegate-*` 挂在 profile（host 平面），实测仍留着 preset 层带来的通用 `subagent`。本轮先核实了工具集的组装规则——web-app bundle 把 23 个 host 行交给 preset 逐会话重组，而 bundle 那行的默认值是上游的 `standard`（完整编码 Agent）；进而核对两处支撑事实：**0.1.2-rc.1 的 spawn provider 本身已带 `toolFilter`/`persona`/`prepareContinuable` 能力位**（所以这套行配置不是 0.1.5 独有），以及 preset 的配置面为 `{ default, roots[{path,trust}], includeShippedRoot, includeUserRoot }`，其中用户根固定为 `<dshHome>/.agent-presets`（`DSH_HOME`，否则 `~/.dsh`），根内先到先得、shipped 根在最前、用户根在最后。据此：三行从 profile 撤下（否则与 preset 重复注册同名工具），profile 的 `agent-presets` 行改为 `config.default: geosentinel`，新增 `plugins/platform/agent-preset.mjs` 在启动时把**运行中安装的** `profile/agent-presets/geosentinel/` 复制进用户根——运行中的版本不读源码树；写入带归属标记（`.geosentinel-preset.json`，含逐文件 sha256），字节相同不改写，标记不匹配的目录一律不碰（管理员自己写的同名预设保持原样并告警）。角色→工具的对应关系收敛为 `catalog.mjs` 的 `ROLE_DELEGATION`，`TEAM_TOOLS` 由它展开，preset 校验、发布守卫与测试都读这一份。发布守卫同步加固：`release/manager.mjs` 新增 `validatePreset`，freeze 时用 profile 里写的默认 preset id 去校验随包发布的 preset（角色行齐、persona 点明角色、工具表等于 `product.json.roleTools`、无通用/外部 spawner 启用、宿主机 shell 与厂商扇出编排保持关闭），缺文件即拒绝发布。**实测**：用 0.1.5 安装里真实的发现代码路径（`dsh-agent-presets` 的 `discoverPresets`）对临时 home 跑了一遍——名单 5 项（4 个 shipped + `geosentinel`，trust=user，无 `broken`，即所有启用行的包名都能按真实 harness base 解析），委派行恰为三条、工具表 21/29/14 与发布表逐项一致。测试：主树与 0.1.5 副本各 **127 项通过**（新增安装器用例 7 项、发布守卫用例、委派测试改为读 preset 与 profile 默认项）。**尚未做**：把这一改动发布到正式平台（需要先行升级主树依赖并取得发布许可，见下方缺口④）。
- **候选 `754daa94e9a1fc4c` 上实测：角色边界至此闭合。** 先在 0.1.5 副本走完 `prepare`（冻结 → 离线安装 → 语法 → 冻结树内测试 → Docker 镜像，全部通过，新守卫同时接受切换后的 profile 与 preset），再以 `preview` 起独立实例（8513）并在上面做真实研究回合。四类证据：① 冻结的 profile 里 `agent-presets` 行确为 `{default: geosentinel}`；② 实例启动日志出现「已写入产品 agent preset（geosentinel）到 …previews\754daa94-9e1b3a\.agent-presets\geosentinel」，该目录与其逐文件哈希标记都在；③ 主管会话**首个也是唯一一个请求** 57 个工具，委派工具恰为 `delegate_data`/`delegate_analysis`/`delegate_event`，**无** `subagent`/`subagent_fork`/外部 spawner，**无** bash/pwsh/workflow/ralph（此前 74 个工具且带 `subagent`）；产品原有能力齐备（`exit_plan_mode`、`ask_user_question`、`render_ui`/`validate_dsh_ui`、`web_search`/`web_fetch`、`read_document`、20 个 `geo_*`、18 个 `mcp__*`）；④ 两次真实派发（数据助手、分析助手）各自的子会话**首个请求**分别为 21 与 29 个工具，与 `product.json.roleTools` 对应表**逐项一致**（此前是「角色表 + `subagent`」）。核对脚本 `.runtime/preview-preset-check.mjs` 可重跑：它只读预览实例写下的文件（冻结 profile、交付目录与标记、会话日志里的 `request/header`），不依赖服务器在跑。
- **产品自带的 agent preset 已就绪（现已挂载为默认）**：核实 0.1.5 的委派工具平面后确认，一个会话的工具集由 agent preset 逐会话组装——web-app bundle 把 23 个 host 行（shell、文件、skills、目标、计划、压缩、子代理、工作流等）一并禁用，再由 preset 重新组合；而 bundle 的默认 preset 是上游的 `standard`，即「功能完整的编码 Agent」。这与 0.1.2 的产品曲面不同：当时插件包把 `tool-bash`/`tool-pwsh` 禁用且产品 profile 从未打开，宿主机 shell 从未进入产品；工作流同理（产品 profile 现在仍在 host 平面禁用 `tool-workflow`）。因此新增产品自有 preset `dsh/profile/agent-presets/geosentinel/`（`agent.cordis.yml` + `preset.yml`，以上游 `standard` 为基），偏离上游仅三处，且每处都在文件内就地标注 `disabled: true` 与理由：① 宿主机 `tool-bash`/`tool-pwsh` 关闭（模型执行的边界是网络封闭的分析容器；这不是新增限制而是与 0.1.2 对等）；② `workflow-worker-thread`/`tool-workflow`/`tool-ralph` 关闭（三者都会派发无 persona、无工具表的新 agent，绕开三角色边界，与 release 守卫的 `requiredDisabled` 同源）；③ persona 去掉上游的「编码 Agent」身份（本 preset 同时覆盖主管与其派生的专家，主管指令仍在 host 平面 profile）。委派组的通用 `subagent`/`subagent_fork` 已换成三角色工具行（persona 与工具表逐项取自 `product.json.roleTools`），两个可选外部 spawner 行保持上游的 `disabled: true` 占位。新增 `tests/product-preset.test.mjs` 5 项，按**生效状态**判定（`disabled: true` 的行不算数）：委派组恰好是三个角色工具、工具表与 `product.json` 逐项一致、无任何通用或外部 spawner 处于启用、宿主机 shell 与厂商扇出编排必须保持关闭、persona 不得再自称编码 Agent。

- **把 preset 里唯一一条跨线不可解析的行关掉（两代安装都验证）**：用同一份发现代码路径分别对 **0.1.2 安装**与 **0.1.5 安装**跑产品 preset，0.1.2 上它被判 `broken`——`row "present" names a plugin that cannot be resolved: @deepseek-ai/dsh-tool-present`（该插件只存在于 0.1.5）。`present` 本就在平台白名单之外、实测不在主管的 57 个工具里，对产品无效；但它会让"从源码树直接跑产品"（当前安装仍是 0.1.2）时默认 preset 整个崩掉，代价与收益不对等，因此该行改为 `disabled: true` 并在文件内写明理由。修后两代安装的名单都是 5 项且 `broken` 为空，三条委派行与工具表不变。测试新增一项，防止这行被静默重新启用（主树与 0.1.5 副本各 128 项通过）。该改动使候选 `754daa94e9a1fc4c` 过期，已重新 `prepare` 出候选 **`6a6734e9cbc0802c`** 并复验：冻结快照的 preset 与 profile 与源码逐字节一致（`present` 行 `disabled: true`）、启动交付日志与交付目录/标记就位、主管仍是 57 个工具（三个角色工具、无 `subagent`/shell）、数据助手子会话首个请求仍是 21 个工具且与 `product.json.roleTools` 逐项一致——即这一行对产品行为零影响，实测结论由该候选承载。
- **浏览器与产品接口验收重新绑到当前候选 `6a6734e9cbc0802c`**（预览实例 8513，普通用户 preview_user，逐项 0 控制台错误、0 失败请求）：① 原生右侧栏「文件」标签列出产品分组（上传的文件 / 分析结果 / 共享数据（只读）/ 过程记录），展开后看到上传件，点击即渲染 Markdown 预览，网络事实为 `session.cwd → fs.tree ×3 → GET /sidebar/file`；② 通过界面附件控件上传：坞里显示 `upload-sample.md / MD 63B`，唯一请求为产品的桥 `POST /api/upload/native?sessionId=…&name=upload-sample.md`，工作区「上传的文件」列出该件，账号面板显示用量；③ PDF 文档预览打开且无错误文本（PDF.js 的 `worker-src 'self' blob:` 生效）；④ **上传件确实进入研究流程**：走产品自己的路由（`POST /geo/api/projects` → `/chats` → `POST /api/upload/native?sessionId=<该会话>&name=read-note.md` → `POST /geo/api/chats/<id>/prompt`）——上传回执为产品的原生信封（`receiptId` = `.dsh-uploads/<chatId>/<前缀>-read-note.md`），智能体 10 秒内的工具轨迹为 `glob → geo_list_files → read_document → read` 并原样答出文件中的口令。**顺带的一条正面观察**：当工作区里确实没有那个文件时（一次脚本把上传与提问放进了不同会话），主管用 `glob`/`geo_list_files` 查证后如实回报「本会话没有该文件」，没有编造内容。
- **三个"看起来像产品缺陷、其实是验收脚本"的坑，已写进脚本注释**（避免下一轮重复误判）：`fill()` 不会触发受控输入状态更新；**Enter 不提交**，这个产品的发送控件是 `发送消息` 按钮（探针实测：点击后 `/geo/api/chats/<id>/prompt` 发出、草稿清空）；文件面板打开后页面上第一个 `textarea` 已不是对话框输入区。另外两条环境事实：预览登录链接十分钟有效（过期返回 403），全新预览账号的侧栏只有「新建项目」且未选项目时对话框是禁用态。据此新增 `read-upload-api.py`（全接口验证上传→读取），`pdf-and-read-flow.py` 的发送步骤改为按发送按钮定位输入区。
- **文件面板的实时变更流按官方契约实现，但运行时尚未确认被消费（候选 `694918a5a2cb775c`）**：原生 `@deepseek-ai/dsh-api-workspace-files` 会拉 `remote.workspaceFiles.changes(sessionId, signal)`，期望的是一串帧：先 `{kind:"ready"}`（消费者在它上面调用 `accept()`），随后 `{kind:"change", change:{absolutePath, version}}` 表示文件出现或变化、`{absolutePath, absent:true}` 表示已消失；线上协议把每一帧包成 `{value, accept}`，因此**迭代结果的值必须是这个信封**（不是帧本身）。产品没有普通用户的文件观测源（宿主的观测只覆盖它自己instrumented的写入，用户工作区走 `/sidebar/api/fs.tree`），所以实现为**派生流**：打开该会话自己的事件流，有动静就提前唤醒，重新读一次分组清单并做差分；另有 15 秒的安全轮询兜住非智能体造成的改动；只走会话自己的视图，管理员的只读库（共享数据（只读））跳过，目录数与文件数都有上限。关键设计点：**订阅从调用那一刻就是活的**，用队列而不是 `async function*`——异步生成器只在被拉取时推进，拉取之间发生的写入会被并进下一次基线而静默丢掉。新增 `tests/native-watch.test.mjs` 两项，用真实命名空间驱动（ready → 新增/改动/删除帧、取消后结束并关闭事件流、只读分组不被遍历、标签与宿主 `SHARE_LABEL` 不许漂移）；它当场抓到实现里的两个真错误（信封层级写错、跳过判断漏了目录自身路径）。`native-client.test.mjs` 里"变更订阅不可用"的旧断言随之改写（并显式取消订阅——留着会让 15 秒定时器把测试进程挂住）。主树与 0.1.5 副本各 **130 项通过**。**运行时未确认**：在预览实例上按可用路径打开文件面板与文件预览（0 控制台错误、0 失败请求），随后 22 秒静默窗口内与一次智能体写入期间，**都没有观察到变更流自身发出的 `fs.tree` 请求**，因此无法断定原生消费者是否长期持有该订阅、也就不能声称实时刷新已经生效。下一步要定位的是消费者侧：临时在叠加层的 `changes` 调用点打点，确认它是否被调用、被调用后多久被取消，再据此调整（例如消费者若每次失效只开一次流，则要让"打开即就绪"的开销足够小）。
- **上一轮"运行时未确认"的答案：缺的不是 `changes`，而是传输层的 `$stream`（候选 `7cfe078968b06264`）**。读消费者代码后确认：原生资源提供者**从不直接调用** `workspaceFiles.changes`，而是把它包在传输层流原语里——`remote.$stream({ name, open: (signal) => openAfter(after, () => remote.workspaceFiles.changes(sessionId, signal)), ended })`，然后 `for await (const item of this.stream)` 读 `item.value` / 调 `item.accept()`，最后一个跟随者离开时调 `dispose()`（同一会话的下一条流会 `await` 它的结果再打开）。产品叠加层此前只提供了 `$host`/`$on`，**没有 `$stream`**，所以变更流永远不可达——这正是上一轮 22 秒静默窗口里一次请求都没有的原因。全库核对：`$stream` 的使用者只有 `dsh-api-workspace-files` 处于引导清单内（会话控制器与工作区控制器都在不引导之列），因此按这套契约实现即可。
- **顺带挖出一个不限于变更流的真缺陷：会话被自动改标题后，文件面板会整体失效**。虚拟根是 `/工作区/<会话标题>`，而产品用首轮消息给会话自动起标题——于是根在会话中途就变了；叠加层把根缓存到会话结束，后续每次 `fs.tree` 都被宿主以 **403「文件路径超出工作区」** 拒绝（运行时实测：静默期一次列出成功、首轮之后的三次列出全 403，每次节拍都产生 3 个控制台错误）。修法两处：变更流**每次节拍重读根**（正是因此才能看到它按新根工作）；文件面板的 `list`/`stat`/`read`/`readAll` 走一个包装器，失败时清缓存重读根并**重试一次**，另给根缓存加 10 秒 TTL。**仍未处理的一点**：面板若持有旧绝对路径（改标题前打开的地址），`readRelated` 仍会以「超出工作区」失败，需重新展开目录取得新地址——已在注释里写明，不做静默猜测。
- **重复失败的列表现已退避并止损**：列举持续失败（预览链接过期回 403、会话结束回 404）时，此前会每 15 秒重试一次、无限期地每次留下一个控制台错误与一次无用请求（运行时实测到该现象）。改为连续失败时指数退避（15s→30s→60s→120s 封顶），连续 8 次后自行结束。回归测试用假定时器把节拍压到毫秒级，断言重试次数有上界。
- **变更流在运行中的候选上验证通过**（`7cfe078968b06264`，预览实例 8513，普通用户）：打开文件面板与文件预览后，静默 22 秒内看到 **5 次**侧栏请求（1 次根重读 + 4 次目录列举，即一次节拍），智能体写入期间看到 **17 次**分三批（每次唤醒 = 根重读 + 4 次列举），**控制台错误 0、失败请求 0**。这说明：原生消费者确实通过 `$stream` 订阅了这条流，流也确实按事件唤醒。**仍要说明的边界**：我没有直接观察到"打开的预览内容自己刷新"这一可见效果，只观察到流被订阅、按事件运行且无错误——因此把这条能力记为"已接通并验证到订阅与帧的产生"，不写成"实时刷新已被肉眼确认"。
- **可见的自动刷新已直接观测到，旧地址也会重定位（候选 `c5f518a3c6f651e6`）**：上一轮只验证到"消费者订阅了流"，本轮把它推到底。① 用产品自己的路径（界面建项目、桥上传、接口发问）让智能体在 `outputs/` 写文件，在原生右侧栏打开它的预览；再让智能体**覆写同一个文件**（新内容长度不同——帧里的 `version` 是字节长度，长度相同会被消费者当成"已经持有"而跳过），随后**不做任何点击**地轮询页面文本：**约 4 秒后打开的预览自己变成了新版本**，控制台错误 0、失败请求 0。这条能力至此端到端闭合：`$stream` → `changes` → 派生差分 → 资源提供者 → 文档预览重绘。② 卸载缺口：面板若持有改标题**之前**发出的绝对地址（`/工作区/<旧标题>/…`），现在会**重定位**到当前根——宿主把根写成 `/工作区/${safeSegment(标题)}`（标题已安全化，恒为一段），剥掉那一段即可保留同一分组/文件的后半段；不满足该形状的地址（如 `/etc/passwd`）仍被拒绝。测试相应加了三项断言（旧地址重定位到新根、非工作区形状仍被拒、改标题后 list 自愈）。
- **验收记录重新绑到最新候选 `c5f518a3c6f651e6`（普通用户，预览实例 8513；带浏览器的项逐项 0 控制台错误、0 失败请求）**：① 原生右侧栏「文件」标签列出产品分组、展开后看到上传件、点击即渲染 Markdown 预览（`session.cwd → fs.tree → GET /sidebar/file`）；② 界面附件控件上传：坞里显示 `upload-sample.md / MD 63B`，唯一请求是产品的桥 `POST /api/upload/native`，工作区列出该件，账号面板显示用量；③ **PDF 文档预览**（它走的 `read`/`readAll` 正是本轮改过的路径）打开且无错误文本；④ 上传件进入研究流程：智能体 10 秒内的轨迹为 `glob → geo_list_files → read_document → read` 并原样答出文件中的口令；⑤ 上一轮同一候选上已实测**打开的预览在智能体覆写后约 4 秒自行刷新**、无点击。**顺带把分步方案流也在同一候选上完整跑通**：主管先 `exit_plan_mode` 提交方案，再 `ask_user_question` 抛出三道带选项与说明的确认题（研究区 / 年度口径 / 交叉核对），按产品自己的契约作答（`{requestId, answer:{answers:[{id, selected:[选项标签]}]}}`——自由文本会被 400 拒绝，第一次就是这么错的）后返回 `{accepted:true}`、pending 清空、**约 5 秒后智能体在本轮继续**（事件 45→48，`running: true`）。
- **一条如实记录的观察（不是缺陷断言）**：上述方案流里，主管在提交方案与征询确认**之前**已经调用了 `web_search`/`web_fetch`/`geo_download_gee`/`geo_download_boundary`。产品 persona 要求多步骤研究"先建立待确认的方案再结束本轮"，所以顺序上是偏离的；它没有造成权限或数据越界（下载走的是既定入口与配额），但是否要在提示或流程上收紧，留给管理员判断，本轮不改源码。
- **仍存的缺口**：① 已闭合（候选 `6a6734e9cbc0802c` 实测主管 57 工具无 `subagent`、数据助手首个请求即 21 项角色表，见上一条）；② 原生会话列表的分组交互（重命名/归档/拖拽排序/搜索）由 `ui-workspace` 提供，产品用自己的列表替代后这些交互仍是产品自己的实现（管理入口在行的齿轮按钮里）；③ 文件面板的实时变更流**已闭合**（契约补全、根失效与旧地址均已修，可见的自动刷新在运行中的候选上实测约 4 秒生效、无错误）；④ 正式发布尚未执行，生产仍是 `9e061099f3ccf519`，且主树依赖树仍是 0.1.2（`check-env` 报 `DSH pin` 与 `Resolved DSH dependency graph` 两条失败、217 个包不匹配），升级流程见 [dsh/RELEASES.md](dsh/RELEASES.md) 的「升级 DSH 内核依赖」——升级与发布都会替换当前管理员会话所依赖的运行时，因此等明确许可后按序执行。
- 单元测试 133 项通过（主树与 0.1.5 副本各一遍）；`prepare → preview` 在副本内通过；生产版本未改动。

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
