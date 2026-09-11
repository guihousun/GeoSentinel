# 插件（`dsh/plugins/`）

本目录是 GeoSentinel 的产品插件，也是 DSH 里"一行 row = 一个能力"的落点。每个子目录就是
一个插件包，靠**一个 composition 行**挂载；行里写的是包名，包名由所在**面**的运行时链接到
本目录：

| 插件 | 包名 | 职责 | 挂载它的面 |
| --- | --- | --- | --- |
| `platform/` | `@geosentinel/dsh-platform` | 邀请与账号、SQLite 元数据、访问校验、工作区与 HTTP API、监测接入、发布服务、共享数据围栏 | 产品 `profile/cordis.patch.yml`；管理员面同样挂载 |
| `research/` | `@geosentinel/dsh-research` | 研究资料的读取与围栏、固定 GEE 获取、隔离 Docker GIS 计算、证据与报告 | 产品 `profile/cordis.patch.yml` |
| `workbench/` | `@geosentinel/dsh-workbench` | 中文界面适配：原生侧栏/右侧栏、项目与对话、全球事件监测、空间数据、简报、主题 | 产品 `profile/cordis.patch.yml` |
| `developer/` | `@geosentinel/dsh-developer` | 管理员开发面：只读就绪检查与开发侧辅助（普通用户不可见） | 仅管理员面：`scripts/admin-start.mjs`、`development/worker.mjs` |

## 挂载契约

1. **行写在 composition 里，不写在插件里。** 产品面是 [`../profile/cordis.patch.yml`](../profile/cordis.patch.yml)；
   管理员面由 [`../development/worker.mjs`](../development/worker.mjs) 与
   [`../scripts/admin-start.mjs`](../scripts/admin-start.mjs) 生成 profile 后追加行。
2. **包名 = 插件目录名。** `@geosentinel/dsh-<目录名>`，由面的运行时链接成模块：
   产品面见 [`../release/runtime.mjs`](../release/runtime.mjs)（platform/research/workbench），
   开发面见 [`../development/worker.mjs`](../development/worker.mjs)（另加 developer）。
   改包名或改目录名会同时打断链接与行，两边必须一起改。
3. **宿主与预设的归属**：注册服务、提供跨会话能力（持久化、沙箱、模型路由、子智能体后端）的行属于
   宿主 composition；只属于某一次会话的（工具、人设、提示段落）属于 agent preset，
   见 [`../AGENTS.md`](../AGENTS.md)。

## 一个插件包必须长成的样子

```
plugins/<name>/
  package.json   { name: "@geosentinel/dsh-<name>", private: true, type: "module",
                   main: "index.mjs",                    // 服务端半边
                   exports: { ".": "./index.mjs",        // 有浏览器半边时再加 "./client"
                              "./client": "./client.js" },
                   dsh: { client: { platform: "web" } } } // 仅客户端插件需要
  index.mjs      export const name = "geosentinel-<name>";   // 诊断标签，四个插件统一这个短名
                 export const inject = ["webServer", …];  // 硬依赖才写；可选服务用 ctx.get()
                 export function apply(ctx, config) { … }
  <模块>.mjs     实现；一个能力一个文件，窄接口
  client.js      仅当插件有浏览器半边（UI、槽位、主题）
```

规则（`dsh/tests/plugin-conventions.test.mjs` 会检查前四条，其余靠评审）：

- **导出形状**：`package.json` 的 name、目录名与 composition 行三者的包名必须一致；`index.mjs`
  导出的 `name` 稳定为 `geosentinel-<目录名>`（诊断标签，与解析无关）；`apply` 必须导出；
  `inject` 若存在必须是服务名数组，不得写成路径。
- **副作用必须可回收**：服务、事件、工具、定时器、槽位、样式、主题全部经 `ctx.effect()` /
  `ctx.on()`（或返回 disposer 的官方 API）注册，停止或更新时能干净移除。
- **不读不写别人的源码**：绝不修改 `node_modules` 里的第三方源码；需要改写行为时，在
  `workbench/native-host.mjs` 这类**服务端补丁点**显式改写并在版本变化时大声失败（现有例子：
  `dsh-file-upload` 与原生上传客户端的补丁）。
- **复用原生优先**：先找 DSH 的槽位、组件、服务与交互契约；产品特有适配保持窄，并且不绕过
  账号/项目归属、版本绑定的方案审批与沙箱策略（见 [`../../AGENTS.md`](../../AGENTS.md)）。
- **不接触凭据**：凭据只由管理员 `.env` 提供，插件不得打印、落盘或随发布快照带走。
- **失败要响**：缺配置、缺镜像、版本不匹配都不要静默降级；能给出可执行的修复提示。

## 加一个插件的清单

1. 建目录与 `package.json`（名字见上），实现 `index.mjs`（+ 需要时的 `client.js`）。
2. 在**所属面**的 composition 里加一行 `{ id: <id>, name: "@geosentinel/dsh-<name>" }`；
   产品面还要把包名加进 [`../release/runtime.mjs`](../release/runtime.mjs) 的链接表。
3. 在 [`../tests/`](../tests) 下加 `node:test` 用例；用户可见行为另写进 `../README.md` 或对应主题文档。
4. 记住冻结范围：`dsh/plugins/**` 属于发布冻结范围，改完必须走一次发布
   （`node tools/release.mjs prepare|preview|publish`，见 [`../RELEASES.md`](../RELEASES.md)）；
   只改文档或 `dsh/tools/**` 不需要发布。
