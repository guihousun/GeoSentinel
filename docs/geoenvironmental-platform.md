# 地缘环境智能计算平台：当前工程总览

更新日期：2026-08-24

本文档描述当前正式 Web 平台的实际工程边界。它不是产品规划或演示稿；部署、改动和验收应以本文件、源码与环境检查结果为准。

## 1. 正式产品入口

当前正式界面是同源 Web 工作台，而不是 Streamlit：

```text
浏览器
  -> http://127.0.0.1:8502（本地）
  -> 花生壳 HTTPS 映射（公网）
  -> run_web.py / web_api.py
  -> web/ 静态前端 + Web API + 既有智能体运行时
```

启动本地验收服务：

```powershell
Set-Location D:\NTL-GPT-Geopolitics
conda activate GeoIntelligence
python run_web.py --host 127.0.0.1 --port 8502 --dev-http
```

公网运行时不使用 `--dev-http`，并保持服务仅绑定 `127.0.0.1`。花生壳（Oray）负责 HTTPS 映射；完整部署参数见 [Web 服务部署指南](deployment/geoenvironmental-web-service.md)。

## 2. 前端与后端代码边界

| 范围 | 位置 | 职责 |
| --- | --- | --- |
| 页面结构与样式 | `web/index.html`、`web/styles.css`、`web/web-runtime.css` | 登录、研究工作台、地图、监测队列与专题页面的视觉呈现。 |
| 页面行为 | `web/app.js`、`web/web-runtime.js` | 地图图层、导航、认证后状态、任务对话、SSE 运行事件与监测队列刷新。 |
| 公共 API | `web_api.py` | FastAPI 服务、静态文件、认证、线程、文件、SSE 和监测接口。 |
| 任务运行时 | `web_runtime.py` | 将 Web 请求接入既有图、数据库、线程状态和隔离工作区。 |
| 启动入口 | `run_web.py` | Web 服务启动、主机与端口设置。 |
| 全局监测 | `monitoring/` | 来源采集、去重、受限监测代理、本地监测存储和状态快照。 |
| 研究智能体与工具 | `graph_factory.py`、`agents/`、`tools/`、`.ntl-gpt/skills/` | 用户研究任务的编排、技能、工具与输出。 |

`Streamlit.py`、`app_ui.py`、`app_logic.py` 等仍保留用于内部诊断和过渡兼容，不是当前公网产品 UI 的修改入口。

## 3. 两类运行状态必须分离

### 用户研究任务

- 一名用户在一个研究线程中提出问题。
- 聊天记录、账号和线程元数据由既有 PostgreSQL 存储层管理。
- 上传文件读取自该线程 `inputs/`；产生的文件写入该线程 `outputs/`；运行记忆写入该线程 `memory/`。
- 不允许跨线程读写，也不允许生成代码绕过 `storage_manager` 的路径解析。

### 全局事件监测

- 面向所有用户共享，不属于任何账号或研究线程。
- 由 `monitoring/` 定时采集公开来源，并通过 `/api/monitor/events` 提供地图和队列数据。
- 监测状态与事件仅存放在本地共享工作区：默认 `<NTL_USER_DATA_DIR>/_monitor`，可用 `NTL_MONITOR_DATA_DIR` 覆盖。
- 不写入 PostgreSQL 聊天记录，不可把监测事件伪装成用户任务产出。
- 监测代理只能在已有来源候选上选择受限严重性、研究焦点和显示文字；不得编造事件、坐标、来源或结论。

## 4. 当前 API 与运行检查

| 路径 | 用途 | 预期 |
| --- | --- | --- |
| `GET /api/healthz` | 服务健康检查 | `status` 为 `ok`。 |
| `GET /api/monitor/status` | 监测运行状态 | 启用时可见 `storage.mode=local_workspace`、最近一次运行状态和来源状态。 |
| `GET /api/monitor/events` | 地图和监测队列 | 仅返回有来源、时间和坐标的当前事件。 |

本地快速验证：

```powershell
Invoke-WebRequest http://127.0.0.1:8502/api/healthz -UseBasicParsing
Invoke-WebRequest http://127.0.0.1:8502/api/monitor/status -UseBasicParsing
```

## 5. 环境配置边界

使用 `GeoIntelligence` Conda 环境。不要修改 `NTL-GPT-Stable` 环境。

以下变量只在 `.env` 中配置，绝不提交：

- Web 会话：`NTL_WEB_SESSION_SECRET`、`NTL_WEB_COOKIE_SECURE`、`NTL_WEB_ALLOWED_HOSTS`
- 数据库：`NTL_HISTORY_DB_URL`、`NTL_LANGGRAPH_POSTGRES_URL`
- 模型与内部能力：`DeepSeek_API_KEY`、`DeepSeek_Coding_URL`、DashScope 相关变量
- 监测：`NTL_MONITOR_ENABLED`、采集周期、来源开关及可选 ACLED OAuth 凭据

变量增删时必须同步更新 `.env.example`、`check_env.py`、根目录 `README.md` 和部署指南。

## 6. Web 交付规则

- 中文是默认界面语言；通用事件类型应准确中文化，无法可靠翻译的专有名词保留来源原文。
- 地图是空间证据视图，不是装饰性 KPI 仪表盘；监测地图默认全球尺度，专题页面可切换到研究区尺度。
- 用户任务进度只显示在该用户的研究对话中；全局监测状态只显示在公共地图与监测队列中。
- 每个聊天气泡按内容自适应高度，不得用聊天面板的空白高度拉伸消息行。
- 在 `1366×768` 和 `1440×900` 优先验证主内容、文本对比度与无横向溢出；没有可靠来源的数据不作为“已发生”或“已验证”展示。

## 7. 相关文档

- [根目录 README](../README.md)：环境、智能体、工具和项目结构。
- [Web 服务部署指南](deployment/geoenvironmental-web-service.md)：Windows、花生壳 HTTPS、会话配置与排障。
- [UI 规范](geoenvironmental-ui-standards.md)：研究工作台、地图专题和认证页的设计与验收规则。
- [首期建设方案](planning/geoenvironmental-intelligence-platform-phase1.md)：范围、工期、报价和验收边界。
