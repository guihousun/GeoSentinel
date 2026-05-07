# GeoSentinel · 地缘环境智能计算平台

GeoSentinel（地缘环境智能计算平台）是一个面向冲突事件跟踪、地缘环境监测和夜间灯光证据分析的本地多智能体系统。

## 智能体架构

平台由 Supervisor Agent（Engineer）统一调度两个专业化 Subagent：

| Agent | 职责 |
|-------|------|
| **Engineer** | 任务分解、CoT 编排、脚本契约设计、工作流进化决策 |
| **Data Searcher** | 多源数据检索：GEE 影像、行政边界（geoBoundaries/Amap/OSM）、官方统计数据、事件信息 |
| **Code Assistant** | Python 地理空间脚本验证与执行：rasterio / geopandas / GEE API / Matplotlib |

## 核心能力

- ConflictNTL 事件获取：从 ISW/CTP StoryMap ArcGIS FeatureServer 抽取指定日期窗口内的事件点。
- 事件筛选：按时间、坐标精度、来源质量和 NTL 适用性生成候选队列。
- AOI 生成：为 exact/general-neighborhood 事件生成 2 km / 5 km buffer，并生成行政区 AOI 任务队列。
- 夜光分析衔接：面向 VNP46A2、官方 VJ-DNB fullchain、栅格统计、预览和 GIF 工具输出 handoff contract。
- 报告沉淀：生成 case report、manifest、runbook 和后续 manuscript 可用的证据表。

## 目录结构

- `Streamlit.py`：中文 Streamlit UI 入口。
- `graph_factory.py`：多智能体图构建、工具组装和 skill discovery。
- `tools/conflict_ntl.py`：ConflictNTL 事件获取、筛选、AOI、报告和 agent-system 工具。
- `tools/__init__.py`：精简后的公开工具导出列表。
- `.ntl-gpt/skills/conflict-ntl-workflow/`：ConflictNTL skill、筛选规则和输出契约。
- `docker-compose.postgres.yml`：本地 Postgres 服务配置。
- `user_data/`：线程工作区，运行时输入、输出和记忆均写入这里，默认不提交到 Git。
- `base_data/`：共享只读数据目录，默认不提交到 Git。

## Conda 环境配置

使用 `geoenv-intelligent-platform` 环境：

```powershell
conda activate geoenv-intelligent-platform
python -m pip install -U pip
python -m pip install -r requirements.txt
```

如果没有该环境，可以从 `environment.yml` 创建：

```powershell
conda env create -f environment.yml
conda activate geoenv-intelligent-platform
python -m pip install -U pip
```

如果使用 Postgres 持久化，还需要确认以下包已安装：

```powershell
python -m pip install langgraph-checkpoint-postgres psycopg[binary]
```

基础检查：

```powershell
python check_env.py
python -m py_compile Streamlit.py app_ui.py app_state.py app_logic.py app_agents.py graph_factory.py tools\conflict_ntl.py
```

## 环境变量

复制 `.env.example` 为 `.env`，然后填写必要凭证：

```powershell
Copy-Item .env.example .env
notepad .env
```

最低模型配置：

```text
DEEPSEEK_API_KEY=
DEEPSEEK_Coding_URL=https://api.deepseek.com
```

可选外部服务：

```text
GEE_DEFAULT_PROJECT_ID=
GOOGLE_OAUTH_CLIENT_ID=
GOOGLE_OAUTH_CLIENT_SECRET=
GOOGLE_OAUTH_REDIRECT_URI=http://localhost:8502
NTL_TOKEN_ENCRYPTION_KEY=
EARTHDATA_TOKEN=
```

不要提交 `.env`、token、GEE 凭证、Earthdata 凭证、用户工作区或下载数据。

## Docker Postgres 设置

本项目支持用 Postgres 持久化 LangGraph checkpoint、DeepAgents Store 和用户历史。开发环境可直接启动：

```powershell
docker compose -f docker-compose.postgres.yml up -d
docker compose -f docker-compose.postgres.yml ps
```

`.env` 中配置：

```text
NTL_LANGGRAPH_POSTGRES_URL=postgresql://geoenv:geoenv_dev_password@127.0.0.1:5432/geoenv_intel
NTL_HISTORY_DB_URL=
NTL_LANGGRAPH_POSTGRES_AUTO_SETUP=1
NTL_DEEPAGENTS_MEMORY_BACKEND=auto
NTL_MEMORY_NAMESPACE_SCOPE=thread
```

说明：

- `NTL_LANGGRAPH_POSTGRES_URL`：LangGraph checkpoint 和 StoreBackend 使用的 Postgres URL。
- `NTL_HISTORY_DB_URL`：用户、线程、聊天历史数据库；为空时复用 `NTL_LANGGRAPH_POSTGRES_URL`。
- `NTL_LANGGRAPH_POSTGRES_AUTO_SETUP=1`：启动时自动创建 LangGraph 所需 schema。
- `NTL_DEEPAGENTS_MEMORY_BACKEND=auto`：有 Postgres 时使用 StoreBackend，否则使用文件系统。

查看日志：

```powershell
docker compose -f docker-compose.postgres.yml logs -f postgres
```

停止服务：

```powershell
docker compose -f docker-compose.postgres.yml down
```

清空本地开发数据库：

```powershell
docker compose -f docker-compose.postgres.yml down -v
```

生产部署时请修改 `POSTGRES_PASSWORD`，并使用 secret manager 或部署平台的环境变量配置，不要使用示例密码。

## 启动平台

使用 `geoenv-intelligent-platform` 环境启动：

```powershell
conda run -n geoenv-intelligent-platform python -m streamlit run Streamlit.py --server.port 8502
```

或在已激活环境中：

```powershell
streamlit run Streamlit.py --server.port 8502
```

访问：

```text
http://127.0.0.1:8502
```

## ConflictNTL 主链

```text
事件源 / ISW
  -> conflict_ntl_fetch_isw_events_tool
  -> conflict_ntl_screen_events_tool
  -> conflict_ntl_generate_analysis_units_tool
  -> VNP46A2 / 官方 VJ-DNB / 栅格统计 handoff
  -> conflict_ntl_build_case_report_tool
```

公开 ConflictNTL 工具：

- `conflict_ntl_fetch_isw_events_tool`
- `conflict_ntl_screen_events_tool`
- `conflict_ntl_generate_analysis_units_tool`
- `conflict_ntl_build_case_report_tool`
- `conflict_ntl_agent_system_tool`

解释原则：夜间灯光异常只能作为观测证据，不能单独确认袭击、损毁、责任方或因果归因。

## 常见验证命令

```powershell
python -m unittest tests.test_conflict_ntl_screening tests.test_conflict_ntl_analysis_units tests.test_conflict_ntl_case_report tests.test_conflict_ntl_agent_system_tool tests.test_conflict_ntl_isw_fetch
python -m py_compile tools\__init__.py tools\conflict_ntl.py
python check_env.py
```
