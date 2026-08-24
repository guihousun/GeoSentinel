<h1 align="center">地缘环境智能计算平台</h1>

<p align="center">
  <strong>GeoSentinel: A Local-First Geoenvironmental Intelligence Platform</strong>
</p>

<p align="center">
  <a href="https://geointer.geosetting-ai.com/"><img alt="Live demo" src="https://img.shields.io/badge/live_demo-geointer.geosetting--ai.com-1b6b50?style=flat-square"></a>
  <img alt="Python 3.11" src="https://img.shields.io/badge/python-3.11-3776AB?style=flat-square&logo=python&logoColor=white">
  <img alt="FastAPI Web" src="https://img.shields.io/badge/FastAPI-Web-009688?style=flat-square&logo=fastapi&logoColor=white">
  <img alt="Google Earth Engine" src="https://img.shields.io/badge/Google_Earth_Engine-ready-4285F4?style=flat-square&logo=googleearthengine&logoColor=white">
  <img alt="Status" src="https://img.shields.io/badge/status-research_preview-f59e0b?style=flat-square">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="docs/README.md">Documentation</a> ·
  <a href="docs/mcp/ntl-gis-core.md">GIS MCP</a> ·
  <a href="docs/mcp/ntl-download.md">Download MCP</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

GeoSentinel（地缘环境智能计算平台）是一个本地优先的地缘环境研究工作台。它以 NTL-GPT 研究运行时为底座，整合任务式智能体协同、空间证据、事件监测、Google Earth Engine、官方 VIIRS/Earthdata 数据获取、GIS 工具、本地 RAG 与隔离线程工作区。正式产品界面为 `web/` 与 `web_api.py` 提供的同源 Web 工作台；Streamlit 仅保留为内部诊断和过渡入口。

> 平台仍在持续迭代。任何研究或业务判断均应结合来源、时间范围、方法条件与人工复核。

工作台将研究问题、智能体对话、运行进度、空间地图、公共监测队列、资料与分析产出组织在同一条可追溯的研究链中。

## What It Does

| Capability | Included workflows |
|---|---|
| NTL data retrieval | NPP-VIIRS, NPP-VIIRS-like, DMSP-OLS, VNP46A1, VNP46A2, SDGSAT-1 |
| Cloud processing | GEE catalog planning, direct export, batch export, server-side reduction |
| Official acquisition | Earthdata/CMR HDF5 retrieval, country or BBox download, mosaicking, progress and audit manifests |
| Local GIS | Validation, reprojection, clipping, mosaicking, zonal statistics, NTL metrics, trend and anomaly analysis |
| Agent workflows | Data Searcher, optional Code Assistant review, and NTL Engineer orchestration |
| Research runtime | Account login, PostgreSQL history, concurrent runs, cancellation, quotas, and thread isolation |
| Interoperability | Local stdio MCP services for GIS and download operations; EasyGEE and QGIS handoffs |

## Architecture

```mermaid
flowchart LR
    WEB["Formal public web workbench"] --> API["web_api.py / SSE"]
    API --> ENG["NTL Engineer"]
    STREAMLIT["Streamlit internal debug UI"] --> ENG
    ENG --> SEARCH["Data Searcher"]
    ENG -. optional review .-> CODE["Code Assistant"]
    SEARCH --> GEE["Google Earth Engine"]
    SEARCH --> EARTH["NASA Earthdata / CMR"]
    ENG --> TOOLS["Runtime GIS and NTL tools"]
    TOOLS --> MCP["ntl-gis-core / ntl-download MCP"]
    API --> STORE["PostgreSQL history"]
    API --> WORK["Per-thread workspace"]
    WORK --> INPUTS["inputs/"]
    WORK --> OUTPUTS["outputs/"]
    WORK --> MEMORY["memory/"]
```

平台在本地主机运行地理计算代码与工具，以保留 Earth Engine 凭据、GDAL/PROJ 库、本地 RAG、共享参考数据和线程工作区。它采用按线程隔离的本地子进程模型，不是托管式远程沙箱。

## Quick Start

### 1. Create the environment

Windows PowerShell:

```powershell
git clone https://github.com/guihousun/GeoSentinel.git GeoSentinel
Set-Location .\GeoSentinel
conda env create -f environment.yml
conda activate GeoIntelligence
Copy-Item .env.example .env
```

macOS or Linux:

```bash
git clone https://github.com/guihousun/GeoSentinel.git GeoSentinel
cd GeoSentinel
conda env create -f environment.yml
conda activate GeoIntelligence
cp .env.example .env
```

### 2. Configure services

Set these values in `.env`:

```env
DeepSeek_API_KEY=your_key
DeepSeek_Coding_URL=your_openai_compatible_endpoint
DASHSCOPE_API_KEY=your_key
DASHSCOPE_Qwen_plus_KEY=your_key
DASHSCOPE_Qwen_plus_URL=your_openai_compatible_endpoint
```

For GEE and official Earthdata downloads, also configure:

```env
GEE_DEFAULT_PROJECT_ID=your_gee_project
EARTHDATA_TOKEN=your_earthdata_token
```

Never commit `.env`, API keys, database passwords, Earthdata tokens, or Earth Engine credentials.

### 3. Validate and launch

```powershell
python check_env.py
python -m streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

Open [http://127.0.0.1:8501](http://127.0.0.1:8501) for the internal Streamlit UI.

### Formal Public Web Workbench

The geopolitical web workbench reuses the same PostgreSQL users, task threads, and workspace artifacts, but runs as a separate public ASGI service:

```powershell
python run_web.py --host 127.0.0.1 --port 8502
```

For the 花生壳（Oray） public mapping, use [geointer.geosetting-ai.com](https://geointer.geosetting-ai.com/) and follow [the deployment guide](docs/deployment/geoenvironmental-web-service.md). Set `NTL_WEB_SESSION_SECRET` and `NTL_WEB_COOKIE_SECURE=1` before any HTTPS deployment.

## Configuration

| Group | Variables |
|---|---|
| Frontend models | `DeepSeek_API_KEY`, `DeepSeek_Coding_URL` |
| Internal VLM and RAG | `DASHSCOPE_API_KEY`, `DASHSCOPE_Qwen_plus_KEY`, `DASHSCOPE_Qwen_plus_URL`, `NTL_VLM_MODEL` |
| Embeddings | `NTL_EMBEDDING_PROVIDER`, `NTL_EMBEDDING_MODEL`, `NTL_EMBEDDING_BASE_URL`, `NTL_EMBEDDING_DIMENSIONS`, `NTL_EMBEDDING_API_KEY` |
| Data services | `GEE_DEFAULT_PROJECT_ID`, `EARTHDATA_TOKEN`, `amap_api_key` |
| Persistence | `NTL_HISTORY_DB_URL`, `NTL_LANGGRAPH_POSTGRES_URL` |
| Runtime limits | `NTL_MAX_ACTIVE_RUNS`, `NTL_MAX_ACTIVE_RUNS_PER_USER`, `NTL_THREAD_WORKSPACE_QUOTA_MB`, `NTL_USER_WORKSPACE_QUOTA_MB` |
| Paths and MCP | `NTL_USER_DATA_DIR`, `NTL_SHARED_DATA_DIR`, `NTL_CONTEXTILY_TMP`, `NTL_MCP_ENV_FILE`, `NTL_MCP_WORKDIR`, `NTL_MCP_STATE_DIR` |
| UI compatibility | `NTL_FORCE_NATIVE_CHAT_INPUT`, `NTL_USE_CUSTOM_MULTIMODAL_CHAT_INPUT` |
| Public web service | `NTL_WEB_SESSION_SECRET`, `NTL_WEB_COOKIE_SECURE`, `NTL_WEB_ALLOWED_HOSTS`, `NTL_WEB_HOST`, `NTL_WEB_PORT`, `NTL_WEB_MAX_UPLOAD_MB` |

See [`.env.example`](.env.example) for the maintained template and run `python check_env.py` after every configuration change.

### PostgreSQL

PostgreSQL is recommended for multi-user and production deployments:

```env
NTL_HISTORY_DB_URL=postgresql://ntl_gpt:your_password@127.0.0.1:5432/ntl_gpt
NTL_LANGGRAPH_POSTGRES_URL=postgresql://ntl_gpt:your_password@127.0.0.1:5432/ntl_gpt
```

```sql
CREATE USER ntl_gpt WITH PASSWORD 'your_password';
CREATE DATABASE ntl_gpt OWNER ntl_gpt;
GRANT ALL PRIVILEGES ON DATABASE ntl_gpt TO ntl_gpt;
```

Local PostgreSQL and Docker PostgreSQL use the same URL format. Use `127.0.0.1` when Streamlit and PostgreSQL are exposed on the same host; use the Compose service name only when both services share a Docker network.

## Local MCP Services

The repository includes two standalone stdio MCP servers. They do not require the Streamlit application or PostgreSQL.

| Service | Purpose | Guide |
|---|---|---|
| `ntl-gis-core` | Deterministic vector, raster, NTL metrics, zonal statistics, trend and anomaly operations | [Setup and tools](docs/mcp/ntl-gis-core.md) |
| `ntl-download` | Explicit GEE exports, batch-task tracking, VNP46A1/VNP46A2 Earthdata downloads and recovery manifests | [Setup and tools](docs/mcp/ntl-download.md) |

Install both through the editable `packages/ntl_toolkit` package included in `environment.yml`. External MCP clients should use a dedicated work directory rather than the Streamlit `user_data` tree.

## Workspace Model

Every conversation thread receives an isolated workspace:

```text
user_data/<thread_id>/
├── inputs/     # uploads and retrieved source data
├── outputs/    # generated rasters, vectors, tables and figures
└── memory/     # thread-local runtime state and failed-run records
```

One run is allowed per thread. Different threads may run concurrently subject to global and per-user limits. Generated paths are resolved through `storage_manager.py`, and shared `base_data` is treated as read-only source data.

## Repository Layout

```text
GeoSentinel/
├── Streamlit.py              # internal Streamlit transition/debug entrypoint
├── web_api.py                # formal public API and static frontend service
├── web_runtime.py            # framework-independent run lifecycle for public API
├── run_web.py                # public web launcher (defaults to 127.0.0.1:8502)
├── web/                      # formal geopolitical web workbench
├── app_ui.py                 # interface and result rendering
├── app_logic.py              # run lifecycle and event streaming
├── graph_factory.py          # agent graph and tool routing
├── agents/                   # agent prompts and definitions
├── tools/                    # runtime geospatial and NTL tools
├── packages/ntl_toolkit/     # reusable GIS/download core
├── mcp_servers/              # local stdio MCP entrypoints
├── .ntl-gpt/skills/          # runtime workflow skills
├── RAG/                      # local knowledge indexes and references
├── tests/                    # application regression tests
├── evaluations/              # MCP evaluation fixtures
├── assets/                   # README/UI assets and runtime models
└── docs/                     # deployment and MCP documentation
```

## Documentation

- [Documentation index](docs/README.md)
- [Windows Server deployment and operations](docs/deployment/windows-server.md)
- [`ntl-gis-core` MCP](docs/mcp/ntl-gis-core.md)
- [`ntl-download` MCP](docs/mcp/ntl-download.md)
- [Contributing guide](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Development

Use the project environment for all checks:

```powershell
conda activate GeoIntelligence
python -m py_compile Streamlit.py app_logic.py app_agents.py graph_factory.py
python -m pytest tests -q
python -m pytest packages/ntl_toolkit/tests -q
```

Changes to routing or tools should test the target prompt and at least one neighboring scenario. New paths must remain inside the active thread workspace, and secrets or generated user data must never be committed.

## Contributing

Issues and focused pull requests are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing changes, especially for agent routing, execution safety, storage paths, or dataset semantics.

For vulnerabilities or accidental credential exposure, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

### 地缘事件监测

Web 研究工作台可选启用后台事件监测：GDACS、NASA EONET 和可限流降级的 GDELT 公开索引按 30 分钟窗口采集。每条进入地图或监测队列的事件都保留在项目本地全局工作区，并带有来源 URL、时间与采集状态；专用 DeepAgent 只会在来源候选存在时选择受限的研判问题模板，不会编造事件或坐标。详细部署参数见 `docs/deployment/geoenvironmental-web-service.md`。
