# AGENTS.md

## Project Snapshot

The current release surface is the DSH product under `dsh/`. Start with `dsh/AGENTS.md`, `dsh/README.md` and `docs/migration-to-dsh.md`. The Python/FastAPI/Streamlit map below is retained as the legacy capability base, not the current recommended deployment. Do not conflate its PostgreSQL history and subprocess execution with DSH's SQLite/session storage and Docker workers.

GeoSentinel（地缘环境智能计算平台）is a local-first geoenvironmental intelligence platform built on the NTL-GPT research runtime. Its formal product surface is the FastAPI-served `web/` workbench; it combines task-based agent orchestration, spatial evidence, geopolitical event monitoring, Google Earth Engine and VIIRS workflows, local geospatial processing tools, and local RAG assets. Streamlit remains an internal diagnostic and transition entrypoint.

### Repository Map
- `dsh/`: opt-in DSH product runtime, isolated from the legacy entrypoints. Read [`dsh/AGENTS.md`](dsh/AGENTS.md) and [`dsh/README.md`](dsh/README.md) before changing its managed plugins or Docker boundary.
- `Streamlit.py`: application entrypoint.
- `web_api.py`, `web_runtime.py`, `run_web.py`, `web/`: formal same-origin public web service; it reuses the existing database, graph and isolated thread workspaces. Streamlit remains the internal transition/debug entrypoint.
- `monitoring/`: global event-monitoring service and local shared monitor store. It is independent of user threads and PostgreSQL chat history; it feeds the public map and monitoring queue through `/api/monitor/*`.
- `app_ui.py`: Streamlit layout, sidebar, uploader, download center, and chat rendering.
- `app_state.py`: session defaults, model/runtime settings, and UI state.
- `app_logic.py`: run lifecycle, event streaming, cancellation, stale-run recovery, output collection, and chat history writes.
- `app_agents.py`: Streamlit-cached graph wrapper. Keep its public signature stable for UI callers.
- `graph_factory_v2.py`: active Deep Agents 0.7.5 graph construction, role-scoped skills and permissions, typed handoffs, local backend routing, runtime memory, and supervisor prompt assembly. `graph_factory.py` is retained only as a transition reference.
- `agents/`: role prompts and stable role metadata. The public role names are `地缘分析师`, `数据助手`, `分析助手`, and `事件助手`; internal `NTL_*` identifiers remain for persisted-runtime compatibility.
- `contracts/`, `orchestration/`: typed TaskPlan, observation/event/analysis/evidence packages, artifact identity, route state, transfer records, and run-evidence infrastructure.
- `tools/`: domain tools for retrieval, GEE, VIIRS, preprocessing, statistics, rendering, and knowledge-base access.
- `storage_manager.py`: canonical workspace, input/output, memory, and shared-data path resolution.
- `history_store.py`: persistent chat, turn summaries, and injected-context records.
- `file_context_service.py`: uploaded file parsing and context extraction.
- `.ntl-gpt/skills/`: Deep Agents runtime skills used by the graph.
- `RAG/`: local knowledge/code/literature indexes and reference assets.
- `check_env.py`, `.env.example`, `environment.yml`: environment bootstrap and readiness checks.

For the current platform surface, code ownership, runtime boundaries, and local operation commands, read [`docs/geoenvironmental-platform.md`](docs/geoenvironmental-platform.md) before changing the formal web workbench or the monitor.

## Operating Principles

### Generalization First
This repository prefers robust, reusable capability upgrades over query-specific fixes.

- Avoid one-off hardcoded branches for a single case unless explicitly approved as a temporary hotfix.
- When fixing routing/workflow logic, abstract into intent/capability signals that can generalize to neighboring tasks.
- Any bugfix in a specific scenario must include at least one non-target variation test.
  Example: earthquake routing fixes should also be checked against wildfire, conflict, or flood style prompts.
- When adding heuristics, keep them centralized and documented; do not scatter ad hoc keyword checks across files.

### Runtime Safety
- Preserve the thread workspace model:
  - read uploaded/user data from the current thread workspace `inputs`
  - write generated artifacts to the current thread workspace `outputs`
  - store per-thread runtime memory under the current thread workspace `memory`
- Do not enable remote DeepAgents sandbox providers by default. This project intentionally relies on the local GEE/geospatial environment because Earth Engine authentication, local credential caches, GDAL/PROJ/Rasterio/GeoPandas native libraries, local RAG assets, `base_data`, and thread workspaces must be available during execution.
- Treat the project execution model as local subprocess workspace isolation, not as a vendor-hosted secure sandbox:
  - `graph_factory.py` uses DeepAgents filesystem backends for virtual file routing and skill discovery.
  - `tools/NTL_Code_generation.py` runs generated code in a subprocess with the current thread workspace as `cwd`.
  - relative `inputs/...` and `outputs/...` paths are expected to resolve inside `user_data/<thread_id>/`.
  - safety depends on preflight checks, path protocol enforcement, subprocess timeout, and workspace scoping.
- Use `storage_manager.resolve_input_path(...)`, `storage_manager.resolve_output_path(...)`, `storage_manager.resolve_workspace_relative_path(...)`, or `storage_manager.resolve_deepagents_path(...)` for file paths.
- Do not add new absolute local paths to agent prompts, tools, or generated scripts.
- Treat `/shared/...` and `base_data` as read-only source data unless the user explicitly asks for data curation.
- Keep Deep Agents virtual-path aliases aligned:
  - `/data/raw/<file>` maps to thread `inputs`
  - `/data/processed/<file>` maps to thread `outputs`
  - `/memories/<file>` maps to thread `memory`
  - `/shared/<file>` maps to `base_data`
- Preserve long-running Streamlit state contracts in `app_logic.py`; do not casually rename run, heartbeat, cancel, event, or terminal-state keys used across reruns.
- Keep agent routing changes coherent across `graph_factory_v2.py`, `agents/`, `.ntl-gpt/skills/`, and `tools/__init__.py`.

### Four-role Architecture
- `NTL_Engineer`（地缘分析师）owns task truth, planning, routing, acceptance, and final evidence synthesis.
- `NTL_Data_Searcher`（数据助手）owns event feeds, boundaries, Earthdata/GEE and other multi-source observation acquisition, QA, coverage, and provenance.
- `NTL_Analyst`（分析助手）owns spatial/temporal, raster/vector, remote-sensing, event-impact, climate/ecosystem, exposure, socioeconomic, and visualization methods.
- `NTL_Event_Tracker`（事件助手）owns source-bounded conflict, disaster, outage, accident, policy, logistics, and recovery timelines.
- Specialists do not delegate to one another. The supervisor routes sequentially and accepts either a typed package or an explicitly bounded summary-only result.
- Nighttime-light data is an important evidence family, not the platform identity and not a mandatory route.
- Correction is opt-in: keep standard product values unless the user explicitly requests a correction or explicitly selects a method whose required workflow includes it. Do not infer angle, seasonal, radiometric, cross-sensor, atmospheric, geometric, or other correction from a model-authored plan.

### Environment and Secrets
- Never commit `.env`, tokens, API keys, Earthdata credentials, GEE credentials, downloaded private data, or local user workspaces.
- Keep `.env.example`, `README.md`, and `check_env.py` in sync when adding, renaming, or removing environment variables.
- Current required DeepSeek and internal DashScope variables are documented in `README.md` and checked by `check_env.py`.
- `DeepSeek_API_KEY` / `DeepSeek_Coding_URL` serve the selectable frontend models. `DASHSCOPE_API_KEY` and `DASHSCOPE_Qwen_plus_KEY` remain separate internal VLM, knowledge-base, and embedding channels; do not collapse them unless the runtime code and docs are updated together.

## Validation Baseline

Run the smallest relevant checks before finishing a change.

- Markdown-only changes:
  - verify UTF-8 readability with Python
  - inspect `git diff -- AGENTS.md` or the touched docs
- Python changes:
  - `python -m py_compile <changed modules>`
- Environment/bootstrap changes:
  - `python check_env.py`
- Graph or agent-routing changes:
  - compile `graph_factory_v2.py`, `app_agents.py`, `web_runtime.py`, `app_logic.py`, and touched agent/tool modules
  - test the target prompt plus at least one neighboring variation
- Tool changes:
  - check direct function/tool invocation when possible
  - verify generated outputs stay inside the thread workspace
- Streamlit UI changes:
  - run `streamlit run Streamlit.py` when dependencies and credentials are available
- Formal web UI or monitor changes:
  - compile the touched Python and JavaScript modules
  - run `python run_web.py --host 127.0.0.1 --port 8502 --dev-http` when safe for local verification
  - confirm `GET /api/healthz` and, when monitoring is enabled, `GET /api/monitor/status`

Known practical check:

```bash
python -m py_compile Streamlit.py app_logic.py app_agents.py graph_factory_v2.py web_runtime.py
```

## Documentation Policy

- Keep documentation lightweight but consistent:
  - `CHANGELOG.md` for high-impact engineering changes; batch minor tweaks.
  - `docs/GeoSentinel*.md` for product capability/version summaries, when such docs exist.
  - `docs/Skill_*.md` only when process or skill norms materially change.
  - Legacy Codex Chinese change-log files under `docs/` are not required maintenance targets.
- Do not create placeholder docs only to satisfy process language. If `docs/` is absent, update the closest real artifact such as `README.md`, `.env.example`, or this file.
- Keep user-facing setup docs aligned with the actual supported entrypoints: `environment.yml`, `check_env.py`, and `Streamlit.py`.
- Keep the current platform overview aligned with `run_web.py`, `web_api.py`, `web_runtime.py`, `web/`, and `monitoring/` when those public surfaces change.

## Encoding Integrity

### Mandatory Text Rules
- Markdown, JSON, and Python source files must be saved as UTF-8; prefer UTF-8 without BOM for docs.
- Do not paste or commit mojibake text in Chinese docs.
- If an encoding issue is discovered, fix encoding first, then continue feature changes.
- When deciding whether text is mojibake, prioritize Python UTF-8 parse results over terminal rendering.
- Terminal display artifacts from code page or font issues are not sufficient evidence of file corruption.
- For suspected encoding issues, run UTF-8 parse checks first, then decide whether a fix is needed.

### Encoding Check Commands
```bash
python - << 'PY'
from pathlib import Path
p = Path('AGENTS.md')
b = p.read_bytes()
print('bom', b.startswith(b'\xef\xbb\xbf'))
b.decode('utf-8')
print('utf8_ok', True)
PY

python - << 'PY'
from pathlib import Path
bad_points = [0x9359, 0x7481, 0x951b, 0x9286, 0x9225, 0x20ac]
text = Path('AGENTS.md').read_text(encoding='utf-8')
hits = [(hex(cp), chr(cp)) for cp in bad_points if chr(cp) in text]
print('mojibake_hits', hits)
PY
```

## Git and Branch Hygiene

- Check `git status -sb` before editing, committing, switching branches, or pushing.
- Do not rewrite or delete user work unless explicitly requested.
- Before pushing a branch, compare with the intended base:
  - `git diff --stat upstream/main HEAD`
  - `git diff --name-status upstream/main HEAD`
- Do not push a branch containing known syntax errors unless the branch is explicitly for preserving a broken snapshot.
- If creating a branch for review, base it on `upstream/main` unless the user asks for a different base.
- Do not commit generated caches, local workspaces, credentials, or transient outputs unless the repository intentionally tracks that asset class.

## Result Bus Isolation Policy

### Mandatory Runtime Safety Rules
- If users request cross-device result notifications, use an independent Git result bus repo, for example `E:\codex-result-bus`.
- Never run result-sync `git push` or `git pull` operations inside the active project workspace.
- Consumer machine OpenClaw should pull and notify only; do not push back.
- If remote is unavailable, persist a local snapshot commit first and report the missing remote as an actionable item.

## Quick Agent Checklist

1. Is this a capability-level change rather than a one-case patch?
2. Are related docs/config/env checks kept in sync?
3. Are file paths resolved through `storage_manager`?
4. Does at least one neighboring scenario still pass?
5. Did Python UTF-8 parsing confirm text integrity when encoding was in question?
6. Is the branch based on the intended upstream and free of unrelated local changes?
