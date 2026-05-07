# AGENTS.md

## Project Snapshot

地缘环境智能计算平台 is a Streamlit application for geostrategic environmental intelligence and nighttime-light evidence analysis. It combines a Chinese chat UI, Deep Agents / LangGraph-style orchestration, ConflictNTL event screening, Google Earth Engine and VIIRS workflows, local geospatial processing tools, and local RAG assets.

### Repository Map
- `Streamlit.py`: application entrypoint.
- `app_ui.py`: Streamlit layout, sidebar, uploader, download center, and chat rendering.
- `app_state.py`: session defaults, model/runtime settings, and UI state.
- `app_logic.py`: run lifecycle, event streaming, cancellation, stale-run recovery, output collection, and chat history writes.
- `app_agents.py`: Streamlit-cached graph wrapper. Keep its public signature stable for UI callers.
- `graph_factory.py`: graph construction, model selection, skill discovery, backend routing, subagent setup, and supervisor prompt assembly.
- `agents/`: system prompts and subagent definitions.
- `tools/`: domain tools for retrieval, GEE, VIIRS, preprocessing, statistics, rendering, and knowledge-base access.
- `storage_manager.py`: canonical workspace, input/output, memory, and shared-data path resolution.
- `history_store.py`: persistent chat, turn summaries, and injected-context records.
- `file_context_service.py`: uploaded file parsing and context extraction.
- `.ntl-gpt/skills/`: Deep Agents runtime skills used by the graph.
- `RAG/`: local knowledge/code/literature indexes and reference assets.
- `check_env.py`, `.env.example`, `environment.yml`: environment bootstrap and readiness checks.

### Project Scope
- The primary capability is `conflict_ntl_analysis`: conflict event intake, screening, AOI generation, VIIRS/VNP46A2 handoff, and case-report evidence packaging.
- The UI is Chinese-only. Do not reintroduce CN/EN toggles unless explicitly requested.
- Do not expose SDGSAT-1 tools in `tools/__init__.py`, prompts, routing skills, or UI examples.

## Operating Principles

### Generalization First
This repository prefers robust, reusable capability upgrades over query-specific fixes.

- Avoid one-off hardcoded branches for a single case unless explicitly approved as a temporary hotfix.
- When fixing routing/workflow logic, abstract into intent/capability signals that can generalize to neighboring tasks.
- Any bugfix in a specific scenario must include at least one non-target variation test.
  Example: earthquake routing fixes should also be checked against wildfire, conflict, or flood style prompts.
- When adding heuristics, keep them centralized and documented; do not scatter ad hoc keyword checks across files.
- For complex tasks, prioritize the `using-superpowers` skill first to choose the right process/implementation skills before coding.

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
- Keep agent routing changes coherent across `graph_factory.py`, `agents/`, `.ntl-gpt/skills/`, and `tools/__init__.py`.

### Environment and Secrets
- Never commit `.env`, tokens, API keys, Earthdata credentials, GEE credentials, downloaded private data, or local user workspaces.
- Keep `.env.example`, `README.md`, and `check_env.py` in sync when adding, renaming, or removing environment variables.
- Current required DashScope variables are documented in `README.md` and checked by `check_env.py`.
- `DASHSCOPE_API_KEY` and `DASHSCOPE_Qwen_plus_KEY` may serve different model channels; do not collapse them unless the runtime code and docs are updated together.

### Feishu/Lark OpenAPI MCP
- Use the official `@larksuiteoapi/lark-mcp` package for Feishu/Lark cloud document access. The `meegle` skill/CLI is for Feishu Project (Meego/Meegle) work items and does not read `/docx/...` cloud documents.
- Install the MCP CLI when needed:
  ```bash
  npm install -g @larksuiteoapi/lark-mcp
  ```
- Required environment variables for application identity:
  ```bash
  FEISHU_APP_ID=<app_id>
  FEISHU_APP_SECRET=<app_secret>
  ```
- Do not print or commit these secrets. When probing the environment, list variable names only.
- Start the MCP server over stdio with the smallest useful tool set. For cloud document reading, enable:
  ```bash
  lark-mcp mcp -a "$FEISHU_APP_ID" -s "$FEISHU_APP_SECRET" \
    --tools "docx.v1.document.get,docx.v1.document.rawContent,docs.v1.content.get,drive.v1.exportTask.create,drive.v1.exportTask.get" \
    --token-mode tenant_access_token \
    --language zh
  ```
- For Windows subprocess clients, spawn via `cmd.exe /c lark-mcp ...` if direct `spawn("lark-mcp")` or `spawn("lark-mcp.cmd")` fails.
- To read a Feishu document URL such as `https://my.feishu.cn/docx/<document_id>`, use the token after `/docx/` as `document_id` / `doc_token`.
- Prefer these MCP tools:
  - `docx_v1_document_get` for title and revision metadata.
  - `docx_v1_document_rawContent` for plain text content.
  - `docs_v1_content_get` with `doc_type=docx` and `content_type=markdown` for Markdown content.
  - `drive_v1_exportTask_create` and `drive_v1_exportTask_get` for exporting documents when raw/Markdown content is not enough.
- If application identity cannot access a document, use OAuth user identity instead. Configure the Feishu app redirect URL as `http://localhost:3000/callback`, then login:
  ```bash
  lark-mcp login -a "$FEISHU_APP_ID" -s "$FEISHU_APP_SECRET" --scope "offline_access docx:document"
  ```
- Then start MCP with user tokens:
  ```bash
  lark-mcp mcp -a "$FEISHU_APP_ID" -s "$FEISHU_APP_SECRET" --oauth --token-mode user_access_token
  ```

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
  - compile `graph_factory.py`, `app_agents.py`, `app_logic.py`, and touched agent/tool modules
  - test the target prompt plus at least one neighboring variation
- Tool changes:
  - check direct function/tool invocation when possible
  - verify generated outputs stay inside the thread workspace
- Streamlit UI changes:
  - run `streamlit run Streamlit.py` when dependencies and credentials are available

Known practical check:

```bash
python -m py_compile Streamlit.py app_logic.py app_agents.py graph_factory.py
```

## Documentation Policy

- Keep documentation lightweight but consistent:
  - `CHANGELOG.md` for high-impact engineering changes; batch minor tweaks.
  - `docs/地缘环境智能计算平台*.md` for product capability/version summaries, when such docs exist.
  - `docs/Skill_*.md` only when process or skill norms materially change.
  - Legacy Codex Chinese change-log files under `docs/` are not required maintenance targets.
- Do not create placeholder docs only to satisfy process language. If `docs/` is absent, update the closest real artifact such as `README.md`, `.env.example`, or this file.
- Keep user-facing setup docs aligned with the actual supported entrypoints: `environment.yml`, `check_env.py`, and `Streamlit.py`.

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
- Consumer machine 地缘环境智能计算平台 should pull and notify only; do not push back.
- If remote is unavailable, persist a local snapshot commit first and report the missing remote as an actionable item.

## Quick Agent Checklist

1. Is this a capability-level change rather than a one-case patch?
2. Are related docs/config/env checks kept in sync?
3. Are file paths resolved through `storage_manager`?
4. Does at least one neighboring scenario still pass?
5. Did Python UTF-8 parsing confirm text integrity when encoding was in question?
6. Is the branch based on the intended upstream and free of unrelated local changes?

