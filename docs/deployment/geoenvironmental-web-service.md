# Geoenvironmental Web Service Deployment

The formal public interface is the static `web/` workbench served by
`web_api.py`. It shares the existing NTL-GPT PostgreSQL account, thread,
history, and workspace model. `Streamlit.py` remains an internal diagnostic
and transition entrypoint; do not expose it as the new public product UI.

## Service Shape

```text
researcher browser
  -> https://geointer.geosetting-ai.com
  -> 花生壳（Oray） HTTPS tunnel
  -> 127.0.0.1:8502 on the Windows host
  -> web_api.py
     -> web/ static workbench
     -> /api authentication, threads, files and SSE runs
     -> existing LangGraph, PostgreSQL and thread workspaces
```

If 花生壳（Oray） terminates HTTPS, Nginx is not required for this new hostname.
Nginx may continue serving the legacy Streamlit hostname independently.

## 1. Configure `.env`

Start from `.env.example`. Keep the existing model, DashScope, PostgreSQL, GEE
and Earthdata settings. Add production web settings:

```env
NTL_WEB_SESSION_SECRET=replace_with_a_long_random_secret
NTL_WEB_COOKIE_SECURE=1
NTL_WEB_ALLOWED_HOSTS=geointer.geosetting-ai.com
NTL_WEB_HOST=127.0.0.1
NTL_WEB_PORT=8502
NTL_WEB_MAX_UPLOAD_MB=200
```

Generate `NTL_WEB_SESSION_SECRET` with a password manager or:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Never publish `.env`, session secrets, database URLs, API keys, Earthdata
tokens, or Google credentials.

## 2. Update the Environment

The public service needs `fastapi`, `uvicorn`, `sse-starlette`, and
`itsdangerous`; they are pinned in `environment.yml`.

For an existing Conda environment:

```powershell
conda activate GeoIntelligence
python -m pip install "fastapi>=0.115,<1" "uvicorn>=0.30,<1" "sse-starlette>=2.2,<3" "itsdangerous>=2.2,<3"
```

Then verify the project configuration:

```powershell
Set-Location D:\NTL-GPT-Geopolitics
python check_env.py
```

## 3. Start the Public Service

Run the API only on localhost. 花生壳（Oray） is responsible for the public
tunnel; do not bind this service to `0.0.0.0` unless the network boundary is
explicitly redesigned and reviewed.

```powershell
Set-Location D:\NTL-GPT-Geopolitics
conda activate GeoIntelligence
python run_web.py --host 127.0.0.1 --port 8502
```

Confirm local health before exposing it:

```powershell
Invoke-WebRequest http://127.0.0.1:8502/api/healthz -UseBasicParsing
```

Expected response contains `"status":"ok"` and
`"service":"geoenvironmental-web"`.

For a local browser login test before the 花生壳 HTTPS mapping is enabled, use
this localhost-only command instead. It intentionally relaxes the cookie flag
for that one process and must not be used for the public service:

```powershell
python run_web.py --host 127.0.0.1 --port 8502 --dev-http
```

## 4. Configure 花生壳（Oray）

Create or update the mapping:

| Field | Value |
| --- | --- |
| Domain | `geointer.geosetting-ai.com` |
| Internal host | `127.0.0.1` |
| Internal port | `8502` |
| Public protocol | HTTPS, terminated by 花生壳（Oray） |

After the mapping is active, open:

`https://geointer.geosetting-ai.com/`

The first request should show the centered login screen. After authentication,
the user should be able to select or create a task, upload research inputs,
run an agent task, observe live progress, and download outputs from the same
thread workspace.

## 5. Restart and Update

Stop the old `run_web.py` process with `Ctrl+C`, pull the project update, then
restart it. A persistent Windows task can be added after the public service is
accepted; it should run the same command, under the same Conda environment,
with stdout/stderr redirected to a monitored log directory.

The service itself is intentionally stateless except for in-memory active run
events. Accounts, thread history and artifacts remain in PostgreSQL and the
thread workspace. An in-progress run should be treated as interrupted after a
process restart.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Login says account service unavailable | Set and test `NTL_HISTORY_DB_URL` or `NTL_LANGGRAPH_POSTGRES_URL`; verify PostgreSQL is reachable. |
| Login works locally but not through the domain | Confirm 花生壳（Oray） points to `127.0.0.1:8502`, and `NTL_WEB_ALLOWED_HOSTS` contains the exact hostname. |
| Login succeeds but immediately resets | Set a persistent `NTL_WEB_SESSION_SECRET`; set `NTL_WEB_COOKIE_SECURE=1` when the public URL is HTTPS. |
| Task start says model service is unavailable | Check `DeepSeek_API_KEY`, `DeepSeek_Coding_URL`, DashScope variables, and `python check_env.py`. |
| Public page does not load | Verify `python run_web.py ...` is listening on `127.0.0.1:8502`, then open `/api/healthz` locally. |

## 地缘事件监测服务

- 设置 `NTL_MONITOR_ENABLED=1` 后，Web 服务生命周期会启动一条全局后台线程，每 `NTL_MONITOR_INTERVAL_MINUTES=30` 分钟执行一次。
- 监测数据固定保存在项目本地全局工作区：默认 `<NTL_USER_DATA_DIR>/_monitor`，也可通过 `NTL_MONITOR_DATA_DIR` 指定。它与 PostgreSQL、用户聊天记录和线程工作区完全分离；`/api/monitor/status` 会显示 `storage.mode=local_workspace`。
- 首批来源为 GDACS、NASA EONET、可限流降级的 GDELT，以及可选的 ACLED 冲突事件接口。ACLED 依赖部署方自己的 OAuth 凭据，未配置时会明确跳过；GDELT 限流、单源不可达或证书失败不会中断其他来源，状态通过 `/api/monitor/status` 暴露。
- `NTL_MONITOR_DEEPAGENT_ENABLED=1` 且 DeepSeek 配置完整时，`Geopolitical_Event_Monitor` DeepAgent 只读取本轮来源候选、保持或降低严重性，并从受限模板选择研判问题。它不能创建没有来源的事件、修改坐标或访问用户工作区。
- 此实现参考 World Monitor 的来源分级、时效窗口与去重思路；没有复制其 AGPL-3.0 代码。World Monitor 本地参考检出位于项目外的 `D:\NTL-GPT-Geopolitics-reference\worldmonitor`。
