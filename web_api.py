"""Same-origin ASGI application for the formal geopolitical research frontend.

The service exposes authenticated account, thread, artifact and run-event APIs
under ``/api`` and serves the static public frontend from ``web/``.  It is
intended to sit behind Nginx or a domain tunnel, not to expose Streamlit.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import logging
import mimetypes
import os
import secrets
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

import history_store
from monitoring import event_monitor_service
from model_config import MODEL_OPTIONS
from storage_manager import storage_manager
from web_runtime import TERMINAL_STATES, WebRuntimeError, web_run_manager


load_dotenv(override=True)

ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
logger = logging.getLogger(__name__)


class CredentialsPayload(BaseModel):
    username: str = Field(max_length=40)
    password: str = Field(max_length=256)


class CreateThreadPayload(BaseModel):
    title: str = Field(default="", max_length=120)


class RunPayload(BaseModel):
    question: str = Field(max_length=12000)
    model_name: str = Field(default=MODEL_OPTIONS[0], max_length=80)
    request_key: str = Field(default="", max_length=200)


def _truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _session_secret() -> str:
    configured = str(os.getenv("NTL_WEB_SESSION_SECRET", "") or "").strip()
    if configured:
        return configured
    # Local development remains possible without silently deriving a secret
    # from database credentials. Production must configure this value.
    generated = secrets.token_urlsafe(48)
    logger.warning("NTL_WEB_SESSION_SECRET is not set; using an ephemeral development session secret.")
    return generated


def _allowed_hosts() -> list[str]:
    raw = str(os.getenv("NTL_WEB_ALLOWED_HOSTS", "") or "").strip()
    if raw:
        return [part.strip() for part in raw.split(",") if part.strip()]
    return ["127.0.0.1", "localhost", "geointer.geosetting-ai.com"]


def _public_user(user: dict[str, Any]) -> dict[str, str]:
    return {
        "user_id": str(user.get("user_id") or ""),
        "username": str(user.get("username") or user.get("user_name") or ""),
    }


def _current_user(request: Request) -> dict[str, str]:
    user_id = str(request.session.get("user_id") or "").strip()
    username = str(request.session.get("username") or "").strip()
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="请先登录后再继续。")
    return {"user_id": user_id, "username": username}


def _require_thread(request: Request, thread_id: str) -> dict[str, str]:
    user = _current_user(request)
    if not history_store.thread_belongs_to_user(user["user_id"], thread_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在或无权访问。")
    return user


def _safe_filename(filename: str) -> str:
    value = str(filename or "").strip()
    if not value or value in {".", ".."} or "/" in value or "\\" in value:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="文件名无效。")
    if len(value) > 180:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="文件名过长。")
    return value


# Uploads may target a free-form subpath below a workspace root (e.g.
# "inputs/raw/aoi.tif") instead of being flattened into the root.  Traversal
# and path separators are rejected; the workspace-relative resolver applies
# the final root containment check.
_ROOT_ALIASES = {"inputs": "inputs", "outputs": "outputs", "in": "inputs", "out": "outputs"}


def _split_upload_target(filename: str) -> tuple[str, str]:
    raw = str(filename or "").strip().replace("\\", "/")
    if not raw or ".." in raw.split("/"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="目标路径无效。")
    parts = [part for part in raw.split("/") if part]
    if not parts:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="文件名无效。")
    first = parts[0].lower()
    if len(parts) > 1 and first in _ROOT_ALIASES:
        root = _ROOT_ALIASES[first]
        rel_parts = parts[1:]
    else:
        root = "inputs"
        rel_parts = parts
    if not rel_parts:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="文件名无效。")
    if any(len(part) > 160 for part in rel_parts):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="路径过长。")
    return root, "/".join(rel_parts)


def _runtime_error_response(error: WebRuntimeError) -> JSONResponse:
    status_code = status.HTTP_409_CONFLICT
    if error.code in {"thread_not_found", "run_not_found", "output_missing"}:
        status_code = status.HTTP_404_NOT_FOUND
    elif error.code == "artifact_preview_too_large":
        status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
    elif error.code in {
        "artifact_preview_invalid",
        "artifact_preview_unavailable",
        "artifact_preview_unsupported",
        "empty_question",
        "unsupported_model",
        "model_configuration_missing",
    }:
        status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": error.code, "message": str(error), "details": error.details}},
    )


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    event_monitor_service.start()
    try:
        yield
    finally:
        event_monitor_service.stop()


def create_app() -> FastAPI:
    if not WEB_ROOT.exists():
        raise RuntimeError(f"Formal frontend directory is missing: {WEB_ROOT}")

    app = FastAPI(
        title="Geoenvironmental Intelligence API",
        version="0.2.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=_lifespan,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts())
    app.add_middleware(
        SessionMiddleware,
        secret_key=_session_secret(),
        session_cookie="geointer_session",
        max_age=60 * 60 * 24 * 7,
        same_site="lax",
        https_only=_truthy("NTL_WEB_COOKIE_SECURE", default=False) and not _truthy("NTL_WEB_FORCE_DEV_HTTP", default=False),
    )

    @app.exception_handler(WebRuntimeError)
    async def web_runtime_error_handler(_request: Request, error: WebRuntimeError) -> JSONResponse:
        return _runtime_error_response(error)

    @app.get("/api/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "service": "geoenvironmental-web", "time": int(time.time())}

    @app.get("/api/monitor/events")
    async def monitor_events(limit: int = 18) -> dict[str, Any]:
        """Global, source-grounded monitor snapshot shared by all sessions."""
        return event_monitor_service.snapshot(limit=limit)

    @app.get("/api/monitor/status")
    async def monitor_status() -> dict[str, Any]:
        return event_monitor_service.snapshot(limit=1)["status"]

    @app.get("/api/me")
    async def current_account(request: Request) -> dict[str, Any]:
        user_id = str(request.session.get("user_id") or "").strip()
        if not user_id:
            return {"authenticated": False}
        user = history_store.get_registered_user(user_id)
        if not user:
            request.session.clear()
            return {"authenticated": False}
        public_user = _public_user(user)
        request.session.update(public_user)
        return {"authenticated": True, "user": public_user, "models": MODEL_OPTIONS}

    @app.post("/api/auth/register", status_code=status.HTTP_201_CREATED)
    async def register_account(request: Request, payload: CredentialsPayload) -> dict[str, Any]:
        try:
            user = history_store.register_user(payload.username, payload.password)
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error
        except RuntimeError as error:
            logger.exception("Public registration service is unavailable")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="账号服务尚未配置或暂时不可用。") from error
        public_user = _public_user(user)
        request.session.clear()
        request.session.update(public_user)
        return {"user": public_user}

    @app.post("/api/auth/login")
    async def login_account(request: Request, payload: CredentialsPayload) -> dict[str, Any]:
        try:
            user = history_store.authenticate_user(payload.username, payload.password)
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error
        except RuntimeError as error:
            logger.exception("Public login service is unavailable")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="账号服务尚未配置或暂时不可用。") from error
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码不正确。")
        public_user = _public_user(user)
        request.session.clear()
        request.session.update(public_user)
        return {"user": public_user}

    @app.post("/api/auth/logout")
    async def logout_account(request: Request) -> dict[str, bool]:
        request.session.clear()
        return {"ok": True}

    @app.get("/api/threads")
    async def list_threads(request: Request) -> dict[str, Any]:
        user = _current_user(request)
        return {"items": history_store.list_user_threads(user["user_id"], limit=100)}

    @app.post("/api/threads", status_code=status.HTTP_201_CREATED)
    async def create_thread(request: Request, payload: CreateThreadPayload) -> dict[str, Any]:
        user = _current_user(request)
        thread_id = history_store.generate_thread_id(user["user_id"])
        title = " ".join(str(payload.title or "").split())
        meta = {"thread_title": title, "thread_title_manual": bool(title)} if title else None
        history_store.bind_thread_to_user(user["user_id"], thread_id, meta=meta)
        storage_manager.get_workspace(thread_id)
        matching = [row for row in history_store.list_user_threads(user["user_id"], limit=0) if row.get("thread_id") == thread_id]
        return {"thread": matching[0] if matching else {"thread_id": thread_id, "thread_title": title}}

    @app.delete("/api/threads/{thread_id}")
    async def delete_thread(request: Request, thread_id: str) -> dict[str, Any]:
        user = _require_thread(request, thread_id)
        # Only blank conversations are pruned automatically by the UI.  A thread
        # that already holds user questions or artifacts must never be removed
        # implicitly; callers get a stable code to surface that warning.
        messages = history_store.load_chat_records(thread_id, limit=1)
        artifacts = web_run_manager.list_artifacts(thread_id)
        if messages or artifacts:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "thread_not_empty",
                    "message": "该任务已有内容，不能自动清理。如确认删除请手动处理。",
                },
            )
        result = history_store.delete_user_thread(user["user_id"], thread_id, delete_workspace=True)
        return {"deleted": bool(result.get("deleted") or result.get("index_removed")), "thread_id": thread_id}

    @app.get("/api/threads/{thread_id}")
    async def get_thread(request: Request, thread_id: str) -> dict[str, Any]:
        user = _require_thread(request, thread_id)
        rows = history_store.list_user_threads(user["user_id"], limit=0)
        thread = next((row for row in rows if str(row.get("thread_id")) == thread_id), None)
        return {
            "thread": thread,
            "messages": history_store.load_chat_records(thread_id, limit=400),
            "artifacts": web_run_manager.list_artifacts(thread_id),
            "files": web_run_manager.list_workspace_files(thread_id),
            "active_run": web_run_manager.active_run_for_thread(thread_id, user["user_id"]),
        }

    @app.get("/api/threads/{thread_id}/workspace-files")
    async def list_workspace_files(request: Request, thread_id: str) -> dict[str, Any]:
        _require_thread(request, thread_id)
        return web_run_manager.list_workspace_files(thread_id)

    @app.get("/api/threads/{thread_id}/file-preview/table/{root_name}/{relative_path:path}")
    async def preview_workspace_table(request: Request, thread_id: str, root_name: str, relative_path: str) -> dict[str, Any]:
        _require_thread(request, thread_id)
        try:
            return web_run_manager.preview_workspace_table(thread_id, root_name, relative_path)
        except (ValueError, PermissionError) as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="文件不存在。") from error

    @app.get("/api/threads/{thread_id}/file-preview/geo/{root_name}/{relative_path:path}")
    async def preview_workspace_geo(request: Request, thread_id: str, root_name: str, relative_path: str) -> dict[str, Any]:
        _require_thread(request, thread_id)
        try:
            return web_run_manager.preview_workspace_geo(thread_id, root_name, relative_path)
        except (ValueError, PermissionError, FileNotFoundError) as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error) or "文件不存在。") from error
        except Exception as error:  # noqa: BLE001 - conversion failures are user-facing preview errors
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"地理空间预览失败：{error}",
            ) from error

    @app.get("/api/threads/{thread_id}/file-preview/content/{root_name}/{relative_path:path}")
    async def preview_workspace_file(request: Request, thread_id: str, root_name: str, relative_path: str):
        _require_thread(request, thread_id)
        try:
            target = web_run_manager.resolve_workspace_file(thread_id, root_name, relative_path)
        except (ValueError, PermissionError) as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="文件不存在。") from error
        if not target.is_file():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="文件不存在。")
        media_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        headers = {
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
        }
        if target.suffix.lower() in {".htm", ".html"}:
            headers["Content-Security-Policy"] = (
                "sandbox allow-scripts; default-src 'self' data: blob:; "
                "img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; connect-src 'self'; font-src 'self' data:"
            )
        return FileResponse(target, media_type=media_type, headers=headers)

    @app.get("/api/threads/{thread_id}/artifacts")
    async def list_artifacts(request: Request, thread_id: str) -> dict[str, Any]:
        _require_thread(request, thread_id)
        return {"items": web_run_manager.list_artifacts(thread_id)}

    @app.get("/api/threads/{thread_id}/artifacts/{relative_path:path}/preview")
    async def preview_artifact(request: Request, thread_id: str, relative_path: str) -> dict[str, Any]:
        _require_thread(request, thread_id)
        try:
            return web_run_manager.preview_table(thread_id, relative_path)
        except (ValueError, PermissionError) as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="输出文件不存在。") from error

    @app.get("/api/threads/{thread_id}/outputs/{relative_path:path}")
    async def download_output(request: Request, thread_id: str, relative_path: str):
        _require_thread(request, thread_id)
        try:
            output = web_run_manager.resolve_output(thread_id, relative_path)
        except (ValueError, PermissionError) as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="输出文件不存在。") from error
        if not output.is_file():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="输出文件不存在。")
        return FileResponse(output, filename=output.name)

    @app.put("/api/threads/{thread_id}/uploads/{filename:path}")
    async def upload_input(request: Request, thread_id: str, filename: str) -> dict[str, Any]:
        user = _require_thread(request, thread_id)
        root_name, rel_path = _split_upload_target(filename)
        max_bytes = max(1, int(os.getenv("NTL_WEB_MAX_UPLOAD_MB", "200") or 200)) * 1024 * 1024
        declared_length = int(request.headers.get("content-length") or 0)
        if declared_length > max_bytes:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="文件超过单文件大小限制。")
        target = storage_manager.resolve_workspace_relative_path(
            f"{root_name}/{rel_path}",
            thread_id=thread_id,
            default_root=root_name,
            create_parent=True,
            allowed_roots=(root_name,),
            allow_memory=False,
        )
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.upload")
        written = 0
        try:
            with storage_manager.workspace_write_lock(thread_id):
                with temporary.open("wb") as handle:
                    async for chunk in request.stream():
                        written += len(chunk)
                        if written > max_bytes:
                            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="文件超过单文件大小限制。")
                        handle.write(chunk)
                old_size = target.stat().st_size if target.exists() else 0
                quota = web_run_manager.workspace_quota_rejection(
                    thread_id,
                    user["user_id"],
                    additional_bytes=max(0, written - old_size),
                )
                if quota:
                    raise WebRuntimeError(quota["code"], "当前工作区存储额度不足。", quota)
                os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink(missing_ok=True)
        return {"name": target.name, "size_bytes": written, "path": f"{root_name}/{rel_path}"}

    @app.post("/api/threads/{thread_id}/runs", status_code=status.HTTP_202_ACCEPTED)
    async def start_run(request: Request, thread_id: str, payload: RunPayload) -> dict[str, Any]:
        user = _require_thread(request, thread_id)
        return web_run_manager.start(
            user_id=user["user_id"],
            thread_id=thread_id,
            question=payload.question,
            model_name=payload.model_name,
            request_key=payload.request_key,
        )

    @app.get("/api/runs/{run_id}")
    async def get_run(request: Request, run_id: str) -> dict[str, Any]:
        user = _current_user(request)
        return web_run_manager.run_summary(run_id, user["user_id"])

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel_run(request: Request, run_id: str) -> dict[str, Any]:
        user = _current_user(request)
        return web_run_manager.cancel(run_id, user["user_id"])

    @app.get("/api/runs/{run_id}/events")
    async def stream_run_events(request: Request, run_id: str, after_seq: int = 0):
        user = _current_user(request)
        web_run_manager.run_summary(run_id, user["user_id"])
        try:
            last_event_id = max(0, int(request.headers.get("last-event-id", "0") or 0))
        except ValueError:
            last_event_id = 0

        async def event_source():
            seq = max(0, int(after_seq or 0), last_event_id)
            while True:
                if await request.is_disconnected():
                    break
                events, state_value = web_run_manager.poll(run_id, after_seq=seq)
                for event in events:
                    seq = max(seq, int(event.get("seq") or 0))
                    yield {
                        "event": "run",
                        "id": str(seq),
                        "data": json.dumps(event, ensure_ascii=False),
                    }
                if state_value in TERMINAL_STATES or state_value == "missing":
                    break
                await asyncio.sleep(0.45)

        return EventSourceResponse(
            event_source(),
            ping=15,
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    app.mount("/", StaticFiles(directory=WEB_ROOT, html=True), name="web")
    return app


app = create_app()
