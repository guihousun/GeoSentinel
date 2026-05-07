from __future__ import annotations

import os
import re
from typing import Any


ASSISTANT_ID = "Engineer"
POSTGRES_URL_ENV = "NTL_LANGGRAPH_POSTGRES_URL"
POSTGRES_AUTO_SETUP_ENV = "NTL_LANGGRAPH_POSTGRES_AUTO_SETUP"
MEMORY_BACKEND_ENV = "NTL_DEEPAGENTS_MEMORY_BACKEND"
MEMORY_NAMESPACE_SCOPE_ENV = "NTL_MEMORY_NAMESPACE_SCOPE"
HISTORY_DB_URL_ENV = "NTL_HISTORY_DB_URL"
MAX_ACTIVE_RUNS_ENV = "NTL_MAX_ACTIVE_RUNS"
MAX_ACTIVE_RUNS_PER_USER_ENV = "NTL_MAX_ACTIVE_RUNS_PER_USER"
THREAD_WORKSPACE_QUOTA_MB_ENV = "NTL_THREAD_WORKSPACE_QUOTA_MB"
USER_WORKSPACE_QUOTA_MB_ENV = "NTL_USER_WORKSPACE_QUOTA_MB"
GEE_DEFAULT_PROJECT_ID_ENV = "GEE_DEFAULT_PROJECT_ID"
DEFAULT_GEE_PROJECT_ID = "empyrean-caster-430308-m2"

DEFAULT_MAX_ACTIVE_RUNS = 10
DEFAULT_MAX_ACTIVE_RUNS_PER_USER = 2
DEFAULT_THREAD_WORKSPACE_QUOTA_MB = 500
DEFAULT_USER_WORKSPACE_QUOTA_MB = 1024


def _env_text(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def env_truthy(name: str, default: bool = False) -> bool:
    value = _env_text(name).lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def positive_int_env(name: str, default: int = 0) -> int:
    value = _env_text(name)
    if not value:
        return max(0, int(default))
    try:
        return max(0, int(value))
    except ValueError:
        return max(0, int(default))


def langgraph_postgres_url(explicit_url: str | None = None) -> str:
    return str(explicit_url or _env_text(POSTGRES_URL_ENV)).strip()


def history_db_url() -> str:
    return _env_text(HISTORY_DB_URL_ENV) or langgraph_postgres_url()


def postgres_auto_setup_enabled() -> bool:
    return env_truthy(POSTGRES_AUTO_SETUP_ENV, default=True)


def memory_backend_mode(postgres_url: str | None = None) -> str:
    mode = _env_text(MEMORY_BACKEND_ENV).lower()
    if mode in {"filesystem", "store"}:
        return mode
    return "store" if langgraph_postgres_url(postgres_url) else "filesystem"


def max_active_runs() -> int:
    return positive_int_env(MAX_ACTIVE_RUNS_ENV, DEFAULT_MAX_ACTIVE_RUNS)


def max_active_runs_per_user() -> int:
    return positive_int_env(MAX_ACTIVE_RUNS_PER_USER_ENV, DEFAULT_MAX_ACTIVE_RUNS_PER_USER)


def thread_workspace_quota_mb() -> int:
    return positive_int_env(THREAD_WORKSPACE_QUOTA_MB_ENV, DEFAULT_THREAD_WORKSPACE_QUOTA_MB)


def user_workspace_quota_mb() -> int:
    return positive_int_env(USER_WORKSPACE_QUOTA_MB_ENV, DEFAULT_USER_WORKSPACE_QUOTA_MB)


def thread_workspace_quota_bytes() -> int:
    return thread_workspace_quota_mb() * 1024 * 1024


def user_workspace_quota_bytes() -> int:
    return user_workspace_quota_mb() * 1024 * 1024


def default_gee_project_id() -> str:
    return _env_text(GEE_DEFAULT_PROJECT_ID_ENV) or DEFAULT_GEE_PROJECT_ID


def build_run_limit_snapshot(controls: list[dict[str, Any]], user_id: str) -> dict[str, int]:
    user_key = str(user_id or "").strip()
    global_active = 0
    user_active = 0
    for control in controls:
        if str(control.get("state")) != "running":
            continue
        global_active += 1
        if user_key and str(control.get("user_id") or "") == user_key:
            user_active += 1
    return {
        "global_active": global_active,
        "global_limit": max_active_runs(),
        "user_active": user_active,
        "user_limit": max_active_runs_per_user(),
    }


def build_runtime_metadata(
    *,
    user_id: str,
    thread_id: str,
    assistant_id: str = ASSISTANT_ID,
    gee_pipeline_mode: str = "default",
    gee_project_id: str = "",
    gee_profile_source: str = "default",
) -> dict[str, str]:
    return {
        "assistant_id": sanitize_namespace_part(assistant_id, ASSISTANT_ID),
        "user_id": sanitize_namespace_part(user_id, "guest"),
        "thread_id": sanitize_namespace_part(thread_id, "debug"),
        "gee_pipeline_mode": sanitize_namespace_part(gee_pipeline_mode, "default"),
        "gee_project_id": sanitize_namespace_part(gee_project_id or default_gee_project_id(), default_gee_project_id()),
        "gee_profile_source": sanitize_namespace_part(gee_profile_source, "default"),
    }


def sanitize_namespace_part(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        text = str(fallback)
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_.:-")
    return text[:80] or str(fallback)


def _mapping_get(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _runtime_config(runtime_or_context: Any) -> dict[str, Any]:
    runtime = getattr(runtime_or_context, "runtime", runtime_or_context)
    config = getattr(runtime, "config", None)
    return config if isinstance(config, dict) else {}


def _runtime_context(runtime_or_context: Any) -> Any:
    runtime = getattr(runtime_or_context, "runtime", runtime_or_context)
    return getattr(runtime, "context", None)


def _first_non_empty(*values: Any, fallback: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return fallback


def deepagents_memory_namespace(
    runtime_or_context: Any,
    *,
    graph_name: str = ASSISTANT_ID,
    fallback_thread_id: str = "debug",
) -> tuple[str, ...]:
    config = _runtime_config(runtime_or_context)
    metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
    configurable = config.get("configurable") if isinstance(config.get("configurable"), dict) else {}
    context = _runtime_context(runtime_or_context)

    assistant_id = sanitize_namespace_part(
        _first_non_empty(
            _mapping_get(metadata, "assistant_id"),
            _mapping_get(configurable, "assistant_id"),
            _mapping_get(context, "assistant_id"),
            graph_name,
            fallback=ASSISTANT_ID,
        ),
        ASSISTANT_ID,
    )
    user_id = sanitize_namespace_part(
        _first_non_empty(
            _mapping_get(metadata, "user_id"),
            _mapping_get(configurable, "user_id"),
            _mapping_get(context, "user_id"),
            fallback="guest",
        ),
        "guest",
    )
    thread_id = sanitize_namespace_part(
        _first_non_empty(
            _mapping_get(metadata, "thread_id"),
            _mapping_get(configurable, "thread_id"),
            _mapping_get(context, "thread_id"),
            fallback_thread_id,
            fallback="debug",
        ),
        "debug",
    )

    scope = _env_text(MEMORY_NAMESPACE_SCOPE_ENV).lower()
    if scope == "user":
        return (assistant_id, user_id)
    return (assistant_id, user_id, thread_id)
