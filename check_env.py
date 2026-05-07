from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DOTENV_PATH = ROOT / ".env"

REQUIRED_ENV = [
    "DASHSCOPE_API_KEY",
    "DASHSCOPE_Qwen_plus_KEY",
    "DASHSCOPE_Qwen_plus_URL",
    "DASHSCOPE_Coding_URL",
]

OPTIONAL_ENV = [
    "MINIMAX_API_KEY",
    "MINIMAX_Coding_URL",
    "OPENAI_API_KEY",
    "GEE_DEFAULT_PROJECT_ID",
    "GOOGLE_OAUTH_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "GOOGLE_OAUTH_REDIRECT_URI",
    "GOOGLE_OAUTH_SCOPES",
    "NTL_TOKEN_ENCRYPTION_KEY",
    "EARTHDATA_TOKEN",
    "NTL_TOOL_PROFILE",
    "NTL_USER_DATA_DIR",
    "NTL_SHARED_DATA_DIR",
    "NTL_CONTEXTILY_TMP",
    "NTL_MAX_ACTIVE_RUNS",
    "NTL_MAX_ACTIVE_RUNS_PER_USER",
    "NTL_THREAD_WORKSPACE_QUOTA_MB",
    "NTL_USER_WORKSPACE_QUOTA_MB",
    "NTL_ADMIN_USERNAMES",
    "NTL_LOCAL_ADMIN_HOST",
    "NTL_LOCAL_ADMIN_PORT",
    "NTL_LOCAL_ADMIN_ACTOR",
    "NTL_FORCE_NATIVE_CHAT_INPUT",
    "NTL_STREAMING_MAIN_REFRESH_SECONDS",
    "NTL_STREAMING_GRAPH_REFRESH_SECONDS",
    "NTL_LANGGRAPH_POSTGRES_URL",
    "NTL_LANGGRAPH_POSTGRES_AUTO_SETUP",
    "NTL_DEEPAGENTS_MEMORY_BACKEND",
    "NTL_MEMORY_NAMESPACE_SCOPE",
]

CORE_IMPORTS = {
    "streamlit": "streamlit",
    "deepagents": "deepagents",
    "geopandas": "geopandas",
    "rasterio": "rasterio",
}


def _print(title: str, lines: list[str]) -> None:
    print(title)
    for line in lines:
        print(f"  - {line}")


def _load_dotenv_snapshot() -> dict[str, str]:
    values: dict[str, str] = {}
    if not DOTENV_PATH.exists():
        return values
    for raw_line in DOTENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def _check_env() -> tuple[list[str], list[str]]:
    env_snapshot = _load_dotenv_snapshot()

    def _value(name: str) -> str:
        project_value = str(env_snapshot.get(name, "") or "").strip()
        if project_value:
            return project_value
        return str(os.getenv(name, "") or "").strip()

    missing = [name for name in REQUIRED_ENV if not _value(name)]
    optional = [name for name in OPTIONAL_ENV if _value(name)]
    return missing, optional


def _check_files() -> list[str]:
    missing = []
    for rel in ("environment.yml", ".env.example", "Streamlit.py", "app_ui.py", "graph_factory.py"):
        if not (ROOT / rel).exists():
            missing.append(rel)
    return missing


def _check_imports() -> tuple[list[str], list[str]]:
    ok = []
    failed = []
    for label, module_name in CORE_IMPORTS.items():
        try:
            importlib.import_module(module_name)
            ok.append(label)
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{label}: {exc}")
    return ok, failed


def _check_optional_postgres(env_snapshot: dict[str, str]) -> list[str]:
    postgres_url = str(env_snapshot.get("NTL_LANGGRAPH_POSTGRES_URL") or os.getenv("NTL_LANGGRAPH_POSTGRES_URL") or "").strip()
    if not postgres_url:
        return []

    failed = []
    for module_name in ("langgraph.checkpoint.postgres", "langgraph.store.postgres"):
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{module_name}: {exc}")
    try:
        from deepagents.backends import StoreBackend  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        failed.append(f"deepagents.backends.StoreBackend: {exc}")
    return failed


def main() -> int:
    print("地缘环境智能计算平台环境检查")
    print(f"Repository: {ROOT}")
    print(f"Python: {sys.executable}")

    env_snapshot = _load_dotenv_snapshot()
    missing_env, optional_present = _check_env()
    missing_files = _check_files()
    ok_imports, failed_imports = _check_imports()
    failed_postgres = _check_optional_postgres(env_snapshot)

    _print("Required env vars", [f"{name}: {'OK' if name not in missing_env else 'MISSING'}" for name in REQUIRED_ENV])
    _print("Optional env vars", [f"{name}: {'SET' if name in optional_present else 'EMPTY'}" for name in OPTIONAL_ENV])
    _print("Core imports", [f"{name}: OK" for name in ok_imports] + [f"{msg}" for msg in failed_imports])

    if missing_files:
        _print("Missing files", missing_files)
    if failed_postgres:
        _print("Optional Postgres persistence", failed_postgres)

    print("")
    if missing_env or missing_files or failed_imports or failed_postgres:
        print("Result: NOT READY")
        if missing_env:
            print("Action: fill required values in .env before starting the app.")
        if failed_imports:
            print("Action: recreate the conda environment with `conda env create -f environment.yml`.")
        if failed_postgres:
            print("Action: install optional Postgres persistence with `pip install langgraph-checkpoint-postgres` or clear NTL_LANGGRAPH_POSTGRES_URL.")
        return 1

    print("Result: READY")
    print("Next: streamlit run Streamlit.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

