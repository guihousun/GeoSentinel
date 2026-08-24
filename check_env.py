from __future__ import annotations

import importlib
import importlib.metadata
import os
import sys
from pathlib import Path

from ssl_compat import configure_outbound_ssl


ROOT = Path(__file__).resolve().parent
DOTENV_PATH = ROOT / ".env"

REQUIRED_ENV = [
    "DeepSeek_API_KEY",
    "DeepSeek_Coding_URL",
    "DASHSCOPE_API_KEY",
    "DASHSCOPE_Qwen_plus_KEY",
    "DASHSCOPE_Qwen_plus_URL",
]

OPTIONAL_ENV = [
    "NTL_VLM_MODEL",
    "GEE_DEFAULT_PROJECT_ID",
    "EARTHDATA_TOKEN",
    "amap_api_key",
    "NTL_TOOL_PROFILE",
    "NTL_CONTEXTILY_TMP",
    "NTL_HISTORY_DB_URL",
    "NTL_LANGGRAPH_POSTGRES_URL",
    "NTL_MAX_ACTIVE_RUNS",
    "NTL_MAX_ACTIVE_RUNS_PER_USER",
    "NTL_THREAD_WORKSPACE_QUOTA_MB",
    "NTL_USER_WORKSPACE_QUOTA_MB",
    "NTL_EMBEDDING_PROVIDER",
    "NTL_EMBEDDING_MODEL",
    "NTL_EMBEDDING_BASE_URL",
    "NTL_EMBEDDING_DIMENSIONS",
    "NTL_EMBEDDING_API_KEY",
    "NTL_FORCE_NATIVE_CHAT_INPUT",
    "NTL_USE_CUSTOM_MULTIMODAL_CHAT_INPUT",
    "NTL_MCP_ENV_FILE",
    "NTL_MCP_WORKDIR",
    "NTL_MCP_STATE_DIR",
    "NTL_WEB_SESSION_SECRET",
    "NTL_WEB_COOKIE_SECURE",
    "NTL_WEB_ALLOWED_HOSTS",
    "NTL_WEB_HOST",
    "NTL_WEB_PORT",
    "NTL_WEB_MAX_UPLOAD_MB",
]

CORE_IMPORTS = {
    "streamlit": "streamlit",
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "deepagents": "deepagents",
    "geopandas": "geopandas",
    "rasterio": "rasterio",
}

PINNED_PACKAGE_VERSIONS = {
    "streamlit": "1.55.0",
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
    for rel in (
        "environment.yml",
        ".env.example",
        "Streamlit.py",
        "web_api.py",
        "web_runtime.py",
        "run_web.py",
        "web/index.html",
        "run_streamlit.py",
        "sitecustomize.py",
        "ssl_compat.py",
        "app_ui.py",
        "graph_factory.py",
    ):
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


def _check_pinned_versions() -> tuple[list[str], list[str]]:
    versions = []
    mismatches = []
    for package, expected in PINNED_PACKAGE_VERSIONS.items():
        try:
            installed = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            installed = "MISSING"
        versions.append(f"{package}: {installed} (required {expected})")
        if installed != expected:
            mismatches.append(f"{package}: installed {installed}, required {expected}")
    return versions, mismatches


def main() -> int:
    print("GeoIntelligence environment check")
    print(f"Repository: {ROOT}")
    print(f"Python: {sys.executable}")

    ssl_mode = "FAILED"
    ssl_error = ""
    try:
        ssl_mode = configure_outbound_ssl()
    except Exception as exc:  # noqa: BLE001
        ssl_error = str(exc)

    missing_env, optional_present = _check_env()
    missing_files = _check_files()
    ok_imports, failed_imports = _check_imports()
    pinned_versions, version_mismatches = _check_pinned_versions()

    _print("Required env vars", [f"{name}: {'OK' if name not in missing_env else 'MISSING'}" for name in REQUIRED_ENV])
    _print("Optional env vars", [f"{name}: {'SET' if name in optional_present else 'EMPTY'}" for name in OPTIONAL_ENV])
    _print("Outbound SSL", [f"{ssl_mode}: {ssl_error or 'OK'}"])
    _print("Pinned package versions", pinned_versions)
    _print("Core imports", [f"{name}: OK" for name in ok_imports] + [f"{msg}" for msg in failed_imports])

    if missing_files:
        _print("Missing files", missing_files)

    print("")
    if missing_env or missing_files or failed_imports or ssl_error or version_mismatches:
        print("Result: NOT READY")
        if missing_env:
            print("Action: fill required values in .env before starting the app.")
        if failed_imports:
            print("Action: recreate the conda environment with `conda env create -f environment.yml`.")
        if ssl_error:
            print("Action: repair the Python CA bundle before starting the app.")
        if version_mismatches:
            print("Action: install the pinned package versions from environment.yml.")
        return 1

    print("Result: READY")
    print("Next: python run_web.py --host 127.0.0.1 --port 8502 (formal public web)")
    print("Internal transition/debug UI: streamlit run Streamlit.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
