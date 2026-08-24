import os
from pathlib import Path

from dotenv import dotenv_values


def load_runtime_environment() -> dict[str, str]:
    env_file = os.getenv("NTL_MCP_ENV_FILE", "").strip()
    if not env_file:
        return {}

    loaded: dict[str, str] = {}
    for key, value in dotenv_values(Path(env_file)).items():
        if value is None or key in os.environ:
            continue
        os.environ[key] = value
        loaded[key] = value
    return loaded


def runtime_workdir() -> Path:
    workdir = os.getenv("NTL_MCP_WORKDIR") or os.getcwd()
    return Path(workdir).expanduser().resolve()
