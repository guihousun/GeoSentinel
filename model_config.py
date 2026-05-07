from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import dotenv_values


MODEL_OPTIONS = [
    "qwen3.6-plus",
    "qwen3.5-plus",
    "MiniMax-M2.7",
    "GPT-5.4",
    "GPT-5.4-mini",
    "GPT-5.4-nano",
]
MINIMAX_DEFAULT_BASE_URL = "https://api.minimaxi.com/v1"
PROJECT_DOTENV = Path(__file__).resolve().parent / ".env"


@dataclass(frozen=True)
class ModelRuntimeConfig:
    provider: str
    api_model: str
    api_key_env: str | None = None
    base_url_env: str | None = None
    default_base_url: str | None = None
    key_label: str = "OpenAI API Key"
    uses_env_api_key: bool = False


def get_model_config(model_name: str) -> ModelRuntimeConfig:
    name = str(model_name or "").strip()
    normalized = name.lower().replace("-", "").replace("_", "").replace(" ", "").replace(".", "")

    if normalized.startswith("qwen"):
        return ModelRuntimeConfig(
            provider="dashscope",
            api_model=name,
            api_key_env="DASHSCOPE_API_KEY",
            base_url_env="DASHSCOPE_Coding_URL",
            key_label="DashScope API Key",
            uses_env_api_key=True,
        )

    if normalized in {"minimax27", "minimaxm27", "codexminimaxm27"}:
        return ModelRuntimeConfig(
            provider="minimax",
            api_model="MiniMax-M2.7",
            api_key_env="MINIMAX_API_KEY",
            base_url_env="MINIMAX_Coding_URL",
            default_base_url=MINIMAX_DEFAULT_BASE_URL,
            key_label="MiniMax API Key",
            uses_env_api_key=True,
        )

    if normalized.startswith("gpt"):
        return ModelRuntimeConfig(provider="openai", api_model=name.lower(), key_label="OpenAI API Key")

    if "claude" in normalized:
        return ModelRuntimeConfig(provider="anthropic", api_model=name, key_label="Anthropic API Key")

    return ModelRuntimeConfig(provider="openai", api_model=name, key_label="OpenAI API Key")


def get_api_model_name(model_name: str) -> str:
    return get_model_config(model_name).api_model


@lru_cache(maxsize=1)
def _project_dotenv_values() -> dict[str, str]:
    if not PROJECT_DOTENV.exists():
        return {}
    return {key: str(value or "") for key, value in dotenv_values(PROJECT_DOTENV).items()}


def _get_configured_env(name: str) -> str:
    project_values = _project_dotenv_values()
    if name in project_values:
        return project_values[name].strip()
    return str(os.getenv(name, "") or "").strip()


def get_base_url(model_name: str) -> str | None:
    config = get_model_config(model_name)
    if config.base_url_env:
        configured = _get_configured_env(config.base_url_env)
        if configured:
            return configured
    return config.default_base_url


def get_env_api_key(model_name: str) -> str:
    config = get_model_config(model_name)
    if not config.api_key_env:
        return ""
    return _get_configured_env(config.api_key_env)


def missing_env_for_model(model_name: str) -> list[str]:
    config = get_model_config(model_name)
    missing: list[str] = []
    if config.api_key_env and not _get_configured_env(config.api_key_env):
        missing.append(config.api_key_env)
    if config.provider == "dashscope" and config.base_url_env and not _get_configured_env(config.base_url_env):
        missing.append(config.base_url_env)
    return missing
