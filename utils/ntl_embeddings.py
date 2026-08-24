"""Shared embedding configuration for NTL-GPT RAG stores."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


DASHSCOPE_COMPATIBLE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DASHSCOPE_DEFAULT_MODEL = "text-embedding-v4"
DASHSCOPE_DEFAULT_DIMENSIONS = 1024
OPENAI_DEFAULT_MODEL = "text-embedding-3-small"


@dataclass(frozen=True)
class EmbeddingConfig:
    provider: str
    model: str
    api_key: str
    base_url: str | None = None
    dimensions: int | None = None


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _optional_int_env(name: str, default: int | None = None) -> int | None:
    raw = _env(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer when set.") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be greater than 0 when set.")
    return value


def build_embedding_config(model: str | None = None) -> EmbeddingConfig:
    """Build embedding config from environment variables.

    DashScope is the default provider because NTL-GPT's primary model channel is Alibaba.
    OpenAI remains available by setting NTL_EMBEDDING_PROVIDER=openai.
    """

    provider = (_env("NTL_EMBEDDING_PROVIDER") or "dashscope").lower()

    if provider == "dashscope":
        api_key = _env("NTL_EMBEDDING_API_KEY") or _env("DASHSCOPE_Qwen_plus_KEY")
        if not api_key:
            raise RuntimeError(
                "DASHSCOPE_Qwen_plus_KEY is required for NTL Knowledge Base embeddings. "
                "Set DASHSCOPE_Qwen_plus_KEY or NTL_EMBEDDING_API_KEY in environment variables or .env."
            )
        return EmbeddingConfig(
            provider=provider,
            model=model or _env("NTL_EMBEDDING_MODEL") or DASHSCOPE_DEFAULT_MODEL,
            api_key=api_key,
            base_url=_env("NTL_EMBEDDING_BASE_URL") or DASHSCOPE_COMPATIBLE_BASE_URL,
            dimensions=_optional_int_env("NTL_EMBEDDING_DIMENSIONS", DASHSCOPE_DEFAULT_DIMENSIONS),
        )

    if provider == "openai":
        api_key = _env("NTL_EMBEDDING_API_KEY") or _env("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is required when NTL_EMBEDDING_PROVIDER=openai. "
                "Set OPENAI_API_KEY or NTL_EMBEDDING_API_KEY in environment variables or .env."
            )
        return EmbeddingConfig(
            provider=provider,
            model=model or _env("NTL_EMBEDDING_MODEL") or OPENAI_DEFAULT_MODEL,
            api_key=api_key,
            base_url=_env("NTL_EMBEDDING_BASE_URL") or None,
            dimensions=_optional_int_env("NTL_EMBEDDING_DIMENSIONS", None),
        )

    raise RuntimeError(
        "Unsupported NTL_EMBEDDING_PROVIDER. Use 'dashscope' or 'openai'."
    )


def create_text_embeddings(model: str | None = None) -> Any:
    """Create a LangChain embedding client using the configured provider."""

    from langchain_openai import OpenAIEmbeddings

    config = build_embedding_config(model=model)
    kwargs: dict[str, Any] = {
        "model": config.model,
        "api_key": config.api_key,
    }
    if config.base_url:
        kwargs["base_url"] = config.base_url
    if config.dimensions is not None:
        kwargs["dimensions"] = config.dimensions
    return OpenAIEmbeddings(**kwargs)
