from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .paths import reserve_output_path, resolve_local_path

DownloadProgress = Callable[[float, float | None, str], None]

_AUTHORIZATION_BEARER = re.compile(r"(Authorization:\s*Bearer\s+)[^\s]+", re.IGNORECASE)
_BEARER = re.compile(r"\bBearer\s+[^\s]+", re.IGNORECASE)


def sanitize_download_text(text: str) -> str:
    """Remove bearer tokens from text retained in local manifests or results."""
    text = _AUTHORIZATION_BEARER.sub(r"\1<REDACTED>", str(text))
    return _BEARER.sub("Bearer <REDACTED>", text)


def resolve_download_output(raw: str, workdir: Path) -> Path:
    """Resolve a local output path and reserve a no-overwrite target."""
    return reserve_output_path(resolve_local_path(raw, workdir))


def write_download_manifest(path: Path, payload: dict[str, Any]) -> Path:
    """Write a JSON manifest after recursively removing bearer tokens."""
    if not isinstance(payload, dict):
        raise ValueError("download manifest payload must be a JSON object")
    sanitized = _sanitize_value(json.loads(json.dumps(payload, default=str)))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_download_manifest(path: Path) -> dict[str, Any]:
    """Read a JSON object manifest while applying current redaction rules."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"download manifest contains invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError("download manifest must contain a JSON object")
    return _sanitize_value(value)


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_download_text(value)
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    return value
