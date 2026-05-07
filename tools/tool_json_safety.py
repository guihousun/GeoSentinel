from __future__ import annotations

import math
from datetime import date, datetime, time
from functools import wraps
from pathlib import Path
from typing import Any

from langchain_core.tools import StructuredTool


def make_json_safe(value: Any) -> Any:
    """Recursively normalize common non-JSON-safe runtime values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [make_json_safe(v) for v in value]

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return make_json_safe(model_dump())

    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        try:
            return make_json_safe(tolist())
        except Exception:
            pass

    item = getattr(value, "item", None)
    if callable(item):
        try:
            return make_json_safe(item())
        except Exception:
            pass

    return value


def _sanitize_tool_result(result: Any, response_format: str) -> Any:
    if response_format == "content_and_artifact" and isinstance(result, tuple) and len(result) == 2:
        content, artifact = result
        return make_json_safe(content), make_json_safe(artifact)
    return make_json_safe(result)


def wrap_tool_json_safe(tool: StructuredTool) -> StructuredTool:
    """Return a StructuredTool wrapper that sanitizes tool results before runtime serialization."""
    if not isinstance(tool, StructuredTool):
        return tool

    response_format = str(getattr(tool, "response_format", "content") or "content")
    func = getattr(tool, "func", None)
    coroutine = getattr(tool, "coroutine", None)

    wrapped_func = None
    if callable(func):
        @wraps(func)
        def wrapped_func(*args: Any, **kwargs: Any) -> Any:
            return _sanitize_tool_result(func(*args, **kwargs), response_format)

    wrapped_coroutine = None
    if callable(coroutine):
        @wraps(coroutine)
        async def wrapped_coroutine(*args: Any, **kwargs: Any) -> Any:
            return _sanitize_tool_result(await coroutine(*args, **kwargs), response_format)

    extra_kwargs: dict[str, Any] = {}
    for attr in ("tags", "metadata", "handle_tool_error", "handle_validation_error"):
        value = getattr(tool, attr, None)
        if value is not None:
            extra_kwargs[attr] = value

    return StructuredTool.from_function(
        func=wrapped_func,
        coroutine=wrapped_coroutine,
        name=tool.name,
        description=tool.description,
        return_direct=bool(getattr(tool, "return_direct", False)),
        args_schema=getattr(tool, "args_schema", None),
        response_format=response_format,
        **extra_kwargs,
    )
