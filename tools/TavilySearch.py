from __future__ import annotations

import ast
import json
import re
from typing import Any, Literal, Optional

from langchain_core.tools import StructuredTool
from langchain_tavily import TavilySearch
from pydantic import BaseModel, Field


_DEFAULT_DESCRIPTION = (
    "A search engine optimized for comprehensive, accurate, and trusted results. "
    "Use for official source lookup, current events, and citation-backed retrieval."
)


class TavilySearchSafeInput(BaseModel):
    query: str = Field(description="Search query to look up.")
    include_domains: Optional[list[str] | str] = Field(
        default=None,
        description="Optional domain filter. Accepts list[str], JSON list string, or comma-separated string.",
    )
    exclude_domains: Optional[list[str] | str] = Field(
        default=None,
        description="Optional exclusion filter. Accepts list[str], JSON list string, or comma-separated string.",
    )
    search_depth: Optional[Literal["basic", "advanced", "fast", "ultra-fast"]] = Field(default=None)
    include_images: Optional[bool] = Field(default=None)
    time_range: Optional[Literal["day", "week", "month", "year"]] = Field(default=None)
    topic: Optional[Literal["general", "news", "finance"]] = Field(default=None)
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)


_BASE_TAVILY: Optional[TavilySearch] = None


def _get_base_tavily() -> TavilySearch:
    global _BASE_TAVILY
    if _BASE_TAVILY is None:
        _BASE_TAVILY = TavilySearch(
            topic="general",
            max_results=5,
            search_depth="advanced",
            auto_parameters=True,
            include_favicon=True,
            include_images=False,
        )
    return _BASE_TAVILY


def _clean_domain_token(token: Any) -> Optional[str]:
    if not isinstance(token, str):
        return None
    value = token.strip().strip('"').strip("'").lower()
    if not value:
        return None
    value = re.sub(r"^https?://", "", value)
    value = value.split("/")[0].strip()
    if not value:
        return None
    if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", value):
        return value
    return None


def _normalize_domain_list(raw_value: Any) -> tuple[Optional[list[str]], Optional[str]]:
    if raw_value is None:
        return None, None

    parsed: Any = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return None, None

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(text)
                except (SyntaxError, ValueError):
                    parsed = None
        elif "," in text:
            parsed = [item.strip() for item in text.split(",") if item.strip()]
        else:
            parsed = [text]

    if not isinstance(parsed, list):
        return None, f"Invalid domain filter format: {raw_value!r}"

    normalized: list[str] = []
    for item in parsed:
        domain = _clean_domain_token(item)
        if domain:
            normalized.append(domain)

    normalized = sorted(set(normalized))
    if normalized:
        return normalized, None

    return None, f"Domain filter was provided but no valid domains were parsed: {raw_value!r}"


def _tavily_search_safe(
    query: str,
    include_domains: Optional[list[str] | str] = None,
    exclude_domains: Optional[list[str] | str] = None,
    search_depth: Optional[Literal["basic", "advanced", "fast", "ultra-fast"]] = None,
    include_images: Optional[bool] = None,
    time_range: Optional[Literal["day", "week", "month", "year"]] = None,
    topic: Optional[Literal["general", "news", "finance"]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict[str, Any]:
    normalized_include, include_warning = _normalize_domain_list(include_domains)
    normalized_exclude, exclude_warning = _normalize_domain_list(exclude_domains)

    invoke_args: dict[str, Any] = {"query": query}
    if normalized_include:
        invoke_args["include_domains"] = normalized_include
    if normalized_exclude:
        invoke_args["exclude_domains"] = normalized_exclude
    if search_depth is not None:
        invoke_args["search_depth"] = search_depth
    if include_images is not None:
        invoke_args["include_images"] = include_images
    if time_range is not None:
        invoke_args["time_range"] = time_range
    if topic is not None:
        invoke_args["topic"] = topic
    if start_date is not None:
        invoke_args["start_date"] = start_date
    if end_date is not None:
        invoke_args["end_date"] = end_date

    result = _get_base_tavily().invoke(invoke_args)

    payload = result if isinstance(result, dict) else {"result": result}
    warnings = [warning for warning in [include_warning, exclude_warning] if warning]

    payload["normalized_domains_applied"] = bool(normalized_include or normalized_exclude)
    if normalized_include:
        payload["normalized_include_domains"] = normalized_include
    if normalized_exclude:
        payload["normalized_exclude_domains"] = normalized_exclude
    if warnings:
        payload["domain_filter_dropped_reason"] = "; ".join(warnings)

    return payload


Tavily_search = StructuredTool.from_function(
    func=_tavily_search_safe,
    name="tavily_search",
    description=_DEFAULT_DESCRIPTION,
    args_schema=TavilySearchSafeInput,
)
