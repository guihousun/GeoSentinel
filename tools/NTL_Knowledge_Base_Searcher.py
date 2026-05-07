# ntl_knowledge_base_searcher.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import re
import uuid
from functools import lru_cache
from typing import Annotated, Optional

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import StructuredTool
from langchain_openai import ChatOpenAI
from langgraph.config import get_stream_writer
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from tools.NTL_Knowledge_Base import (
    NTL_Code_Knowledge,
    NTL_Literature_Knowledge,
    NTL_Solution_Knowledge,
)
from utils.ntl_kb_aliases import (
    TOOL_ALIAS_MAP,
    normalize_tool_name,
    normalize_workflow_payload,
)


class State(TypedDict):
    messages: Annotated[list, add_messages]
    response_mode: Optional[str]  # auto | workflow | theory | code | mixed
    locale: Optional[str]  # en | zh, etc.
    need_citations: Optional[bool]
    intent_profile: Optional[dict]


TOOLS = [NTL_Literature_Knowledge, NTL_Solution_Knowledge, NTL_Code_Knowledge]


@lru_cache(maxsize=1)
def _tool_registry_snapshot() -> dict[str, str]:
    from tools import Code_tools, Engineer_tools, data_searcher_tools, specialized_tool_catalog

    snapshot: dict[str, str] = {}
    for tool in Engineer_tools + data_searcher_tools + Code_tools + specialized_tool_catalog:
        name = getattr(tool, "name", "")
        if not name or name == "NTL_Knowledge_Base":
            continue
        snapshot[name] = getattr(tool, "description", "No description provided.")
    return snapshot


def _tool_manual_str() -> str:
    items = [f"- **{name}**: {desc}" for name, desc in _tool_registry_snapshot().items()]
    return "\n".join(items)


def _extract_first_json_dict(text: str) -> tuple[dict | None, str]:
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            obj, end = decoder.raw_decode(text[idx:])
        except Exception:
            continue
        if isinstance(obj, dict):
            rest = text[:idx] + text[idx + end :]
            return obj, rest.strip()
    return None, text


def _extract_empty_store_status(content: str) -> dict | None:
    payload, _ = _extract_first_json_dict(content)
    if isinstance(payload, dict) and payload.get("status") == "empty_store":
        return payload
    return None


def _is_methodology_reproduction_query(query: str) -> bool:
    q = (query or "").lower()
    keywords = (
        "reproduce",
        "replicate",
        "methodology",
        "methods",
        "equation",
        "parameter setting",
        "experiment setup",
        "according to the paper",
        "paper method",
        "according to paper",
        "based on paper",
        "reproduction",
        "methodological",
        "formula",
        "parameter setting",
        "experiment setup",
        "according to literature",
        "based on literature",
    )
    return any(keyword in q for keyword in keywords)


def _extract_latest_user_query(messages: list) -> str:
    for item in reversed(messages or []):
        if isinstance(item, tuple) and len(item) >= 2:
            role = str(item[0]).lower()
            if role in {"user", "human"}:
                return str(item[1])
        role = str(getattr(item, "type", "")).lower()
        if role in {"human", "user"}:
            return str(getattr(item, "content", ""))
    return ""


def _build_searcher_llm() -> ChatOpenAI:
    api_key = (
        os.getenv("DASHSCOPE_Qwen_plus_KEY")
        or os.getenv("DASHSCOPE_API_KEY")
        or os.getenv("DOUBAO_API_KEY")
    )
    if not api_key:
        raise RuntimeError(
            "DASHSCOPE_Qwen_plus_KEY (or DASHSCOPE_API_KEY / DOUBAO_API_KEY) is required for "
            "NTL_Knowledge_Base_Searcher qwen-plus model."
        )
    base_url = (
        os.getenv("DASHSCOPE_Qwen_plus_URL")
        or os.getenv("DASHSCOPE_Coding_URL")
        or os.getenv("DOUBAO_BASE_URL")
        or os.getenv("ARK_OPENAI_BASE_URL")
        or "https://ark.cn-beijing.volces.com/api/v3"
    )
    return ChatOpenAI(
        api_key=api_key,
        base_url=base_url,
        model="qwen-plus"
    )


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _count_matches(text: str, terms: tuple[str, ...]) -> int:
    return sum(1 for term in terms if term in text)


def _extract_first_json_list(text: str) -> tuple[list | None, str]:
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch != "[":
            continue
        try:
            obj, end = decoder.raw_decode(text[idx:])
        except Exception:
            continue
        if isinstance(obj, list):
            rest = text[:idx] + text[idx + end :]
            return obj, rest.strip()
    return None, text


def _safe_json_loads(text: str):
    if not isinstance(text, str):
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    payload, _ = _extract_first_json_dict(text)
    if isinstance(payload, dict):
        return payload
    lst, _ = _extract_first_json_list(text)
    if isinstance(lst, list):
        return lst
    return None


def _fallback_intent_profile(query: str, mode: str = "auto") -> dict:
    q = (query or "").lower()
    retrieval_like = _contains_any(q, ("download", "retrieve", "fetch", "collect", "obtain"))
    official_like = _contains_any(q, ("official", "authoritative", "usgs", "reliefweb", "government", "ocha", "un"))
    event_like = _contains_any(
        q,
        (
            "earthquake",
            "wildfire",
            "fire",
            "flood",
            "hurricane",
            "typhoon",
            "cyclone",
            "drought",
            "war",
            "conflict",
            "battle",
            "disaster",
            "emergency",
        ),
    )
    geospatial_like = _contains_any(
        q, ("gee", "earth engine", "viirs", "vnp46", "ntl", "antl", "radiance", "zonal", "raster")
    )
    analysis_like = _contains_any(
        q,
        (
            "assess",
            "assessment",
            "analysis",
            "impact",
            "damage",
            "compute",
            "calculate",
            "quantify",
            "pre-event",
            "post-event",
            "before event",
            "after event",
            "first night",
            "time series",
            "recovery",
        ),
    )
    if "_is_methodology_reproduction_query" in globals():
        methodology_like = bool(_is_methodology_reproduction_query(query))
    else:
        methodology_like = _contains_any(q, ("reproduce", "replicate", "method", "equation", "paper"))

    if methodology_like:
        intent_type = "methodology_reproduction"
    elif event_like and geospatial_like and analysis_like:
        intent_type = "event_impact_assessment"
    elif retrieval_like:
        intent_type = "data_retrieval"
    elif mode == "theory":
        intent_type = "theory_explanation"
    elif mode == "code":
        intent_type = "code_generation"
    else:
        intent_type = "general_query"

    return {
        "intent_type": intent_type,
        "requires_workflow": mode in {"workflow", "auto", "mixed"} or intent_type in {"event_impact_assessment", "data_retrieval"},
        "needs_official_sources": official_like or (event_like and analysis_like),
        "needs_geospatial_analysis": geospatial_like or intent_type == "event_impact_assessment",
        "needs_server_side_execution": geospatial_like and (analysis_like or event_like),
        "prefer_literature": methodology_like or mode == "theory",
        "prefer_code": mode in {"code", "mixed"} or geospatial_like,
        "prefer_solution": mode in {"workflow", "auto", "mixed"} and not methodology_like,
    }


def _propose_task_level_fallback(query: str, intent_profile: dict | None = None, mode: str = "auto") -> dict:
    # Minimal default only (non-rule fallback). Normal path is LLM classification.
    _ = (query, intent_profile, mode)
    return {
        "proposed_task_level": "L2",
        "task_level_reason_codes": ["low_confidence_match"],
        "task_level_confidence": 0.5,
    }


def _normalize_task_level_payload(payload: dict | None, fallback: dict) -> dict:
    allowed_levels = {"L1", "L2", "L3"}
    allowed_reason_codes = {
        "built_in_tool_matched",
        "download_only",
        "analysis_with_tool",
        "no_tool_custom_code",
        "algorithm_gap",
        "low_confidence_match",
    }
    data = dict(payload or {})
    level = str(data.get("proposed_task_level") or "").upper().strip()
    if level not in allowed_levels:
        level = str(fallback.get("proposed_task_level", "L2")).upper()
    reason_codes = data.get("task_level_reason_codes")
    if not isinstance(reason_codes, list):
        reason_codes = fallback.get("task_level_reason_codes", [])
    reason_codes = [str(code).strip() for code in reason_codes if str(code).strip() in allowed_reason_codes]
    if not reason_codes:
        reason_codes = fallback.get("task_level_reason_codes", ["low_confidence_match"])
    confidence_raw = data.get("task_level_confidence", fallback.get("task_level_confidence", 0.5))
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = float(fallback.get("task_level_confidence", 0.5))
    confidence = max(0.0, min(1.0, confidence))
    return {
        "proposed_task_level": level,
        "task_level_reason_codes": reason_codes,
        "task_level_confidence": confidence,
    }


def _classify_task_level_with_fallback(query: str, intent_profile: dict | None = None, mode: str = "auto") -> dict:
    """已禁用：task_level 分类已合并到 _repair_and_augment_output 中"""
    return _propose_task_level_fallback(query=query, intent_profile=intent_profile, mode=mode)


def _augment_intent_with_task_level(intent_profile: dict, query: str, mode: str = "auto") -> dict:
    intent = dict(intent_profile or {})
    fallback = _propose_task_level_fallback(query=query, intent_profile=intent, mode=mode)
    existing_payload = {
        "proposed_task_level": intent.get("proposed_task_level"),
        "task_level_reason_codes": intent.get("task_level_reason_codes"),
        "task_level_confidence": intent.get("task_level_confidence"),
    }
    proposal = _normalize_task_level_payload(existing_payload, fallback)
    if not intent.get("proposed_task_level"):
        if "_classify_task_level_with_fallback" in globals():
            proposal = _classify_task_level_with_fallback(query=query, intent_profile=intent, mode=mode)
        else:
            proposal = fallback
    intent["proposed_task_level"] = proposal.get("proposed_task_level", "L2")
    reason_codes = proposal.get("task_level_reason_codes", [])
    intent["task_level_reason_codes"] = reason_codes if isinstance(reason_codes, list) else []
    try:
        intent["task_level_confidence"] = float(proposal.get("task_level_confidence", 0.5))
    except Exception:
        intent["task_level_confidence"] = 0.5
    intent["task_level_proposal_source"] = "NTL_Knowledge_Base_Searcher"
    return intent


def _normalize_intent_payload(payload, mode: str, query: str) -> dict:
    fallback = _fallback_intent_profile(query, mode)
    if not isinstance(payload, dict):
        if "_augment_intent_with_task_level" in globals():
            return _augment_intent_with_task_level(fallback, query, mode)
        return fallback

    normalized = dict(fallback)
    normalized.update(payload)

    bool_fields = (
        "requires_workflow",
        "needs_official_sources",
        "needs_geospatial_analysis",
        "needs_server_side_execution",
        "prefer_literature",
        "prefer_code",
        "prefer_solution",
    )
    for field in bool_fields:
        normalized[field] = bool(normalized.get(field, fallback[field]))

    intent_type = str(normalized.get("intent_type") or "").strip().lower()
    if not intent_type:
        intent_type = fallback["intent_type"]
    normalized["intent_type"] = intent_type
    if "_augment_intent_with_task_level" in globals():
        return _augment_intent_with_task_level(normalized, query, mode)
    return normalized


def _classify_query_intent_with_fallback(query: str, mode: str = "auto") -> dict:
    import os

    fallback = _fallback_intent_profile(query, mode)
    if "_augment_intent_with_task_level" in globals():
        fallback = _augment_intent_with_task_level(fallback, query, mode)
    api_key = os.getenv("DASHSCOPE_API_KEY") or os.getenv("QWEN_API_KEY")
    if not api_key:
        return fallback

    prompt = (
        "Classify the user query intent for a geospatial/NTL workflow planner.\n"
        "Return JSON object only. No markdown, no commentary.\n\n"
        "Required keys:\n"
        "- intent_type: one of methodology_reproduction|event_impact_assessment|data_retrieval|theory_explanation|code_generation|general_query\n"
        "- requires_workflow: boolean\n"
        "- needs_official_sources: boolean\n"
        "- needs_geospatial_analysis: boolean\n"
        "- needs_server_side_execution: boolean\n"
        "- prefer_literature: boolean\n"
        "- prefer_code: boolean\n"
        "- prefer_solution: boolean\n\n"
        f"response_mode: {mode}\n"
        f"query: {query}"
    )

    try:
        llm = _build_searcher_llm()
        response = llm.invoke(prompt)
        content = getattr(response, "content", response)
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    txt = item.get("text")
                    if txt:
                        parts.append(str(txt))
            content = "\n".join(parts)
        elif not isinstance(content, str):
            content = str(content)

        parsed = _safe_json_loads(content)
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            parsed = parsed[0]
        return _normalize_intent_payload(parsed, mode, query)
    except Exception:
        return fallback


def _is_event_analysis_intent(intent_profile: dict) -> bool:
    if not isinstance(intent_profile, dict):
        return False
    if str(intent_profile.get("intent_type", "")).lower() == "event_impact_assessment":
        return True
    return bool(
        intent_profile.get("needs_geospatial_analysis")
        and intent_profile.get("needs_server_side_execution")
        and intent_profile.get("needs_official_sources")
    )


def _infer_tool_from_intent(intent_profile: dict, valid_tools: set[str]) -> str:
    # Disabled by policy: do not auto-score/override tool names.
    # Step names should be produced by the agent output and only normalized via alias rules.
    _ = (intent_profile, valid_tools)
    return ""


def _infer_tool_from_query(query: str, valid_tools: set[str], intent_profile: dict | None = None, mode: str = "auto") -> str:
    # Disabled by policy: no query-level hardcoded fallback mapping.
    _ = (query, valid_tools, intent_profile, mode)
    return ""


def _build_force_json_fallback_payload(
    user_query: str,
    valid_tools: set[str],
    intent_profile: dict | None = None,
    mode: str = "auto",
) -> dict:
    intent = intent_profile if isinstance(intent_profile, dict) else _classify_query_intent_with_fallback(user_query, mode)

    if _is_event_analysis_intent(intent):
        steps: list[dict] = []
        if "tavily_search" in valid_tools:
            steps.append(
                {
                    "type": "builtin_tool",
                    "name": "tavily_search",
                    "input": {
                        "query": (
                            "Retrieve authoritative event metadata from official sources "
                            "(event time, location/epicenter, intensity, and affected areas)."
                        )
                    },
                }
            )
        steps.extend(
            [
                {
                    "type": "geospatial_code",
                    "description": (
                        "Initialize GEE Python API and build an event-centric analysis region. "
                        "Select the appropriate daily NTL product (prefer VNP46A2 for VIIRS daily analyses) "
                        "and compute period statistics for pre-event baseline, first post-event overpass night, "
                        "and post-event recovery windows. For daily VNP46A2, determine first-night using event "
                        "time at epicenter local timezone: if event occurs after the local nightly overpass "
                        "(often within a candidate range such as ~00:30-02:30 local), first-night must shift to local day D+1 (not D). "
                        "For UTC-indexed daily products/files, convert that local first-night acquisition time "
                        "back to UTC before choosing the image/file date; record both the local first-night "
                        "date and UTC file date in outputs. If the UTC date boundary is ambiguous, require pixel-level UTC_Time only when the relevant source covers the target date; otherwise use LAADS/CMR granule timing or official metadata. "
                        "Save daily/period metrics to "
                        "'outputs/event_daily_antl_metrics.csv'."
                    ),
                },
                {
                    "type": "geospatial_code",
                    "description": (
                        "Read 'outputs/event_daily_antl_metrics.csv' and compute impact/recovery indicators "
                        "dynamically using explicit formulas and assumptions. Save structured report to "
                        "'outputs/event_impact_assessment_report.json'."
                    ),
                },
            ]
        )
        return {
            "status": "ok",
            "task_id": "generated_event_analysis_workflow",
            "task_name": "Event impact assessment with GEE daily NTL",
            "category": "Generated",
            "description": "Intent-driven fallback workflow for event-impact geospatial analysis.",
            "steps": steps,
            "output": "outputs/event_impact_assessment_report.json",
        }

    fallback_tool = _infer_tool_from_query(user_query, valid_tools, intent_profile=intent, mode=mode)
    if fallback_tool:
        return {
            "status": "ok",
            "task_id": "generated_text_fallback_workflow",
            "task_name": f"Run {fallback_tool}",
            "category": "Generated",
            "description": "Generated from non-JSON model output to keep workflow contract stable.",
            "steps": [
                {
                    "type": "builtin_tool",
                    "name": fallback_tool,
                    "input": {},
                }
            ],
            "output": "",
        }

    return {
        "status": "no_valid_tool",
        "reason": (
            "Model returned non-JSON workflow output and no fallback tool "
            "could be inferred from intent."
        ),
        "sources": [],
    }


def _repair_and_augment_output(
    raw_content: str,
    *,
    user_query: str,
    mode: str,
    intent_profile: dict | None,
    valid_tools: set[str],
) -> dict | None:
    """修复非 JSON 输出并同时添加 task_level（合并版，一次 LLM 调用）"""
    if not isinstance(raw_content, str):
        return None
    text = raw_content.strip()
    if not text:
        return None

    tool_list = ", ".join(sorted(valid_tools))
    intent_type = str((intent_profile or {}).get("intent_type", "")).strip().lower()
    
    prompt = (
        "You are an NTL workflow parser. Rewrite the raw assistant output into a valid JSON object.\n"
        "Return JSON only. No markdown.\n\n"
        
        "=== Required JSON Structure ===\n"
        "1. Workflow fields (if mode is workflow/auto/mixed):\n"
        "   task_id, task_name, category, description, steps(list), output\n"
        "2. Task Level fields:\n"
        "   proposed_task_level: L1|L2|L3\n"
        "   task_level_reason_codes: array from [built_in_tool_matched, download_only, analysis_with_tool, no_tool_custom_code, algorithm_gap, low_confidence_match]\n"
        "   task_level_confidence: number in [0,1]\n\n"
        
        "=== Task Level Definitions ===\n"
        "- L1: download/retrieval only with built-in tools (single-step data fetch)\n"
        "- L2: analysis/statistics/comparison with direct built-in tool support (1-2 steps)\n"
        "- L3: complex multi-step workflows requiring coordination, external data sources, event analysis, or custom algorithms\n\n"
        "=== L3 Indicators (any of these → L3) ===\n"
        "- Event impact assessment (earthquake, flood, wildfire, etc.)\n"
        "- Multi-source data integration (USGS, ReliefWeb, etc.)\n"
        "- Multiple time period comparisons (pre/post event, baseline, etc.)\n"
        "- More than 3 workflow steps\n"
        "- Custom damage assessment or impact summary\n"
        "- Pixel-level analysis or custom algorithms\n"
        "- Requires GEE script generation for Code Assistant execution\n"
        "- Server-side processing with custom code (not direct tool call)\n\n"
        
        "=== Tool Registry ===\n"
        f"Use only: {tool_list}\n"
        "If a step has no executable tool, use {\"type\":\"instruction\",\"description\":\"...\"}\n"
        "If unrecoverable, return {\"status\":\"no_valid_tool\",\"reason\":\"...\",\"sources\":[]}\n\n"
        
        f"mode: {mode}\n"
        f"intent_type: {intent_type}\n"
        f"user_query: {user_query}\n"
        f"intent_profile: {json.dumps(intent_profile or {}, ensure_ascii=False)}\n\n"
        f"raw_output: {text}"
    )
    try:
        llm = _build_searcher_llm()
        response = llm.invoke(prompt)
        content = getattr(response, "content", response)
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    txt = item.get("text")
                    if txt:
                        parts.append(str(txt))
            content = "\n".join(parts)
        elif not isinstance(content, str):
            content = str(content)
        parsed = _safe_json_loads(content)
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            parsed = parsed[0]
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _build_kb_response_contract(
    payload: dict,
    mode: str = "workflow",
    intent_profile: dict | None = None,
    supplementary_text: str = "",
) -> dict:
    if not isinstance(payload, dict):
        payload = {"status": "error", "reason": "Invalid payload type."}

    workflow = payload.get("workflow")
    if not isinstance(workflow, dict):
        if any(k in payload for k in ("task_id", "task_name", "category", "description", "steps", "output")):
            workflow = {
                "task_id": payload.get("task_id", ""),
                "task_name": payload.get("task_name", ""),
                "category": payload.get("category", ""),
                "description": payload.get("description", ""),
                "steps": payload.get("steps", []),
                "output": payload.get("output", ""),
            }
        else:
            workflow = {}

    status = str(payload.get("status") or ("ok" if workflow else "error")).strip().lower()
    reason = str(payload.get("reason") or "").strip()
    message = str(payload.get("message") or reason).strip()
    sources = payload.get("sources", [])
    if not isinstance(sources, list):
        sources = [sources] if sources else []

    intent = intent_profile if isinstance(intent_profile, dict) else {}
    contract = {
        "schema": "ntl.kb.response.v2",
        "status": status,
        "mode": mode,
        "intent": intent,
        "message": message,
        "reason": reason,
        "sources": sources,
        "workflow": workflow,
    }
    proposed_level = intent.get("proposed_task_level")
    if proposed_level:
        contract["proposed_task_level"] = proposed_level
    reason_codes = intent.get("task_level_reason_codes")
    if isinstance(reason_codes, list):
        contract["task_level_reason_codes"] = reason_codes
    if "task_level_confidence" in intent:
        contract["task_level_confidence"] = intent.get("task_level_confidence")
    if "task_level_proposal_source" in intent:
        contract["task_level_proposal_source"] = intent.get("task_level_proposal_source")

    for key in ("task_id", "task_name", "category", "description", "steps", "output"):
        value = workflow.get(key)
        if value is None or value == "":
            value = payload.get(key)
        if value is not None and value != "":
            contract[key] = value

    if "steps" not in contract or not isinstance(contract.get("steps"), list):
        contract["steps"] = []
    if "output" not in contract:
        contract["output"] = ""

    if supplementary_text:
        contract["supplementary_text"] = supplementary_text
    return contract


def _build_non_executable_workflow_payload(
    original_payload: dict,
    normalized_payload: dict,
    invalid_names: list[str],
) -> dict:
    """
    Preserve agent-authored workflow semantics when builtin tool names are invalid.
    Invalid builtin steps are downgraded to non-executable analysis steps while
    keeping original step titles/descriptions.
    """
    invalid_set = set(str(x) for x in (invalid_names or []))
    normalized = dict(normalized_payload or {})
    original_steps = (
        list(original_payload.get("steps", []))
        if isinstance(original_payload, dict) and isinstance(original_payload.get("steps"), list)
        else []
    )
    normalized_steps = normalized.get("steps", [])
    if not isinstance(normalized_steps, list):
        normalized_steps = []

    preserved_steps: list[dict] = []
    for idx, step in enumerate(normalized_steps):
        if not isinstance(step, dict):
            continue
        current = dict(step)
        step_type = str(current.get("type", ""))
        step_name = str(current.get("name", ""))
        if step_type == "builtin_tool" and step_name in invalid_set:
            raw_step = original_steps[idx] if idx < len(original_steps) and isinstance(original_steps[idx], dict) else {}
            raw_title = (
                str(raw_step.get("name") or raw_step.get("tool_name") or raw_step.get("action") or "").strip()
            )
            title = raw_title or step_name or f"step_{idx + 1}"
            description = str(
                current.get("description")
                or raw_step.get("description")
                or raw_step.get("note")
                or ""
            ).strip()
            if not description:
                description = f"Agent-proposed step: {title}"
            downgraded = {
                "type": "analysis_step",
                "name": title,
                "description": description,
            }
            step_input = current.get("input")
            if isinstance(step_input, dict) and step_input:
                downgraded["input"] = step_input
            preserved_steps.append(downgraded)
            continue
        preserved_steps.append(current)

    normalized["steps"] = preserved_steps
    normalized["status"] = "no_valid_tool"
    normalized["reason"] = (
        "Invalid or unavailable tool names after normalization: "
        + ", ".join(sorted(invalid_set))
    )
    return normalized


def _validate_and_normalize_workflow_output(
    content: str,
    user_query: str = "",
    allow_trailing_text: bool = False,
    force_json: bool = False,
    intent_profile: dict | None = None,
    response_mode: str = "workflow",
) -> str:
    """
    浼樺寲鐗堟湰锛氬鐞嗗寘鍚?intent_analysis 鐨勬柊鏍煎紡鍝嶅簲
    """
    valid_tools = set(_tool_registry_snapshot().keys())

    # Use the provided intent_profile, or fall back to rule-based intent.
    intent = intent_profile if isinstance(intent_profile, dict) else _fallback_intent_profile(user_query, response_mode)
    if "_augment_intent_with_task_level" in globals():
        intent = _augment_intent_with_task_level(intent, user_query, response_mode)

    # 鎻愬彇 JSON
    data, rest = _extract_first_json_dict(content)

    # If model returned {"intent_analysis": ..., "response": ...}, split them.
    if isinstance(data, dict) and "intent_analysis" in data:
        # 鏇存柊 intent锛圠LM 鐢熸垚鐨勪紭鍏堜簬瑙勫垯鐢熸垚鐨勶級
        llm_intent = data.get("intent_analysis", {})
        if isinstance(llm_intent, dict):
            intent.update(llm_intent)
        # Get the actual response payload.
        data = data.get("response", data)

    if not isinstance(data, dict):
        if force_json:
            repaired = _repair_and_augment_output(
                content,
                user_query=user_query,
                mode=response_mode,
                intent_profile=intent,
                valid_tools=valid_tools,
            )
            if isinstance(repaired, dict):
                data = repaired
                if "intent_analysis" in data and isinstance(data.get("intent_analysis"), dict):
                    intent.update(data.get("intent_analysis", {}))
                    data = data.get("response", data)
            if not isinstance(data, dict):
                data = _build_force_json_fallback_payload(
                    user_query,
                    valid_tools,
                    intent_profile=intent,
                    mode=response_mode,
                )
        else:
            data = {
                "status": "error",
                "reason": "Model returned non-JSON output.",
                "message": "workflow payload unavailable for this request",
                "sources": [],
            }
            return json.dumps(
                _build_kb_response_contract(
                    data,
                    mode=response_mode,
                    intent_profile=intent,
                    supplementary_text=rest if allow_trailing_text and rest else "",
                ),
                ensure_ascii=False,
                indent=2,
            )
    if data.get("status") == "no_valid_tool":
        return json.dumps(
            _build_kb_response_contract(
                data,
                mode=response_mode,
                intent_profile=intent,
                supplementary_text=rest if allow_trailing_text and rest else "",
            ),
            ensure_ascii=False,
            indent=2,
        )

    if "steps" not in data and "tool_name" in data:
        normalized_tool = normalize_tool_name(str(data.get("tool_name", "")).strip())
        parameters = data.get("parameters")
        if not isinstance(parameters, dict):
            parameters = {}

        data = {
            "task_id": "generated_tool_workflow",
            "task_name": f"Run {normalized_tool or 'tool'}",
            "category": "Generated",
            "description": "Generated single-tool workflow from tool_name payload.",
            "steps": [
                {
                    "type": "builtin_tool",
                    "name": normalized_tool,
                    "input": parameters,
                }
            ],
            "output": data.get("output", ""),
        }
    elif "steps" not in data:
        embedded_workflow = data.get("workflow")
        embedded_steps = embedded_workflow.get("steps") if isinstance(embedded_workflow, dict) else None
        if isinstance(embedded_workflow, dict) and isinstance(embedded_steps, (list, dict)):
            promoted = dict(data)
            for key in ("task_id", "task_name", "category", "description", "steps", "output", "result", "sources"):
                if key not in promoted and key in embedded_workflow:
                    promoted[key] = embedded_workflow.get(key)
            if "steps" not in promoted:
                promoted["steps"] = embedded_steps
            if "output" not in promoted and "output" in embedded_workflow:
                promoted["output"] = embedded_workflow.get("output")
            data = promoted
        elif force_json:
            data = _build_force_json_fallback_payload(
                user_query,
                valid_tools,
                intent_profile=intent,
                mode=response_mode,
            )
            if data.get("status") == "ok" and isinstance(data, dict):
                data["task_id"] = "generated_dict_fallback_workflow"
        else:
            return json.dumps(
                _build_kb_response_contract(
                    data,
                    mode=response_mode,
                    intent_profile=intent,
                    supplementary_text=rest if allow_trailing_text and rest else "",
                ),
                ensure_ascii=False,
                indent=2,
            )

    normalized, invalid_names = normalize_workflow_payload(data, valid_tools)

    if invalid_names:
        data = _build_non_executable_workflow_payload(data, normalized, invalid_names)
        return json.dumps(
            _build_kb_response_contract(
                data,
                mode=response_mode,
                intent_profile=intent,
                supplementary_text=rest if allow_trailing_text and rest else "",
            ),
            ensure_ascii=False,
            indent=2,
        )

    return json.dumps(
        _build_kb_response_contract(
            normalized,
            mode=response_mode,
            intent_profile=intent,
            supplementary_text=rest if allow_trailing_text and rest else "",
        ),
        ensure_ascii=False,
        indent=2,
    )


def agent(state: State):
    """
    浼樺寲鍚庣殑 Agent锛氬湪鐢熸垚鍝嶅簲鐨勫悓鏃惰繘琛屾剰鍥惧垎鏋?    """
    mode = (state.get("response_mode") or "auto").lower()
    need_citations = bool(state.get("need_citations", True))
    locale = state.get("locale", "en")
    user_query = _extract_latest_user_query(state.get("messages", []))

    # 鑾峰彇宸ュ叿淇℃伅
    alias_lines = "\n".join(
        f"- `{legacy}` -> `{canonical}`" for legacy, canonical in sorted(TOOL_ALIAS_MAP.items())
    )

    # Optimized system prompt with explicit intent-analysis requirement.
    system_prompt_text = SystemMessage(
        f"""You are the NTL Knowledge Base Agent.
Your mission is to analyze user intent and generate grounded NTL workflows/theory/code.

### Intent Analysis
Analyze the query and output:

```json
{{
  "intent_analysis": {{
    "intent_type": "event_impact_assessment|methodology_reproduction|data_retrieval|theory_explanation|code_generation|general_query",
    "requires_workflow": true/false,
    "needs_official_sources": true/false,
    "needs_geospatial_analysis": true/false,
    "needs_server_side_execution": true/false,
    "prefer_literature": true/false,
    "prefer_code": true/false,
    "prefer_solution": true/false,
    "proposed_task_level": "L1|L2|L3",
    "task_level_reason_codes": ["built_in_tool_matched|download_only|analysis_with_tool|no_tool_custom_code|algorithm_gap|low_confidence_match"],
    "task_level_confidence": 0.0
  }}
}}
```

Intent Type Guidelines:
- **methodology_reproduction**: Query asks about reproducing paper methods, equations, parameters
- **event_impact_assessment**: Query involves disasters (earthquake, flood, wildfire) with NTL analysis
- **data_retrieval**: Query asks to download, fetch, or collect data
- **theory_explanation**: Query asks "what is", "why", definitions, theoretical concepts
- **code_generation**: Query asks for example code, scripts, implementation
- **general_query**: Other general questions about NTL

### Tool Selection
You have access to 3 knowledge stores. Follow this strict retrieval budget policy:
- Always prioritize **NTL_Solution_Knowledge** first.
- Query at most **1-2 stores** per request (never all 3 by default).
- Choose the second store by mode:
  - **theory** -> add **NTL_Literature_Knowledge**
  - **workflow/code/mixed/auto** -> add **NTL_Code_Knowledge**
- Only use a single store when confidence is already high from Solution retrieval.

**Store Selection Guide:**
- **NTL_Literature_Knowledge**: Use for theory, formulas, scientific definitions, methodology reproduction
- **NTL_Solution_Knowledge**: Use for workflows, best practices, tool usage patterns, datasets
- **NTL_Code_Knowledge**: Use for Python/GEE code snippets, implementation examples

### Available Tools
{_tool_manual_str()}

### Response Modes
1) **workflow**: Return strict JSON object with intent_analysis and response (no markdown code fences).
2) **theory**: Concise grounded bullets with intent_analysis.
3) **code**: Minimal runnable code snippet with intent_analysis.
4) **mixed**: workflow JSON + theory bullets + short code, all with intent_analysis.

### Output Format
Your response MUST be a valid JSON object with this structure:

```json
{{
  "intent_analysis": {{
    "intent_type": "...",
    "requires_workflow": true/false,
    "needs_official_sources": true/false,
    "needs_geospatial_analysis": true/false,
    "needs_server_side_execution": true/false,
    "prefer_literature": true/false,
    "prefer_code": true/false,
    "prefer_solution": true/false,
    "proposed_task_level": "L1|L2|L3",
    "task_level_reason_codes": ["..."],
    "task_level_confidence": 0.0
  }},
  "response": {{
    // For workflow mode:
    "task_id": "...",
    "task_name": "...",
    "category": "...",
    "description": "...",
    "steps": [
      {{
        "type": "instruction",
        "description": "Step description without tool call"
      }},
      {{
        "type": "builtin_tool",
        "name": "ActualToolNameFromRegistry",
        "input": {{...}},
        "description": "What this tool call does"
      }}
    ],
    "output": "..."
  }}
}}
```

Rules:
- Use only registered tools. Use defaults for missing parameters.
- Return `no_valid_tool` only when intent cannot be mapped to any tool.
- Preserve retrieved workflow details; use fallbacks only when necessary.

**CRITICAL: Tool Name Requirements**
- ONLY use tool names from the Available Tools list above
- If a workflow step doesn't need a tool call, use {{"type": "instruction", "description": "..."}}
- Valid step types: "builtin_tool" (for actual tool calls), "instruction" (for guidance only)
- Handle Code_RAG empty status by reporting "code corpus unavailable".
- Treat "first night after event" as the first post-event nighttime overpass at epicenter local time.
- If the target daily product/file is UTC-indexed, convert that local acquisition time to UTC before selecting the image/file date.

Language: {locale}
Citations Required: {need_citations}
Current Mode: {mode}
"""
    )

    prompt_template = ChatPromptTemplate.from_messages([system_prompt_text] + state["messages"])
    formatted_prompt = prompt_template.format_prompt()

    llm_gpt = _build_searcher_llm()

    # 缁戝畾鎵€鏈夊伐鍏凤紝璁?LLM 鑷繁鍒ゆ柇浣跨敤鍝釜
    model = llm_gpt.bind_tools(TOOLS)
    response = model.invoke(formatted_prompt)

    return {
        "messages": [response],
        "response_mode": mode,
        "need_citations": need_citations,
        "locale": locale,
    }


workflow = StateGraph(State)
workflow.add_node("agent", agent)
workflow.add_node("tools", ToolNode(tools=TOOLS))
workflow.add_edge(START, "agent")
workflow.add_conditional_edges("agent", tools_condition, {"tools": "tools", "__end__": END})
workflow.add_edge("tools", "agent")
memory = MemorySaver()
graph = workflow.compile(checkpointer=memory)


class NTL_Knowledge_Searcher_Input(BaseModel):
    query: str = Field(..., description="Your question plus brief context.")
    response_mode: str = Field(
        default="auto",
        description="One of: auto|workflow|theory|code|mixed",
    )
    locale: str = Field(default="en", description="Output language preference (e.g., en|zh).")
    need_citations: bool = Field(
        default=True,
        description="Whether to include simple store/doc citations.",
    )


def _safe_stream_writer():
    try:
        return get_stream_writer()
    except Exception:
        return lambda *_args, **_kwargs: None


def _emit_kb_progress(
    writer,
    *,
    run_id: str,
    phase: str,
    status: str,
    label: str,
    meta: Optional[dict] = None,
):
    payload = {
        "event_type": "kb_progress",
        "tool": "NTL_Knowledge_Base",
        "run_id": run_id,
        "phase": phase,
        "status": status,
        "label": label,
        "meta": meta or {},
    }
    try:
        writer(payload)
    except Exception:
        pass


def _NTL_Knowledge_Searcher(
    query: str,
    response_mode: str = "auto",
    locale: str = "en",
    need_citations: bool = True,
) -> str:
    mode = (response_mode or "auto").lower()
    unique_id = str(uuid.uuid4())[:8]
    run_id = f"kb_{unique_id}"
    writer = _safe_stream_writer()
    _emit_kb_progress(
        writer,
        run_id=run_id,
        phase="query_received",
        status="done",
        label="Query received",
    )
    _emit_kb_progress(
        writer,
        run_id=run_id,
        phase="knowledge_retrieval",
        status="running",
        label="Retrieving knowledge from KB stores",
    )

    try:
        events = graph.stream(
            input={
                "messages": [("user", query)],
                "response_mode": response_mode,
                "locale": locale,
                "need_citations": need_citations,
            },
            config={"configurable": {"thread_id": f"rag_{unique_id}"}, "recursion_limit": 25},
            stream_mode="values",
        )

        final_answer = ""
        intent_profile = {}
        retrieval_done = False
        for event in events:
            content = event["messages"][-1].content
            if not retrieval_done:
                retrieval_done = True
                _emit_kb_progress(
                    writer,
                    run_id=run_id,
                    phase="knowledge_retrieval",
                    status="done",
                    label="Knowledge retrieval completed",
                )
            if isinstance(content, str):
                final_answer = content
                parsed = _safe_json_loads(content)
                if isinstance(parsed, dict):
                    intent_profile = parsed.get("intent_analysis", {})
            else:
                final_answer = json.dumps(content, ensure_ascii=False)
                intent_profile = content.get("intent_analysis", {}) if isinstance(content, dict) else {}

        if not intent_profile:
            intent_profile = _fallback_intent_profile(query, mode)
        intent_profile = _augment_intent_with_task_level(intent_profile, query, mode)

        empty_store_payload = _extract_empty_store_status(final_answer)
        if empty_store_payload and empty_store_payload.get("store") == "Code_RAG":
            notice = (
                "code corpus unavailable: Code_RAG currently has no indexed documents. "
                "Rebuild command: conda run -n 地缘环境智能计算平台 python agents/NTL_Knowledge_Base_manager.py "
                "--profile code --code-guide-dir RAG/code_guide --tool-dir tools "
                "--persist-dir RAG/Code_RAG --collection-name Code_RAG --reset "
                "--report-path RAG/Code_RAG/rebuild_report.json"
            )
            _emit_kb_progress(
                writer,
                run_id=run_id,
                phase="workflow_assembly",
                status="done",
                label="Workflow assembly finished with empty code store notice",
            )
            _emit_kb_progress(
                writer,
                run_id=run_id,
                phase="structured_output",
                status="done",
                label="Structured output ready",
            )
            if mode == "code":
                return notice
            if mode in {"mixed", "workflow", "auto"}:
                return json.dumps(
                    _build_kb_response_contract(
                        {
                            "status": "code_corpus_unavailable",
                            "reason": notice,
                            "sources": [],
                        },
                        mode=mode,
                        intent_profile=intent_profile,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )

        if mode in {"workflow", "auto", "mixed"}:
            _emit_kb_progress(
                writer,
                run_id=run_id,
                phase="workflow_assembly",
                status="running",
                label="Normalizing and assembling workflow output",
            )
            normalized = _validate_and_normalize_workflow_output(
                final_answer,
                query,
                allow_trailing_text=mode == "mixed",
                force_json=mode in {"workflow", "auto"},
                intent_profile=intent_profile,
                response_mode=mode,
            )
            _emit_kb_progress(
                writer,
                run_id=run_id,
                phase="workflow_assembly",
                status="done",
                label="Workflow assembly completed",
            )
            _emit_kb_progress(
                writer,
                run_id=run_id,
                phase="structured_output",
                status="done",
                label="Structured output ready",
            )
            return normalized

        _emit_kb_progress(
            writer,
            run_id=run_id,
            phase="workflow_assembly",
            status="done",
            label="Workflow assembly skipped in current mode",
        )
        _emit_kb_progress(
            writer,
            run_id=run_id,
            phase="structured_output",
            status="done",
            label="Text output ready",
        )
        return final_answer
    except Exception as exc:
        _emit_kb_progress(
            writer,
            run_id=run_id,
            phase="structured_output",
            status="error",
            label="Knowledge base execution failed",
            meta={"error_summary": str(exc)[:280]},
        )
        raise


NTL_Knowledge_Base = StructuredTool.from_function(
    func=_NTL_Knowledge_Searcher,
    name="NTL_Knowledge_Base",
    description=(
        "Retrieve grounded knowledge for Nighttime Light (NTL) tasks from three internal stores:\n"
        "- Literature (theory, equations)\n"
        "- Solution (workflows, tools, datasets)\n"
        "- Code (concise Python/GEE snippets)\n\n"
        "Supports response modes: workflow | theory | code | mixed | auto."
    ),
    args_schema=NTL_Knowledge_Searcher_Input,
)

