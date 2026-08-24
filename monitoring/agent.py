from __future__ import annotations

import json
import os
import re
from typing import Any

from deepagents import create_deep_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from model_config import get_api_model_name, get_base_url, get_env_api_key, get_model_config


ALLOWED_FOCI = {
    "infrastructure_risk",
    "cross_border_connectivity",
    "humanitarian_exposure",
    "food_logistics",
    "event_validation",
}
SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3}


def _enabled() -> bool:
    value = str(os.getenv("NTL_MONITOR_DEEPAGENT_ENABLED", "1") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _model_name() -> str:
    return str(os.getenv("NTL_MONITOR_MODEL", "deepseek-v4-flash") or "deepseek-v4-flash").strip()


def _extract_text(result: Any) -> str:
    messages = result.get("messages", []) if isinstance(result, dict) else []
    for message in reversed(messages):
        content = getattr(message, "content", "")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts = []
            for part in content:
                parts.append(str(part.get("text") or "") if isinstance(part, dict) else str(part))
            text = "\n".join(parts).strip()
            if text:
                return text
    return ""


def _parse_json(text: str) -> dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    candidates = [fenced.group(1)] if fenced else []
    candidates.append(text.strip())
    brace = re.search(r"(\{.*\})", text, flags=re.DOTALL)
    if brace:
        candidates.append(brace.group(1))
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            return value
    return {}


def analyze_candidates(candidates: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], str]:
    """Use a dedicated, tool-limited DeepAgent to select bounded research templates."""
    if not candidates:
        return {}, "skipped_no_candidates"
    if not _enabled():
        return {}, "skipped_disabled"

    # Monitoring is a production background job. Do not export public-source prompts or
    # event metadata to optional LangSmith tracing endpoints from this web process.
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"

    model_name = _model_name()
    api_key = get_env_api_key(model_name)
    base_url = get_base_url(model_name)
    if not api_key or not base_url:
        return {}, "skipped_model_not_configured"
    if get_model_config(model_name).provider != "deepseek":
        return {}, "skipped_unsupported_model"

    compact = [
        {
            "candidate_id": item.get("candidate_id"),
            "source": item.get("source_name"),
            "title": item.get("title"),
            "summary": item.get("summary"),
            "event_type": item.get("event_type"),
            "severity": item.get("severity"),
            "published_at": item.get("published_at"),
            "location_name": item.get("location_name"),
            "country": item.get("country"),
            "source_url": item.get("source_url"),
        }
        for item in candidates[:24]
    ]

    @tool("read_monitor_candidates")
    def read_monitor_candidates() -> str:
        """Read the source-grounded candidate events gathered in this global monitoring batch."""
        return json.dumps(compact, ensure_ascii=False)

    llm = ChatOpenAI(
        api_key=SecretStr(api_key),
        base_url=base_url,
        model=get_api_model_name(model_name),
        temperature=0,
        timeout=max(20, int(os.getenv("NTL_MONITOR_AGENT_TIMEOUT_S", "80") or 80)),
        max_retries=1,
    )
    prompt = """你是“地缘事件监测智能体”。你独立于用户会话和主智能体，只能处理当前批次已经有来源的候选事件。

必须先调用 read_monitor_candidates。对于每个保留的 candidate_id：
- 严重性只能维持或降低来源给出的等级，不能上调；
- 只能从 infrastructure_risk、cross_border_connectivity、humanitarian_exposure、food_logistics、event_validation 选择 1-3 个研究焦点；
- 证据不足时选择 event_validation；
- 生成 display_title：将事件类型、预警等级等通用词准确翻成中文；专有名词、机构名、地名保留来源原文，不得自行音译或补充事实；
- 生成 display_location：仅在明确、无歧义时写“中文（来源英文）”；否则直接保留来源原文；
- 不得编造事件、来源、地名、坐标、自由摘要或研究结论。

最终回答必须且只能是 JSON：
{"events":[{"candidate_id":"...","severity":"low|medium|high","focuses":["..."],"display_title":"...","display_location":"..."}]}
"""
    agent = create_deep_agent(
        model=llm,
        tools=[read_monitor_candidates],
        system_prompt=prompt,
        name="Geopolitical_Event_Monitor",
        checkpointer=False,
    )
    try:
        result = agent.invoke({"messages": [HumanMessage(content="完成当前批次的受限监测研判。")]})
    except Exception as exc:  # noqa: BLE001
        return {}, f"fallback_agent_error:{type(exc).__name__}"

    payload = _parse_json(_extract_text(result))
    known = {str(item.get("candidate_id") or ""): item for item in compact}
    updates: dict[str, dict[str, Any]] = {}
    for item in payload.get("events", []) if isinstance(payload.get("events"), list) else []:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id") or "")
        source = known.get(candidate_id)
        if not source:
            continue
        source_severity = str(source.get("severity") or "low")
        proposed = str(item.get("severity") or source_severity)
        severity = proposed if SEVERITY_RANK.get(proposed, 1) <= SEVERITY_RANK.get(source_severity, 1) else source_severity
        focuses = [str(value) for value in (item.get("focuses") or []) if str(value) in ALLOWED_FOCI]
        display_title = " ".join(str(item.get("display_title") or "").split())[:300]
        display_location = " ".join(str(item.get("display_location") or "").split())[:180]
        updates[candidate_id] = {
            "severity": severity,
            "focuses": focuses[:3] or ["event_validation"],
            "display_title": display_title,
            "display_location": display_location,
        }
    return updates, "enriched" if updates else "fallback_invalid_agent_output"
