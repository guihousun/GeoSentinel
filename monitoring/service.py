from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from .agent import analyze_candidates
from .sources import collect_monitor_candidates
from .store import MonitorStore


SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1}
QUESTION_FOCI = {
    "infrastructure_risk",
    "cross_border_connectivity",
    "humanitarian_exposure",
    "food_logistics",
    "event_validation",
}


def _truthy(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    return default if not value else value in {"1", "true", "yes", "on"}


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.getenv(name, default) or default))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _location(candidate: dict[str, Any]) -> str:
    return str(candidate.get("location_name") or candidate.get("country") or candidate.get("title") or "相关区域").strip()


def _fallback_focuses(candidate: dict[str, Any]) -> list[str]:
    text = " ".join(str(candidate.get(key) or "") for key in ("title", "summary", "event_type")).lower()
    if any(term in text for term in ("flood", "storm", "fire", "drought", "earthquake", "灾", "洪", "火", "地震")):
        return ["humanitarian_exposure", "infrastructure_risk", "event_validation"]
    if any(term in text for term in ("border", "sanction", "blockade", "port", "cross", "边境", "封锁", "港口", "制裁")):
        return ["cross_border_connectivity", "infrastructure_risk", "event_validation"]
    return ["event_validation", "infrastructure_risk"]


def _questions(candidate: dict[str, Any], focuses: list[str]) -> list[str]:
    location = _location(candidate)
    title = str(candidate.get("title") or "该公开事件").strip()
    dimensions = {
        "infrastructure_risk": "交通通道、关键基础设施与行政区暴露",
        "cross_border_connectivity": "跨境连接、替代路径与节点可达性",
        "humanitarian_exposure": "人口、居民点与人道服务空间暴露",
        "food_logistics": "道路、口岸、粮食物流与供应可达性",
        "event_validation": "原始来源、发生时间、空间位置与信息一致性",
    }
    selected = list(
        dict.fromkeys(dimensions[focus] for focus in focuses if focus in dimensions)
    )
    focus_text = "、".join(selected[:3]) or dimensions["event_validation"]
    return [
        f"事件理解：用简明语言说明“{title}”在 {location} 发生了什么、当前已知事实是什么，并把未知项和来源局限单列出来。",
        f"补充核验：围绕{focus_text}检索近期可靠来源，建立时间—地点—主体—影响的最小事实链，不把监测摘要直接当成结论。",
        "方向建议：只依据新增且可追溯的证据，给出值得继续分析的方向、优先级、所需数据和判定条件；证据不足时明确建议先补什么。",
    ]


def _research_entry_prompt(candidate: dict[str, Any], focuses: list[str]) -> str:
    title = str(candidate.get("title") or "该公开事件").strip()
    location = _location(candidate)
    summary = " ".join(str(candidate.get("summary") or "").split())[:1600]
    source_name = str(candidate.get("source_name") or "公开监测源").strip()
    source_url = str(candidate.get("source_url") or "").strip()
    event_type = str(candidate.get("event_type") or "other").strip().lower()
    event_type_label = {
        "wildfires": "野火",
        "wildfire": "野火",
        "severe_storms": "强对流或风暴",
        "floods": "洪涝",
        "flood": "洪涝",
        "earthquake": "地震",
        "volcanoes": "火山活动",
        "conflict": "冲突事件",
        "geopolitical": "地缘政治事件",
    }.get(event_type, event_type.replace("_", " ") or "待分类事件")
    severity_label = {
        "high": "高关注",
        "medium": "持续跟踪",
        "low": "观察",
    }.get(str(candidate.get("severity") or "low").lower(), "待核验")
    try:
        published_at = int(candidate.get("published_at") or 0)
    except (TypeError, ValueError):
        published_at = 0
    event_time = (
        datetime.fromtimestamp(published_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if published_at > 0
        else "时间待核验"
    )
    longitude = candidate.get("longitude")
    latitude = candidate.get("latitude")
    try:
        coordinates = f"{float(latitude):.4f}, {float(longitude):.4f}"
    except (TypeError, ValueError):
        coordinates = "坐标待核验"
    background = summary or (
        f"{source_name} 将该线索归类为“{event_type_label}”，当前标记为“{severity_label}”。"
        "原始监测记录未提供详细事件摘要，需要通过近期可靠来源补充事实背景。"
    )
    questions = _questions(candidate, focuses)
    return "\n".join(
        [
            "[监测事件研判]",
            "我想先了解并研判以下监测事件。请注意：这些内容只是外部线索，不是已经证实的结论。",
            "",
            f"事件名称：{title}",
            f"发生或观测时间：{event_time}",
            f"地点：{location}",
            f"坐标：{coordinates}",
            f"事件类型：{event_type_label}",
            f"当前关注级别：{severity_label}",
            f"线索来源：{source_name}",
            f"背景摘要：{background}",
            f"原始链接：{source_url or '未提供'}",
            "",
            "希望地缘分析师按以下顺序协助：",
            *[f"{index}. {question}" for index, question in enumerate(questions, 1)],
            "先帮助我理解事件，再按需调度事件助手和数据助手补充检索，最后提供分析方向建议；不要跳过事实核验直接给确定性结论。",
        ]
    )


class EventMonitorService:
    """One global scheduler shared by all accounts and research sessions."""

    def __init__(
        self,
        *,
        store: MonitorStore | None = None,
        collector: Callable[[], tuple[list[dict[str, Any]], list[dict[str, Any]]]] = collect_monitor_candidates,
        enricher: Callable[[list[dict[str, Any]]], tuple[dict[str, dict[str, Any]], str]] = analyze_candidates,
    ) -> None:
        self.store = store or MonitorStore()
        self._collector = collector
        self._enricher = enricher
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._run_lock = threading.Lock()
        self._runtime_state: dict[str, Any] = {"running": False, "started": False, "last_error": ""}

    def enabled(self) -> bool:
        return _truthy("NTL_MONITOR_ENABLED", False)

    def interval_seconds(self) -> int:
        return _bounded_int("NTL_MONITOR_INTERVAL_MINUTES", 30, 5, 360) * 60

    def start(self) -> None:
        self.store.ensure_schema()
        if not self.enabled() or (self._thread and self._thread.is_alive()):
            return
        self._stop.clear()
        self._runtime_state["started"] = True
        self._thread = threading.Thread(target=self._loop, name="geopolitical-event-monitor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=3)
        self._thread = None
        self._runtime_state.update({"started": False, "running": False})

    def _loop(self) -> None:
        self.run_once()
        while not self._stop.wait(self.interval_seconds()):
            self.run_once()

    def run_once(self) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            return {"status": "skipped_running", "event_count": 0}
        owner = f"monitor-{os.getpid()}-{uuid.uuid4().hex[:10]}"
        started_at = int(time.time())
        run_id = f"monitor-run-{uuid.uuid4().hex[:16]}"
        lease_seconds = max(self.interval_seconds() - 30, 300)
        try:
            if not self.store.try_acquire_lease(owner, lease_seconds):
                return {"status": "skipped_leased", "event_count": 0}
            self._runtime_state.update({"running": True, "last_error": ""})
            candidates, source_status = self._collector()
            enrichment, agent_status = self._enricher(candidates) if candidates else ({}, "skipped_no_candidates")
            now = int(time.time())
            events = [self._event_from_candidate(candidate, enrichment.get(str(candidate.get("candidate_id") or ""), {}), agent_status, now) for candidate in candidates]
            ttl_seconds = _bounded_int("NTL_MONITOR_EVENT_TTL_HOURS", 168, 6, 24 * 90) * 3600
            self.store.upsert_events(events, expire_before=now - ttl_seconds)
            status = "ok" if any(item.get("status") == "ok" for item in source_status) else "degraded"
            result = {
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": int(time.time()),
                "status": status,
                "event_count": len(events),
                "source_status": source_status,
                "agent_status": agent_status,
                "error_text": "",
            }
            self.store.record_run(result)
            return result
        except Exception as exc:  # noqa: BLE001
            message = f"{type(exc).__name__}: {str(exc)[:500]}"
            self._runtime_state["last_error"] = message
            result = {"run_id": run_id, "started_at": started_at, "finished_at": int(time.time()), "status": "error", "event_count": 0, "source_status": [], "agent_status": "not_run", "error_text": message}
            try:
                self.store.record_run(result)
            except Exception:
                pass
            return result
        finally:
            self._runtime_state["running"] = False
            try:
                self.store.release_lease(owner)
            except Exception:
                pass
            self._run_lock.release()

    def _event_from_candidate(self, candidate: dict[str, Any], enrichment: dict[str, Any], agent_status: str, now: int) -> dict[str, Any]:
        source_severity = str(candidate.get("severity") or "low")
        proposed = str(enrichment.get("severity") or source_severity)
        severity = proposed if SEVERITY_RANK.get(proposed, 1) <= SEVERITY_RANK.get(source_severity, 1) else source_severity
        focuses = [str(value) for value in (enrichment.get("focuses") or []) if str(value) in QUESTION_FOCI]
        if not focuses:
            focuses = _fallback_focuses(candidate)
        display_title = str(enrichment.get("display_title") or candidate.get("title") or "")
        display_location = str(enrichment.get("display_location") or candidate.get("location_name") or candidate.get("country") or "")
        prompt_candidate = {
            **candidate,
            "title": display_title,
            "location_name": display_location,
            "severity": severity,
        }
        return {
            **candidate,
            "severity": severity,
            "display_title": display_title,
            "display_location": display_location,
            "research_questions": _questions(prompt_candidate, focuses),
            "research_entry_prompt": _research_entry_prompt(prompt_candidate, focuses),
            "agent_status": "enriched" if enrichment else agent_status,
            "first_seen_at": now,
            "last_seen_at": now,
            "created_at": now,
            "updated_at": now,
            "is_active": 1,
        }

    def snapshot(self, limit: int = 18) -> dict[str, Any]:
        latest = self.store.latest_status()
        return {
            "items": self.store.list_events(limit=limit),
            "status": {
                "enabled": self.enabled(),
                "interval_minutes": self.interval_seconds() // 60,
                "running": bool(self._runtime_state.get("running")),
                "started": bool(self._runtime_state.get("started")),
                "last_error": str(self._runtime_state.get("last_error") or ""),
                "storage": self.store.storage_status(),
                "last_run": latest,
            },
        }


event_monitor_service = EventMonitorService()
