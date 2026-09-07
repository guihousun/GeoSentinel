"""Framework-independent public-web runtime for the geopolitical frontend.

This module deliberately owns no Streamlit state.  It reuses the existing
LangGraph graph, PostgreSQL history store, and isolated thread workspaces so
the public web UI and the internal Streamlit UI operate on the same research
records without sharing a UI lifecycle.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage

import app_state
import history_store
from agents.role_specs import ROLE_SPECS, get_role_spec
from model_config import MODEL_OPTIONS, get_env_api_key, missing_env_for_model
from runtime_limits import (
    ModelCallBudgetExceeded,
    begin_model_call_budget,
    end_model_call_budget,
)
from runtime_governance import build_run_limit_snapshot
from storage_manager import current_thread_id, storage_manager


load_dotenv(override=True)

logger = logging.getLogger(__name__)

TERMINAL_STATES = frozenset({"success", "error", "interrupted", "partial"})
MAX_RETAINED_EVENTS = 800
RUN_WORKER_START_GRACE_SECONDS = 5.0
RECENT_REQUEST_TTL_SECONDS = 10.0
# Runs that exceed the wall-clock ceiling are closed with a stable error so the
# UI never appears stuck in "running" while a worker keeps retrying a slow model
# call (e.g. a long subagent turn with a 180s x 3 retry policy).
RUN_WALL_CLOCK_TIMEOUT_S = float(os.getenv("NTL_RUN_WALL_CLOCK_TIMEOUT_S", "1500") or 1500)
IMAGE_PREVIEW_EXTENSIONS = frozenset({".gif", ".jpeg", ".jpg", ".png", ".webp"})
GEO_PREVIEW_EXTENSIONS = frozenset({
    ".tif", ".tiff", ".shp", ".geojson", ".json", ".kml", ".kmz",
})
# Human-facing labels for the workspace file list: process artifacts (scripts,
# manifests, run logs, audit JSON) are secondary outputs and are grouped
# separately from deliverable research products.
PROCESS_ARTIFACT_MARKERS = (
    "manifest", "_manifest", ".jsonl", "execution_history", "evidence_report",
    "route_state", "contract", "package", "observation", "task_plan",
)
TABLE_PREVIEW_EXTENSIONS = frozenset({".csv", ".json", ".tsv", ".xlsx"})
HTML_PREVIEW_EXTENSIONS = frozenset({".htm", ".html"})
MARKDOWN_PREVIEW_EXTENSIONS = frozenset({".markdown", ".md"})
PDF_PREVIEW_EXTENSIONS = frozenset({".pdf"})
TEXT_PREVIEW_EXTENSIONS = frozenset({".css", ".js", ".log", ".py", ".txt"})
ARTIFACT_PREVIEW_MAX_ROWS = 24
ARTIFACT_PREVIEW_MAX_COLUMNS = 20
ARTIFACT_PREVIEW_MAX_BYTES = 8 * 1024 * 1024


class WebRuntimeError(RuntimeError):
    """A user-facing execution rejection with a stable machine-readable code."""

    def __init__(self, code: str, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _format_bytes(num_bytes: int) -> str:
    size = max(0, int(num_bytes or 0))
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    return f"{int(value)}{unit}" if unit == "B" else f"{value:.1f}{unit}"


def _artifact_preview_kind(path: Path) -> str:
    extension = path.suffix.lower()
    if extension in IMAGE_PREVIEW_EXTENSIONS:
        return "image"
    if extension in GEO_PREVIEW_EXTENSIONS:
        return "geo"
    if extension in TABLE_PREVIEW_EXTENSIONS:
        return "table"
    if extension in HTML_PREVIEW_EXTENSIONS:
        return "html"
    if extension in MARKDOWN_PREVIEW_EXTENSIONS:
        return "markdown"
    if extension in PDF_PREVIEW_EXTENSIONS:
        return "pdf"
    if extension in TEXT_PREVIEW_EXTENSIONS:
        return "text"
    return ""


def _is_process_artifact(path: Path, name: str = "") -> bool:
    """Process artifacts are scripts, manifests, run logs and audit records.

    They are legitimate workspace files but are not client deliverables; the
    file list groups them below the research products so the first screen of a
    task shows the actual outputs (rasters, tables, images).
    """
    raw_name = str(name or path.name or "").lower()
    marker = any(marker in raw_name for marker in PROCESS_ARTIFACT_MARKERS)
    suffix = path.suffix.lower()
    return marker or suffix in {".py", ".log", ".pyc"}


def _preview_cell(value: Any, limit: int = 240) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return ""
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, default=str)
    else:
        text = str(value)
    text = text.replace("\x00", "").strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def _preview_headers(values: Iterable[Any], max_columns: int) -> list[str]:
    headers: list[str] = []
    used: dict[str, int] = {}
    for index, value in enumerate(list(values)[:max_columns]):
        base = _preview_cell(value) or f"列 {index + 1}"
        used[base] = used.get(base, 0) + 1
        headers.append(base if used[base] == 1 else f"{base} ({used[base]})")
    return headers


def _preview_rows(values: Iterable[Iterable[Any]], column_count: int, max_rows: int) -> tuple[list[list[str]], bool]:
    rows: list[list[str]] = []
    truncated = False
    for value in values:
        if len(rows) >= max_rows:
            truncated = True
            break
        cells = list(value)[:column_count]
        cells.extend([""] * (column_count - len(cells)))
        rows.append([_preview_cell(cell) for cell in cells])
    return rows, truncated


def _read_delimited_preview(path: Path, max_rows: int, max_columns: int) -> dict[str, Any]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("gb18030", errors="replace")
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        first_row = next(reader)
    except StopIteration:
        return {"columns": [], "rows": [], "truncated": False}
    columns = _preview_headers(first_row, max_columns)
    rows, truncated = _preview_rows(reader, len(columns), max_rows)
    return {"columns": columns, "rows": rows, "truncated": truncated}


def _read_xlsx_preview(path: Path, max_rows: int, max_columns: int) -> dict[str, Any]:
    try:
        from openpyxl import load_workbook
    except ImportError as error:
        raise WebRuntimeError("artifact_preview_unavailable", "当前环境未安装表格预览组件，请下载文件查看。") from error
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        iterator = sheet.iter_rows(values_only=True)
        try:
            first_row = next(iterator)
        except StopIteration:
            return {"columns": [], "rows": [], "truncated": False, "sheet_name": sheet.title}
        columns = _preview_headers(first_row, max_columns)
        rows, truncated = _preview_rows(iterator, len(columns), max_rows)
        return {"columns": columns, "rows": rows, "truncated": truncated, "sheet_name": sheet.title}
    finally:
        workbook.close()


def _read_json_preview(path: Path, max_rows: int, max_columns: int) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        records = payload["rows"]
        declared_columns = payload.get("columns")
    elif isinstance(payload, list):
        records = payload
        declared_columns = None
    elif isinstance(payload, dict):
        records = [{"字段": key, "值": value} for key, value in payload.items()]
        declared_columns = ["字段", "值"]
    else:
        records = [{"值": payload}]
        declared_columns = ["值"]

    if records and all(isinstance(record, dict) for record in records):
        column_keys = list(declared_columns or [])
        if not column_keys:
            for record in records:
                for key in record:
                    if key not in column_keys:
                        column_keys.append(key)
                    if len(column_keys) >= max_columns:
                        break
                if len(column_keys) >= max_columns:
                    break
        column_keys = column_keys[:max_columns]
        columns = _preview_headers(column_keys, max_columns)
        rows, truncated = _preview_rows(
            ([record.get(column, "") for column in column_keys] for record in records),
            len(columns),
            max_rows,
        )
    else:
        sequence_rows = [record if isinstance(record, (list, tuple)) else [record] for record in records]
        columns = _preview_headers(declared_columns, max_columns) if declared_columns else []
        width = len(columns) or min(max((len(row) for row in sequence_rows), default=0), max_columns)
        if not columns:
            columns = [f"列 {index + 1}" for index in range(width)]
        rows, truncated = _preview_rows(sequence_rows, width, max_rows)
    return {"columns": columns, "rows": rows, "truncated": truncated}


def _table_preview_from_path(path: Path, max_rows: int, max_columns: int) -> dict[str, Any]:
    if path.stat().st_size > ARTIFACT_PREVIEW_MAX_BYTES:
        raise WebRuntimeError("artifact_preview_too_large", "文件较大，请下载后查看完整内容。")
    extension = path.suffix.lower()
    if extension in {".csv", ".tsv"}:
        preview = _read_delimited_preview(path, max_rows, max_columns)
    elif extension == ".xlsx":
        preview = _read_xlsx_preview(path, max_rows, max_columns)
    elif extension == ".json":
        preview = _read_json_preview(path, max_rows, max_columns)
    else:
        raise WebRuntimeError("artifact_preview_unsupported", "该文件类型暂不支持在线表格预览。")
    return {
        "kind": "table",
        "name": path.name,
        "columns": preview.get("columns", []),
        "rows": preview.get("rows", []),
        "preview_row_count": len(preview.get("rows", [])),
        "truncated": bool(preview.get("truncated")),
        **({"sheet_name": preview["sheet_name"]} if preview.get("sheet_name") else {}),
    }


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if isinstance(text, str):
                    parts.append(text)
                else:
                    parts.append(json.dumps(item, ensure_ascii=False, default=str))
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part)
    if content is None:
        return ""
    return str(content)


def _canonical_stream_agent(*values: Any, default: str = "") -> str:
    """Resolve stream metadata to one of GeoSentinel's four public roles."""

    for value in values:
        if value in (None, "", (), [], {}):
            continue
        if isinstance(value, (dict, list, tuple)):
            raw = json.dumps(value, ensure_ascii=False, default=str)
        else:
            raw = str(value)
        cleaned = re.sub(r"\s*\(streaming\)\s*", "", raw, flags=re.IGNORECASE).strip()
        try:
            return get_role_spec(cleaned).name
        except KeyError:
            normalized = cleaned.lower().replace("-", "_").replace(" ", "_")
            for canonical in ROLE_SPECS:
                aliases = {canonical.lower(), canonical.removeprefix("NTL_").lower()}
                if any(alias in normalized for alias in aliases):
                    return canonical
    return default


def _stream_message_parts(payload: Any) -> tuple[Optional[BaseMessage], dict[str, Any]]:
    """Return the message chunk and metadata from LangGraph messages mode."""

    if isinstance(payload, BaseMessage):
        return payload, {}
    if isinstance(payload, (list, tuple)) and len(payload) == 2:
        message, metadata = payload
        if isinstance(message, BaseMessage) and isinstance(metadata, dict):
            return message, metadata
    return None, {}


def _message_fingerprint(message: BaseMessage) -> str:
    return json.dumps(
        {
            "type": message.__class__.__name__,
            "id": str(getattr(message, "id", "") or ""),
            "name": str(getattr(message, "name", "") or ""),
            "tool_call_id": str(getattr(message, "tool_call_id", "") or ""),
            "content": _content_to_text(getattr(message, "content", "")),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _iter_message_lists(payload: Any, seen: Optional[set[int]] = None) -> Iterable[list[Any]]:
    seen = seen if seen is not None else set()
    if isinstance(payload, (dict, list, tuple)):
        marker = id(payload)
        if marker in seen:
            return
        seen.add(marker)
    if isinstance(payload, dict):
        messages = payload.get("messages")
        if isinstance(messages, list):
            yield messages
        for value in payload.values():
            yield from _iter_message_lists(value, seen)
    elif isinstance(payload, (list, tuple)):
        for item in payload:
            yield from _iter_message_lists(item, seen)


def _collect_new_messages(payload: Any, seen_fingerprints: set[str]) -> list[BaseMessage]:
    delta: list[BaseMessage] = []
    for message_list in _iter_message_lists(payload):
        for message in message_list:
            if not isinstance(message, BaseMessage):
                continue
            fingerprint = _message_fingerprint(message)
            if fingerprint in seen_fingerprints:
                continue
            seen_fingerprints.add(fingerprint)
            delta.append(message)
    return delta


def _is_transfer_message(text: str) -> bool:
    text = str(text or "").strip().lower()
    return bool(
        text
        and any(
            re.match(pattern, text)
            for pattern in (
                r"^successfully transferred to\b",
                r"^transferred to\b",
                r"^successfully handed off to\b",
                r"^handed off to\b",
                r"^routing to\b",
            )
        )
    )


def _extract_meaningful_ai_text(messages: list[Any], preferred_agents: Optional[list[str]] = None) -> Optional[str]:
    preferred = {str(item).strip().lower() for item in preferred_agents or [] if str(item).strip()}
    fallback: Optional[str] = None
    for message in reversed(messages or []):
        if not isinstance(message, AIMessage):
            continue
        name = str(getattr(message, "name", "") or "").lower()
        if "(streaming)" in name:
            continue
        text = _content_to_text(getattr(message, "content", "")).strip()
        text = re.sub(
            r"^\s*\*{0,2}\s*(Data_Searcher|Code_Assistant)\s*\(streaming\)\s*\*{0,2}\s*:?\s*\n?",
            "",
            text,
            flags=re.IGNORECASE | re.MULTILINE,
        ).strip()
        if not text or _is_transfer_message(text):
            continue
        if preferred and name in preferred:
            return text
        if fallback is None:
            fallback = text
    return fallback


def _get_state_messages(conversation: Any, config: dict[str, Any]) -> list[Any]:
    try:
        snapshot = conversation.get_state(config=config)
        values = getattr(snapshot, "values", None)
        messages = values.get("messages", []) if isinstance(values, dict) else []
        return messages if isinstance(messages, list) else []
    except Exception:  # The graph may not have a persisted state on its first turn.
        return []


def _start_stream(conversation: Any, state: dict[str, Any], config: dict[str, Any], stream_mode: Any):
    try:
        return conversation.stream(state, config=config, stream_mode=stream_mode, subgraphs=True)
    except TypeError:
        return conversation.stream(state, config=config, stream_mode=stream_mode)


def _iter_events(conversation: Any, state: dict[str, Any], config: dict[str, Any]):
    """Normalize Deep Agents / LangGraph stream shape across supported versions."""
    yielded = False
    try:
        events = _start_stream(conversation, state, config, ["messages", "values", "updates", "custom"])
        for item in events:
            yielded = True
            if isinstance(item, tuple):
                if len(item) == 3:
                    namespace, mode, payload = item
                    if isinstance(mode, str) and mode in {"messages", "values", "updates", "custom"}:
                        yield mode, payload, namespace
                        continue
                if len(item) == 2:
                    left, right = item
                    if isinstance(left, str) and left in {"messages", "values", "updates", "custom"}:
                        yield left, right, ()
                        continue
                    if isinstance(right, str) and right in {"messages", "values", "updates", "custom"}:
                        yield right, left, ()
                        continue
                    if hasattr(left, "content") and isinstance(right, dict):
                        yield "messages", (left, right), ()
                        continue
                    if isinstance(left, tuple) and isinstance(right, tuple) and len(right) == 2 and hasattr(right[0], "content"):
                        yield "messages", right, left
                        continue
                    if isinstance(left, tuple) and isinstance(right, dict):
                        yield "values", right, left
                        continue
            if isinstance(item, dict) and "messages" in item:
                yield "values", item, ()
            else:
                yield "updates", item, ()
    except Exception:
        if yielded:
            raise
        for item in _start_stream(conversation, state, config, "values"):
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], tuple):
                yield "values", item[1], item[0]
            else:
                yield "values", item, ()


def _serialize_message(message: BaseMessage, agent_name: str = "") -> dict[str, Any]:
    if isinstance(message, ToolMessage):
        role = "tool"
    elif isinstance(message, AIMessage):
        role = "assistant"
    else:
        role = "message"
    text = _content_to_text(getattr(message, "content", "")).strip()
    canonical_agent = _canonical_stream_agent(agent_name, getattr(message, "name", ""))
    return {
        "role": role,
        "agent": canonical_agent,
        "tool_name": str(getattr(message, "name", "") or "") if isinstance(message, ToolMessage) else "",
        "message_id": str(getattr(message, "id", "") or ""),
        "text": text[:6000],
    }


def _tool_usage(logs: list[list[BaseMessage]]) -> tuple[list[str], dict[str, int]]:
    sequence: list[str] = []
    counts: dict[str, int] = {}
    for messages in logs:
        for message in messages:
            if not isinstance(message, ToolMessage):
                continue
            name = str(getattr(message, "name", "") or "tool").strip() or "tool"
            sequence.append(name)
            counts[name] = counts.get(name, 0) + 1
    return sequence, counts


def _time_footer(text: str, elapsed_s: float) -> str:
    return f"{str(text or '').strip()}\n\n耗时：{elapsed_s:.1f} 秒"


def _classify_runtime_error(error: Exception) -> dict[str, Any]:
    detail = f"{type(error).__name__}: {error}".lower()
    rules = (
        (
            "model_call_limit",
            ("modelcallbudgetexceeded", "model call limit reached"),
            "本次任务已达到 50 次模型调用安全上限，系统已停止继续调度。已完成的检索和文件仍保留；建议缩小范围或拆分为连续任务。",
            False,
        ),
        (
            "upstream_rate_limited",
            ("429", "rate limit", "rate_limit", "resource_exhausted", "too many requests"),
            "上游模型或数据服务当前限流。本次任务未自动重放，以避免重复调用工具；请稍后重试。",
            True,
        ),
        (
            "upstream_timeout",
            ("timeout", "timed out", "deadline exceeded"),
            "上游模型或数据服务响应超时。本次任务未自动重放，以避免重复调用工具；请稍后重试。",
            True,
        ),
        (
            "upstream_transport",
            ("connection reset", "connection refused", "connection aborted", "unexpected_eof", "ssl", "tls"),
            "上游服务连接中断。已保留当前会话和已生成文件，可稍后重试。",
            True,
        ),
        (
            "service_authentication",
            ("401", "403", "unauthorized", "forbidden", "invalid api key", "authentication"),
            "外部服务认证或访问权限不可用，请检查相应服务配置。",
            False,
        ),
        (
            "recursion_limit",
            ("recursion limit", "graphrecursionerror", "maximum recursion"),
            "任务步骤超过安全上限，已停止运行。请缩小问题范围或拆分任务。",
            False,
        ),
        (
            "workspace_storage",
            ("no space left", "disk full", "quota", "workspace quota"),
            "工作区存储空间不足，已停止写入。请清理或扩展配额后重试。",
            False,
        ),
    )
    for code, markers, message, retryable in rules:
        if any(marker in detail for marker in markers):
            return {"code": code, "message": message, "retryable": retryable}
    return {
        "code": "runtime_failure",
        "message": "任务未能完成。已保留当前会话和已生成文件，请查看调度记录后重试。",
        "retryable": False,
    }


def _runtime_error_payload(error: Exception, elapsed_s: float) -> dict[str, Any]:
    logger.exception("Public web run failed", exc_info=error)
    profile = _classify_runtime_error(error)
    return {
        "text": _time_footer(str(profile["message"]), elapsed_s),
        "code": str(profile["code"]),
        "retryable": bool(profile["retryable"]),
        "elapsed_s": elapsed_s,
    }


class WebRunManager:
    """Thread-safe run registry shared by the ASGI API and SSE consumers."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._thread_active_run: dict[str, str] = {}
        self._graphs: dict[tuple[str, str], Any] = {}
        self._recent_request_keys: dict[tuple[str, str, str], tuple[float, str]] = {}

    def _recent_duplicate_locked(
        self, user_id: str, thread_id: str, request_key: str
    ) -> Optional[dict[str, Any]]:
        if not request_key:
            return None
        now = time.time()
        expired = [
            key
            for key, (created_at, _run_id) in self._recent_request_keys.items()
            if now - created_at > RECENT_REQUEST_TTL_SECONDS
        ]
        for key in expired:
            self._recent_request_keys.pop(key, None)
        recent = self._recent_request_keys.get((user_id, thread_id, request_key))
        if not recent:
            return None
        control = self._runs.get(recent[1])
        if not control:
            return None
        return {
            "run_id": str(control["run_id"]),
            "thread_id": str(control["thread_id"]),
            "state": str(control.get("state") or "running"),
            "deduplicated": True,
        }

    def _running_controls_locked(self, user_id: str = "") -> list[dict[str, Any]]:
        self._reconcile_orphaned_runs_locked()
        wanted = str(user_id or "").strip()
        return [
            control
            for control in self._runs.values()
            if str(control.get("state")) == "running" and (not wanted or str(control.get("user_id")) == wanted)
        ]

    def _reconcile_orphaned_runs_locked(self) -> None:
        """Close runs whose worker exited before publishing a terminal state."""

        now = time.time()
        for run_id, control in list(self._runs.items()):
            if str(control.get("state")) != "running":
                continue
            worker = control.get("worker_thread")
            start_ts = float(control.get("start_ts") or now)
            if worker is None or now - start_ts < RUN_WORKER_START_GRACE_SECONDS:
                continue
            try:
                alive = bool(worker.is_alive())
            except Exception:
                alive = False
            if alive:
                if now - start_ts > RUN_WALL_CLOCK_TIMEOUT_S:
                    elapsed_s = max(0.0, now - start_ts)
                    control["state"] = "error"
                    control["end_ts"] = now
                    control["heartbeat_ts"] = now
                    thread_id = str(control.get("thread_id") or "")
                    if self._thread_active_run.get(thread_id) == run_id:
                        self._thread_active_run.pop(thread_id, None)
                    text = _time_footer(
                        "任务超过平台运行时限，系统已停止本次运行以释放资源。已生成文件与记录仍保留，可重新提交任务。",
                        elapsed_s,
                    )
                    self._emit(
                        run_id,
                        "error",
                        {
                            "text": text,
                            "code": "run_timeout",
                            "retryable": True,
                            "elapsed_s": elapsed_s,
                        },
                    )
                    self._emit(
                        run_id,
                        "done",
                        {"status": "error", "thread_id": thread_id, "elapsed_s": elapsed_s},
                    )
                continue

            elapsed_s = max(0.0, now - start_ts)
            control["state"] = "error"
            control["end_ts"] = now
            control["heartbeat_ts"] = now
            thread_id = str(control.get("thread_id") or "")
            if self._thread_active_run.get(thread_id) == run_id:
                self._thread_active_run.pop(thread_id, None)
            text = _time_footer(
                "任务执行线程意外结束，系统已自动释放运行锁。已生成文件仍保留，可重新提交任务。",
                elapsed_s,
            )
            self._emit(
                run_id,
                "error",
                {
                    "text": text,
                    "code": "worker_terminated",
                    "retryable": True,
                    "elapsed_s": elapsed_s,
                },
            )
            self._emit(
                run_id,
                "done",
                {"status": "error", "thread_id": thread_id, "elapsed_s": elapsed_s},
            )

    def _user_thread_ids(self, user_id: str, thread_id: str) -> list[str]:
        thread_ids = [str(row.get("thread_id") or "").strip() for row in history_store.list_user_threads(user_id, limit=0)]
        if thread_id and thread_id not in thread_ids:
            thread_ids.append(thread_id)
        return [item for item in thread_ids if item]

    def workspace_quota_rejection(self, thread_id: str, user_id: str, *, additional_bytes: int = 0) -> Optional[dict[str, Any]]:
        thread_snapshot = storage_manager.thread_quota_snapshot(thread_id, additional_bytes=additional_bytes)
        if not bool(thread_snapshot.get("allowed", True)):
            return {
                "code": "thread_workspace_quota_reached",
                "usage_bytes": int(thread_snapshot.get("usage_bytes") or 0),
                "projected_bytes": int(thread_snapshot.get("projected_bytes") or 0),
                "limit_bytes": int(thread_snapshot.get("limit_bytes") or 0),
            }
        user_snapshot = storage_manager.user_quota_snapshot(
            self._user_thread_ids(user_id, thread_id), additional_bytes=additional_bytes
        )
        if not bool(user_snapshot.get("allowed", True)):
            return {
                "code": "user_workspace_quota_reached",
                "usage_bytes": int(user_snapshot.get("usage_bytes") or 0),
                "projected_bytes": int(user_snapshot.get("projected_bytes") or 0),
                "limit_bytes": int(user_snapshot.get("limit_bytes") or 0),
            }
        return None

    def _get_graph(self, model_name: str) -> Any:
        model = str(model_name or "").strip()
        if model not in MODEL_OPTIONS:
            raise WebRuntimeError("unsupported_model", "所选模型不受支持。")
        missing = missing_env_for_model(model)
        if missing:
            raise WebRuntimeError("model_configuration_missing", "模型服务尚未配置。", {"missing": missing})
        api_key = get_env_api_key(model)
        if not api_key:
            raise WebRuntimeError("model_configuration_missing", "模型服务尚未配置。")
        from graph_factory_v2 import build_ntl_graph

        cache_key = (model, api_key)
        with self._lock:
            graph = self._graphs.get(cache_key)
            if graph is None:
                graph = build_ntl_graph(
                    model_name=model,
                    api_key=api_key,
                    request_timeout_s=int(getattr(app_state, "LLM_REQUEST_TIMEOUT_S", 120)),
                    graph_name="NTL_Engineer",
                )
                self._graphs[cache_key] = graph
            return graph

    @staticmethod
    def _build_context_message(question: str, thread_id: str) -> Optional[str]:
        snippets = history_store.retrieve_relevant_context(thread_id=thread_id, query=question, top_n=4, max_chars=6000)
        if not snippets:
            return None
        lines = [
            "User-provided file context snippets (retrieved by relevance).",
            "Use only when relevant to the current question and cite the source file/page when used.",
        ]
        for index, item in enumerate(snippets, 1):
            source = str(item.get("source_file") or "unknown")
            page = item.get("page")
            page_text = f", page {page}" if page else ""
            lines.append(f"[{index}] source={source}{page_text}")
            lines.append(str(item.get("text") or "").strip())
        return "\n".join(lines)

    def _emit(self, run_id: str, kind: str, payload: Optional[dict[str, Any]] = None) -> None:
        with self._lock:
            control = self._runs.get(run_id)
            if control is None:
                return
            seq = int(control.get("next_seq", 1))
            control["next_seq"] = seq + 1
            events = control.setdefault("events", [])
            events.append(
                {
                    "seq": seq,
                    "run_id": run_id,
                    "thread_id": str(control.get("thread_id") or ""),
                    "ts": time.time(),
                    "kind": kind,
                    "payload": payload or {},
                }
            )
            if len(events) > MAX_RETAINED_EVENTS:
                del events[: len(events) - MAX_RETAINED_EVENTS]

    def start(
        self,
        *,
        user_id: str,
        thread_id: str,
        question: str,
        model_name: str,
        request_key: str = "",
    ) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        thread_id = str(thread_id or "").strip()
        question = str(question or "").strip()
        request_key = str(request_key or "").strip()[:200]
        if not question:
            raise WebRuntimeError("empty_question", "请输入研究问题。")
        if not history_store.thread_belongs_to_user(user_id, thread_id):
            raise WebRuntimeError("thread_not_found", "当前任务不存在或无权访问。")
        quota = self.workspace_quota_rejection(thread_id, user_id)
        if quota:
            raise WebRuntimeError(quota["code"], "当前工作区存储额度不足。", quota)
        with self._lock:
            duplicate = self._recent_duplicate_locked(user_id, thread_id, request_key)
            if duplicate:
                return duplicate
        graph = self._get_graph(model_name)
        with self._lock:
            duplicate = self._recent_duplicate_locked(user_id, thread_id, request_key)
            if duplicate:
                return duplicate
            active_run_id = self._thread_active_run.get(thread_id)
            active = self._runs.get(active_run_id or "")
            if active and str(active.get("state")) == "running":
                raise WebRuntimeError("thread_run_in_progress", "当前任务仍在运行。", {"run_id": active_run_id})
            snapshot = build_run_limit_snapshot(self._running_controls_locked(), user_id)
            if snapshot["global_limit"] > 0 and snapshot["global_active"] >= snapshot["global_limit"]:
                raise WebRuntimeError("global_run_limit_reached", "当前平台运行任务已达到并发上限。", snapshot)
            if snapshot["user_limit"] > 0 and snapshot["user_active"] >= snapshot["user_limit"]:
                raise WebRuntimeError("user_run_limit_reached", "当前账号运行任务已达到并发上限。", snapshot)

            context_message = self._build_context_message(question, thread_id)
            messages: list[dict[str, str]] = []
            if context_message:
                messages.append({"role": "system", "content": context_message})
            messages.append({"role": "user", "content": question})
            run_id = uuid.uuid4().hex
            now = time.time()
            control = {
                "run_id": run_id,
                "thread_id": thread_id,
                "user_id": user_id,
                "question": question,
                "model_name": model_name,
                "conversation": graph,
                "state_payload": {"messages": messages},
                "config": {
                    "configurable": {"thread_id": thread_id, "user_id": user_id},
                    "recursion_limit": app_state.RECURSION_LIMIT,
                },
                "events": [],
                "next_seq": 1,
                "state": "running",
                "stop_requested": False,
                "start_ts": now,
                "heartbeat_ts": now,
                "end_ts": None,
                "worker_thread": None,
                "request_key": request_key,
            }
            self._runs[run_id] = control
            self._thread_active_run[thread_id] = run_id
            if request_key:
                self._recent_request_keys[(user_id, thread_id, request_key)] = (now, run_id)

        try:
            history_store.append_chat_record(thread_id, role="user", content=question, kind="text")
            history_store.touch_thread_activity(user_id, thread_id, last_question=question)
        except Exception:
            logger.exception("Unable to persist user question for public web run")

        worker = threading.Thread(target=self._worker, args=(run_id,), daemon=True, name=f"geopolitics-run-{run_id[:8]}")
        with self._lock:
            self._runs[run_id]["worker_thread"] = worker
        worker.start()
        return {"run_id": run_id, "thread_id": thread_id, "state": "running"}

    def _worker(self, run_id: str) -> None:
        with self._lock:
            control = self._runs.get(run_id)
        if not control:
            return
        thread_id = str(control["thread_id"])
        user_id = str(control["user_id"])
        question = str(control["question"])
        conversation = control["conversation"]
        state = control["state_payload"]
        config = control["config"]
        start_ts = float(control["start_ts"])
        final_answer: Optional[str] = None
        last_messages: list[BaseMessage] = []
        logs: list[list[BaseMessage]] = []
        interrupted = False
        run_timeout = False
        run_error: Optional[Exception] = None
        error_payload: Optional[dict[str, Any]] = None
        model_budget, model_budget_token = begin_model_call_budget()

        self._emit(run_id, "status", {"state": "running"})
        token = current_thread_id.set(thread_id)
        try:
            seen_fingerprints = {_message_fingerprint(item) for item in _get_state_messages(conversation, config) if isinstance(item, BaseMessage)}
            for mode, payload, _namespace in _iter_events(conversation, state, config):
                with self._lock:
                    current = self._runs.get(run_id)
                    if current is None or bool(current.get("stop_requested")):
                        interrupted = True
                        break
                    if str(current.get("state")) not in ("running", ""):
                        run_timeout = True
                        break
                    if time.time() - start_ts > RUN_WALL_CLOCK_TIMEOUT_S:
                        run_timeout = True
                        break
                    current["heartbeat_ts"] = time.time()

                if mode == "messages":
                    message, metadata = _stream_message_parts(payload)
                    if not isinstance(message, (AIMessage, AIMessageChunk)):
                        continue
                    text = _content_to_text(getattr(message, "content", ""))
                    if not text:
                        continue
                    agent_name = _canonical_stream_agent(
                        getattr(message, "name", ""),
                        metadata,
                        _namespace,
                        default="NTL_Engineer" if not _namespace else "",
                    )
                    if not agent_name:
                        continue
                    self._emit(
                        run_id,
                        "agent_stream_delta",
                        {
                            "agent": agent_name,
                            "message_id": str(getattr(message, "id", "") or ""),
                            "text": text[:2000],
                        },
                    )
                    continue
                if mode == "custom":
                    safe_payload = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
                    self._emit(run_id, "reasoning_custom", {"detail": safe_payload})
                    continue
                delta_messages = _collect_new_messages(payload, seen_fingerprints)
                if not delta_messages:
                    continue
                logs.append(delta_messages)
                last_messages = delta_messages
                namespace_agent = _canonical_stream_agent(_namespace)
                self._emit(
                    run_id,
                    "reasoning_delta",
                    {
                        "messages": [
                            _serialize_message(message, namespace_agent)
                            for message in delta_messages
                        ]
                    },
                )
                candidate = _extract_meaningful_ai_text(delta_messages)
                if candidate:
                    final_answer = candidate
        except Exception as error:  # noqa: BLE001
            run_error = error
        finally:
            current_thread_id.reset(token)
            end_model_call_budget(model_budget_token)

        state_messages = _get_state_messages(conversation, config)
        preferred = _extract_meaningful_ai_text(state_messages, preferred_agents=["NTL_Engineer"])
        if preferred:
            final_answer = preferred
        elif not final_answer:
            final_answer = _extract_meaningful_ai_text(last_messages)

        elapsed_s = max(0.0, time.time() - start_ts)
        status = "partial"
        assistant_text: Optional[str] = None
        if run_timeout:
            status = "error"
            error_payload = {
                "code": "run_timeout",
                "text": _time_footer(
                    "任务超过平台运行时限，系统已停止本次运行以释放资源。已生成文件与记录仍保留，可重新提交任务。",
                    elapsed_s,
                ),
                "elapsed_s": elapsed_s,
            }
            assistant_text = str(error_payload["text"])
            self._emit(run_id, "error", error_payload)
        elif interrupted:
            status = "interrupted"
            assistant_text = _time_footer("任务已按你的要求停止。已生成文件和已完成记录仍保留。", elapsed_s)
            self._emit(run_id, "interrupted", {"text": assistant_text, "elapsed_s": elapsed_s})
        elif isinstance(run_error, ModelCallBudgetExceeded):
            status = "partial"
            error_payload = _runtime_error_payload(run_error, elapsed_s)
            assistant_text = str(error_payload["text"])
            self._emit(run_id, "error", error_payload)
        elif final_answer:
            status = "success"
            assistant_text = _time_footer(final_answer, elapsed_s)
            self._emit(run_id, "final_answer", {"text": assistant_text, "elapsed_s": elapsed_s})
            if run_error:
                self._emit(run_id, "warning", {"text": "任务已返回结果，但调度过程出现额外异常。"})
        elif run_error:
            status = "error"
            error_payload = _runtime_error_payload(run_error, elapsed_s)
            assistant_text = str(error_payload["text"])
            self._emit(run_id, "error", error_payload)
        else:
            assistant_text = _time_footer("任务结束，但未形成可显示的最终结论。请调整问题后重试。", elapsed_s)
            self._emit(run_id, "no_final", {"text": assistant_text, "elapsed_s": elapsed_s})

        try:
            if assistant_text:
                history_store.append_chat_record(thread_id, role="assistant", content=assistant_text, kind="text")
            sequence, counts = _tool_usage(logs)
            history_store.append_turn_summary(
                thread_id,
                {
                    "user_id": user_id,
                    "thread_id": thread_id,
                    "question": question,
                    "final_answer_excerpt": str(final_answer or "")[:300],
                    "tool_sequence": sequence,
                    "tool_calls_by_name": counts,
                    "status": status,
                    "error_code": str((error_payload or {}).get("code") or ""),
                    "duration_s": round(elapsed_s, 3),
                    "llm_calls": model_budget.count,
                    "llm_call_limit": model_budget.limit,
                },
            )
            history_store.touch_thread_activity(
                user_id,
                thread_id,
                last_question=question,
                last_answer_excerpt=str(final_answer or "")[:240],
            )
        except Exception:
            logger.exception("Unable to persist public web run result")

        artifacts = self.list_artifacts(thread_id)
        if artifacts:
            self._emit(run_id, "artifacts", {"items": artifacts})
        with self._lock:
            current = self._runs.get(run_id)
            if current:
                current["state"] = status
                current["end_ts"] = time.time()
            self._thread_active_run.pop(thread_id, None)
        self._emit(
            run_id,
            "done",
            {
                "status": status,
                "thread_id": thread_id,
                "elapsed_s": elapsed_s,
                "llm_calls": model_budget.count,
                "llm_call_limit": model_budget.limit,
            },
        )

    def poll(self, run_id: str, after_seq: int = 0) -> tuple[list[dict[str, Any]], str]:
        with self._lock:
            self._reconcile_orphaned_runs_locked()
            control = self._runs.get(str(run_id))
            if control is None:
                return [], "missing"
            events = [event.copy() for event in control.get("events", []) if int(event.get("seq", 0)) > int(after_seq)]
            retained = control.get("events", [])
            first_seq = int(retained[0].get("seq", 0)) if retained else 0
            if int(after_seq) > 0 and first_seq > int(after_seq) + 1:
                events.insert(
                    0,
                    {
                        "seq": first_seq - 1,
                        "run_id": str(run_id),
                        "thread_id": str(control.get("thread_id") or ""),
                        "ts": time.time(),
                        "kind": "stream_gap",
                        "payload": {
                            "requested_after_seq": int(after_seq),
                            "first_retained_seq": first_seq,
                        },
                    },
                )
            return events, str(control.get("state") or "unknown")

    def run_summary(self, run_id: str, user_id: str) -> dict[str, Any]:
        with self._lock:
            self._reconcile_orphaned_runs_locked()
            control = self._runs.get(str(run_id))
            if control is None or str(control.get("user_id")) != str(user_id):
                raise WebRuntimeError("run_not_found", "运行任务不存在或无权访问。")
            return {
                "run_id": str(control["run_id"]),
                "thread_id": str(control["thread_id"]),
                "state": str(control.get("state") or "unknown"),
                "start_ts": float(control.get("start_ts") or 0),
                "heartbeat_ts": float(control.get("heartbeat_ts") or 0),
                "end_ts": float(control.get("end_ts") or 0) if control.get("end_ts") else None,
            }

    def cancel(self, run_id: str, user_id: str) -> dict[str, Any]:
        with self._lock:
            self._reconcile_orphaned_runs_locked()
            control = self._runs.get(str(run_id))
            if control is None or str(control.get("user_id")) != str(user_id):
                raise WebRuntimeError("run_not_found", "运行任务不存在或无权访问。")
            if str(control.get("state")) != "running":
                return {"run_id": str(run_id), "requested": False, "state": str(control.get("state") or "unknown")}
            already_requested = bool(control.get("stop_requested"))
            control["stop_requested"] = True
            if not already_requested:
                self._emit(
                    str(run_id),
                    "stopping",
                    {"text": "已收到停止请求，正在当前安全边界结束任务。"},
                )
            return {"run_id": str(run_id), "requested": True, "state": "stopping"}

    def active_run_for_thread(self, thread_id: str, user_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            self._reconcile_orphaned_runs_locked()
            run_id = self._thread_active_run.get(str(thread_id))
            control = self._runs.get(run_id or "")
            if not control or str(control.get("user_id")) != str(user_id):
                return None
            return self.run_summary(str(run_id), user_id)

    @staticmethod
    def _list_workspace_root(thread_id: str, root_name: str, limit: int = 120) -> list[dict[str, Any]]:
        if root_name not in {"inputs", "outputs"}:
            raise ValueError("Unsupported workspace file root.")
        workspace = storage_manager.get_workspace(thread_id)
        file_root = (workspace / root_name).resolve()
        if not file_root.exists():
            return []
        items: list[dict[str, Any]] = []
        for path in sorted(file_root.rglob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True):
            if not path.is_file():
                continue
            try:
                relative = path.resolve().relative_to(file_root).as_posix()
                safe_path = storage_manager.resolve_workspace_relative_path(
                    f"{root_name}/{relative}",
                    thread_id=thread_id,
                    default_root=root_name,
                    allowed_roots=(root_name,),
                    allow_memory=False,
                )
                stat = safe_path.stat()
            except (OSError, ValueError, PermissionError):
                continue
            items.append(
                {
                    "root": root_name,
                    "path": relative,
                    "name": safe_path.name,
                    "size_bytes": int(stat.st_size),
                    "size_label": _format_bytes(int(stat.st_size)),
                    "modified_ts": float(stat.st_mtime),
                    "preview_kind": _artifact_preview_kind(safe_path),
                    "is_process": _is_process_artifact(safe_path),
                }
            )
            if len(items) >= limit:
                break
        return items

    @staticmethod
    def list_workspace_files(thread_id: str, limit_per_root: int = 240) -> dict[str, list[dict[str, Any]]]:
        safe_limit = min(max(1, int(limit_per_root)), 500)
        inputs = WebRunManager._list_workspace_root(thread_id, "inputs", safe_limit)
        outputs = WebRunManager._list_workspace_root(thread_id, "outputs", safe_limit)
        # Unified, workspace-relative file stream: every managed file carries its
        # own root tag so clients can present a single free-form list instead of
        # two isolated buckets.  Sorting stays newest-first across both roots.
        merged = sorted(
            [*inputs, *outputs],
            key=lambda item: float(item.get("modified_ts") or 0),
            reverse=True,
        )
        return {
            "inputs": inputs,
            "outputs": outputs,
            "files": merged,
        }

    @staticmethod
    def list_artifacts(thread_id: str, limit: int = 120) -> list[dict[str, Any]]:
        return WebRunManager._list_workspace_root(thread_id, "outputs", limit)

    @staticmethod
    def resolve_workspace_file(thread_id: str, root_name: str, relative_path: str) -> Path:
        if root_name not in {"inputs", "outputs"}:
            raise ValueError("Unsupported workspace file root.")
        cleaned_path = str(relative_path or "").lstrip("/").replace("\\", "/")
        return storage_manager.resolve_workspace_relative_path(
            f"{root_name}/{cleaned_path}",
            thread_id=thread_id,
            default_root=root_name,
            allowed_roots=(root_name,),
            allow_memory=False,
        )

    @staticmethod
    def resolve_output(thread_id: str, relative_path: str) -> Path:
        return WebRunManager.resolve_workspace_file(thread_id, "outputs", relative_path)

    @staticmethod
    def preview_workspace_geo(
        thread_id: str,
        root_name: str,
        relative_path: str,
        max_px: int = 1200,
    ) -> dict[str, Any]:
        """Browser-facing preview of common geospatial artifacts.

        Raster (GeoTIFF) and vector (ESRI Shapefile / GeoJSON / KML) files are
        converted into a lightweight, browser-renderable payload: a PNG preview
        (via GDAL rasterization) plus a WGS84 envelope or FeatureCollection for
        map placement.  Nothing is written into the workspace; the result is a
        fresh in-memory representation of the source file on every request.
        """
        import base64
        import io

        from osgeo import gdal, ogr, osr

        path = WebRunManager.resolve_workspace_file(thread_id, root_name, relative_path)
        suffix = path.suffix.lower()
        # Resolve sidecars for shapefiles (.shp + .dbf + .prj + .shx).
        candidates = [path]
        if suffix == ".shp":
            for side in (".dbf", ".prj", ".shx", ".cpg", ".qix"):
                sibling = path.with_suffix(side)
                if sibling.is_file():
                    candidates.append(sibling)
        for candidate in candidates:
            if not candidate.is_file():
                raise FileNotFoundError(f"缺少组件文件：{candidate.name}")

        if suffix in {".kml", ".kmz"}:
            return WebRunManager._geo_preview_vector_driver(path, max_px=max_px)

        if suffix in {".shp", ".geojson", ".json"}:
            return WebRunManager._geo_preview_vector(path, max_px=max_px)

        return WebRunManager._geo_preview_raster(path, max_px=max_px)

    @staticmethod
    def _geo_preview_common(base64_png: str, envelope: dict[str, Any] | None, *, name: str, kind: str, note: str | None = None) -> dict[str, Any]:
        return {
            "status": "ok",
            "kind": kind,
            "name": name,
            "image_png_base64": base64_png,
            "envelope": envelope,
            "note": note,
        }

    @staticmethod
    def _geo_preview_raster(path: Path, max_px: int = 1200) -> dict[str, Any]:
        import base64

        from osgeo import gdal, osr

        gdal.UseExceptions()
        dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
        if dataset is None:
            raise RuntimeError("无法读取栅格文件（GDAL 打开失败）。")
        driver = gdal.GetDriverByName("MEM")
        full = dataset.GetRasterBand(1).ReadAsArray()

        import numpy as np

        stats = None
        try:
            stats = dataset.GetRasterBand(1).GetStatistics(True, True)
        except Exception:
            pass
        width, height = dataset.RasterXSize, dataset.RasterYSize
        # Downscale to a browser-friendly preview with aspect preservation.
        scale = min(1.0, max_px / max(1, width), max_px / max(1, height))
        target_w = max(2, int(width * scale))
        target_h = max(2, int(height * scale))
        src_band = dataset.GetRasterBand(1)
        gt = dataset.GetGeoTransform()
        envelope_geo = None
        if gt and (gt[0] != 0 or gt[1] != 1 or gt[2] != 0 or gt[3] != 0 or gt[4] != 0 or gt[5] != 1):
            # Valid georeference (not a plain pixel grid).
            from osgeo import osr as _osr
            proj = dataset.GetProjection()
            if proj:
                src_srs = _osr.SpatialReference()
                src_srs.ImportFromWkt(proj)
                if src_srs.IsProjected():
                    dst_srs = _osr.SpatialReference()
                    dst_srs.ImportFromEPSG(4326)
                    transform = _osr.CoordinateTransformation(src_srs, dst_srs)
                    corners_px = [
                        (0, 0), (width, 0), (0, height), (width, height),
                    ]
                    lons: list[float] = []
                    lats: list[float] = []
                    for px, py in corners_px:
                        x = gt[0] + px * gt[1] + py * gt[2]
                        y = gt[3] + px * gt[4] + py * gt[5]
                        lng, lat, *_ = transform.TransformPoint(x, y)
                        lons.append(lng)
                        lats.append(lat)
                    envelope_geo = {
                        "minx": float(min(lons)),
                        "maxx": float(max(lons)),
                        "miny": float(min(lats)),
                        "maxy": float(max(lats)),
                    }
                else:
                    corners: list[tuple[float, float]] = [
                        (0, 0), (width, 0), (0, height), (width, height),
                    ]
                    lons = []
                    lats = []
                    for px, py in corners:
                        lons.append(gt[0] + px * gt[1] + py * gt[2])
                        lats.append(gt[3] + px * gt[4] + py * gt[5])
                    envelope_geo = {
                        "minx": float(min(lons)),
                        "maxx": float(max(lons)),
                        "miny": float(min(lats)),
                        "maxy": float(max(lats)),
                    }
        # Read + direct downscale; preview only (no reprojection).
        block = src_band.ReadAsArray(0, 0, width, height)
        import numpy as _np
        arr = None
        if width == target_w and height == target_h:
            arr = block.astype("float64")
        else:
            from PIL import Image as _Img
            _img = _Img.fromarray(_np.flipud(block.astype("float64")), mode="F")
            _img = _img.resize((target_w, target_h), _Img.Resampling.BILINEAR)
            arr = _np.flipud(_np.asarray(_img)).astype("float64")
        # Percentile stretch directly on the float preview array.
        if arr.size:
            finite = arr[np.isfinite(arr)]
            if finite.size:
                lo = float(np.percentile(finite, 2))
                hi = float(np.percentile(finite, 98))
                if hi > lo:
                    arr = np.clip((arr - lo) / (hi - lo), 0.0, 1.0) * 255.0
                else:
                    arr = np.clip(arr, 0.0, 255.0)
            else:
                arr = np.zeros_like(arr)
        arr = arr.astype("uint8")
        arr = np.flipud(arr)
        from PIL import Image

        image = Image.fromarray(arr, mode="L").convert("RGB")
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        png_b64 = base64.b64encode(buffer.getvalue()).decode("ascii")
        data_type = int(src_band.DataType) if src_band is not None else 0
        note = (
            f"{width} x {height} 像素，波段 1 类型 {data_type}；"
            "预览为 2-98 分位增强。"
            if stats is not None
            else None
        )
        dataset = None
        block = None
        return WebRunManager._geo_preview_common(
            png_b64, envelope_geo, name=path.name, kind="raster", note=note
        )

    @staticmethod
    def _geo_preview_vector(path: Path, max_px: int = 1200) -> dict[str, Any]:
        import base64

        from osgeo import gdal, ogr, osr

        gdal.UseExceptions()
        ds = ogr.Open(str(path), 0)
        if ds is None:
            raise RuntimeError("无法读取矢量文件（GDAL 打开失败）。")
        layer = ds.GetLayer(0)
        if layer is None:
            raise RuntimeError("矢量图层为空。")
        srs = layer.GetSpatialRef()
        env = layer.GetExtent()  # (minx, maxx, miny, maxy)
        # Transform extent to WGS84 for map placement.
        envelope_geo = None
        if srs is not None:
            src_srs = osr.SpatialReference()
            src_srs.ImportFromSrs(srs)
            if not src_srs.IsProjected():
                envelope_geo = {
                    "minx": float(env[0]), "maxx": float(env[1]),
                    "miny": float(env[2]), "maxy": float(env[3]),
                }
            else:
                dst_srs = osr.SpatialReference()
                dst_srs.ImportFromEPSG(4326)
                transform = osr.CoordinateTransformation(src_srs, dst_srs)
                ring_pts = [
                    transform.TransformPoint(env[0], env[2]),
                    transform.TransformPoint(env[1], env[2]),
                    transform.TransformPoint(env[0], env[3]),
                    transform.TransformPoint(env[1], env[3]),
                ]
                lons = [p[0] for p in ring_pts]
                lats = [p[1] for p in ring_pts]
                envelope_geo = {
                    "minx": float(min(lons)), "maxx": float(max(lons)),
                    "miny": float(min(lats)), "maxy": float(max(lats)),
                }
        # Render into a PNG canvas (projected overlay keeps geometry correct).
        mem_ds = gdal.GetDriverByName("MEM").Create("", max_px, max_px, 1, gdal.GDT_Byte)
        mem_ds.SetProjection(srs.ExportToWkt() if srs else None)
        mem_ds.SetGeoTransform(
            [env[0], (env[1] - env[0]) / max_px, 0, env[3], 0, -(env[3] - env[2]) / max_px]
        )
        band = mem_ds.GetRasterBand(1)
        band.Fill(0)
        band.SetNoDataValue(0)
        from osgeo import gdal as _gdal

        gdal.RasterizeLayer(mem_ds, [1], layer, burn_values=[230])
        arr = band.ReadAsArray()
        arr = np.flipud(arr)
        from PIL import Image

        image = Image.fromarray(arr, mode="L").convert("RGB")
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        png_b64 = base64.b64encode(buffer.getvalue()).decode("ascii")
        feature_count = layer.GetFeatureCount()
        mem_ds = None
        ds = None
        return WebRunManager._geo_preview_common(
            png_b64,
            envelope_geo,
            name=path.name,
            kind="vector",
            note=f"图元数 {feature_count}；预览为矢量素面叠加。",
        )

    @staticmethod
    def _geo_preview_vector_driver(path: Path, max_px: int = 1200) -> dict[str, Any]:
        # KML/KMZ conversions are handled by the generic vector path in many
        # GDAL builds; fall back to the standard vector renderer.
        return WebRunManager._geo_preview_vector(path, max_px=max_px)

    @staticmethod
    def preview_workspace_table(
        thread_id: str,
        root_name: str,
        relative_path: str,
        max_rows: int = ARTIFACT_PREVIEW_MAX_ROWS,
        max_columns: int = ARTIFACT_PREVIEW_MAX_COLUMNS,
    ) -> dict[str, Any]:
        target = WebRunManager.resolve_workspace_file(thread_id, root_name, relative_path)
        if not target.is_file():
            raise WebRuntimeError("output_missing", "文件不存在。")
        safe_rows = min(max(1, int(max_rows)), ARTIFACT_PREVIEW_MAX_ROWS)
        safe_columns = min(max(1, int(max_columns)), ARTIFACT_PREVIEW_MAX_COLUMNS)
        try:
            preview = _table_preview_from_path(target, safe_rows, safe_columns)
        except WebRuntimeError:
            raise
        except Exception as error:
            logger.warning("Workspace table preview failed for %s: %s", target.name, error)
            raise WebRuntimeError("artifact_preview_invalid", "表格内容无法在线预览，请下载文件查看。") from error
        preview["root"] = root_name
        preview["path"] = str(relative_path or "").lstrip("/").replace("\\", "/")
        return preview

    @staticmethod
    def preview_table(
        thread_id: str,
        relative_path: str,
        max_rows: int = ARTIFACT_PREVIEW_MAX_ROWS,
        max_columns: int = ARTIFACT_PREVIEW_MAX_COLUMNS,
    ) -> dict[str, Any]:
        return WebRunManager.preview_workspace_table(
            thread_id,
            "outputs",
            relative_path,
            max_rows=max_rows,
            max_columns=max_columns,
        )


web_run_manager = WebRunManager()
