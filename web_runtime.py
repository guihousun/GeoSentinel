"""Framework-independent public-web runtime for the geopolitical frontend.

This module deliberately owns no Streamlit state.  It reuses the existing
LangGraph graph, PostgreSQL history store, and isolated thread workspaces so
the public web UI and the internal Streamlit UI operate on the same research
records without sharing a UI lifecycle.
"""

from __future__ import annotations

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
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

import app_state
import history_store
from model_config import MODEL_OPTIONS, get_env_api_key, missing_env_for_model
from runtime_governance import build_run_limit_snapshot
from storage_manager import current_thread_id, storage_manager


load_dotenv(override=True)

logger = logging.getLogger(__name__)

TERMINAL_STATES = frozenset({"success", "error", "interrupted", "partial"})
MAX_RETAINED_EVENTS = 800


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


def _serialize_message(message: BaseMessage) -> dict[str, Any]:
    if isinstance(message, ToolMessage):
        role = "tool"
    elif isinstance(message, AIMessage):
        role = "assistant"
    else:
        role = "message"
    text = _content_to_text(getattr(message, "content", "")).strip()
    return {
        "role": role,
        "agent": str(getattr(message, "name", "") or ""),
        "tool_name": str(getattr(message, "name", "") or "") if isinstance(message, ToolMessage) else "",
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


def _runtime_error_text(error: Exception, elapsed_s: float) -> str:
    logger.exception("Public web run failed", exc_info=error)
    return _time_footer("任务未能完成。请查看调度记录、检查数据或服务配置后重试。", elapsed_s)


class WebRunManager:
    """Thread-safe run registry shared by the ASGI API and SSE consumers."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._thread_active_run: dict[str, str] = {}
        self._graphs: dict[tuple[str, str], Any] = {}

    def _running_controls_locked(self, user_id: str = "") -> list[dict[str, Any]]:
        wanted = str(user_id or "").strip()
        return [
            control
            for control in self._runs.values()
            if str(control.get("state")) == "running" and (not wanted or str(control.get("user_id")) == wanted)
        ]

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
        from graph_factory import build_ntl_graph

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

    def start(self, *, user_id: str, thread_id: str, question: str, model_name: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        thread_id = str(thread_id or "").strip()
        question = str(question or "").strip()
        if not question:
            raise WebRuntimeError("empty_question", "请输入研究问题。")
        if not history_store.thread_belongs_to_user(user_id, thread_id):
            raise WebRuntimeError("thread_not_found", "当前任务不存在或无权访问。")
        quota = self.workspace_quota_rejection(thread_id, user_id)
        if quota:
            raise WebRuntimeError(quota["code"], "当前工作区存储额度不足。", quota)
        graph = self._get_graph(model_name)
        with self._lock:
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
            }
            self._runs[run_id] = control
            self._thread_active_run[thread_id] = run_id

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
        run_error: Optional[Exception] = None

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
                    current["heartbeat_ts"] = time.time()

                if mode == "messages":
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
                self._emit(
                    run_id,
                    "reasoning_delta",
                    {"messages": [_serialize_message(message) for message in delta_messages]},
                )
                candidate = _extract_meaningful_ai_text(delta_messages)
                if candidate:
                    final_answer = candidate
        except Exception as error:  # noqa: BLE001
            run_error = error
        finally:
            current_thread_id.reset(token)

        state_messages = _get_state_messages(conversation, config)
        preferred = _extract_meaningful_ai_text(state_messages, preferred_agents=["NTL_Engineer"])
        if preferred:
            final_answer = preferred
        elif not final_answer:
            final_answer = _extract_meaningful_ai_text(last_messages)

        elapsed_s = max(0.0, time.time() - start_ts)
        status = "partial"
        assistant_text: Optional[str] = None
        if final_answer:
            status = "success"
            assistant_text = _time_footer(final_answer, elapsed_s)
            self._emit(run_id, "final_answer", {"text": assistant_text, "elapsed_s": elapsed_s})
            if run_error:
                self._emit(run_id, "warning", {"text": "任务已返回结果，但调度过程出现额外异常。"})
        elif run_error:
            status = "error"
            assistant_text = _runtime_error_text(run_error, elapsed_s)
            self._emit(run_id, "error", {"text": assistant_text, "elapsed_s": elapsed_s})
        elif interrupted:
            status = "interrupted"
            assistant_text = _time_footer("任务已停止。可调整问题或条件后再次运行。", elapsed_s)
            self._emit(run_id, "interrupted", {"text": assistant_text, "elapsed_s": elapsed_s})
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
                    "duration_s": round(elapsed_s, 3),
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
        self._emit(run_id, "done", {"status": status, "thread_id": thread_id, "elapsed_s": elapsed_s})

    def poll(self, run_id: str, after_seq: int = 0) -> tuple[list[dict[str, Any]], str]:
        with self._lock:
            control = self._runs.get(str(run_id))
            if control is None:
                return [], "missing"
            events = [event.copy() for event in control.get("events", []) if int(event.get("seq", 0)) > int(after_seq)]
            return events, str(control.get("state") or "unknown")

    def run_summary(self, run_id: str, user_id: str) -> dict[str, Any]:
        with self._lock:
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
            control = self._runs.get(str(run_id))
            if control is None or str(control.get("user_id")) != str(user_id):
                raise WebRuntimeError("run_not_found", "运行任务不存在或无权访问。")
            if str(control.get("state")) != "running":
                return {"run_id": str(run_id), "requested": False, "state": str(control.get("state") or "unknown")}
            control["stop_requested"] = True
            return {"run_id": str(run_id), "requested": True, "state": "stopping"}

    def active_run_for_thread(self, thread_id: str, user_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            run_id = self._thread_active_run.get(str(thread_id))
            control = self._runs.get(run_id or "")
            if not control or str(control.get("user_id")) != str(user_id):
                return None
            return self.run_summary(str(run_id), user_id)

    @staticmethod
    def list_artifacts(thread_id: str, limit: int = 120) -> list[dict[str, Any]]:
        workspace = storage_manager.get_workspace(thread_id)
        output_root = (workspace / "outputs").resolve()
        if not output_root.exists():
            return []
        items: list[dict[str, Any]] = []
        for path in sorted(output_root.rglob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True):
            if not path.is_file():
                continue
            try:
                relative = path.resolve().relative_to(output_root).as_posix()
                safe_path = storage_manager.resolve_workspace_relative_path(
                    f"outputs/{relative}",
                    thread_id=thread_id,
                    default_root="outputs",
                    allowed_roots=("outputs",),
                    allow_memory=False,
                )
                stat = safe_path.stat()
            except (OSError, ValueError, PermissionError):
                continue
            items.append(
                {
                    "path": relative,
                    "name": safe_path.name,
                    "size_bytes": int(stat.st_size),
                    "size_label": _format_bytes(int(stat.st_size)),
                    "modified_ts": float(stat.st_mtime),
                }
            )
            if len(items) >= limit:
                break
        return items

    @staticmethod
    def resolve_output(thread_id: str, relative_path: str) -> Path:
        cleaned_path = str(relative_path or "").lstrip("/").replace("\\", "/")
        return storage_manager.resolve_workspace_relative_path(
            f"outputs/{cleaned_path}",
            thread_id=thread_id,
            default_root="outputs",
            allowed_roots=("outputs",),
            allow_memory=False,
        )


web_run_manager = WebRunManager()
