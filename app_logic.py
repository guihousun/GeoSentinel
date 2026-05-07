import json
import os
import re
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Optional

import streamlit as st
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage, message_to_dict

import app_agents
import app_state
import file_context_service
import history_store
from runtime_governance import (
    ASSISTANT_ID,
    build_runtime_metadata,
    build_run_limit_snapshot,
)
from storage_manager import (
    current_gee_encrypted_refresh_token,
    current_gee_project_id,
    current_gee_token_scopes,
    current_thread_id,
    storage_manager,
)


_RUN_REGISTRY_LOCK = threading.RLock()
_RUN_REGISTRY: dict[str, dict[str, Any]] = {}
_THREAD_ACTIVE_RUN: dict[str, str] = {}
OUTPUT_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def _format_bytes(num_bytes: int) -> str:
    size = max(0, int(num_bytes or 0))
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)}{unit}"
    return f"{value:.1f}{unit}"


def _user_thread_ids(user_id: str, current_thread_id: str) -> list[str]:
    tids: list[str] = []
    uid = str(user_id or "").strip()
    if uid:
        try:
            tids.extend(str(row.get("thread_id") or "").strip() for row in history_store.list_user_threads(uid, limit=0))
        except Exception:
            pass
    tid = str(current_thread_id or "").strip()
    if tid and tid not in tids:
        tids.append(tid)
    return [item for item in tids if item]


def _workspace_quota_rejection(thread_id: str, user_id: str, *, additional_bytes: int = 0) -> Optional[dict[str, Any]]:
    thread_snapshot = storage_manager.thread_quota_snapshot(thread_id, additional_bytes=additional_bytes)
    if not bool(thread_snapshot.get("allowed", True)):
        return {
            "started": False,
            "reason": "thread_workspace_quota_reached",
            "usage_bytes": int(thread_snapshot.get("usage_bytes") or 0),
            "projected_bytes": int(thread_snapshot.get("projected_bytes") or 0),
            "limit_bytes": int(thread_snapshot.get("limit_bytes") or 0),
            "usage_label": _format_bytes(int(thread_snapshot.get("usage_bytes") or 0)),
            "projected_label": _format_bytes(int(thread_snapshot.get("projected_bytes") or 0)),
            "limit_label": _format_bytes(int(thread_snapshot.get("limit_bytes") or 0)),
        }

    user_snapshot = storage_manager.user_quota_snapshot(
        _user_thread_ids(user_id, thread_id),
        additional_bytes=additional_bytes,
    )
    if not bool(user_snapshot.get("allowed", True)):
        return {
            "started": False,
            "reason": "user_workspace_quota_reached",
            "usage_bytes": int(user_snapshot.get("usage_bytes") or 0),
            "projected_bytes": int(user_snapshot.get("projected_bytes") or 0),
            "limit_bytes": int(user_snapshot.get("limit_bytes") or 0),
            "usage_label": _format_bytes(int(user_snapshot.get("usage_bytes") or 0)),
            "projected_label": _format_bytes(int(user_snapshot.get("projected_bytes") or 0)),
            "limit_label": _format_bytes(int(user_snapshot.get("limit_bytes") or 0)),
        }
    return None


def _workspace_relative_ref(path: Path, workspace: Path) -> str:
    try:
        return path.resolve().relative_to(workspace.resolve()).as_posix()
    except Exception:
        return str(path)


def _normalize_artifact_ref(value: Any, workspace: Path) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if not raw:
        return ""
    try:
        p = Path(raw)
        if p.is_absolute():
            return _workspace_relative_ref(p, workspace)
    except Exception:
        pass
    return raw


def _running_controls_locked(user_id: Optional[str] = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    wanted_user = str(user_id or "").strip()
    for control in _RUN_REGISTRY.values():
        if str(control.get("state")) != "running":
            continue
        if wanted_user and str(control.get("user_id") or "") != wanted_user:
            continue
        rows.append(control)
    return rows


def get_run_limit_snapshot(user_id: Optional[str] = None) -> dict[str, int]:
    with _RUN_REGISTRY_LOCK:
        controls = list(_RUN_REGISTRY.values())
    return build_run_limit_snapshot(controls, user_id=str(user_id or "").strip())


def _run_limit_rejection_locked(user_id: str) -> Optional[dict[str, Any]]:
    snapshot = build_run_limit_snapshot(_running_controls_locked(), user_id=user_id)
    global_limit = int(snapshot["global_limit"] or 0)
    if global_limit:
        active_count = int(snapshot["global_active"] or 0)
        if active_count >= global_limit:
            return {
                "started": False,
                "reason": "global_run_limit_reached",
                "active_runs": active_count,
                "limit": global_limit,
            }

    user_limit = int(snapshot["user_limit"] or 0)
    if user_limit:
        user_active_count = int(snapshot["user_active"] or 0)
        if user_active_count >= user_limit:
            return {
                "started": False,
                "reason": "user_run_limit_reached",
                "active_runs": user_active_count,
                "limit": user_limit,
            }
    return None


def _ensure_runtime_state_defaults() -> None:
    st.session_state.setdefault("active_run_id", None)
    st.session_state.setdefault("active_run_thread_id", None)
    st.session_state.setdefault("stopping", False)
    st.session_state.setdefault("run_last_rendered_event_seq", 0)
    st.session_state.setdefault("pending_model_change", None)
    st.session_state.setdefault("pending_activate_request", None)
    st.session_state.setdefault("run_last_terminal_kind", "")
    st.session_state.setdefault("ui_force_refresh_once", False)
    st.session_state.setdefault("pending_map_focus_layer", None)


def should_recover_stale_run(
    is_running: bool,
    run_started_ts: Optional[float],
    heartbeat_ts: Optional[float],
    now_ts: Optional[float] = None,
    grace_s: int = 300,
) -> bool:
    """Detect stale running-state that can block next-turn interaction."""
    if not is_running:
        return False
    now = float(now_ts if now_ts is not None else time.time())
    grace = max(30, int(grace_s))

    candidates = []
    for ts in (heartbeat_ts, run_started_ts):
        if ts is None:
            continue
        try:
            candidates.append(float(ts))
        except Exception:
            continue

    if not candidates:
        return True

    last_ts = max(candidates)
    return (now - last_ts) > grace


def recover_runtime_health():
    """Self-heal stale run state after long runs or interrupted reruns."""
    is_running = bool(st.session_state.get("is_running", False))
    if not is_running:
        return

    run_started_ts = st.session_state.get("run_started_ts")
    run_heartbeat_ts = st.session_state.get("run_heartbeat_ts")
    grace_s = 300

    if should_recover_stale_run(
        is_running=is_running,
        run_started_ts=run_started_ts,
        heartbeat_ts=run_heartbeat_ts,
        grace_s=grace_s,
    ):
        st.session_state["is_running"] = False
        st.session_state["stopping"] = False
        st.session_state["cancel_requested"] = False
        st.session_state["runtime_recovered_notice"] = (
            "Detected stale run state after a long task. "
            "Auto-recovered interaction so the next question can proceed."
        )


def _attach_time_cost_footer(answer_text: str, elapsed_s: float, ui_lang: str = "EN") -> str:
    """Append a subtle runtime footer at the end of an assistant response."""
    label = "本次回答耗时" if str(ui_lang).upper() == "CN" else "Time cost"
    footer = (
        "<div style='margin-top:0.45rem;font-size:0.78rem;"
        "color:#94a3b8;line-height:1.2;'>"
        f"{label}: {elapsed_s:.2f}s"
        "</div>"
    )
    return f"{answer_text}\n\n{footer}"


def _is_timeout_error_text(text: str) -> bool:
    low = (text or "").lower()
    markers = [
        "readtimeout",
        "connecttimeout",
        "timed out",
        "timeouterror",
        "httpx.readtimeout",
    ]
    return any(marker in low for marker in markers)


def _build_runtime_error_notice(err: Exception, ui_lang: str = "EN") -> str:
    raw = str(err or "").strip()
    short = raw.split("Traceback", 1)[0].strip() if raw else "Unknown runtime error."
    if len(short) > 260:
        short = short[:260] + "..."
    is_cn = str(ui_lang).upper() == "CN"

    if _is_timeout_error_text(raw):
        if is_cn:
            return (
                "运行在代理交接后发生超时（ReadTimeout），因此未产生最终结果。\n"
                f"错误摘要：`{short}`\n"
                "建议：重试一次；若持续出现，请提高模型请求超时或拆分任务。"
            )
        return (
            "The run timed out after agent handoff (ReadTimeout), so no final result was produced.\n"
            f"Error summary: `{short}`\n"
            "Suggestion: retry once; if it repeats, increase model timeout or split the task."
        )

    if is_cn:
        return (
            "运行在代理交接后失败，未产生最终结果。\n"
            f"错误摘要：`{short}`\n"
            "建议：检查最近工具输出与推理日志后重试。"
        )
    return (
        "The run failed after agent handoff and no final result was produced.\n"
        f"Error summary: `{short}`\n"
        "Suggestion: inspect recent tool outputs/reasoning logs and retry."
    )


def ensure_conversation_initialized():
    if not st.session_state.get("initialized"):
        return

    user_key = st.session_state.get("user_api_key")
    if not user_key:
        st.error("API Key is missing via session state.")
        return

    st.session_state.conversation = app_agents.get_ntl_graph(
        model_name=st.session_state["cfg_model"],
        api_key=user_key,
        request_timeout_s=int(getattr(app_state, "LLM_REQUEST_TIMEOUT_S", 120)),
        session_tag=st.session_state.thread_id,
    )


def _collect_recent_outputs(
    seconds: int = 120,
    thread_id: Optional[str] = None,
    since_ts: Optional[float] = None,
):
    tid = str(thread_id or st.session_state.get("thread_id") or current_thread_id.get() or "debug")
    workspace = storage_manager.get_workspace(tid)
    out_dir = workspace / "outputs"
    if not out_dir.exists():
        return
    now = time.time()

    def _is_run_output(path) -> bool:
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return False
        if since_ts is not None:
            try:
                return mtime >= float(since_ts) - 5.0
            except Exception:
                pass
        return (now - mtime) < seconds

    existing_image_refs: set[str] = set()
    for role, content in st.session_state.get("chat_history", []):
        if str(role) == "assistant_img":
            ref = _normalize_artifact_ref(content, workspace)
            if ref:
                existing_image_refs.add(ref)
    try:
        for row in history_store.load_chat_records(tid, limit=1000):
            if str(row.get("role", "")) == "assistant_img":
                ref = _normalize_artifact_ref(row.get("content", ""), workspace)
                if ref:
                    existing_image_refs.add(ref)
    except Exception:
        pass

    image_files = [
        f
        for f in out_dir.rglob("*")
        if f.is_file() and f.suffix.lower() in OUTPUT_IMAGE_SUFFIXES and _is_run_output(f)
    ]
    image_files.sort(key=lambda p: (p.stat().st_mtime if p.exists() else 0, _workspace_relative_ref(p, workspace)))
    for f in image_files:
        rel_ref = _workspace_relative_ref(f, workspace)
        if rel_ref in existing_image_refs:
            continue
        st.session_state.chat_history.append(("assistant_img", rel_ref))
        existing_image_refs.add(rel_ref)
        try:
            history_store.append_chat_record(tid, role="assistant_img", content=rel_ref, kind="image")
        except Exception:
            pass

    for f in out_dir.glob("*.csv"):
        if _is_run_output(f):
            st.session_state.chat_history.append(("assistant_table", str(f)))
            try:
                history_store.append_chat_record(tid, role="assistant_table", content=str(f), kind="table")
            except Exception:
                pass

    latest_geo = None
    latest_mtime = -1.0
    for pattern in ("*.tif", "*.tiff", "*.shp", "*.geojson"):
        for f in out_dir.glob(pattern):
            if not _is_run_output(f):
                continue
            try:
                mtime = os.path.getmtime(f)
            except OSError:
                continue
            if mtime > latest_mtime:
                latest_geo = f
                latest_mtime = mtime
    if latest_geo:
        st.session_state["pending_map_focus_layer"] = str(latest_geo)
        st.session_state["current_map_layer"] = str(latest_geo)
        if latest_geo.suffix.lower() in {".tif", ".tiff"}:
            st.session_state.current_map_tif = str(latest_geo)


def _build_injected_context_system_message(question: str, thread_id: str) -> Optional[str]:
    top_n = int(st.session_state.get("injected_context_top_n", 4) or 4)
    max_chars = int(st.session_state.get("injected_context_max_chars", 6000) or 6000)
    snippets = history_store.retrieve_relevant_context(
        thread_id=thread_id,
        query=question,
        top_n=max(1, min(8, top_n)),
        max_chars=max(1200, max_chars),
    )
    st.session_state["last_injected_context_used"] = snippets
    if not snippets:
        return None
    lines = [
        "User-provided file context snippets (retrieved by relevance).",
        "Use only if relevant to the current question. Cite the source file/page when used.",
    ]
    for i, item in enumerate(snippets, 1):
        source_file = str(item.get("source_file", "unknown"))
        page = item.get("page")
        page_text = f", page {page}" if page else ""
        score = item.get("score")
        score_text = f", score={score}" if score is not None else ""
        text = str(item.get("text", "")).strip()
        lines.append(f"[{i}] source={source_file}{page_text}{score_text}")
        lines.append(text)
    return "\n".join(lines)


def _extract_tool_usage(logs: list) -> tuple[list[str], dict]:
    sequence: list[str] = []
    counts: dict[str, int] = {}
    for event in logs or []:
        if not isinstance(event, dict):
            continue
        for msg in event.get("messages", []) or []:
            if not isinstance(msg, ToolMessage):
                continue
            name = str(getattr(msg, "name", "") or "tool").strip() or "tool"
            sequence.append(name)
            counts[name] = counts.get(name, 0) + 1
    return sequence, counts


def _json_safe_analysis_value(value: Any) -> Any:
    if isinstance(value, BaseMessage):
        return message_to_dict(value)
    if isinstance(value, dict):
        return {str(k): _json_safe_analysis_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_analysis_value(item) for item in value]
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return str(value)


def _serialize_analysis_logs(logs: list) -> list:
    if not isinstance(logs, list):
        return []
    return [_json_safe_analysis_value(event) for event in logs if isinstance(event, dict)]


def inject_selected_files_to_context(file_names: list[str], max_pages: int = 120) -> dict:
    thread_id = str(st.session_state.get("thread_id") or "debug")
    vlm_model_name = str(st.session_state.get("cfg_model") or "deepseek-v4-pro")
    result = file_context_service.build_context_items_for_files(
        thread_id=thread_id,
        file_names=file_names,
        max_pages=max_pages,
        vlm_model_name=vlm_model_name,
        vlm_timeout_s=90,
    )
    items = result.get("items", []) or []
    merge_stats = {"inserted": 0, "updated": 0, "total": 0}
    if items:
        merge_stats = history_store.upsert_injected_context_items(thread_id, items)
    result["merge_stats"] = merge_stats
    result["injected_files"] = history_store.injected_file_overview(thread_id)
    return result


def clear_injected_context() -> None:
    thread_id = str(st.session_state.get("thread_id") or "debug")
    history_store.clear_injected_context(thread_id)


def _chunk_to_text(chunk) -> str:
    content = getattr(chunk, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if item.get("type") == "text" and item.get("text"):
                    parts.append(str(item.get("text")))
                elif item.get("text"):
                    parts.append(str(item.get("text")))
        return "".join(parts)
    if content is None:
        return ""
    return str(content)


def _message_fingerprint(msg: BaseMessage) -> str:
    """Build a stable dedupe key for streamed message objects."""
    mid = getattr(msg, "id", None)
    if mid:
        return f"id:{mid}"

    msg_type = getattr(msg, "type", msg.__class__.__name__)
    msg_name = getattr(msg, "name", "")
    tool_call_id = getattr(msg, "tool_call_id", "")
    text = _chunk_to_text(msg).strip()
    return f"{msg_type}|{msg_name}|{tool_call_id}|{text}"


def _iter_message_lists(payload):
    """Yield all message lists from top-level or nested stream payload shapes."""
    if isinstance(payload, dict):
        msgs = payload.get("messages")
        if isinstance(msgs, list):
            yield msgs
        for value in payload.values():
            yield from _iter_message_lists(value)
    elif isinstance(payload, (list, tuple)):
        for item in payload:
            yield from _iter_message_lists(item)


def _collect_new_messages(payload, seen_fingerprints: set[str]) -> list[BaseMessage]:
    """Collect unseen LangChain messages from an arbitrary stream payload."""
    delta: list[BaseMessage] = []
    for msg_list in _iter_message_lists(payload):
        if not isinstance(msg_list, list):
            continue
        for msg in msg_list:
            if not isinstance(msg, BaseMessage):
                continue
            fp = _message_fingerprint(msg)
            if fp in seen_fingerprints:
                continue
            seen_fingerprints.add(fp)
            delta.append(msg)
    return delta


def _messages_not_seen_before_run(messages, initial_fingerprints: set[str]) -> list[BaseMessage]:
    """Return state messages that were created during the current run only."""
    if not isinstance(messages, list):
        return []
    initial = set(initial_fingerprints or set())
    delta: list[BaseMessage] = []
    for msg in messages:
        if not isinstance(msg, BaseMessage):
            continue
        if _message_fingerprint(msg) in initial:
            continue
        delta.append(msg)
    return delta


def _is_transfer_message(text: str) -> bool:
    if not isinstance(text, str):
        return False
    t = text.strip().lower()
    if not t:
        return False
    patterns = [
        r"^successfully transferred to\b",
        r"^transferred to\b",
        r"^successfully handed off to\b",
        r"^handed off to\b",
        r"^routing to\b",
    ]
    return any(re.match(p, t) for p in patterns)


def _extract_meaningful_ai_text(messages, preferred_agents: Optional[list[str]] = None) -> Optional[str]:
    if not isinstance(messages, list):
        return None
    preferred = {
        str(name or "").strip().lower()
        for name in (preferred_agents or [])
        if str(name or "").strip()
    }
    fallback = None
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            msg_name = (getattr(msg, "name", None) or "").lower()
            if "(streaming)" in msg_name:
                continue
            text = _chunk_to_text(msg).strip()
            text = re.sub(
                r"^\s*\*{0,2}\s*(Data_Searcher|Code_Assistant)\s*\(streaming\)\s*\*{0,2}\s*:?\s*\n?",
                "",
                text,
                flags=re.IGNORECASE | re.MULTILINE,
            ).strip()
            if text and not _is_transfer_message(text):
                if preferred and msg_name in preferred:
                    return text
                if fallback is None:
                    fallback = text
    return fallback


def _get_state_messages(conversation, config):
    try:
        snapshot = conversation.get_state(config=config)
        values = getattr(snapshot, "values", None)
        if isinstance(values, dict):
            messages = values.get("messages", [])
            if isinstance(messages, list):
                return messages
    except Exception:
        pass
    return []


def _start_stream(conversation, state, config, stream_mode, subgraphs=True):
    # Compatibility: some wrappers may not accept subgraphs keyword.
    try:
        return conversation.stream(state, config=config, stream_mode=stream_mode, subgraphs=subgraphs)
    except TypeError:
        return conversation.stream(state, config=config, stream_mode=stream_mode)


def _iter_events(conversation, state, config):
    """Yield normalized (mode, payload, namespace) from graph stream."""
    yielded = False
    try:
        events = _start_stream(
            conversation,
            state,
            config,
            stream_mode=["messages", "values", "updates", "custom"],
            subgraphs=True,
        )
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
                    if isinstance(left, tuple) and isinstance(right, tuple) and len(right) == 2:
                        if hasattr(right[0], "content") and isinstance(right[1], dict):
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
        events = _start_stream(conversation, state, config, stream_mode="values", subgraphs=True)
        for item in events:
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], tuple):
                yield "values", item[1], item[0]
            else:
                yield "values", item, ()


def _append_analysis_history_snapshot() -> None:
    prev_logs = st.session_state.get("analysis_logs", [])
    if not prev_logs:
        return
    history = st.session_state.setdefault("analysis_history", [])
    history.append(
        {
            "question": st.session_state.get("last_question", ""),
            "logs": list(prev_logs),
            "created_at": int(time.time()),
        }
    )
    st.session_state["analysis_history"] = history[-12:]


def _append_chat_if_new(role: str, content: str) -> None:
    item = (role, content)
    history = st.session_state.setdefault("chat_history", [])
    if not history or history[-1] != item:
        history.append(item)


def _emit_run_event(run_id: str, kind: str, payload: dict) -> None:
    with _RUN_REGISTRY_LOCK:
        control = _RUN_REGISTRY.get(run_id)
        if not control:
            return
        seq = int(control.get("next_seq", 1))
        control["next_seq"] = seq + 1
        control["events"].append(
            {
                "seq": seq,
                "run_id": run_id,
                "thread_id": str(control.get("thread_id")),
                "ts": time.time(),
                "kind": kind,
                "payload": payload or {},
            }
        )


def _get_run_control(run_id: str) -> Optional[dict[str, Any]]:
    with _RUN_REGISTRY_LOCK:
        return _RUN_REGISTRY.get(str(run_id))


def request_stop_active_run(thread_id: Optional[str] = None, detach_session: bool = False) -> bool:
    _ensure_runtime_state_defaults()
    tid = str(thread_id or st.session_state.get("active_run_thread_id") or st.session_state.get("thread_id") or "")
    if not tid:
        return False
    requested = False
    requested_run_id = None
    with _RUN_REGISTRY_LOCK:
        run_id = _THREAD_ACTIVE_RUN.get(tid)
        if run_id:
            control = _RUN_REGISTRY.get(run_id)
            if control and str(control.get("state")) == "running":
                control["stop_requested"] = True
                requested = True
                requested_run_id = run_id
                if detach_session:
                    _THREAD_ACTIVE_RUN.pop(tid, None)
    if requested:
        st.session_state["cancel_requested"] = True
        st.session_state["stopping"] = True
    if detach_session:
        # Release this browser session immediately; the worker will finish in the background.
        active_tid = str(st.session_state.get("active_run_thread_id") or "")
        if (not active_tid) or active_tid == tid:
            st.session_state["is_running"] = False
            st.session_state["stopping"] = False
            st.session_state["cancel_requested"] = False
            st.session_state["active_run_id"] = None
            st.session_state["active_run_thread_id"] = None
            st.session_state["run_ended_ts"] = time.time()
            st.session_state["detached_run_id"] = requested_run_id
    return requested


def _build_run_payload(user_question: str, run_thread_id: str, user_id: str) -> tuple[dict, dict]:
    context_system_msg = _build_injected_context_system_message(user_question, run_thread_id)
    state_messages = []
    if context_system_msg:
        state_messages.append({"role": "system", "content": context_system_msg})
    gee_pipeline_mode = str(st.session_state.get("gee_pipeline_mode") or "default").strip() or "default"
    gee_project_id = str(st.session_state.get("gee_effective_project_id") or st.session_state.get("gee_project_id") or "").strip()
    gee_profile_source = str(st.session_state.get("gee_profile_source") or "default").strip() or "default"
    if gee_project_id:
        state_messages.append(
            {
                "role": "system",
                "content": (
                    "Runtime GEE pipeline context:\n"
                    f"- gee_pipeline_mode: {gee_pipeline_mode}\n"
                    f"- gee_profile_source: {gee_profile_source}\n"
                    f"- gee_project_id: {gee_project_id}\n"
                    f"- For Earth Engine Python scripts, initialize with ee.Initialize(project={gee_project_id!r}).\n"
                    "- If this project is denied by IAM/API/quota, report it as GEE configuration work."
                ),
            }
        )
    state_messages.append({"role": "user", "content": user_question})
    state = {"messages": state_messages}
    metadata = build_runtime_metadata(
        assistant_id=ASSISTANT_ID,
        user_id=user_id,
        thread_id=run_thread_id,
        gee_pipeline_mode=gee_pipeline_mode,
        gee_project_id=gee_project_id,
        gee_profile_source=gee_profile_source,
    )
    config = {
        "configurable": metadata,
        "metadata": metadata,
        "recursion_limit": app_state.RECURSION_LIMIT,
    }
    return state, config


def start_user_run(user_question: str) -> dict[str, Any]:
    _ensure_runtime_state_defaults()
    question = str(user_question or "").strip()
    if not question:
        return {"started": False, "reason": "empty_question"}
    conversation = st.session_state.get("conversation")
    if conversation is None:
        return {"started": False, "reason": "conversation_uninitialized"}

    run_thread_id = str(st.session_state.get("thread_id") or "debug")
    run_user_id = str(st.session_state.get("user_id") or "guest")
    active_session_run_id = str(st.session_state.get("active_run_id") or "").strip()
    active_session_thread_id = str(st.session_state.get("active_run_thread_id") or "").strip()
    if (
        bool(st.session_state.get("is_running", False))
        and active_session_run_id
        and active_session_thread_id == run_thread_id
    ):
        return {
            "started": False,
            "reason": "thread_run_in_progress",
            "run_id": active_session_run_id,
        }
    quota_rejection = _workspace_quota_rejection(run_thread_id, run_user_id)
    if quota_rejection:
        return quota_rejection
    with _RUN_REGISTRY_LOCK:
        active_run_id = _THREAD_ACTIVE_RUN.get(run_thread_id)
        if active_run_id:
            active_control = _RUN_REGISTRY.get(active_run_id)
            if active_control and str(active_control.get("state")) == "running":
                return {"started": False, "reason": "thread_run_in_progress", "run_id": active_run_id}
        limit_rejection = _run_limit_rejection_locked(run_user_id)
        if limit_rejection:
            return limit_rejection

    run_id = uuid.uuid4().hex
    state, config = _build_run_payload(question, run_thread_id, run_user_id)
    now = time.time()
    control = {
        "run_id": run_id,
        "thread_id": run_thread_id,
        "user_id": run_user_id,
        "question": question,
        "conversation": conversation,
        "state_payload": state,
        "config": config,
        "events": [],
        "next_seq": 1,
        "state": "running",
        "stop_requested": False,
        "start_ts": now,
        "heartbeat_ts": now,
        "end_ts": None,
        "ui_lang": str(st.session_state.get("ui_lang", "EN") or "EN"),
        "analysis_logs": [],
    }
    try:
        profile = history_store.get_user_gee_profile(run_user_id)
        if profile.get("source") == "user" and profile.get("oauth_connected"):
            control["gee_encrypted_refresh_token"] = str(profile.get("encrypted_refresh_token") or "")
            control["gee_token_scopes"] = str(profile.get("token_scopes") or "")
    except Exception:
        pass
    with _RUN_REGISTRY_LOCK:
        active_run_id = _THREAD_ACTIVE_RUN.get(run_thread_id)
        if active_run_id:
            active_control = _RUN_REGISTRY.get(active_run_id)
            if active_control and str(active_control.get("state")) == "running":
                return {"started": False, "reason": "thread_run_in_progress", "run_id": active_run_id}
        quota_rejection = _workspace_quota_rejection(run_thread_id, run_user_id)
        if quota_rejection:
            return quota_rejection
        limit_rejection = _run_limit_rejection_locked(run_user_id)
        if limit_rejection:
            return limit_rejection
        _RUN_REGISTRY[run_id] = control
        _THREAD_ACTIVE_RUN[run_thread_id] = run_id

    _append_analysis_history_snapshot()
    st.session_state["analysis_logs"] = []
    st.session_state["last_question"] = question
    st.session_state["is_running"] = True
    st.session_state["stopping"] = False
    st.session_state["cancel_requested"] = False
    st.session_state["active_run_id"] = run_id
    st.session_state["active_run_thread_id"] = run_thread_id
    st.session_state["run_last_rendered_event_seq"] = 0
    st.session_state["run_started_ts"] = now
    st.session_state["run_heartbeat_ts"] = now
    st.session_state["run_ended_ts"] = None
    st.session_state["run_last_terminal_kind"] = ""

    try:
        history_store.append_chat_record(run_thread_id, role="user", content=question, kind="text")
        history_store.touch_thread_activity(control["user_id"], run_thread_id, last_question=question)
    except Exception:
        pass

    worker = threading.Thread(target=_worker_run_main, args=(run_id,), daemon=True, name=f"ntl-run-{run_id[:8]}")
    control["worker_thread"] = worker
    worker.start()
    return {"started": True, "run_id": run_id}


def poll_user_run(run_id: str, after_seq: int = 0) -> tuple[list[dict], str]:
    with _RUN_REGISTRY_LOCK:
        control = _RUN_REGISTRY.get(str(run_id))
        if not control:
            return [], "missing"
        events = [e for e in control.get("events", []) if int(e.get("seq", 0)) > int(after_seq)]
        return events, str(control.get("state") or "unknown")


def _worker_run_main(run_id: str) -> None:
    control = _get_run_control(run_id)
    if not control:
        return

    run_thread_id = str(control.get("thread_id") or "debug")
    question = str(control.get("question") or "")
    conversation = control.get("conversation")
    state = control.get("state_payload") or {}
    config = control.get("config") or {}
    ui_lang = str(control.get("ui_lang") or "EN")

    final_answer: Optional[str] = None
    last_event = None
    interrupted_reason = None
    run_exception = None
    start_ts = float(control.get("start_ts") or time.time())

    _emit_run_event(run_id, "status", {"state": "running"})
    token = current_thread_id.set(run_thread_id)
    gee_token = current_gee_project_id.set(str(config.get("configurable", {}).get("gee_project_id") or ""))
    gee_refresh_token = current_gee_encrypted_refresh_token.set(str(control.get("gee_encrypted_refresh_token") or ""))
    gee_scopes_token = current_gee_token_scopes.set(str(control.get("gee_token_scopes") or ""))
    try:
        initial_message_fingerprints: set[str] = set()
        for existing_msg in _get_state_messages(conversation, config):
            if isinstance(existing_msg, BaseMessage):
                initial_message_fingerprints.add(_message_fingerprint(existing_msg))
        seen_message_fingerprints: set[str] = set(initial_message_fingerprints)

        for mode, payload, namespace in _iter_events(conversation, state, config):
            now = time.time()
            with _RUN_REGISTRY_LOCK:
                inner = _RUN_REGISTRY.get(run_id)
                if not inner:
                    interrupted_reason = "run_lost"
                    break
                inner["heartbeat_ts"] = now
                if bool(inner.get("stop_requested")):
                    interrupted_reason = "user_cancel"
                    break

            if mode == "messages":
                continue

            if mode == "custom":
                if isinstance(payload, dict):
                    if payload.get("event_type") == "kb_progress":
                        log_event = {"kb_progress": [payload]}
                    else:
                        log_event = {"custom": [payload]}
                    with _RUN_REGISTRY_LOCK:
                        inner = _RUN_REGISTRY.get(run_id)
                        if inner is not None:
                            inner["analysis_logs"].append(log_event)
                    _emit_run_event(run_id, "reasoning_custom", {"log_event": log_event})
                continue

            delta_messages = _collect_new_messages(payload, seen_message_fingerprints)
            if not delta_messages:
                continue
            delta_event = {"messages": delta_messages}
            with _RUN_REGISTRY_LOCK:
                inner = _RUN_REGISTRY.get(run_id)
                if inner is not None:
                    inner["analysis_logs"].append(delta_event)
            _emit_run_event(run_id, "reasoning_delta", {"log_event": delta_event})
            last_event = delta_event
            candidate = _extract_meaningful_ai_text(delta_messages)
            if candidate:
                final_answer = candidate
    except Exception as err:  # noqa: BLE001
        run_exception = err
    finally:
        current_thread_id.reset(token)
        current_gee_project_id.reset(gee_token)
        current_gee_encrypted_refresh_token.reset(gee_refresh_token)
        current_gee_token_scopes.reset(gee_scopes_token)

    preferred_agents = ["Engineer"]
    if not final_answer and last_event and "messages" in last_event:
        final_answer = _extract_meaningful_ai_text(last_event.get("messages"), preferred_agents=preferred_agents)

    state_messages = _messages_not_seen_before_run(
        _get_state_messages(conversation, config),
        initial_message_fingerprints if "initial_message_fingerprints" in locals() else set(),
    )
    preferred_from_state = _extract_meaningful_ai_text(state_messages, preferred_agents=preferred_agents)
    if preferred_from_state:
        final_answer = preferred_from_state
    elif not final_answer:
        final_answer = _extract_meaningful_ai_text(state_messages)

    elapsed_s = max(0.0, time.time() - start_ts)
    assistant_text = None
    status = "partial"

    if final_answer:
        assistant_text = _attach_time_cost_footer(final_answer, elapsed_s, ui_lang=ui_lang)
        _emit_run_event(run_id, "final_answer", {"text": assistant_text, "elapsed_s": elapsed_s})
        status = "success"
        if run_exception:
            err_text = _attach_time_cost_footer(_build_runtime_error_notice(run_exception, ui_lang=ui_lang), elapsed_s, ui_lang=ui_lang)
            _emit_run_event(run_id, "error", {"text": err_text, "elapsed_s": elapsed_s})
    elif run_exception:
        assistant_text = _attach_time_cost_footer(_build_runtime_error_notice(run_exception, ui_lang=ui_lang), elapsed_s, ui_lang=ui_lang)
        _emit_run_event(run_id, "error", {"text": assistant_text, "elapsed_s": elapsed_s})
        status = "error"
    elif interrupted_reason:
        assistant_text = _attach_time_cost_footer(
            f"Run interrupted ({interrupted_reason}). Please refine the task or retry.",
            elapsed_s,
            ui_lang=ui_lang,
        )
        _emit_run_event(run_id, "interrupted", {"text": assistant_text, "elapsed_s": elapsed_s})
        status = "interrupted"
    elif last_event and "messages" in last_event:
        assistant_text = _attach_time_cost_footer(
            "Run finished without a final answer from the main agent. Please retry.",
            elapsed_s,
            ui_lang=ui_lang,
        )
        _emit_run_event(run_id, "no_final", {"text": assistant_text, "elapsed_s": elapsed_s})
        status = "partial"

    try:
        if assistant_text:
            history_store.append_chat_record(run_thread_id, role="assistant", content=assistant_text, kind="text")
    except Exception:
        pass

    try:
        with _RUN_REGISTRY_LOCK:
            logs = list((_RUN_REGISTRY.get(run_id) or {}).get("analysis_logs", []))
        sequence, counts = _extract_tool_usage(logs)
        history_store.append_turn_summary(
            run_thread_id,
            {
                "user_id": str(control.get("user_id") or "guest"),
                "thread_id": run_thread_id,
                "question": question,
                "final_answer_excerpt": str(final_answer or "")[:300],
                "tool_sequence": sequence,
                "tool_calls_by_name": counts,
                "analysis_logs": _serialize_analysis_logs(logs),
                "status": status,
                "duration_s": round(elapsed_s, 3),
            },
        )
        history_store.touch_thread_activity(
            str(control.get("user_id") or "guest"),
            run_thread_id,
            last_question=question,
            last_answer_excerpt=str(final_answer or "")[:240],
        )

    except Exception:
        pass

    with _RUN_REGISTRY_LOCK:
        inner = _RUN_REGISTRY.get(run_id)
        if inner is not None:
            inner["state"] = status
            inner["end_ts"] = time.time()
    _emit_run_event(
        run_id,
        "done",
        {
            "status": status,
            "thread_id": run_thread_id,
            "elapsed_s": elapsed_s,
            "start_ts": start_ts,
        },
    )
    with _RUN_REGISTRY_LOCK:
        if _THREAD_ACTIVE_RUN.get(run_thread_id) == run_id:
            _THREAD_ACTIVE_RUN.pop(run_thread_id, None)


def consume_active_run_events() -> bool:
    _ensure_runtime_state_defaults()
    run_id = str(st.session_state.get("active_run_id") or "").strip()
    if not run_id:
        return False

    after_seq = int(st.session_state.get("run_last_rendered_event_seq", 0) or 0)
    events, state = poll_user_run(run_id, after_seq=after_seq)
    consumed = False
    for event in events:
        consumed = True
        seq = int(event.get("seq", 0))
        st.session_state["run_last_rendered_event_seq"] = seq
        kind = str(event.get("kind") or "")
        payload = event.get("payload") or {}
        if kind in {"reasoning_delta", "reasoning_custom"}:
            log_event = payload.get("log_event")
            if isinstance(log_event, dict):
                st.session_state.setdefault("analysis_logs", []).append(log_event)
            heartbeat = float(event.get("ts") or time.time())
            st.session_state["run_heartbeat_ts"] = heartbeat
            continue
        if kind in {"final_answer", "error", "interrupted", "no_final"}:
            text = str(payload.get("text") or "").strip()
            st.session_state["run_last_terminal_kind"] = kind
            if text:
                _append_chat_if_new("assistant", text)
            continue
        if kind == "done":
            run_thread = str(payload.get("thread_id") or st.session_state.get("active_run_thread_id") or "")
            if run_thread and run_thread == str(st.session_state.get("thread_id") or ""):
                _collect_recent_outputs(
                    seconds=120,
                    thread_id=run_thread,
                    since_ts=payload.get("start_ts") or st.session_state.get("run_started_ts"),
                )
                # Trigger one full-page rerun so map/output preview can pick up new artifacts.
                st.session_state["ui_force_refresh_once"] = True
            st.session_state["run_ended_ts"] = time.time()
            st.session_state["is_running"] = False
            st.session_state["stopping"] = False
            st.session_state["cancel_requested"] = False
            st.session_state["active_run_id"] = None
            st.session_state["active_run_thread_id"] = None

    if not events and state in {"success", "error", "interrupted", "partial", "missing"}:
        run_thread = str(st.session_state.get("active_run_thread_id") or st.session_state.get("thread_id") or "")
        if run_thread and run_thread == str(st.session_state.get("thread_id") or ""):
            _collect_recent_outputs(
                seconds=120,
                thread_id=run_thread,
                since_ts=st.session_state.get("run_started_ts"),
            )
        st.session_state["is_running"] = False
        st.session_state["stopping"] = False
        st.session_state["cancel_requested"] = False
        st.session_state["active_run_id"] = None
        st.session_state["active_run_thread_id"] = None
        st.session_state["ui_force_refresh_once"] = True
    return consumed


def handle_userinput(
    user_question,
    reasoning_placeholder,
    chat_container,
    reasoning_graph_placeholder=None,
    reasoning_graph_show_sub_steps=False,
):
    # Backward-compatible wrapper. New flow is start + consume.
    _ = (reasoning_placeholder, chat_container, reasoning_graph_placeholder, reasoning_graph_show_sub_steps)
    result = start_user_run(user_question)
    consume_active_run_events()
    return result
