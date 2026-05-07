import streamlit as st
from pathlib import Path
from langchain_core.messages import messages_from_dict

import history_store
from model_config import MODEL_OPTIONS
from storage_manager import storage_manager

RECURSION_LIMIT = 101
CHAT_CONTAINER_HEIGHT = 600
ANALYSIS_CONTAINER_HEIGHT = 600
LLM_REQUEST_TIMEOUT_S = 120


def _new_thread_id_for_user(user_id: str) -> str:
    return history_store.generate_thread_id(user_id)


def _pending_thread_id() -> str:
    return _new_thread_id_for_user("pending")


def _load_chat_history_for_thread(thread_id: str):
    records = history_store.load_chat_records(thread_id, limit=400)
    workspace = storage_manager.get_workspace(thread_id)
    out = []
    for row in records:
        role = str(row.get("role", "")).strip()
        content = str(row.get("content", "") or "")
        if not role or not content.strip():
            continue
        if role == "assistant_img":
            try:
                content_path = Path(content)
                if content_path.is_absolute():
                    content = content_path.resolve().relative_to(workspace.resolve()).as_posix()
            except Exception:
                pass
        out.append((role, content))
    return out


def _rehydrate_reasoning_message(value):
    if not isinstance(value, dict):
        return value
    if "type" not in value or "data" not in value:
        return value
    try:
        return messages_from_dict([value])[0]
    except Exception:
        return value


def _rehydrate_analysis_logs(logs):
    if not isinstance(logs, list):
        return []
    out = []
    for event in logs:
        if not isinstance(event, dict):
            continue
        restored = dict(event)
        messages = restored.get("messages")
        if isinstance(messages, list):
            restored["messages"] = [_rehydrate_reasoning_message(item) for item in messages]
        out.append(restored)
    return out


def _load_analysis_history_for_thread(thread_id: str, limit: int = 12):
    rows = history_store.load_turn_summaries(thread_id, limit=limit)
    history = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        logs = _rehydrate_analysis_logs(row.get("analysis_logs") or row.get("reasoning_logs") or [])
        if not logs:
            continue
        history.append(
            {
                "question": str(row.get("question") or ""),
                "logs": logs,
                "created_at": int(row.get("ts") or row.get("created_at") or 0),
            }
        )
    return history[-limit:] if limit > 0 else history


def sync_gee_profile_state(user_id: str | None = None) -> dict:
    uid = str(user_id or st.session_state.get("user_id", "") or "").strip()
    if not uid:
        profile = {
            "mode": "default",
            "gee_project_id": "",
            "effective_project_id": "",
            "source": "default",
            "status": "unvalidated",
            "last_error": "",
            "default_project_id": "",
            "user_project_configured": False,
        }
    else:
        profile = history_store.get_user_gee_profile(uid)
    st.session_state["gee_pipeline_mode"] = str(profile.get("mode") or "default")
    st.session_state["gee_project_id"] = str(profile.get("gee_project_id") or "")
    st.session_state["gee_effective_project_id"] = str(profile.get("effective_project_id") or "")
    st.session_state["gee_profile_source"] = str(profile.get("source") or "default")
    st.session_state["gee_profile_status"] = str(profile.get("status") or "unvalidated")
    st.session_state["gee_profile_last_error"] = str(profile.get("last_error") or "")
    st.session_state["gee_default_project_id"] = str(profile.get("default_project_id") or "")
    st.session_state["gee_user_project_configured"] = bool(profile.get("user_project_configured"))
    st.session_state["gee_oauth_connected"] = bool(profile.get("oauth_connected"))
    st.session_state["gee_google_email"] = str(profile.get("google_email") or "")
    return profile


def set_active_thread(thread_id: str):
    st.session_state.thread_id = str(thread_id)
    st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
    st.session_state.chat_history = _load_chat_history_for_thread(st.session_state.thread_id)
    st.session_state.analysis_logs = []
    st.session_state.analysis_history = _load_analysis_history_for_thread(st.session_state.thread_id)
    st.session_state.last_question = ""
    st.session_state["runtime_recovered_notice"] = None


def init_app():
    # Cleanup legacy timeout controls removed from UI (manual interrupt only).
    for legacy_key in ("run_max_duration_s", "run_stall_timeout_s", "run_decision_pending"):
        if legacy_key in st.session_state:
            del st.session_state[legacy_key]

    st.session_state.setdefault("cfg_model", MODEL_OPTIONS[0])
    st.session_state.setdefault("initialized", False)
    st.session_state.setdefault("chat_history", [])
    st.session_state.setdefault("run_counter", 0)
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("auth_user_id", "")
    st.session_state.setdefault("auth_username", "")

    st.session_state.setdefault("injected_context_top_n", 4)
    st.session_state.setdefault("injected_context_max_chars", 6000)

    authed_user_id = str(st.session_state.get("auth_user_id", "") or "").strip()
    authed_username = str(st.session_state.get("auth_username", "") or "").strip()
    if not st.session_state.get("authenticated") or not authed_user_id:
        st.session_state["authenticated"] = False
        st.session_state["auth_user_id"] = ""
        st.session_state["auth_username"] = ""
        st.session_state["auth_is_admin"] = False
        st.session_state["user_id"] = ""
        st.session_state["user_name"] = ""
        sync_gee_profile_state("")
        st.session_state["initialized"] = False
        if not str(st.session_state.get("thread_id", "") or "").strip():
            st.session_state.thread_id = _pending_thread_id()
        st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
        st.session_state["chat_history"] = []
    else:
        st.session_state["user_id"] = authed_user_id
        st.session_state["user_name"] = authed_username or history_store.user_display_name(authed_user_id)
        st.session_state["auth_is_admin"] = history_store.is_admin_user(authed_user_id)
        current_thread_id = str(st.session_state.get("thread_id", "") or "").strip()
        if (not current_thread_id) or (not history_store.thread_belongs_to_user(authed_user_id, current_thread_id)):
            known_threads = history_store.list_user_threads(authed_user_id, limit=100)
            if known_threads:
                st.session_state.thread_id = str(known_threads[0].get("thread_id") or "")
            else:
                st.session_state.thread_id = _new_thread_id_for_user(authed_user_id)
                history_store.bind_thread_to_user(authed_user_id, st.session_state.thread_id)
            st.session_state["chat_history"] = []

        st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
        history_store.ensure_user_profile(
            st.session_state["user_id"], st.session_state.get("user_name", "")
        )
        history_store.bind_thread_to_user(st.session_state["user_id"], st.session_state.thread_id)
        sync_gee_profile_state(st.session_state["user_id"])
        if not st.session_state.get("chat_history"):
            st.session_state.chat_history = _load_chat_history_for_thread(st.session_state.thread_id)
        if not st.session_state.get("analysis_history"):
            st.session_state.analysis_history = _load_analysis_history_for_thread(st.session_state.thread_id)

    st.session_state.setdefault("analysis_logs", [])
    st.session_state.setdefault("analysis_history", [])
    st.session_state.setdefault("last_question", "")
    st.session_state.setdefault("is_running", False)
    st.session_state.setdefault("stopping", False)
    st.session_state.setdefault("cancel_requested", False)
    st.session_state.setdefault("run_started_ts", None)
    st.session_state.setdefault("run_heartbeat_ts", None)
    st.session_state.setdefault("run_ended_ts", None)
    st.session_state.setdefault("runtime_recovered_notice", None)
    st.session_state.setdefault("active_run_id", None)
    st.session_state.setdefault("active_run_thread_id", None)
    st.session_state.setdefault("run_last_rendered_event_seq", 0)
    st.session_state.setdefault("pending_model_change", None)
    st.session_state.setdefault("pending_activate_request", None)
    st.session_state.setdefault("ui_force_refresh_once", False)
    st.session_state.setdefault("pending_map_focus_layer", None)


def apply_authenticated_user(user_id: str, username: str) -> None:
    st.session_state["authenticated"] = True
    st.session_state["auth_user_id"] = str(user_id or "").strip()
    st.session_state["auth_username"] = str(username or "").strip()
    st.session_state["auth_is_admin"] = history_store.is_admin_user(st.session_state["auth_user_id"])
    st.session_state["user_id"] = st.session_state["auth_user_id"]
    st.session_state["user_name"] = st.session_state["auth_username"]
    st.session_state["initialized"] = False
    st.session_state["pending_activate_request"] = None
    st.session_state["pending_model_change"] = None
    sync_gee_profile_state(st.session_state["auth_user_id"])
    if "user_api_key" in st.session_state:
        del st.session_state["user_api_key"]

    known_threads = history_store.list_user_threads(st.session_state["auth_user_id"], limit=100)
    if known_threads:
        set_active_thread(str(known_threads[0].get("thread_id") or ""))
    else:
        new_thread_id = _new_thread_id_for_user(st.session_state["auth_user_id"])
        set_active_thread(new_thread_id)
        history_store.bind_thread_to_user(st.session_state["auth_user_id"], new_thread_id)


def clear_authenticated_user() -> None:
    st.session_state["authenticated"] = False
    st.session_state["auth_user_id"] = ""
    st.session_state["auth_username"] = ""
    st.session_state["auth_is_admin"] = False
    st.session_state["user_id"] = ""
    st.session_state["user_name"] = ""
    st.session_state["initialized"] = False
    st.session_state["chat_history"] = []
    st.session_state["analysis_logs"] = []
    st.session_state["analysis_history"] = []
    st.session_state["last_question"] = ""
    st.session_state["pending_activate_request"] = None
    st.session_state["pending_model_change"] = None
    sync_gee_profile_state("")
    st.session_state["thread_id"] = _pending_thread_id()
    st.session_state["user_workspace"] = storage_manager.get_workspace(st.session_state["thread_id"])
    if "user_api_key" in st.session_state:
        del st.session_state["user_api_key"]


def reset_chat():
    st.session_state.chat_history = []
    st.session_state.run_counter = 0
    st.session_state.cancel_requested = False
    st.session_state.stopping = False
    st.session_state.run_started_ts = None
    st.session_state.run_heartbeat_ts = None
    st.session_state.run_ended_ts = None
    st.session_state.runtime_recovered_notice = None
    st.session_state.active_run_id = None
    st.session_state.active_run_thread_id = None
    st.session_state.run_last_rendered_event_seq = 0
