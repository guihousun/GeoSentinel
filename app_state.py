import streamlit as st

import history_store
from model_config import MODEL_OPTIONS
from storage_manager import storage_manager

RECURSION_LIMIT = 101
CHAT_CONTAINER_HEIGHT = 600
ANALYSIS_CONTAINER_HEIGHT = 600
LLM_REQUEST_TIMEOUT_S = 120


def _new_thread_id_for_user(user_id: str) -> str:
    return history_store.generate_thread_id(user_id)


def _load_chat_history_for_thread(thread_id: str):
    records = history_store.load_chat_records(thread_id, limit=400)
    out = []
    for row in records:
        role = str(row.get("role", "")).strip()
        content = str(row.get("content", "") or "")
        if not role or not content.strip():
            continue
        out.append((role, content))
    return out


def set_active_thread(thread_id: str):
    st.session_state.thread_id = str(thread_id)
    st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
    st.session_state.chat_history = _load_chat_history_for_thread(st.session_state.thread_id)
    st.session_state.analysis_logs = []
    st.session_state.analysis_history = []
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
        st.session_state["user_id"] = ""
        st.session_state["user_name"] = ""
        st.session_state["initialized"] = False
        if "user_api_key" in st.session_state:
            del st.session_state["user_api_key"]
        if not str(st.session_state.get("thread_id", "") or "").strip():
            st.session_state.thread_id = "pending-auth"
        st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
        st.session_state["chat_history"] = []
    else:
        st.session_state["user_id"] = authed_user_id
        st.session_state["user_name"] = authed_username or history_store.user_display_name(authed_user_id)
        current_thread_id = str(st.session_state.get("thread_id", "") or "").strip()
        user_threads = history_store.list_user_threads(authed_user_id, limit=100)
        user_thread_ids = [str(row.get("thread_id", "") or "") for row in user_threads]
        if (not current_thread_id) or current_thread_id not in user_thread_ids:
            if user_thread_ids:
                st.session_state.thread_id = user_thread_ids[0]
            else:
                st.session_state.thread_id = _new_thread_id_for_user(authed_user_id)
                history_store.bind_thread_to_user(authed_user_id, st.session_state.thread_id)
            st.session_state["chat_history"] = []

        st.session_state.user_workspace = storage_manager.get_workspace(st.session_state.thread_id)
        history_store.ensure_user_profile(
            st.session_state["user_id"], st.session_state.get("user_name", "")
        )
        history_store.bind_thread_to_user(st.session_state["user_id"], st.session_state.thread_id)
        if not st.session_state.get("chat_history"):
            st.session_state.chat_history = _load_chat_history_for_thread(st.session_state.thread_id)


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
    st.session_state["user_id"] = st.session_state["auth_user_id"]
    st.session_state["user_name"] = st.session_state["auth_username"]
    st.session_state["initialized"] = False
    st.session_state["pending_activate_request"] = None
    st.session_state["pending_model_change"] = None
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
    st.session_state["user_id"] = ""
    st.session_state["user_name"] = ""
    st.session_state["initialized"] = False
    st.session_state["chat_history"] = []
    st.session_state["analysis_logs"] = []
    st.session_state["analysis_history"] = []
    st.session_state["last_question"] = ""
    st.session_state["pending_activate_request"] = None
    st.session_state["pending_model_change"] = None
    st.session_state["thread_id"] = "pending-auth"
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
