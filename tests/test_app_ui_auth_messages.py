from unittest.mock import patch

import app_ui
from langchain_core.messages import AIMessage, ToolMessage


def test_known_auth_error_uses_chinese_translation() -> None:
    with patch.object(app_ui, "_tr", side_effect=lambda zh, en: zh):
        message = app_ui._localized_auth_error(
            ValueError("Password must be at least 8 characters.")
        )

    assert message == "密码至少需要 8 个字符。"


def test_known_auth_error_preserves_english_translation() -> None:
    with patch.object(app_ui, "_tr", side_effect=lambda zh, en: en):
        message = app_ui._localized_auth_error(
            ValueError("That username is already registered.")
        )

    assert message == "That username is already registered."


def test_unknown_auth_error_is_not_rewritten() -> None:
    with patch.object(app_ui, "_tr", side_effect=lambda zh, en: zh):
        message = app_ui._localized_auth_error(RuntimeError("Unexpected auth failure."))

    assert message == "Unexpected auth failure."


def test_pending_thread_selection_is_applied_before_widget_render() -> None:
    state = {}
    with patch.object(app_ui.st, "session_state", state):
        app_ui._queue_sidebar_thread_selection("thread-new")
        selected = app_ui._apply_pending_sidebar_thread_selection(
            ["thread-old", "thread-new"]
        )

    assert selected == "thread-new"
    assert state[app_ui._SIDEBAR_THREAD_SELECTOR_KEY] == "thread-new"
    assert app_ui._PENDING_SIDEBAR_THREAD_SELECTOR_KEY not in state


def test_invalid_pending_thread_selection_does_not_override_widget() -> None:
    state = {
        app_ui._SIDEBAR_THREAD_SELECTOR_KEY: "thread-old",
        app_ui._PENDING_SIDEBAR_THREAD_SELECTOR_KEY: "thread-deleted",
    }
    with patch.object(app_ui.st, "session_state", state):
        selected = app_ui._apply_pending_sidebar_thread_selection(["thread-old"])

    assert selected is None
    assert state[app_ui._SIDEBAR_THREAD_SELECTOR_KEY] == "thread-old"
    assert app_ui._PENDING_SIDEBAR_THREAD_SELECTOR_KEY not in state


def test_code_assistant_is_not_requested_by_default() -> None:
    lifecycle = app_ui._build_subagent_lifecycle_state([], is_running=False)

    assert lifecycle["Code_Assistant"]["status"] == "not_requested"


def test_general_purpose_is_displayed_as_ntl_engineer() -> None:
    assert app_ui._normalize_subagent_name("general-purpose") == "NTL_Engineer"

    grouped = app_ui._build_reasoning_sections(
        [{"messages": [AIMessage(content="Working", name="general-purpose")]}]
    )

    assert grouped[0]["agent"] == "NTL_Engineer"


def test_knowledge_base_is_not_a_subagent_card() -> None:
    assert "Knowledge_Base_Searcher" not in app_ui._SUBAGENT_CARD_ORDER
    assert app_ui._SUBAGENT_CARD_ORDER == [
        "Data_Searcher",
        "Code_Assistant",
        "NTL_Engineer",
    ]


def test_internal_human_message_is_labeled_as_subagent_handoff() -> None:
    with patch.object(app_ui, "_tr", side_effect=lambda zh, en: en):
        label = app_ui._human_message_display_label(
            "You are the Data_Searcher agent. Retrieve the requested boundary."
        )

    assert label == "Task Handoff → Data Searcher"


def test_end_user_human_message_is_labeled_as_user() -> None:
    with patch.object(app_ui, "_tr", side_effect=lambda zh, en: en):
        label = app_ui._human_message_display_label("Download Shanghai NTL data.")

    assert label == "User"


def test_code_assistant_review_stage_tracks_execution_and_validation() -> None:
    logs = [
        {
            "messages": [
                AIMessage(
                    content="",
                    name="Code_Assistant",
                    tool_calls=[
                        {
                            "name": "execute_geospatial_script_tool",
                            "args": {"script_name": "task.py"},
                            "id": "call-1",
                            "type": "tool_call",
                        }
                    ],
                )
            ]
        }
    ]
    assert app_ui._code_assistant_review_stage(logs, is_running=True) == "executing"

    logs.append(
        {
            "messages": [
                ToolMessage(
                    content='{"status":"success","contract_output_audit":{"pass":true}}',
                    name="execute_geospatial_script_tool",
                    tool_call_id="call-1",
                )
            ]
        }
    )
    assert app_ui._code_assistant_review_stage(logs, is_running=True) == "validating"
    assert app_ui._code_assistant_review_stage(logs, is_running=False) == "done"


def test_code_assistant_review_stage_tracks_preflight_message() -> None:
    logs = [
        {
            "messages": [
                AIMessage(
                    content="Running mandatory static preflight before execution.",
                    name="Code_Assistant",
                )
            ]
        }
    ]

    assert app_ui._code_assistant_review_stage(logs, is_running=True) == "preflight"
