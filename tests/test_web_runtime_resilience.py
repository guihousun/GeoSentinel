from __future__ import annotations

import time

from langchain_core.messages import AIMessage

import web_runtime
from runtime_limits import (
    ModelCallBudgetExceeded,
    begin_model_call_budget,
    consume_model_call,
    end_model_call_budget,
)
from web_runtime import WebRunManager, _classify_runtime_error


class _DeadWorker:
    def is_alive(self) -> bool:
        return False


def _control(*, state: str = "running", events: list[dict] | None = None) -> dict:
    return {
        "run_id": "run-resilience",
        "thread_id": "thread-resilience",
        "user_id": "user-resilience",
        "state": state,
        "start_ts": time.time() - 30,
        "heartbeat_ts": time.time() - 30,
        "end_ts": None,
        "worker_thread": _DeadWorker(),
        "events": list(events or []),
        "next_seq": max([int(item.get("seq", 0)) for item in events or []] + [0]) + 1,
    }


def test_runtime_errors_are_classified_without_exposing_raw_details() -> None:
    timeout = _classify_runtime_error(TimeoutError("provider timed out with internal detail"))
    limited = _classify_runtime_error(RuntimeError("HTTP 429 too many requests"))
    auth = _classify_runtime_error(RuntimeError("HTTP 401 unauthorized"))
    limit = _classify_runtime_error(ModelCallBudgetExceeded("model call limit reached: 50/50"))

    assert timeout["code"] == "upstream_timeout" and timeout["retryable"] is True
    assert limited["code"] == "upstream_rate_limited" and limited["retryable"] is True
    assert auth["code"] == "service_authentication" and auth["retryable"] is False
    assert limit["code"] == "model_call_limit" and limit["retryable"] is False
    assert "internal detail" not in timeout["message"]


def test_model_call_budget_stops_before_call_51() -> None:
    budget, token = begin_model_call_budget(50)
    try:
        for expected in range(1, 51):
            assert consume_model_call() == expected
        try:
            consume_model_call()
        except ModelCallBudgetExceeded:
            pass
        else:
            raise AssertionError("the 51st model call must be rejected")
    finally:
        end_model_call_budget(token)

    assert budget.count == 50


def test_dead_worker_is_closed_and_releases_thread_lock() -> None:
    manager = WebRunManager()
    manager._runs["run-resilience"] = _control()
    manager._thread_active_run["thread-resilience"] = "run-resilience"

    summary = manager.run_summary("run-resilience", "user-resilience")
    events, state = manager.poll("run-resilience")

    assert summary["state"] == "error"
    assert state == "error"
    assert "thread-resilience" not in manager._thread_active_run
    assert [event["kind"] for event in events][-2:] == ["error", "done"]
    assert events[-2]["payload"]["code"] == "worker_terminated"
    assert events[-2]["payload"]["retryable"] is True


def test_poll_reports_retained_event_gap_before_resuming() -> None:
    manager = WebRunManager()
    retained = [
        {"seq": 10, "kind": "reasoning_delta", "payload": {}, "run_id": "run-resilience"},
        {"seq": 11, "kind": "done", "payload": {"status": "success"}, "run_id": "run-resilience"},
    ]
    manager._runs["run-resilience"] = _control(state="success", events=retained)

    events, state = manager.poll("run-resilience", after_seq=3)

    assert state == "success"
    assert events[0]["kind"] == "stream_gap"
    assert events[0]["seq"] == 9
    assert events[0]["payload"] == {"requested_after_seq": 3, "first_retained_seq": 10}
    assert [event["seq"] for event in events[1:]] == [10, 11]


def test_recent_case_request_returns_original_run_once() -> None:
    manager = WebRunManager()
    manager._runs["run-resilience"] = _control()
    manager._recent_request_keys[("user-resilience", "thread-resilience", "monitor:case-1")] = (
        time.time(),
        "run-resilience",
    )

    duplicate = manager._recent_duplicate_locked(
        "user-resilience", "thread-resilience", "monitor:case-1"
    )
    variation = manager._recent_duplicate_locked(
        "user-resilience", "thread-resilience", "monitor:case-2"
    )

    assert duplicate == {
        "run_id": "run-resilience",
        "thread_id": "thread-resilience",
        "state": "running",
        "deduplicated": True,
    }
    assert variation is None


def test_repeated_cancel_is_idempotent_and_emits_one_stopping_event() -> None:
    manager = WebRunManager()
    control = _control()
    control["start_ts"] = time.time()
    manager._runs["run-resilience"] = control

    first = manager.cancel("run-resilience", "user-resilience")
    second = manager.cancel("run-resilience", "user-resilience")

    assert first["state"] == "stopping"
    assert second["state"] == "stopping"
    assert [event["kind"] for event in manager._runs["run-resilience"]["events"]].count(
        "stopping"
    ) == 1


def test_manual_stop_wins_over_intermediate_agent_text(monkeypatch) -> None:
    manager = WebRunManager()

    class _Snapshot:
        values = {
            "messages": [AIMessage(content="中间步骤文本", name="NTL_Engineer")]
        }

    class _Conversation:
        def stream(self, *_args, **_kwargs):
            yield {"messages": [AIMessage(content="中间步骤文本", name="NTL_Engineer")]}
            manager._runs["run-resilience"]["stop_requested"] = True
            yield {"messages": []}

        def get_state(self, **_kwargs):
            return _Snapshot()

    control = _control()
    control.update(
        {
            "question": "测试停止",
            "conversation": _Conversation(),
            "state_payload": {"messages": [{"role": "user", "content": "测试停止"}]},
            "config": {"configurable": {"thread_id": "thread-resilience"}},
            "stop_requested": False,
        }
    )
    manager._runs["run-resilience"] = control
    manager._thread_active_run["thread-resilience"] = "run-resilience"
    monkeypatch.setattr(web_runtime.history_store, "append_chat_record", lambda *_a, **_k: None)
    monkeypatch.setattr(web_runtime.history_store, "append_turn_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(web_runtime.history_store, "touch_thread_activity", lambda *_a, **_k: None)
    monkeypatch.setattr(manager, "list_artifacts", lambda *_a, **_k: [])

    manager._worker("run-resilience")

    events = manager._runs["run-resilience"]["events"]
    assert manager._runs["run-resilience"]["state"] == "interrupted"
    assert any(event["kind"] == "interrupted" for event in events)
    assert not any(event["kind"] == "final_answer" for event in events)
