from __future__ import annotations

import time

from langchain_core.messages import AIMessage, AIMessageChunk

import web_runtime
from web_runtime import (
    WebRunManager,
    _canonical_stream_agent,
    _serialize_message,
    _stream_message_parts,
)


def test_stream_metadata_resolves_all_four_public_roles() -> None:
    assert _canonical_stream_agent("NTL_Engineer") == "NTL_Engineer"
    assert _canonical_stream_agent({"checkpoint_ns": "NTL_Data_Searcher:task-1"}) == "NTL_Data_Searcher"
    assert _canonical_stream_agent(("NTL_Analyst:task-2",)) == "NTL_Analyst"
    assert _canonical_stream_agent("event_tracker") == "NTL_Event_Tracker"


def test_messages_mode_payload_preserves_chunk_and_metadata() -> None:
    chunk = AIMessageChunk(content="正在分析", id="stream-message-1")
    message, metadata = _stream_message_parts(
        (chunk, {"langgraph_checkpoint_ns": "NTL_Analyst:task-2"})
    )

    assert message is chunk
    assert metadata["langgraph_checkpoint_ns"] == "NTL_Analyst:task-2"


def test_reasoning_snapshot_uses_canonical_agent_and_message_id() -> None:
    message = AIMessage(content="事件时间线已核验", name="NTL_Event_Tracker", id="message-7")

    payload = _serialize_message(message)

    assert payload["role"] == "assistant"
    assert payload["agent"] == "NTL_Event_Tracker"
    assert payload["message_id"] == "message-7"
    assert payload["text"] == "事件时间线已核验"


def test_worker_emits_incremental_text_for_all_four_roles(monkeypatch) -> None:
    manager = WebRunManager()

    class _Snapshot:
        values = {"messages": [AIMessage(content="最终结论", name="NTL_Engineer")]}

    class _Conversation:
        def stream(self, *_args, **_kwargs):
            yield (), "messages", (
                AIMessageChunk(content="总览", id="engineer-1"),
                {"langgraph_node": "model"},
            )
            yield ("NTL_Data_Searcher:task-1",), "messages", (
                AIMessageChunk(content="数据", id="data-1"),
                {"langgraph_node": "model"},
            )
            yield ("task-2",), "messages", (
                AIMessageChunk(content="分析", id="analyst-1"),
                {"checkpoint_ns": "NTL_Analyst:task-2"},
            )
            yield ("task-3",), "messages", (
                AIMessageChunk(content="事件", id="event-1", name="NTL_Event_Tracker"),
                {"langgraph_node": "model"},
            )

        def get_state(self, **_kwargs):
            return _Snapshot()

    now = time.time()
    manager._runs["run-stream"] = {
        "run_id": "run-stream",
        "thread_id": "thread-stream",
        "user_id": "user-stream",
        "question": "测试四角色流式输出",
        "conversation": _Conversation(),
        "state_payload": {"messages": [{"role": "user", "content": "测试"}]},
        "config": {"configurable": {"thread_id": "thread-stream"}},
        "events": [],
        "next_seq": 1,
        "state": "running",
        "stop_requested": False,
        "start_ts": now,
        "heartbeat_ts": now,
        "end_ts": None,
        "worker_thread": None,
    }
    manager._thread_active_run["thread-stream"] = "run-stream"
    monkeypatch.setattr(web_runtime.history_store, "append_chat_record", lambda *_a, **_k: None)
    monkeypatch.setattr(web_runtime.history_store, "append_turn_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(web_runtime.history_store, "touch_thread_activity", lambda *_a, **_k: None)
    monkeypatch.setattr(manager, "list_artifacts", lambda *_a, **_k: [])

    manager._worker("run-stream")

    streamed = [
        event["payload"]
        for event in manager._runs["run-stream"]["events"]
        if event["kind"] == "agent_stream_delta"
    ]
    assert [item["agent"] for item in streamed] == [
        "NTL_Engineer",
        "NTL_Data_Searcher",
        "NTL_Analyst",
        "NTL_Event_Tracker",
    ]
    assert [item["text"] for item in streamed] == ["总览", "数据", "分析", "事件"]
