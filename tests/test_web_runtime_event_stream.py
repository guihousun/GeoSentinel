from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SOURCE = ROOT / "web" / "web-runtime.js"
INDEX_SOURCE = ROOT / "web" / "index.html"
RUNTIME_STYLE_SOURCE = ROOT / "web" / "web-runtime.css"
THEME_STYLE_SOURCE = ROOT / "web" / "theme-preview.css"


def test_event_source_run_id_is_kept_in_runtime_state() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert 'eventSourceRunId: ""' in source
    assert "state.eventSource && state.eventSourceRunId === runId" in source
    assert "state.eventSourceRunId = runId" in source
    assert "source.dataset" not in source


def test_event_stream_resumes_from_sequence_and_deduplicates_events() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert 'eventSeq: 0' in source
    assert "events?after_seq=${encodeURIComponent(state.eventSeq)}" in source
    assert "seq <= state.eventSeq" in source
    assert "scheduleRunStreamReconnect(runId)" in source
    assert "2 ** Math.min(state.eventReconnectAttempts - 1, 4)" in source


def test_event_stream_recovers_terminal_or_missing_run_from_history() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert "async function recoverRunStream(runId)" in source
    assert 'error.code === "run_not_found"' in source
    assert 'loadThread(state.threadId, { reconnect: false })' in source
    assert 'kind === "stream_gap"' in source


def test_closing_event_stream_clears_source_and_run_id() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")
    close_block = source.split("function closeEventStream(", 1)[1].split("function handleRunEvent", 1)[0]

    assert "state.eventSource?.close()" in close_block
    assert 'state.eventSource = null' in close_block
    assert 'state.eventSourceRunId = ""' in close_block


def test_index_uses_event_stream_cache_version() -> None:
    index = INDEX_SOURCE.read_text(encoding="utf-8")

    assert re.search(r'web-runtime\.js\?v=[a-z0-9-]+', index)


def test_monitor_case_entry_requires_user_confirmation_before_send() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert "CASE_REQUEST_COOLDOWN_MS = 10 * 1000" in source
    assert "state.recentCaseRequests.get(eventKey)" in source
    assert "该事件的研判问题已经在输入框中" in source
    assert "const prompt = monitorResearchPrompt(item)" in source
    assert "input.value = prompt" in source
    assert "请检查或修改后点击发送" in source
    assert "输入框中已有未发送内容，系统没有覆盖" in source
    assert 'button.textContent = "填入研判问题"' in source
    assert 'sendQuestion(monitorResearchPrompt(item)' not in source


def test_monitor_draft_contains_context_and_keeps_server_dedup_key_on_submit() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert "发生或观测时间：${eventTime}" in source
    assert "地点：${location}" in source
    assert "坐标：${coordinates}" in source
    assert "背景摘要：${summary}" in source
    assert "请先帮助我理解事件" in source
    assert "pending && value.includes(pending.title)" in source
    assert "`monitor:${pending.eventKey}`" in source
    assert "request_key: requestKey" in source


def test_manual_stop_has_immediate_and_streamed_stopping_feedback() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert 'kind === "stopping"' in source
    assert 'stop.textContent = "停止中"' in source
    assert '/cancel`, { method: "POST" }' in source


def test_four_role_text_is_rendered_incrementally_without_html_injection() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")
    index = INDEX_SOURCE.read_text(encoding="utf-8")

    assert 'agentStreams: new Map()' in source
    assert 'kind === "agent_stream_delta"' in source
    assert "updateAgentStream(payload.agent, payload.text" in source
    assert "snapshot && entry.hasDelta" in source
    assert "entry.content.textContent = entry.text" in source
    assert 'id="agent-stream-list"' in index
    assert 'aria-label="智能体实时输出"' in index


def test_agent_stream_completion_preserves_role_specific_status() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert "function finishAgentStreams(status" in source
    assert "setAgentState(agentName, label, false)" in source
    assert "finishAgentStreams(payload.status" in source


def test_agent_stream_details_are_collapsed_until_the_user_opens_them() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")
    index = INDEX_SOURCE.read_text(encoding="utf-8")

    assert 'id="task-stream-toggle"' in index
    assert 'aria-expanded="false"' in index
    assert 'id="task-stream-content" hidden' in index
    assert "function setTaskStreamExpanded(expanded)" in source
    assert "content.hidden = !next" in source
    assert "setTaskStreamExpanded(false)" in source
    assert "setTaskStreamExpanded(true)" not in source


def test_task_progress_is_anchored_below_the_latest_user_question() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")
    index = INDEX_SOURCE.read_text(encoding="utf-8")

    assert 'id="task-stream-home" hidden' in index
    assert "function parkTaskStream()" in source
    assert "function positionTaskStreamAfterLatestQuestion()" in source
    assert 'const questions = $$(".message.user", conversation)' in source
    assert 'latestQuestion.insertAdjacentElement("afterend", taskStream)' in source
    assert 'if (targetId === "chat-messages") parkTaskStream();' in source
    assert "if (running) positionTaskStreamAfterLatestQuestion();" in source


def test_task_progress_has_one_subtle_scroller_and_no_blinking_caret() -> None:
    styles = RUNTIME_STYLE_SOURCE.read_text(encoding="utf-8")
    theme_styles = THEME_STYLE_SOURCE.read_text(encoding="utf-8")

    content_rule = re.search(r"\.task-stream-content\s*\{([^}]*)\}", styles, re.S)
    assert content_rule
    assert "overflow-y: auto" in content_rule.group(1)
    assert "scrollbar-color:" in content_rule.group(1)
    assert ".task-stream-content::-webkit-scrollbar-thumb" in styles

    for selector in (".agent-stream-list", ".agent-stream-text", ".task-stream #run-activity-list"):
        rule = re.search(rf"{re.escape(selector)}\s*\{{([^}}]*)\}}", styles, re.S)
        assert rule
        assert "overflow-y" not in rule.group(1)
        assert "max-height" not in rule.group(1)

    assert "agent-stream-caret" not in styles
    assert "agent-stream-caret" not in theme_styles


def test_cool_slate_is_locked_before_first_paint_and_theme_picker_is_hidden() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")
    index = INDEX_SOURCE.read_text(encoding="utf-8")
    theme_styles = THEME_STYLE_SOURCE.read_text(encoding="utf-8")

    assert '<html lang="zh-CN" data-preview-theme="cool-slate">' in index
    assert 'class="theme-preview-control" hidden aria-hidden="true"' in index
    assert 'const LOCKED_PREVIEW_THEME = "cool-slate"' in source
    assert "control.hidden = true" in source
    assert "applyPreviewTheme(LOCKED_PREVIEW_THEME);" in source
    assert "window.localStorage.getItem(PREVIEW_THEME_KEY)" not in source
    assert ".theme-preview-control[hidden] { display: none !important; }" in theme_styles


def test_generated_images_and_tables_are_rendered_with_safe_dom_nodes() -> None:
    source = RUNTIME_SOURCE.read_text(encoding="utf-8")

    assert "function renderArtifactPreviews(items, targetId)" in source
    assert 'image.loading = "lazy"' in source
    assert 'image.alt = `生成图片：' in source
    assert 'document.createElement("table")' in source
    assert 'cell.scope = "col"' in source
    assert "/artifacts/${previewPath}/preview" in source
    assert "renderArtifactPreviews(payload.artifacts, \"chat-messages\")" in source
    assert ".innerHTML" not in source
