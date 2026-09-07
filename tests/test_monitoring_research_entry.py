from __future__ import annotations

from monitoring.service import _questions, _research_entry_prompt


def _candidate(title: str = "Border flood alert") -> dict:
    return {
        "title": title,
        "location_name": "示例边境地区",
        "summary": "A public feed reports flooding near a cross-border transport corridor.",
        "source_name": "Public Feed",
        "source_url": "https://example.test/event/1",
        "published_at": 1756080000,
        "longitude": 98.7654,
        "latitude": 21.2345,
        "event_type": "flood",
        "severity": "medium",
    }


def test_monitor_questions_follow_understand_verify_then_direct_sequence() -> None:
    questions = _questions(
        _candidate(), ["event_validation", "cross_border_connectivity"]
    )

    assert len(questions) == 3
    assert questions[0].startswith("事件理解：")
    assert questions[1].startswith("补充核验：")
    assert questions[2].startswith("方向建议：")
    assert "跨境连接" in questions[1]


def test_monitor_entry_treats_external_case_as_unverified_context() -> None:
    prompt = _research_entry_prompt(
        _candidate("Wildfire logistics alert"),
        ["humanitarian_exposure", "infrastructure_risk"],
    )

    assert "外部线索" in prompt
    assert "不是已经证实的结论" in prompt
    assert "发生或观测时间：2025-08-25 00:00 UTC" in prompt
    assert "地点：示例边境地区" in prompt
    assert "坐标：21.2345, 98.7654" in prompt
    assert "事件类型：洪涝" in prompt
    assert "当前关注级别：持续跟踪" in prompt
    assert "背景摘要：A public feed reports flooding" in prompt
    assert "先帮助我理解事件" in prompt
    assert "事件助手和数据助手补充检索" in prompt
    assert "最后提供分析方向建议" in prompt


def test_monitor_entry_supplies_background_fallback_when_source_summary_is_empty() -> None:
    candidate = _candidate("Wildfire without description")
    candidate["summary"] = ""
    candidate["event_type"] = "wildfire"

    prompt = _research_entry_prompt(candidate, ["event_validation"])

    assert "背景摘要：Public Feed 将该线索归类为“野火”" in prompt
    assert "原始监测记录未提供详细事件摘要" in prompt
