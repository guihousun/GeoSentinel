from __future__ import annotations

from pathlib import Path

from agents.NTL_Analyst import system_prompt_analyst
from agents.NTL_Data_Searcher import hierarchical_system_prompt_data_searcher
from agents.NTL_Event_Tracker import system_prompt_event_tracker
from graph_factory_v2 import _full_system_prompt, architecture_descriptor


ROOT = Path(__file__).resolve().parents[1]


def _prompt_text(message: object) -> str:
    return str(getattr(message, "content", message))


def test_all_agent_prompts_use_geosentinel_product_identity() -> None:
    prompts = {
        "engineer": _full_system_prompt(),
        "data_searcher": _prompt_text(hierarchical_system_prompt_data_searcher),
        "analyst": _prompt_text(system_prompt_analyst),
        "event_tracker": _prompt_text(system_prompt_event_tracker),
    }

    for role, prompt in prompts.items():
        assert "GeoSentinel" in prompt, role
        assert "地缘环境智能计算平台" in prompt, role
        assert "NTL-GPT" not in prompt, role

    assert "Supervisor Agent of the NTL-GPT multi-agent system" not in prompts["engineer"]
    assert "four-role NTL-GPT system" not in prompts["data_searcher"]


def test_user_facing_agent_names_are_five_chinese_characters_or_fewer() -> None:
    expected_names = {
        "engineer": "地缘分析师",
        "data_searcher": "数据助手",
        "analyst": "分析助手",
        "event_tracker": "事件助手",
    }
    prompts = {
        "engineer": _full_system_prompt(),
        "data_searcher": _prompt_text(hierarchical_system_prompt_data_searcher),
        "analyst": _prompt_text(system_prompt_analyst),
        "event_tracker": _prompt_text(system_prompt_event_tracker),
    }

    for role, name in expected_names.items():
        assert len(name) <= 5
        assert f"user-facing name: `{name}`" in prompts[role]

    index = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
    runtime = (ROOT / "web" / "web-runtime.js").read_text(encoding="utf-8")
    for name in expected_names.values():
        assert name in index or name in runtime

    for old_name in ("数据检索助手", "知识库助手", "知识助手", "代码助手", "地缘环境数据检索助手", "地缘环境知识助手", "地理空间代码复核助手"):
        assert old_name not in index
        assert old_name not in runtime


def test_platform_scope_is_broader_than_nighttime_light() -> None:
    engineer = _full_system_prompt()
    searcher = _prompt_text(hierarchical_system_prompt_data_searcher)
    analyst = _prompt_text(system_prompt_analyst)

    assert "not a mandatory route" in engineer
    assert "conflict" in engineer and "wildfire" in engineer and "flood" in engineer
    assert "product/date/AOI" in engineer
    assert "nighttime-light" in analyst


def test_internal_compatibility_identifiers_are_preserved() -> None:
    engineer = _full_system_prompt()
    searcher = _prompt_text(hierarchical_system_prompt_data_searcher)
    analyst = _prompt_text(system_prompt_analyst)

    assert "NTL_Engineer" in engineer
    assert "NTL_Data_Searcher" in searcher
    assert "NTL_Analyst" in analyst
    descriptor = architecture_descriptor("full")
    assert descriptor["contract_schema_version"] == "ntl.contract.v1"
    assert descriptor["role_names"] == ["NTL_Engineer", "NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"]


def test_graph_wrapper_reinforces_geosentinel_identity() -> None:
    factory_source = (ROOT / "graph_factory_v2.py").read_text(encoding="utf-8-sig")
    kb_source = (ROOT / "tools" / "NTL_Knowledge_Base_Searcher.py").read_text(encoding="utf-8-sig")

    assert "地缘环境智能计算总协调器" in factory_source
    assert "Nighttime light is one evidence layer" in factory_source
    assert "nighttime light analysis supervisor" not in factory_source
    assert "GeoSentinel's geoenvironmental knowledge retrieval engine" in kb_source


def test_neighboring_event_domains_remain_explicitly_supported() -> None:
    engineer = _full_system_prompt()
    for domain in ("earthquake", "wildfire", "flood", "climate", "population", "logistics"):
        assert domain in engineer
