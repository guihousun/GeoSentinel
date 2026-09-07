from __future__ import annotations

from pathlib import Path

from agents.role_specs import ROLE_DISPLAY_NAMES, ROLE_SPECS, SPECIALIST_ROLE_NAMES
from graph_factory_v2 import _full_system_prompt, architecture_descriptor, filesystem_runtime_descriptor
from tools import analyst_tools, data_searcher_tools, engineer_tools, event_tracker_tools


ROOT = Path(__file__).resolve().parents[1]


def test_four_roles_have_short_specialized_display_names() -> None:
    assert ROLE_DISPLAY_NAMES == {
        "NTL_Engineer": "地缘分析师",
        "NTL_Data_Searcher": "数据助手",
        "NTL_Analyst": "分析助手",
        "NTL_Event_Tracker": "事件助手",
    }
    assert all(len(name) <= 5 for name in ROLE_DISPLAY_NAMES.values())
    assert SPECIALIST_ROLE_NAMES == (
        "NTL_Data_Searcher",
        "NTL_Analyst",
        "NTL_Event_Tracker",
    )


def test_role_scoped_skills_exist_and_do_not_fall_back_to_global_skill_root() -> None:
    for role, spec in ROLE_SPECS.items():
        assert spec.skill_sources
        assert "/skills/" not in spec.skill_sources, role
        for virtual_source in spec.skill_sources:
            relative = virtual_source.removeprefix("/skills/").strip("/")
            skill_root = ROOT / ".ntl-gpt" / "skills" / relative
            assert skill_root.is_dir(), (role, virtual_source)
            if relative in {"common", "engineer", "data_searcher", "analyst", "event_tracker"}:
                assert any(skill_root.glob("*/SKILL.md")), (role, virtual_source)


def test_tools_are_partitioned_by_geoenvironmental_responsibility() -> None:
    assert "execute_geospatial_script_tool" in engineer_tools.export_names
    assert "GEE_raster_download_tool" in data_searcher_tools.export_names
    assert "NTL_raster_statistics_tool" in analyst_tools.export_names
    assert "conflict_ntl_fetch_isw_events_tool" in event_tracker_tools.export_names
    assert not any("SDGSAT" in name.upper() for group in (
        engineer_tools,
        data_searcher_tools,
        analyst_tools,
        event_tracker_tools,
    ) for name in group.export_names)


def test_architecture_descriptor_and_permissions_match_local_workspace_model() -> None:
    descriptor = architecture_descriptor("full")
    assert descriptor["delegation_enabled"] is True
    assert descriptor["role_names"] == list(ROLE_SPECS)

    runtime = filesystem_runtime_descriptor(
        ROLE_SPECS["NTL_Engineer"].skill_sources,
        memory_access=True,
    )
    assert runtime["routes"]["/inputs/"] == "ContextFilesystemBackend(thread.inputs)"
    assert runtime["routes"]["/outputs/"] == "ContextFilesystemBackend(thread.outputs)"
    assert runtime["routes"]["/shared/"] == "FilesystemBackend(read-only by permissions)"
    assert any(rule["mode"] == "deny" and "/**" in rule["paths"] for rule in runtime["permissions"])


def test_correction_is_explicit_user_opt_in() -> None:
    prompt = _full_system_prompt()
    assert "correction operations are opt-in" in prompt
    assert "standard product values by default" in prompt
    assert "model-generated plan alone cannot authorize correction" in prompt


def test_active_prompts_skills_and_ui_do_not_expose_disabled_sensor_tools() -> None:
    paths = [
        ROOT / "graph_factory_v2.py",
        ROOT / "tools" / "__init__.py",
        ROOT / "app_ui.py",
        ROOT / "web" / "index.html",
        ROOT / "web" / "web-runtime.js",
    ]
    for namespace in ("common", "engineer", "data_searcher", "analyst", "event_tracker"):
        paths.extend((ROOT / ".ntl-gpt" / "skills" / namespace).glob("*/SKILL.md"))

    for path in paths:
        assert "SDGSAT" not in path.read_text(encoding="utf-8-sig"), path
