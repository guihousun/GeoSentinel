from __future__ import annotations

from agents.NTL_Code_Assistant import Code_Assistant_system_prompt_text
from agents.NTL_Data_Searcher import system_prompt_data_searcher
from agents.NTL_Engineer import system_prompt_text
from tools import _GROUPS


def test_engineer_has_direct_script_execution_tools() -> None:
    engineer_tools = set(_GROUPS["Engineer_tools"])

    assert "execute_geospatial_script_tool" in engineer_tools
    assert "GeoCode_COT_Validation_tool" in engineer_tools


def test_engineer_has_read_only_geodata_inspection_tools() -> None:
    engineer_tools = set(_GROUPS["Engineer_tools"])

    assert "geodata_inspector_tool" in engineer_tools
    assert "geodata_quick_check_tool" in engineer_tools


def test_l2_routes_through_builtin_tools_without_custom_script() -> None:
    engineer_prompt = str(system_prompt_text.content)

    assert "L2 MUST NOT create an `ntl.script.contract.v2`" in engineer_prompt
    assert "NTL_download_tool" in engineer_prompt
    assert "NTL_raster_statistics" in engineer_prompt
    assert "BUILTIN_TOOL_GAP" in engineer_prompt
    assert "This section applies only after the task is classified L3" in engineer_prompt


def test_searcher_avoids_redundant_binary_download_checks() -> None:
    searcher_prompt = str(system_prompt_data_searcher.content)

    assert "retrieve and validate all requested artifacts in the same handoff" in searcher_prompt
    assert "do not read a binary GeoTIFF as text" in searcher_prompt
    assert "At most one workspace listing is enough" in searcher_prompt


def test_prompts_use_contract_v2_without_v1_compatibility() -> None:
    engineer_prompt = str(system_prompt_text.content)
    reviewer_prompt = str(Code_Assistant_system_prompt_text.content)

    assert "ntl.script.contract.v2" in engineer_prompt
    assert "ntl.script.contract.v2" in reviewer_prompt
    assert "ntl.script.contract.v1" not in engineer_prompt
    assert "ntl.script.contract.v1" not in reviewer_prompt


def test_code_assistant_requires_explicit_review_request() -> None:
    reviewer_prompt = str(Code_Assistant_system_prompt_text.content)

    assert "review_requested: true" in reviewer_prompt
    assert 'status: "review_not_requested"' in reviewer_prompt
    assert "cannot ask the user questions" in reviewer_prompt


def test_knowledge_base_is_a_skill_first_supplemental_tool() -> None:
    engineer_prompt = str(system_prompt_text.content)

    assert "NTL_Knowledge_Base` is a supplemental tool, not a subagent" in engineer_prompt
    assert "skill_gap_confirmed=true" in engineer_prompt
    assert "Do not use the knowledge tool merely because confidence is low" in engineer_prompt


def test_china_named_admin_boundary_routes_to_amap_data_searcher() -> None:
    engineer_prompt = str(system_prompt_text.content)
    searcher_prompt = str(system_prompt_data_searcher.content)

    assert "China administrative AOI rule" in engineer_prompt
    assert "delegate to Data_Searcher first" in engineer_prompt
    assert "get_administrative_division_data" in engineer_prompt
    assert "call `get_administrative_division_data` first" in searcher_prompt
    assert "Do not repeatedly substitute GAUL/geoBoundaries" in searcher_prompt
