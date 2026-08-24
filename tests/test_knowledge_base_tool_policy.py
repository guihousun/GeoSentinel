from __future__ import annotations

import json
from pathlib import Path

from tools.NTL_Knowledge_Base_Searcher import NTL_Knowledge_Base


def test_workflow_mode_requires_confirmed_skill_gap() -> None:
    payload = json.loads(
        NTL_Knowledge_Base.invoke(
            {
                "query": "Build an NTL workflow",
                "response_mode": "workflow",
                "skill_gap_confirmed": False,
            }
        )
    )

    assert payload["status"] == "skill_first_required"


def test_graph_registers_knowledge_base_as_tool_not_subagent() -> None:
    source = (Path(__file__).resolve().parents[1] / "graph_factory.py").read_text(encoding="utf-8")

    assert "knowledge_base_subagent" not in source
    assert "system_prompt_kb_searcher" not in source
    assert "tools=[*Engineer_tools, NTL_Knowledge_Base]" in source
    assert "subagents=[data_searcher_subagent, code_assistant_subagent]" in source
