from __future__ import annotations

from pathlib import Path

import pytest

from tools import _GROUPS, _EXPORTS
from tools import vnp46a2_official_h5_country_tool as module


@pytest.fixture()
def isolated_workspace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "thread"
    monkeypatch.setattr(module, "_thread_id", lambda config=None: "thread")
    monkeypatch.setattr(module.storage_manager, "get_workspace", lambda thread_id: workspace)
    return workspace


def test_official_vnp46a2_tool_is_registered_for_runtime_agents() -> None:
    name = "official_vnp46a2_h5_country_mosaic_tool"
    assert name in _EXPORTS
    assert name in _GROUPS["data_searcher_tools"]
    assert name in _GROUPS["Engineer_tools"]
    assert name in _GROUPS["specialized_tool_catalog"]


def test_country_h5_plan_is_workspace_scoped_and_audited(isolated_workspace: Path) -> None:
    result = module.run_official_vnp46a2_h5_country_mosaic(
        start_date="2026-02-13",
        end_date="2026-02-15",
        countries=["isr"],
        output_root="vnp46a2_test",
        execution_mode="plan",
    )

    assert result["status"] == "plan"
    assert result["countries"] == ["ISR"]
    assert result["output_root"] == str(isolated_workspace / "outputs" / "vnp46a2_test")
    assert [Path(command[1]).name for command in result["commands"]] == [
        "prepare_vnp46a2_osm_boundaries_2026.py",
        "download_vnp46a2_official_h5_osm_countries_2026.py",
        "mosaic_vnp46a2_official_h5_osm_countries_2026.py",
        "audit_vnp46a2_country_coverage.py",
    ]
    assert "downloaded_without_mosaic must be 0 before completion" in result["audit_requirements"]


def test_country_h5_run_requires_token_before_subprocess(
    isolated_workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(module, "_runtime_env", lambda: {})

    def unexpected_subprocess(*args: object, **kwargs: object) -> None:
        raise AssertionError("download subprocess must not run without an Earthdata token")

    monkeypatch.setattr(module.subprocess, "run", unexpected_subprocess)
    result = module.run_official_vnp46a2_h5_country_mosaic(
        start_date="2026-02-13",
        end_date="2026-02-13",
        countries=["ISR"],
        phase="download",
        execution_mode="run",
    )

    assert result["status"] == "needs_configuration"
    assert "EARTHDATA_TOKEN" in result["error"]


def test_country_h5_targets_must_match_requested_countries(isolated_workspace: Path) -> None:
    with pytest.raises(ValueError, match="must also be present in countries"):
        module.run_official_vnp46a2_h5_country_mosaic(
            start_date="2026-02-13",
            end_date="2026-02-13",
            countries=["ISR"],
            targets=["IRN:2026-02-13"],
        )


def test_country_h5_organize_plan_requires_an_audited_workspace_source(isolated_workspace: Path) -> None:
    source = isolated_workspace / "outputs" / "audited_run"
    source.mkdir(parents=True)
    result = module.run_official_vnp46a2_h5_country_mosaic(
        start_date="2026-02-13",
        end_date="2026-02-13",
        countries=["ISR"],
        output_root="vnp46a2_test",
        phase="organize",
        package_source_root="audited_run",
        execution_mode="plan",
    )

    command = result["commands"][0]
    assert Path(command[1]).name == "organize_vnp46a2_final_results.py"
    assert "--source-root" in command
    assert str(source) in command
    assert str(isolated_workspace / "outputs" / "vnp46a2_test" / "final_package") in command
