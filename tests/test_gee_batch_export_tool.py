from __future__ import annotations

from pathlib import Path

import pytest

from ntl_toolkit.schemas import OutputArtifact, ToolResult
from tools import GEE_batch_export as batch_tool


def test_batch_export_uses_thread_memory_and_returns_virtual_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured = {}
    manifest = tmp_path / "gee_exports" / "test.json"
    monkeypatch.setattr(batch_tool, "_resolve_thread_id", lambda _config: "thread-1")
    monkeypatch.setattr(
        batch_tool.storage_manager,
        "resolve_workspace_relative_path",
        lambda *args, **kwargs: manifest,
    )

    def fake_submit(request):
        captured["request"] = request
        return ToolResult.succeeded(
            tool="submit_gee_batch_export",
            summary="submitted",
            outputs=[OutputArtifact(path=request.manifest_path, media_type="application/json", role="manifest")],
            metrics={"task_id": "task-1", "state": "READY"},
        ).model_copy(update={"job_id": "task-1"})

    monkeypatch.setattr(batch_tool, "submit_gee_batch_export", fake_submit)

    result = batch_tool.gee_batch_export(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B4", "B3", "B2"],
        bbox=[120.0, 30.0, 120.5, 30.5],
        description="test",
        manifest_name="gee_exports/test.json",
        scale=10,
    )

    assert result["status"] == "succeeded"
    assert result["job_id"] == "task-1"
    assert result["outputs"][0]["path"] == "/memories/gee_exports/test.json"
    assert captured["request"].manifest_path == str(manifest)


def test_status_uses_returned_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    manifest = tmp_path / "gee_exports" / "test.json"
    monkeypatch.setattr(batch_tool, "_resolve_thread_id", lambda _config: "thread-1")
    monkeypatch.setattr(
        batch_tool.storage_manager,
        "resolve_workspace_relative_path",
        lambda *args, **kwargs: manifest,
    )
    monkeypatch.setattr(
        batch_tool,
        "inspect_gee_batch_export",
        lambda path, project=None: ToolResult.succeeded(
            tool="inspect_gee_batch_export",
            summary="running",
            outputs=[OutputArtifact(path=path, media_type="application/json", role="manifest")],
            metrics={"state": "RUNNING", "progress_percent": 50},
        ),
    )

    result = batch_tool.gee_export_status("/memories/gee_exports/test.json")

    assert result["status"] == "succeeded"
    assert result["metrics"]["state"] == "RUNNING"
    assert result["metrics"]["manifest"] == "/memories/gee_exports/test.json"
