from __future__ import annotations

from pathlib import Path

import pytest

from ntl_toolkit.schemas import OutputArtifact, ToolResult
from tools import GEE_generic_download as generic


def test_generic_download_uses_thread_inputs_and_multiband_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured = {}
    monkeypatch.setattr(generic, "_resolve_thread_id", lambda _config: "thread-1")
    monkeypatch.setattr(
        generic.storage_manager,
        "resolve_workspace_relative_path",
        lambda *args, **kwargs: tmp_path / "sentinel.tif",
    )
    monkeypatch.setattr(
        generic.storage_manager,
        "thread_quota_snapshot",
        lambda *args, **kwargs: {"allowed": True, "limit_bytes": 1_000_000_000},
    )

    def fake_download(request):
        captured["request"] = request
        return ToolResult.succeeded(
            tool="download_gee_raster",
            summary="ok",
            outputs=[OutputArtifact(path=request.output, media_type="image/tiff")],
        )

    monkeypatch.setattr(generic, "download_gee_raster", fake_download)

    result = generic.gee_raster_download(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B2", "B3", "B4"],
        bbox=[120.0, 30.0, 120.01, 30.01],
        out_name="sentinel.tif",
        start_date="2026-01-01",
        end_date="2026-01-05",
        asset_type="ImageCollection",
        reducer="median",
        scale=10,
        processing_preset="sentinel2_cloud_score_plus",
    )

    request = captured["request"]
    assert result["status"] == "succeeded"
    assert request.bands == ["B2", "B3", "B4"]
    assert request.processing_preset == "sentinel2_cloud_score_plus"
    assert request.output == str(tmp_path / "sentinel.tif")
    assert result["metrics"]["thread_id"] == "thread-1"


def test_generic_download_stops_before_execution_when_quota_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(generic, "_resolve_thread_id", lambda _config: "thread-1")
    monkeypatch.setattr(
        generic.storage_manager,
        "resolve_workspace_relative_path",
        lambda *args, **kwargs: tmp_path / "large.tif",
    )
    monkeypatch.setattr(
        generic.storage_manager,
        "thread_quota_snapshot",
        lambda *args, **kwargs: {"allowed": False, "limit_bytes": 1},
    )
    monkeypatch.setattr(
        generic,
        "download_gee_raster",
        lambda _request: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    result = generic.gee_raster_download(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B2"],
        bbox=[73.0, 18.0, 135.0, 54.0],
        out_name="large.tif",
        start_date="2026-01-01",
        end_date="2026-01-05",
        scale=10,
    )

    assert result["status"] == "failed"
    assert result["error"]["code"] == "THREAD_WORKSPACE_QUOTA_EXCEEDED"
