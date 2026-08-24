from __future__ import annotations

import json

import pytest

from tools import GEE_specialist_toolkit as toolkit


def _metadata(
    dataset_id: str,
    *,
    asset_type: str = "ImageCollection",
    bands: list[str] | None = None,
    scale: int = 10,
) -> str:
    return json.dumps(
        {
            "status": "ok",
            "dataset_id": dataset_id,
            "asset_type": asset_type,
            "band_names": bands or ["value"],
            "collection_size": 5 if asset_type == "ImageCollection" else None,
            "temporal_resolution": "daily" if asset_type == "ImageCollection" else "static",
            "temporal_coverage": {"start": "2017-01-01", "end": "2030-01-01"},
            "curated_scale_m": scale,
        }
    )


def test_general_explicit_dataset_gets_live_plan_without_ntl_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        toolkit,
        "gee_dataset_metadata",
        lambda dataset_id, check_temporal=True: _metadata(
            dataset_id,
            bands=["B2", "B3", "B4", "B8"],
        ),
    )

    payload = json.loads(
        toolkit.gee_request_plan(
            query="Download Sentinel-2 RGB",
            dataset_id="COPERNICUS/S2_SR_HARMONIZED",
            bands=["B2", "B3", "B4"],
            start_date="2026-01-01",
            end_date="2026-01-05",
            bbox=[120.0, 30.0, 120.02, 30.02],
            temporal_resolution="daily",
            scale_m=10,
        )
    )

    assert payload["dataset"]["domain"] == "general_gee"
    assert payload["dataset"]["selected"]["dataset_id"] == "COPERNICUS/S2_SR_HARMONIZED"
    assert payload["dataset"]["validation"]["status"] == "verified"
    assert payload["execution"]["mode"] == "direct_local"


def test_explicit_dataset_only_probes_the_requested_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, bool]] = []

    def fake_metadata(dataset_id: str, check_temporal: bool = True) -> str:
        calls.append((dataset_id, check_temporal))
        return _metadata(dataset_id, bands=["B4", "B3", "B2"])

    monkeypatch.setattr(toolkit, "gee_dataset_metadata", fake_metadata)

    json.loads(
        toolkit.gee_request_plan(
            query="Download Sentinel-2 RGB",
            dataset_id="COPERNICUS/S2_SR_HARMONIZED",
            bands=["B4", "B3", "B2"],
            start_date="2026-01-01",
            end_date="2026-01-05",
            bbox=[120.0, 30.0, 120.02, 30.02],
            scale_m=10,
        )
    )

    assert calls == [("COPERNICUS/S2_SR_HARMONIZED", False)]


def test_invalid_explicit_dataset_never_becomes_default_ntl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        toolkit,
        "gee_dataset_metadata",
        lambda dataset_id, check_temporal=True: json.dumps(
            {"status": "error", "dataset_id": dataset_id, "error": "Asset not found"}
        ),
    )

    payload = json.loads(
        toolkit.gee_request_plan(
            query="Download my custom collection",
            dataset_id="users/example/missing",
            bands=["value"],
            start_date="2026-01-01",
            end_date="2026-01-02",
            bbox=[120.0, 30.0, 120.1, 30.1],
            temporal_resolution="daily",
            scale_m=30,
        )
    )

    assert payload["dataset"]["selected"]["dataset_id"] == "users/example/missing"
    assert payload["dataset"]["domain"] == "general_gee"
    assert payload["execution"]["mode"] == "needs_input"


def test_legacy_router_delegates_non_ntl_dataset_to_unified_planner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        toolkit,
        "gee_dataset_metadata",
        lambda dataset_id, check_temporal=True: _metadata(dataset_id, bands=["B4", "B8"]),
    )

    payload = json.loads(
        toolkit.gee_dataset_router(
            query="Sentinel-2 vegetation composite",
            temporal_resolution="daily",
            start_date="2026-01-01",
            end_date="2026-01-05",
            analysis_intent="composite_export",
            prefer_dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        )
    )

    assert payload["schema"] == "ntl.gee.plan.v1"
    assert payload["dataset"]["selected"]["dataset_id"] == "COPERNICUS/S2_SR_HARMONIZED"
    assert payload["dataset"]["domain"] == "general_gee"


def test_legacy_ntl_router_keeps_stable_vnp46a2_contract() -> None:
    payload = json.loads(
        toolkit.gee_dataset_router(
            query="Download VNP46A2 nighttime lights",
            temporal_resolution="daily",
            start_date="2026-01-01",
            end_date="2026-01-02",
            prefer_dataset="VNP46A2",
        )
    )

    assert payload["selected_dataset"]["dataset_id"] == "NASA/VIIRS/002/VNP46A2"
    assert payload["selected_dataset"]["band"] == "Gap_Filled_DNB_BRDF_Corrected_NTL"
    assert payload["recommended_execution_mode"] == "direct_download"


def test_catalog_first_general_request_validates_top_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        toolkit,
        "gee_catalog_discovery",
        lambda query, max_results=8, temporal_resolution=None: json.dumps(
            {
                "status": "ok",
                "official_candidates": [
                    {
                        "dataset_id": "COPERNICUS/S2_SR_HARMONIZED",
                        "title": "Sentinel-2 Surface Reflectance Harmonized",
                        "match_score": 100,
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(
        toolkit,
        "gee_dataset_metadata",
        lambda dataset_id, check_temporal=True: _metadata(dataset_id, bands=["B4", "B8"]),
    )

    payload = json.loads(
        toolkit.gee_request_plan(
            query="Sentinel-2 NDVI",
            bands=["B4", "B8"],
            start_date="2026-01-01",
            end_date="2026-01-10",
            bbox=[120.0, 30.0, 120.02, 30.02],
            temporal_resolution="daily",
            scale_m=10,
        )
    )

    assert payload["dataset"]["selected"]["dataset_id"] == "COPERNICUS/S2_SR_HARMONIZED"
    assert "live_metadata_checked" in payload["dataset"]["selection_reasons"]
