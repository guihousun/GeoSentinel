from __future__ import annotations

from datetime import date

from ntl_toolkit.core.gee_planning import (
    DatasetCandidate,
    GeeRequest,
    PlannerPolicy,
    build_gee_plan,
    classify_request_domain,
)


def _candidate(
    dataset_id: str,
    *,
    title: str = "Dataset",
    scale_m: float = 500,
    temporal_resolution: str = "daily",
    live_checked: bool = True,
    asset_type: str = "ImageCollection",
    official: bool = True,
) -> DatasetCandidate:
    return DatasetCandidate(
        dataset_id=dataset_id,
        title=title,
        asset_type=asset_type,
        bands=["value"],
        default_bands=["value"],
        scale_m=scale_m,
        temporal_resolution=temporal_resolution,
        temporal_start=date(2015, 1, 1),
        temporal_end=date(2030, 1, 1),
        source="live_metadata",
        official=official,
        live_checked=live_checked,
        collection_size=5,
        score=10,
    )


def test_explicit_general_dataset_is_never_replaced_by_ntl_default() -> None:
    request = GeeRequest(
        query="Download Sentinel-2 NDVI",
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B4", "B8"],
        start_date="2026-01-01",
        end_date="2026-01-15",
        bbox=(120.0, 30.0, 120.1, 30.1),
        temporal_resolution="daily",
        scale_m=10,
    )
    candidates = [
        _candidate("NASA/VIIRS/002/VNP46A2", title="VNP46A2"),
        _candidate(
            "COPERNICUS/S2_SR_HARMONIZED",
            title="Sentinel-2 Surface Reflectance",
            scale_m=10,
        ),
    ]

    plan = build_gee_plan(request, candidates)

    assert plan.dataset.domain == "general_gee"
    assert plan.dataset.selected is not None
    assert plan.dataset.selected.dataset_id == "COPERNICUS/S2_SR_HARMONIZED"
    assert "explicit_dataset_id_preserved" in plan.dataset.selection_reasons


def test_unvalidated_explicit_dataset_requires_live_metadata() -> None:
    request = GeeRequest(
        query="Download an arbitrary GEE dataset",
        dataset_id="users/example/custom_collection",
        bands=["value"],
        start_date="2026-01-01",
        end_date="2026-01-02",
        bbox=(120.0, 30.0, 120.1, 30.1),
        temporal_resolution="daily",
        scale_m=30,
    )

    plan = build_gee_plan(request, [])

    assert plan.dataset.selected is not None
    assert plan.dataset.selected.dataset_id == request.dataset_id
    assert plan.execution.mode == "needs_input"
    assert plan.execution.reason_codes == ["LIVE_METADATA_REQUIRED_FOR_GENERAL_DATASET"]


def test_ntl_dataset_stays_on_specialized_lane() -> None:
    request = GeeRequest(
        query="Download VNP46A2 nighttime lights",
        dataset_id="NASA/VIIRS/002/VNP46A2",
        bands=["Gap_Filled_DNB_BRDF_Corrected_NTL"],
        start_date="2026-02-27",
        end_date="2026-02-27",
        bbox=(50.0, 25.0, 50.1, 25.1),
        temporal_resolution="daily",
        scale_m=500,
    )
    candidate = _candidate("NASA/VIIRS/002/VNP46A2", title="VNP46A2")

    plan = build_gee_plan(request, [candidate])

    assert classify_request_domain(request) == "ntl"
    assert plan.dataset.domain == "ntl"
    assert plan.execution.mode == "direct_local"


def test_official_hdf5_request_routes_to_earthdata() -> None:
    request = GeeRequest(
        query="Download official VNP46A2 HDF5",
        dataset_id="NASA/VIIRS/002/VNP46A2",
        bands=["DNB_BRDF_Corrected_NTL"],
        start_date="2026-02-27",
        end_date="2026-02-27",
        bbox=(44.0, 25.0, 63.5, 40.0),
        temporal_resolution="daily",
        require_official_hdf5=True,
    )

    plan = build_gee_plan(request, [_candidate("NASA/VIIRS/002/VNP46A2")])

    assert plan.execution.mode == "official_earthdata"
    assert "OFFICIAL_NTL_PROVENANCE_REQUIRED" in plan.execution.reason_codes


def test_statistics_use_server_side_even_for_small_aoi() -> None:
    request = GeeRequest(
        query="Calculate mean Sentinel-2 NDVI",
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B4", "B8"],
        start_date="2026-01-01",
        end_date="2026-01-15",
        bbox=(120.0, 30.0, 120.01, 30.01),
        temporal_resolution="daily",
        output_kind="table",
        analysis_kind="statistics",
        scale_m=10,
    )

    plan = build_gee_plan(
        request,
        [_candidate("COPERNICUS/S2_SR_HARMONIZED", scale_m=10)],
    )

    assert plan.execution.mode == "server_reduce"


def test_large_high_resolution_raster_uses_batch_export() -> None:
    request = GeeRequest(
        query="Download national Sentinel-2 composite",
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B2", "B3", "B4"],
        start_date="2026-01-01",
        end_date="2026-01-31",
        bbox=(73.0, 18.0, 135.0, 54.0),
        temporal_resolution="daily",
        analysis_kind="composite",
        scale_m=10,
    )

    plan = build_gee_plan(
        request,
        [_candidate("COPERNICUS/S2_SR_HARMONIZED", scale_m=10)],
    )

    assert plan.execution.mode == "batch_export"
    assert plan.execution.estimate.estimated_output_pixels is not None
    assert plan.execution.estimate.estimated_output_pixels > 25_000_000


def test_small_srtm_image_can_download_directly() -> None:
    request = GeeRequest(
        query="Download SRTM elevation",
        dataset_id="USGS/SRTMGL1_003",
        bands=["elevation"],
        bbox=(120.0, 30.0, 120.1, 30.1),
        temporal_resolution="static",
        scale_m=30,
    )
    candidate = _candidate(
        "USGS/SRTMGL1_003",
        title="SRTM elevation",
        scale_m=30,
        temporal_resolution="static",
        asset_type="Image",
    )

    plan = build_gee_plan(request, [candidate])

    assert plan.execution.mode == "direct_local"
    assert plan.execution.estimate.estimated_images == 1


def test_policy_limits_are_configurable() -> None:
    request = GeeRequest(
        query="Download a raster",
        dataset_id="USGS/SRTMGL1_003",
        bands=["elevation"],
        bbox=(120.0, 30.0, 120.01, 30.01),
        temporal_resolution="static",
        scale_m=30,
    )
    candidate = _candidate(
        "USGS/SRTMGL1_003",
        scale_m=30,
        temporal_resolution="static",
        asset_type="Image",
    )

    plan = build_gee_plan(
        request,
        [candidate],
        PlannerPolicy(direct_max_output_pixels=1, direct_max_estimated_bytes=4),
    )

    assert plan.execution.mode == "batch_export"
