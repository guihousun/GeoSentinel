"""
Reference case: daily event-impact ANTL over epicenter buffers.

Adapted from the Myanmar earthquake ANTL runtime case. This keeps buffer
construction and zonal statistics in Earth Engine and explicitly handles
first-night selection for VIIRS-style nighttime observations.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import ee

try:
    from storage_manager import storage_manager
except Exception:
    storage_manager = None


NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v1",
    "objective": "Compare daily VNP46A2 ANTL before/after an event over epicenter buffers.",
    "gee_project_id_env": "GEE_DEFAULT_PROJECT_ID",
    "event": {
        "name": "Myanmar M7.7 earthquake",
        "event_date_utc": "2025-03-28T06:20:00Z",
        "event_local_time": "12:50",
        "local_timezone": "Asia/Yangon",
        "epicenter_lon": 95.9,
        "epicenter_lat": 22.0,
        "local_first_post_event_night": "2025-03-29",
        "local_first_night_acquisition_range": "2025-03-29 00:30-02:30 MMT",
        "utc_first_night_acquisition_range": "2025-03-28 18:00-20:00 UTC",
        "utc_first_night_file_date": "2025-03-28",
        "first_night_rule": (
            "Event occurred after the approximate local VIIRS overpass, so the local first-night "
            "label is D+1. Do not invent an exact local acquisition time unless UTC_Time or official "
            "metadata confirms it. Use GEE VNP46A1.UTC_Time only when VNP46A1 covers the target date; "
            "for recent events use LAADS/CMR granule timing or official metadata. For UTC-indexed "
            "daily products/files, query the UTC file date derived from the verified local acquisition "
            "time or candidate range."
        ),
    },
    "periods": [
        {"period": "pre_event_baseline", "start": "2025-03-14", "end_exclusive": "2025-03-22"},
        {"period": "first_night_impact", "start": "2025-03-28", "end_exclusive": "2025-03-29"},
        {"period": "post_event_recovery", "start": "2025-04-04", "end_exclusive": "2025-04-12"},
    ],
    "buffers_km": [25, 50, 100],
    "dataset": {
        "dataset_id": "NASA/VIIRS/002/VNP46A2",
        "band": "Gap_Filled_DNB_BRDF_Corrected_NTL",
    },
    "method": {
        "scale": 500,
        "tile_scale": 4,
        "max_pixels_per_region": 1e13,
    },
    "output": {
        "csv_filename": "event_buffer_daily_antl.csv",
        "columns": ["buffer_km", "period", "start_date", "end_exclusive", "image_count", "mean_antl"],
    },
    "validation_checks": [
        "each period has at least one image unless the date is outside dataset coverage",
        "each buffer-period row has valid pixel support or reports null explicitly",
        "first-night local label and UTC-indexed query date are both recorded",
        "exact local acquisition time is only claimed when pixel-level UTC_Time or official metadata confirms it",
        "recent events beyond GEE VNP46A1 coverage use LAADS/CMR granule timing or official metadata",
    ],
}


def resolve_output_path(filename: str) -> str:
    if storage_manager is not None:
        return storage_manager.resolve_output_path(filename)
    Path("outputs").mkdir(parents=True, exist_ok=True)
    return str(Path("outputs") / filename)


def initialize_ee() -> None:
    project_id = os.environ.get(NTL_SCRIPT_CONTRACT["gee_project_id_env"])
    if not project_id:
        raise ValueError("Set GEE_DEFAULT_PROJECT_ID before running this script.")
    ee.Initialize(project=project_id)


def build_buffer_zones() -> ee.FeatureCollection:
    event = NTL_SCRIPT_CONTRACT["event"]
    point = ee.Geometry.Point([event["epicenter_lon"], event["epicenter_lat"]])
    features = []
    for km in NTL_SCRIPT_CONTRACT["buffers_km"]:
        features.append(ee.Feature(point.buffer(km * 1000), {"buffer_km": km}))
    zones = ee.FeatureCollection(features)
    if int(zones.size().getInfo()) != len(NTL_SCRIPT_CONTRACT["buffers_km"]):
        raise ValueError("Buffer FeatureCollection size mismatch.")
    return zones


def compute_period_stats(zones: ee.FeatureCollection, period: dict[str, str]) -> list[dict[str, object]]:
    dataset = NTL_SCRIPT_CONTRACT["dataset"]
    method = NTL_SCRIPT_CONTRACT["method"]
    collection = (
        ee.ImageCollection(dataset["dataset_id"])
        .filterDate(period["start"], period["end_exclusive"])
        .select(dataset["band"])
    )
    image_count = int(collection.size().getInfo())
    if image_count <= 0:
        raise ValueError(f"No VNP46A2 images for {period['period']} {period['start']}..{period['end_exclusive']}")
    period_mean = collection.mean().rename("ntl_value")
    stats = period_mean.reduceRegions(
        collection=zones,
        reducer=ee.Reducer.mean(),
        scale=method["scale"],
        tileScale=method["tile_scale"],
        maxPixelsPerRegion=method["max_pixels_per_region"],
    )
    rows = []
    for feature in stats.getInfo().get("features", []):
        props = feature.get("properties", {}) or {}
        value = props.get("mean")
        rows.append({
            "buffer_km": int(props["buffer_km"]),
            "period": period["period"],
            "start_date": period["start"],
            "end_exclusive": period["end_exclusive"],
            "image_count": image_count,
            "mean_antl": None if value is None else round(float(value), 6),
        })
    return rows


def write_csv(rows: list[dict[str, object]]) -> str:
    output_path = resolve_output_path(NTL_SCRIPT_CONTRACT["output"]["csv_filename"])
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=NTL_SCRIPT_CONTRACT["output"]["columns"])
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> None:
    initialize_ee()
    zones = build_buffer_zones()
    rows = []
    for period in NTL_SCRIPT_CONTRACT["periods"]:
        rows.extend(compute_period_stats(zones, period))
    rows.sort(key=lambda row: (row["buffer_km"], row["period"]))
    output_path = write_csv(rows)
    print("status=success")
    print(f"rows={len(rows)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
