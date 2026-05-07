"""
GEE Python server-side NTL analysis template for 地缘环境智能计算平台.

Adapt this file before execution. Keep the NTL_SCRIPT_CONTRACT block updated.
The default example is annual NTL zonal mean by administrative features.
"""

from __future__ import annotations

import csv
from pathlib import Path

import ee

try:
    from storage_manager import storage_manager
except Exception:  # Allows template linting outside the app runtime.
    storage_manager = None


NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v1",
    "objective": "Compute server-side NTL statistics and export a compact CSV table.",
    "gee_project_id": "REPLACE_WITH_RUNTIME_GEE_PROJECT_ID",
    "dataset": {
        "dataset_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "band": "b1",
        "start_date": "2020-01-01",
        "end_date_exclusive": "2021-01-01",
    },
    "boundary": {
        "type": "ee.FeatureCollection",
        "asset": "projects/empyrean-caster-430308-m2/assets/province",
        "name_property": "name",
    },
    "method": {
        "reducer": "mean",
        "scale": 500,
        "max_pixels_per_region": 1e13,
    },
    "output": {
        "csv_filename": "gee_ntl_zonal_stats.csv",
        "sort_by": "ntl_mean",
        "descending": True,
    },
    "validation_checks": [
        "image collection size > 0",
        "feature collection size > 0",
        "output row count > 0",
        "expected columns exist",
    ],
    "failure_gates": [
        "USER_PROJECT_DENIED",
        "serviceusage.serviceUsageConsumer",
        "authentication failure",
        "API disabled",
        "quota denied",
        "missing dataset or band",
        "empty collection",
        "empty feature collection",
    ],
}


def resolve_output_path(filename: str) -> str:
    if storage_manager is not None:
        return storage_manager.resolve_output_path(filename)
    Path("outputs").mkdir(parents=True, exist_ok=True)
    return str(Path("outputs") / filename)


def initialize_ee() -> None:
    project_id = NTL_SCRIPT_CONTRACT["gee_project_id"]
    if not project_id or project_id.startswith("REPLACE_"):
        raise ValueError("Set NTL_SCRIPT_CONTRACT['gee_project_id'] before execution.")
    ee.Initialize(project=project_id)


def load_image() -> ee.Image:
    dataset = NTL_SCRIPT_CONTRACT["dataset"]
    collection = (
        ee.ImageCollection(dataset["dataset_id"])
        .filterDate(dataset["start_date"], dataset["end_date_exclusive"])
        .select(dataset["band"])
    )
    count = int(collection.size().getInfo())
    if count <= 0:
        raise ValueError(
            f"Empty image collection: {dataset['dataset_id']} "
            f"{dataset['start_date']}..{dataset['end_date_exclusive']} band={dataset['band']}"
        )
    return collection.mean().rename("ntl_value")


def load_zones() -> ee.FeatureCollection:
    boundary = NTL_SCRIPT_CONTRACT["boundary"]
    zones = ee.FeatureCollection(boundary["asset"])
    count = int(zones.size().getInfo())
    if count <= 0:
        raise ValueError(f"Empty feature collection: {boundary['asset']}")
    return zones


def run_zonal_stats(image: ee.Image, zones: ee.FeatureCollection) -> ee.FeatureCollection:
    method = NTL_SCRIPT_CONTRACT["method"]
    reducer_name = method["reducer"]
    if reducer_name != "mean":
        raise ValueError(f"Unsupported reducer in template: {reducer_name}")

    stats = image.reduceRegions(
        collection=zones,
        reducer=ee.Reducer.mean(),
        scale=method["scale"],
        crs="EPSG:4326",
        maxPixelsPerRegion=method["max_pixels_per_region"],
    )

    name_property = NTL_SCRIPT_CONTRACT["boundary"]["name_property"]

    def normalize(feature):
        return ee.Feature(None, {
            "region_name": feature.get(name_property),
            "ntl_mean": feature.get("mean"),
        })

    return stats.map(normalize)


def feature_collection_to_rows(features: ee.FeatureCollection) -> list[dict[str, object]]:
    payload = features.getInfo()
    rows: list[dict[str, object]] = []
    for item in payload.get("features", []):
        props = item.get("properties", {}) or {}
        rows.append({
            "region_name": props.get("region_name"),
            "ntl_mean": props.get("ntl_mean"),
        })
    rows = [row for row in rows if row["region_name"] is not None and row["ntl_mean"] is not None]
    if not rows:
        raise ValueError("No valid zonal-stat rows returned from GEE.")
    sort_key = NTL_SCRIPT_CONTRACT["output"]["sort_by"]
    reverse = bool(NTL_SCRIPT_CONTRACT["output"]["descending"])
    rows.sort(key=lambda row: float(row[sort_key]), reverse=reverse)
    return rows


def write_csv(rows: list[dict[str, object]]) -> str:
    output_path = resolve_output_path(NTL_SCRIPT_CONTRACT["output"]["csv_filename"])
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["region_name", "ntl_mean"])
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> None:
    initialize_ee()
    image = load_image()
    zones = load_zones()
    stats = run_zonal_stats(image, zones)
    rows = feature_collection_to_rows(stats)
    output_path = write_csv(rows)
    print(f"status=success")
    print(f"rows={len(rows)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()

