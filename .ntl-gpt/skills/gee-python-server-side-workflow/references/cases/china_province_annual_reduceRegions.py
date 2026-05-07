"""
Reference case: China province-level annual NTL ranking with GEE reduceRegions.

Adapted from real 地缘环境智能计算平台 runtime scripts for the prompt:
"Calculate 2020 mean nighttime light for China's 34 province-level
administrative regions and return a table sorted descending."

This is a reference pattern. Before execution, confirm the boundary source and
province inclusion policy required by the user.
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
    "objective": "Compute 2020 mean NTL for China province-level units using server-side GEE zonal statistics.",
    "gee_project_id_env": "GEE_DEFAULT_PROJECT_ID",
    "dataset": {
        "dataset_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "band": "b1",
        "start_date": "2020-01-01",
        "end_date_exclusive": "2021-01-01",
    },
    "boundary": {
        "primary_adm1_asset": "WM/geoLab/geoBoundaries/600/ADM1",
        "primary_adm0_asset": "WM/geoLab/geoBoundaries/600/ADM0",
        "fallback_asset": "projects/empyrean-caster-430308-m2/assets/province",
        "name_property_candidates": ["shapeName", "ADM1_NAME", "name", "Name", "province"],
        "expected_rows": 34,
        "required_regions": ["Taiwan", "Hong Kong", "Macau"],
    },
    "method": {
        "reducer": "mean",
        "scale": 500,
        "tile_scale": 8,
        "max_pixels_per_region": 1e13,
    },
    "output": {
        "csv_filename": "china_province_ntl_mean_2020.csv",
        "columns": ["rank", "region_name", "ntl_mean"],
        "sort_by": "ntl_mean",
        "descending": True,
    },
    "validation_checks": [
        "image collection size > 0",
        "province feature count == 34",
        "non-null NTL rows == 34",
        "Taiwan, Hong Kong, and Macau are present",
        "all NTL means are finite and non-negative",
    ],
    "failure_gates": [
        "USER_PROJECT_DENIED",
        "missing Earth Engine project/IAM/API permission",
        "empty annual NTL collection",
        "empty province FeatureCollection",
        "all reducer outputs are null",
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


def load_annual_image() -> ee.Image:
    dataset = NTL_SCRIPT_CONTRACT["dataset"]
    collection = (
        ee.ImageCollection(dataset["dataset_id"])
        .filterDate(dataset["start_date"], dataset["end_date_exclusive"])
        .select(dataset["band"])
    )
    if int(collection.size().getInfo()) <= 0:
        raise ValueError(f"Empty NTL collection for {dataset['start_date']}..{dataset['end_date_exclusive']}")
    return collection.mean().rename("ntl_value")


def load_china_provinces() -> ee.FeatureCollection:
    boundary = NTL_SCRIPT_CONTRACT["boundary"]
    adm1 = ee.FeatureCollection(boundary["primary_adm1_asset"])
    adm0 = ee.FeatureCollection(boundary["primary_adm0_asset"])
    mainland = adm1.filter(ee.Filter.eq("shapeGroup", "CHN"))
    taiwan_parts = adm1.filter(ee.Filter.eq("shapeGroup", "TWN"))
    taiwan = ee.Feature(taiwan_parts.geometry(), {"shapeName": "Taiwan Province", "shapeGroup": "TWN"})
    hong_kong = ee.Feature(
        adm0.filter(ee.Filter.eq("shapeGroup", "HKG")).geometry(),
        {"shapeName": "Hong Kong SAR", "shapeGroup": "HKG"},
    )
    macau = ee.Feature(
        adm0.filter(ee.Filter.eq("shapeGroup", "MAC")).geometry(),
        {"shapeName": "Macau SAR", "shapeGroup": "MAC"},
    )
    zones = mainland.merge(ee.FeatureCollection([taiwan, hong_kong, macau]))
    count = int(zones.size().getInfo())
    if count == boundary["expected_rows"]:
        return zones

    fallback = ee.FeatureCollection(boundary["fallback_asset"])
    fallback_count = int(fallback.size().getInfo())
    if fallback_count != boundary["expected_rows"]:
        raise ValueError(f"Province FeatureCollection must contain exactly 34 regions; got {fallback_count}")
    return fallback


def pick_first_existing_property(feature: ee.Feature, candidates: list[str]) -> ee.String:
    names = feature.propertyNames()
    values = ee.List(candidates).map(lambda key: ee.Algorithms.If(names.contains(key), feature.get(key), None))
    non_null = values.removeAll([None])
    return ee.String(ee.Algorithms.If(non_null.size().gt(0), non_null.get(0), feature.id()))


def compute_zonal_stats(image: ee.Image, zones: ee.FeatureCollection) -> ee.FeatureCollection:
    method = NTL_SCRIPT_CONTRACT["method"]
    raw = image.reduceRegions(
        collection=zones,
        reducer=ee.Reducer.mean(),
        scale=method["scale"],
        tileScale=method["tile_scale"],
        maxPixelsPerRegion=method["max_pixels_per_region"],
    )
    name_candidates = NTL_SCRIPT_CONTRACT["boundary"]["name_property_candidates"]

    def normalize(feature):
        return ee.Feature(None, {
            "region_name": pick_first_existing_property(feature, name_candidates),
            "ntl_mean": feature.get("mean"),
        })

    return raw.map(normalize)


def rows_from_feature_collection(fc: ee.FeatureCollection) -> list[dict[str, object]]:
    features = fc.getInfo().get("features", [])
    rows = []
    for feature in features:
        props = feature.get("properties", {}) or {}
        value = props.get("ntl_mean")
        if value is None:
            continue
        value = float(value)
        if value < 0:
            continue
        rows.append({"region_name": props.get("region_name"), "ntl_mean": round(value, 6)})
    expected_rows = NTL_SCRIPT_CONTRACT["boundary"]["expected_rows"]
    if len(rows) != expected_rows:
        raise ValueError(f"Expected exactly {expected_rows} valid province rows; got {len(rows)}.")
    names = {str(row["region_name"]).lower() for row in rows}
    for required in NTL_SCRIPT_CONTRACT["boundary"]["required_regions"]:
        if not any(required.lower() in name for name in names):
            raise ValueError(f"Missing required China province-level unit: {required}")
    rows.sort(key=lambda row: row["ntl_mean"], reverse=True)
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
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
    image = load_annual_image()
    zones = load_china_provinces()
    stats = compute_zonal_stats(image, zones)
    rows = rows_from_feature_collection(stats)
    output_path = write_csv(rows)
    print("status=success")
    print(f"rows={len(rows)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()

