"""
Reference case: local uploaded shapefile boundaries + cloud NTL image + GEE reduceRegions.

Adapted from the Shanghai district ANTL runtime case. Use this when the user
already provided a small boundary file and wants table statistics rather than a
downloaded raster.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import ee
import geopandas as gpd

try:
    from storage_manager import storage_manager
except Exception:
    storage_manager = None


NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v1",
    "objective": "Compute annual NTL mean for local shapefile features using GEE server-side reduceRegions.",
    "gee_project_id_env": "GEE_DEFAULT_PROJECT_ID",
    "input": {
        "boundary_filename": "shanghai_districts_boundary.shp",
        "name_property_candidates": ["name", "NAME", "district", "DISTRICT", "shapeName"],
    },
    "dataset": {
        "dataset_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "band": "b1",
        "start_date": "2020-01-01",
        "end_date_exclusive": "2021-01-01",
    },
    "method": {
        "reducer": "mean",
        "scale": 500,
        "tile_scale": 4,
        "max_pixels_per_region": 1e13,
    },
    "output": {
        "csv_filename": "local_boundary_ntl_mean_2020.csv",
        "columns": ["region_name", "ntl_mean"],
    },
    "validation_checks": [
        "local shapefile exists and has features",
        "image collection size > 0",
        "returned row count equals local feature count when all features have valid pixels",
    ],
}


def resolve_input_path(filename: str) -> str:
    if storage_manager is not None:
        return storage_manager.resolve_input_path(filename)
    return str(Path("inputs") / filename)


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


def load_local_zones() -> ee.FeatureCollection:
    boundary_path = resolve_input_path(NTL_SCRIPT_CONTRACT["input"]["boundary_filename"])
    gdf = gpd.read_file(boundary_path)
    if gdf.empty:
        raise ValueError(f"Boundary file has no features: {boundary_path}")
    if gdf.crs is None:
        raise ValueError("Boundary CRS is missing; set or reproject before converting to GEE.")
    gdf = gdf.to_crs("EPSG:4326")
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    payload = json.loads(gdf.to_json())
    features = []
    for item in payload["features"]:
        geom = ee.Geometry(item["geometry"])
        props = item.get("properties", {}) or {}
        features.append(ee.Feature(geom, props))
    zones = ee.FeatureCollection(features)
    if int(zones.size().getInfo()) <= 0:
        raise ValueError("Converted local boundary FeatureCollection is empty.")
    return zones


def load_annual_image() -> ee.Image:
    dataset = NTL_SCRIPT_CONTRACT["dataset"]
    collection = (
        ee.ImageCollection(dataset["dataset_id"])
        .filterDate(dataset["start_date"], dataset["end_date_exclusive"])
        .select(dataset["band"])
    )
    if int(collection.size().getInfo()) <= 0:
        raise ValueError(f"Empty image collection for {dataset['start_date']}..{dataset['end_date_exclusive']}")
    return collection.mean().rename("ntl_value")


def feature_name(feature: ee.Feature) -> ee.String:
    candidates = NTL_SCRIPT_CONTRACT["input"]["name_property_candidates"]
    names = feature.propertyNames()
    values = ee.List(candidates).map(lambda key: ee.Algorithms.If(names.contains(key), feature.get(key), None))
    non_null = values.removeAll([None])
    return ee.String(ee.Algorithms.If(non_null.size().gt(0), non_null.get(0), feature.id()))


def compute_stats(image: ee.Image, zones: ee.FeatureCollection) -> ee.FeatureCollection:
    method = NTL_SCRIPT_CONTRACT["method"]
    stats = image.reduceRegions(
        collection=zones,
        reducer=ee.Reducer.mean(),
        scale=method["scale"],
        tileScale=method["tile_scale"],
        maxPixelsPerRegion=method["max_pixels_per_region"],
    )
    return stats.map(lambda f: ee.Feature(None, {"region_name": feature_name(f), "ntl_mean": f.get("mean")}))


def rows_from_stats(stats: ee.FeatureCollection) -> list[dict[str, object]]:
    rows = []
    for feature in stats.getInfo().get("features", []):
        props = feature.get("properties", {}) or {}
        value = props.get("ntl_mean")
        if value is not None:
            rows.append({"region_name": props.get("region_name"), "ntl_mean": round(float(value), 6)})
    if not rows:
        raise ValueError("No valid zonal-stat rows returned.")
    rows.sort(key=lambda row: row["region_name"] or "")
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
    zones = load_local_zones()
    image = load_annual_image()
    stats = compute_stats(image, zones)
    rows = rows_from_stats(stats)
    output_path = write_csv(rows)
    print("status=success")
    print(f"rows={len(rows)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
