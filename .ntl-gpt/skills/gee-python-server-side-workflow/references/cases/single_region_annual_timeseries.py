"""
Reference case: one-region annual NTL time series using server-side map/reduceRegion.

Adapted from the Shanghai annual trend runtime case. This pattern avoids
downloading one raster per year when the output is a compact time-series table.
"""

from __future__ import annotations

import csv
import math
import os
from pathlib import Path

import ee

try:
    from storage_manager import storage_manager
except Exception:
    storage_manager = None


NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v1",
    "objective": "Extract annual mean NTL time series for one region using GEE server-side reductions.",
    "gee_project_id_env": "GEE_DEFAULT_PROJECT_ID",
    "region": {
        "boundary_asset": "FAO/GAUL/2015/level1",
        "filters": [{"property": "ADM0_NAME", "equals": "China"}, {"property": "ADM1_NAME", "equals": "Shanghai Shi"}],
        "label": "Shanghai",
    },
    "dataset": {
        "dataset_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "band": "b1",
        "start_date": "2000-01-01",
        "end_date_exclusive": "2021-01-01",
    },
    "method": {
        "scale": 500,
        "tile_scale": 4,
        "max_pixels": 1e13,
    },
    "output": {
        "annual_csv_filename": "region_annual_ntl_timeseries.csv",
        "trend_csv_filename": "region_annual_ntl_trend.csv",
    },
    "validation_checks": [
        "region FeatureCollection size > 0",
        "image collection has expected annual images",
        "returned rows have non-null NTL mean values",
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


def load_region_geometry() -> ee.Geometry:
    cfg = NTL_SCRIPT_CONTRACT["region"]
    zones = ee.FeatureCollection(cfg["boundary_asset"])
    for item in cfg["filters"]:
        zones = zones.filter(ee.Filter.eq(item["property"], item["equals"]))
    count = int(zones.size().getInfo())
    if count <= 0:
        raise ValueError(f"Empty region boundary for {cfg['label']}")
    return zones.geometry()


def build_timeseries(region: ee.Geometry) -> ee.FeatureCollection:
    dataset = NTL_SCRIPT_CONTRACT["dataset"]
    collection = (
        ee.ImageCollection(dataset["dataset_id"])
        .filterDate(dataset["start_date"], dataset["end_date_exclusive"])
        .filterBounds(region)
        .select(dataset["band"])
    )
    if int(collection.size().getInfo()) <= 0:
        raise ValueError("Empty annual NTL collection.")

    method = NTL_SCRIPT_CONTRACT["method"]

    def per_image(image):
        value = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=method["scale"],
            maxPixels=method["max_pixels"],
            tileScale=method["tile_scale"],
            bestEffort=True,
        ).get(dataset["band"])
        return ee.Feature(None, {
            "region": NTL_SCRIPT_CONTRACT["region"]["label"],
            "year": image.date().format("YYYY"),
            "date": image.date().format("YYYY-MM-dd"),
            "ntl_mean": value,
        })

    return ee.FeatureCollection(collection.map(per_image))


def rows_from_timeseries(fc: ee.FeatureCollection) -> list[dict[str, object]]:
    rows = []
    for feature in fc.getInfo().get("features", []):
        props = feature.get("properties", {}) or {}
        value = props.get("ntl_mean")
        if value is None:
            continue
        rows.append({
            "region": props.get("region"),
            "year": int(props["year"]),
            "date": props.get("date"),
            "ntl_mean": float(value),
        })
    if len(rows) < 2:
        raise ValueError("Need at least two valid annual values for trend analysis.")
    rows.sort(key=lambda row: row["year"])
    return rows


def ordinary_least_squares(rows: list[dict[str, object]]) -> dict[str, float | int | str]:
    xs = [float(row["year"]) for row in rows]
    ys = [float(row["ntl_mean"]) for row in rows]
    x_bar = sum(xs) / len(xs)
    y_bar = sum(ys) / len(ys)
    denom = sum((x - x_bar) ** 2 for x in xs)
    slope = sum((x - x_bar) * (y - y_bar) for x, y in zip(xs, ys)) / denom if denom else math.nan
    intercept = y_bar - slope * x_bar if not math.isnan(slope) else math.nan
    return {
        "region": NTL_SCRIPT_CONTRACT["region"]["label"],
        "start_year": int(min(xs)),
        "end_year": int(max(xs)),
        "n_years": len(rows),
        "mean_ntl": y_bar,
        "ols_slope_per_year": slope,
        "ols_intercept": intercept,
    }


def write_csv(path: str, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    initialize_ee()
    region = load_region_geometry()
    fc = build_timeseries(region)
    rows = rows_from_timeseries(fc)
    trend = ordinary_least_squares(rows)
    annual_path = resolve_output_path(NTL_SCRIPT_CONTRACT["output"]["annual_csv_filename"])
    trend_path = resolve_output_path(NTL_SCRIPT_CONTRACT["output"]["trend_csv_filename"])
    write_csv(annual_path, rows, ["region", "year", "date", "ntl_mean"])
    write_csv(trend_path, [trend], list(trend.keys()))
    print("status=success")
    print(f"rows={len(rows)}")
    print(f"output={annual_path}")
    print(f"output={trend_path}")


if __name__ == "__main__":
    main()
