from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


DEFAULT_GEE_PROJECT = "empyrean-caster-430308-m2"
DEFAULT_GEE_DAILY_DATASET = "NASA/VIIRS/002/VNP46A2"

GEE_MONITOR_PRODUCTS: list[dict[str, str]] = [
    {
        "dataset_id": "NASA/VIIRS/002/VNP46A2",
        "source": "GEE VNP46A2 (Daily)",
        "temporal_resolution": "daily",
    },
    {
        "dataset_id": "NOAA/VIIRS/001/VNP46A1",
        "source": "GEE VNP46A1 (Daily)",
        "temporal_resolution": "daily",
    },
    {
        "dataset_id": "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
        "source": "GEE VCMSLCFG (Monthly)",
        "temporal_resolution": "monthly",
    },
    {
        "dataset_id": "NOAA/VIIRS/DNB/ANNUAL_V22",
        "source": "GEE VIIRS Annual V22",
        "temporal_resolution": "annual",
    },
    {
        "dataset_id": "NOAA/VIIRS/DNB/ANNUAL_V21",
        "source": "GEE VIIRS Annual V21",
        "temporal_resolution": "annual",
    },
    {
        "dataset_id": "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS",
        "source": "GEE DMSP-OLS Annual",
        "temporal_resolution": "annual",
    },
    {
        "dataset_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "source": "GEE NPP-VIIRS-Like (Annual)",
        "temporal_resolution": "annual",
    },
]


def _to_day(millis: int | float | None) -> str | None:
    if millis is None:
        return None
    try:
        return datetime.fromtimestamp(float(millis) / 1000, tz=UTC).strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return None


def get_gee_monitor_products() -> list[dict[str, str]]:
    return list(GEE_MONITOR_PRODUCTS)


def query_gee_latest_date_for_bbox(
    bbox: tuple[float, float, float, float],
    dataset_id: str = DEFAULT_GEE_DAILY_DATASET,
    project_id: str = DEFAULT_GEE_PROJECT,
) -> tuple[str | None, str | None]:
    try:
        import ee
    except Exception as exc:  # noqa: BLE001
        return None, f"ee import failed: {exc}"

    try:
        ee.Initialize(project=project_id)
        minx, miny, maxx, maxy = bbox
        geom = ee.Geometry.Rectangle([minx, miny, maxx, maxy])
        millis = (
            ee.ImageCollection(dataset_id)
            .filterBounds(geom)
            .aggregate_max("system:time_start")
            .getInfo()
        )
        if millis is None:
            return None, "no_data"
        day = datetime.fromtimestamp(millis / 1000, tz=UTC).strftime("%Y-%m-%d")
        return day, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def query_gee_products_latest(
    *,
    bbox: tuple[float, float, float, float] | None,
    project_id: str = DEFAULT_GEE_PROJECT,
) -> tuple[list[dict[str, Any]], str | None]:
    try:
        import ee
    except Exception as exc:  # noqa: BLE001
        err = f"ee import failed: {exc}"
        rows = []
        for item in GEE_MONITOR_PRODUCTS:
            rows.append(
                {
                    "source": item["source"],
                    "dataset_id": item["dataset_id"],
                    "temporal_resolution": item["temporal_resolution"],
                    "latest_global_date": None,
                    "latest_bbox_date": None,
                    "error": err,
                }
            )
        return rows, err

    try:
        ee.Initialize(project=project_id)
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
        rows = []
        for item in GEE_MONITOR_PRODUCTS:
            rows.append(
                {
                    "source": item["source"],
                    "dataset_id": item["dataset_id"],
                    "temporal_resolution": item["temporal_resolution"],
                    "latest_global_date": None,
                    "latest_bbox_date": None,
                    "error": err,
                }
            )
        return rows, err

    geom = None
    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        geom = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

    rows: list[dict[str, Any]] = []
    for item in GEE_MONITOR_PRODUCTS:
        dataset_id = item["dataset_id"]
        row: dict[str, Any] = {
            "source": item["source"],
            "dataset_id": dataset_id,
            "temporal_resolution": item["temporal_resolution"],
            "latest_global_date": None,
            "latest_bbox_date": None,
            "error": None,
        }
        try:
            col = ee.ImageCollection(dataset_id)
            global_millis = col.aggregate_max("system:time_start").getInfo()
            row["latest_global_date"] = _to_day(global_millis)
            if geom is not None:
                bbox_millis = col.filterBounds(geom).aggregate_max("system:time_start").getInfo()
                row["latest_bbox_date"] = _to_day(bbox_millis)
            else:
                row["error"] = "bbox_missing"
        except Exception as exc:  # noqa: BLE001
            row["error"] = str(exc)
        rows.append(row)
    return rows, None
