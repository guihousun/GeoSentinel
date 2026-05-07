from __future__ import annotations

import calendar
import json
import os
import re
import requests
import time
from html import unescape
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from dotenv import dotenv_values

DEFAULT_GEE_PROJECT = "empyrean-caster-430308-m2"


def _configured_gee_project_id() -> str:
    dotenv_path = Path(__file__).resolve().parents[1] / ".env"
    project_id = ""
    if dotenv_path.exists():
        project_id = str(dotenv_values(dotenv_path).get("GEE_DEFAULT_PROJECT_ID") or "").strip()
    if not project_id:
        project_id = str(os.getenv("GEE_DEFAULT_PROJECT_ID") or "").strip()
    return project_id or DEFAULT_GEE_PROJECT


REQUIRED_GEE_PROJECT = _configured_gee_project_id()
EE_CATALOG_PAGE = "https://developers.google.com/earth-engine/datasets/catalog"
_CATALOG_CACHE: Dict[str, object] = {"items": [], "fetched_at": 0.0}
_DATASET_ID_CACHE: Dict[str, str] = {}
_CURATED_DATASET_REGISTRY_CACHE: Dict[str, object] = {"items": [], "loaded": False}
_COMMON_QUERY_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "for",
    "from",
    "global",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dataset_id: str
    temporal_resolution: Literal["annual", "monthly", "daily"]
    band: str
    start_date: date
    end_date: date
    spatial_resolution_m: int
    note: str


def _today() -> date:
    return date.today()


def _vnp46a2_end() -> date:
    # NASA/VIIRS/002/VNP46A2 has a typical few-day latency.
    return _today() - timedelta(days=4)


DATASETS: List[DatasetSpec] = [
    DatasetSpec(
        name="NPP-VIIRS-Like",
        dataset_id="projects/sat-io/open-datasets/npp-viirs-ntl",
        temporal_resolution="annual",
        band="b1",
        start_date=date(2000, 1, 1),
        end_date=date(2024, 12, 31),
        spatial_resolution_m=500,
        note="Primary annual long-term product.",
    ),
    DatasetSpec(
        name="NPP-VIIRS",
        dataset_id="NOAA/VIIRS/DNB/ANNUAL_V22",
        temporal_resolution="annual",
        band="average",
        start_date=date(2012, 1, 1),
        end_date=date(2024, 12, 31),
        spatial_resolution_m=500,
        note="Annual composites; keep V21/V22 compatibility in implementation.",
    ),
    DatasetSpec(
        name="DMSP-OLS",
        dataset_id="NOAA/DMSP-OLS/NIGHTTIME_LIGHTS",
        temporal_resolution="annual",
        band="avg_vis",
        start_date=date(1992, 1, 1),
        end_date=date(2013, 12, 31),
        spatial_resolution_m=1000,
        note="Legacy annual nightlight product.",
    ),
    DatasetSpec(
        name="NOAA_VCMSLCFG",
        dataset_id="NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
        temporal_resolution="monthly",
        band="avg_rad",
        start_date=date(2014, 1, 1),
        end_date=date(2025, 3, 31),
        spatial_resolution_m=500,
        note="Monthly VIIRS product.",
    ),
    DatasetSpec(
        name="VNP46A2",
        dataset_id="NASA/VIIRS/002/VNP46A2",
        temporal_resolution="daily",
        band="Gap_Filled_DNB_BRDF_Corrected_NTL",
        start_date=date(2012, 1, 19),
        end_date=_vnp46a2_end(),
        spatial_resolution_m=500,
        note="Daily NTL product; best choice for daily analyses.",
    ),
    DatasetSpec(
        name="VNP46A1",
        dataset_id="NOAA/VIIRS/001/VNP46A1",
        temporal_resolution="daily",
        band="DNB_At_Sensor_Radiance_500m",
        start_date=date(2012, 1, 19),
        end_date=date(2024, 11, 3),
        spatial_resolution_m=500,
        note="Daily at-sensor product with earlier end date.",
    ),
]

DATASET_BY_NAME = {d.name.lower(): d for d in DATASETS}
DATASET_BY_ID = {d.dataset_id: d for d in DATASETS}

DEFAULT_DATASET_BY_TEMPORAL = {
    "annual": "NPP-VIIRS-Like",
    "monthly": "NOAA_VCMSLCFG",
    "daily": "VNP46A2",
}


def _parse_date(value: str, role: Literal["start", "end"]) -> date:
    raw = value.strip()
    if re.fullmatch(r"\d{4}", raw):
        year = int(raw)
        return date(year, 1, 1) if role == "start" else date(year, 12, 31)

    if re.fullmatch(r"\d{4}-\d{2}", raw):
        year, month = [int(x) for x in raw.split("-")]
        if role == "start":
            return date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return datetime.strptime(raw, "%Y-%m-%d").date()

    raise ValueError(f"Unsupported date format: {value}. Use YYYY, YYYY-MM, or YYYY-MM-DD.")


def _resolve_dataset(
    temporal_resolution: str,
    prefer_dataset: Optional[str],
    prefer_dataset_id: Optional[str],
) -> DatasetSpec:
    if prefer_dataset_id:
        spec = DATASET_BY_ID.get(prefer_dataset_id)
        if spec and spec.temporal_resolution == temporal_resolution:
            return spec

    if prefer_dataset:
        spec = DATASET_BY_NAME.get(prefer_dataset.lower())
        if spec and spec.temporal_resolution == temporal_resolution:
            return spec

    default_name = DEFAULT_DATASET_BY_TEMPORAL[temporal_resolution]
    return DATASET_BY_NAME[default_name.lower()]


def _load_curated_dataset_registry() -> List[Dict]:
    if _CURATED_DATASET_REGISTRY_CACHE.get("loaded"):
        return list(_CURATED_DATASET_REGISTRY_CACHE.get("items", []) or [])

    registry_path = (
        Path(__file__).resolve().parents[1]
        / ".ntl-gpt"
        / "skills"
        / "gee-dataset-selection"
        / "references"
        / "gee-dataset-registry.json"
    )
    items: List[Dict] = []
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
        raw_items = payload.get("datasets", [])
        if isinstance(raw_items, list):
            items = [item for item in raw_items if isinstance(item, dict)]
    except Exception:
        items = []
    _CURATED_DATASET_REGISTRY_CACHE["items"] = items
    _CURATED_DATASET_REGISTRY_CACHE["loaded"] = True
    return items


def _find_curated_dataset_entry(dataset_id: str) -> Optional[Dict]:
    for item in _load_curated_dataset_registry():
        if item.get("dataset_id") == dataset_id:
            return item
        ids = item.get("dataset_ids")
        if isinstance(ids, list) and dataset_id in ids:
            return item
    return None


def _curated_band_names(entry: Optional[Dict]) -> List[str]:
    if not entry:
        return []
    bands = entry.get("primary_bands", [])
    if not isinstance(bands, list):
        return []
    return [str(b.get("band")) for b in bands if isinstance(b, dict) and b.get("band")]


def _estimate_image_count(temporal_resolution: str, start_dt: date, end_dt: date) -> int:
    if temporal_resolution == "daily":
        return (end_dt - start_dt).days + 1
    if temporal_resolution == "monthly":
        return (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month) + 1
    if temporal_resolution == "annual":
        return (end_dt.year - start_dt.year) + 1
    return 1


def _query_requires_server_side(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    api_markers = (
        "gee python api",
        "earth engine python api",
        "google earth engine python",
    )
    analysis_markers = (
        "compute antl",
        "compute mean",
        "mean nighttime light",
        "average nighttime light",
        "zonal statistics",
        "zonal stats",
        "reduceregions",
        "daily antl",
        "impact assessment",
        "damage assessment",
        "pre-event",
        "post-event",
        "first night",
        "time series",
        "统计",
        "分区统计",
        "计算",
        "均值",
        "平均",
        "排序",
        "排名",
        "评估",
        "震后",
        "震前",
    )
    scale_markers = (
        "country",
        "national",
        "province-level",
        "provincial",
        "multi-province",
        "all provinces",
        "china",
        "中国",
        "全国",
        "省级",
        "各省",
        "34个省",
        "34 个省",
    )
    analysis_hit = any(m in q for m in analysis_markers)
    scale_hit = any(m in q for m in scale_markers)
    return any(m in q for m in api_markers) or analysis_hit or (analysis_hit and scale_hit)


def _execution_mode(
    temporal_resolution: str,
    start_dt: date,
    end_dt: date,
    analysis_intent: str,
    query: str = "",
) -> str:
    image_count = _estimate_image_count(temporal_resolution, start_dt, end_dt)
    intent = (analysis_intent or "").strip().lower()
    heavy_intents = {"long_series", "composite_export", "time_series"}
    server_side_intents = {"zonal_stats", "single_stat"}

    if intent in heavy_intents or intent in server_side_intents or _query_requires_server_side(query):
        return "gee_server_side"

    if temporal_resolution == "daily":
        return "gee_server_side" if image_count > 14 else "direct_download"
    if temporal_resolution == "monthly":
        return "direct_download" if image_count <= 12 else "gee_server_side"
    if temporal_resolution == "annual":
        return "direct_download" if image_count <= 12 else "gee_server_side"
    return "direct_download"


class GEEDatasetRouterInput(BaseModel):
    query: str = Field(..., description="User request summary for GEE task routing.")
    temporal_resolution: Literal["annual", "monthly", "daily"] = Field(
        ...,
        description="Temporal resolution required by the task.",
    )
    start_date: str = Field(..., description="Start date (YYYY, YYYY-MM, or YYYY-MM-DD).")
    end_date: str = Field(..., description="End date (YYYY, YYYY-MM, or YYYY-MM-DD).")
    analysis_intent: str = Field(
        default="quick_download",
        description="Intent tag, e.g., time_series, zonal_stats, export_image, quick_download.",
    )
    prefer_dataset: Optional[str] = Field(
        default=None,
        description="Optional dataset alias, e.g., NPP-VIIRS-Like, VNP46A2.",
    )
    prefer_dataset_id: Optional[str] = Field(
        default=None,
        description="Optional exact Earth Engine dataset ID.",
    )


def gee_dataset_router(
    query: str,
    temporal_resolution: str,
    start_date: str,
    end_date: str,
    analysis_intent: str = "quick_download",
    prefer_dataset: Optional[str] = None,
    prefer_dataset_id: Optional[str] = None,
) -> str:
    start_dt = _parse_date(start_date, "start")
    end_dt = _parse_date(end_date, "end")
    if end_dt < start_dt:
        raise ValueError("End date must be >= start date.")

    spec = _resolve_dataset(
        temporal_resolution=temporal_resolution,
        prefer_dataset=prefer_dataset,
        prefer_dataset_id=prefer_dataset_id,
    )

    supported = start_dt >= spec.start_date and end_dt <= spec.end_date
    mode = _execution_mode(
        temporal_resolution,
        start_dt,
        end_dt,
        analysis_intent,
        query=query,
    )
    image_count = _estimate_image_count(temporal_resolution, start_dt, end_dt)

    alternatives = [
        {
            "name": d.name,
            "dataset_id": d.dataset_id,
            "band": d.band,
            "supported_range": f"{d.start_date.isoformat()} to {d.end_date.isoformat()}",
        }
        for d in DATASETS
        if d.temporal_resolution == temporal_resolution and d.dataset_id != spec.dataset_id
    ]

    message = "supported"
    if not supported:
        message = (
            f"Requested range {start_dt.isoformat()} to {end_dt.isoformat()} is outside "
            f"{spec.name} coverage {spec.start_date.isoformat()} to {spec.end_date.isoformat()}."
        )

    if mode == "direct_download":
        next_action = (
            f"Route to Data_Searcher direct retrieval via NTL_download_tool "
            f"(estimated {image_count} {temporal_resolution} images, lightweight request)."
        )
    else:
        next_action = (
            f"Route to Data_Searcher server-side planning output, then pass blueprint to "
            f"Code_Assistant execution (estimated {image_count} {temporal_resolution} images "
            f"or heavy analysis intent: {analysis_intent})."
        )

    result = {
        "query": query,
        "status": "supported" if supported else "unsupported",
        "message": message,
        "selected_dataset": {
            "name": spec.name,
            "dataset_id": spec.dataset_id,
            "band": spec.band,
            "spatial_resolution_m": spec.spatial_resolution_m,
            "supported_range": f"{spec.start_date.isoformat()} to {spec.end_date.isoformat()}",
            "note": spec.note,
        },
        "requested_range": f"{start_dt.isoformat()} to {end_dt.isoformat()}",
        "estimated_image_count": image_count,
        "recommended_execution_mode": mode,
        "alternatives": alternatives,
        "next_action": next_action,
    }
    return json.dumps(result, indent=2, ensure_ascii=False)


class GEEScriptBlueprintInput(BaseModel):
    language: Literal["python", "javascript"] = Field(
        default="python",
        description="Generate script template for Python or JavaScript.",
    )
    dataset_id: str = Field(..., description="Earth Engine dataset ID.")
    band: str = Field(..., description="Band name to use.")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD.")
    end_date: str = Field(..., description="End date in YYYY-MM-DD.")
    analysis_mode: Literal["single_stat", "time_series", "zonal_stats", "composite_export"] = Field(
        default="time_series",
        description="Type of analysis workflow.",
    )
    reducer: Literal["mean", "sum", "max", "min"] = Field(
        default="mean",
        description="Reducer for server-side calculations.",
    )
    output_format: Literal["csv", "tif"] = Field(
        default="csv",
        description="Expected exported output format.",
    )
    output_filename: str = Field(
        ...,
        description="Output filename without absolute path. For Python it will be resolved with storage_manager.",
    )
    region_template: str = Field(
        default="ee.Geometry.Rectangle([120.9, 30.9, 122.1, 31.9])",
        description="Python/JS expression used as region placeholder.",
    )
    zones_template: str = Field(
        default='ee.FeatureCollection("FAO/GAUL/2015/level1").filterBounds(region)',
        description=(
            "Python/JS expression for zonal-statistics zones. For China province-level stats, use "
            "ee.FeatureCollection('projects/empyrean-caster-430308-m2/assets/province')."
        ),
    )


def _python_blueprint(
    dataset_id: str,
    band: str,
    start_date: str,
    end_date: str,
    analysis_mode: str,
    reducer: str,
    output_format: str,
    output_filename: str,
    region_template: str,
    zones_template: str,
) -> str:
    reducer_expr = f"ee.Reducer.{reducer}()"

    if output_format == "tif":
        return f"""import ee
import geemap
from storage_manager import storage_manager

PROJECT_ID = "{REQUIRED_GEE_PROJECT}"
ee.Initialize(project=PROJECT_ID)

region = {region_template}
image = (
    ee.ImageCollection("{dataset_id}")
    .filterDate("{start_date}", "{end_date}")
    .select("{band}")
    .mean()
    .clip(region)
)

out_tif = storage_manager.resolve_output_path("{output_filename}")
geemap.ee_export_image(
    ee_object=image,
    filename=out_tif,
    scale=500,
    region=region,
    crs="EPSG:4326",
    file_per_band=False,
)
print(out_tif)
"""

    if analysis_mode == "zonal_stats":
        return f"""import ee
import pandas as pd
from storage_manager import storage_manager

PROJECT_ID = "{REQUIRED_GEE_PROJECT}"
ee.Initialize(project=PROJECT_ID)

region = {region_template}
zones = {zones_template}

image = (
    ee.ImageCollection("{dataset_id}")
    .filterDate("{start_date}", "{end_date}")
    .select("{band}")
    .mean()
)

stats_fc = image.reduceRegions(
    collection=zones,
    reducer={reducer_expr},
    scale=500,
    maxPixelsPerRegion=1e13,
)

rows = [f["properties"] for f in stats_fc.getInfo()["features"]]
df = pd.DataFrame(rows)
out_csv = storage_manager.resolve_output_path("{output_filename}")
df.to_csv(out_csv, index=False)
print(out_csv)
"""

    if analysis_mode == "single_stat":
        return f"""import ee
import pandas as pd
from storage_manager import storage_manager

PROJECT_ID = "{REQUIRED_GEE_PROJECT}"
ee.Initialize(project=PROJECT_ID)

region = {region_template}
image = (
    ee.ImageCollection("{dataset_id}")
    .filterDate("{start_date}", "{end_date}")
    .select("{band}")
    .mean()
)

value = image.reduceRegion(
    reducer={reducer_expr},
    geometry=region,
    scale=500,
    maxPixels=1e13,
    bestEffort=True,
).get("{band}")

out_csv = storage_manager.resolve_output_path("{output_filename}")
pd.DataFrame([{{"metric": "{reducer}", "value": value.getInfo()}}]).to_csv(out_csv, index=False)
print(out_csv)
"""

    return f"""import ee
import pandas as pd
from storage_manager import storage_manager

PROJECT_ID = "{REQUIRED_GEE_PROJECT}"
ee.Initialize(project=PROJECT_ID)

region = {region_template}
collection = (
    ee.ImageCollection("{dataset_id}")
    .filterDate("{start_date}", "{end_date}")
    .filterBounds(region)
    .select("{band}")
)

def per_image_stat(img):
    value = img.reduceRegion(
        reducer={reducer_expr},
        geometry=region,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
    ).get("{band}")
    return ee.Feature(None, {{
        "date": img.date().format("YYYY-MM-dd"),
        "value": value,
    }})

stats_fc = ee.FeatureCollection(collection.map(per_image_stat))
rows = [f["properties"] for f in stats_fc.getInfo()["features"]]
df = pd.DataFrame(rows).sort_values("date")
out_csv = storage_manager.resolve_output_path("{output_filename}")
df.to_csv(out_csv, index=False)
print(out_csv)
"""


def _javascript_blueprint(
    dataset_id: str,
    band: str,
    start_date: str,
    end_date: str,
    analysis_mode: str,
    reducer: str,
    output_format: str,
    output_filename: str,
    region_template: str,
    zones_template: str,
) -> str:
    if output_format == "tif":
        return f"""var region = {region_template};
var image = ee.ImageCollection("{dataset_id}")
  .filterDate("{start_date}", "{end_date}")
  .select("{band}")
  .mean()
  .clip(region);

Map.centerObject(region, 8);
Map.addLayer(image, {{min: 0, max: 60}}, "NTL");

Export.image.toDrive({{
  image: image,
  description: "{output_filename}".replace(".tif", ""),
  fileNamePrefix: "{output_filename}".replace(".tif", ""),
  region: region,
  scale: 500,
  maxPixels: 1e13
}});
"""

    if analysis_mode == "zonal_stats":
        return f"""var region = {region_template};
var zones = {zones_template};
var image = ee.ImageCollection("{dataset_id}")
  .filterDate("{start_date}", "{end_date}")
  .select("{band}")
  .mean();

var stats = image.reduceRegions({{
  collection: zones,
  reducer: ee.Reducer.{reducer}(),
  scale: 500,
  maxPixelsPerRegion: 1e13
}});

Export.table.toDrive({{
  collection: stats,
  description: "{output_filename}".replace(".csv", ""),
  fileNamePrefix: "{output_filename}".replace(".csv", ""),
  fileFormat: "CSV"
}});
"""

    return f"""var region = {region_template};
var col = ee.ImageCollection("{dataset_id}")
  .filterDate("{start_date}", "{end_date}")
  .filterBounds(region)
  .select("{band}");

var stats = col.map(function(img) {{
  var val = img.reduceRegion({{
    reducer: ee.Reducer.{reducer}(),
    geometry: region,
    scale: 500,
    maxPixels: 1e13,
    bestEffort: true
  }}).get("{band}");
  return ee.Feature(null, {{
    date: img.date().format("YYYY-MM-dd"),
    value: val
  }});
}});

Export.table.toDrive({{
  collection: ee.FeatureCollection(stats),
  description: "{output_filename}".replace(".csv", ""),
  fileNamePrefix: "{output_filename}".replace(".csv", ""),
  fileFormat: "CSV"
}});
"""


def gee_script_blueprint(
    language: str = "python",
    dataset_id: str = "",
    band: str = "",
    start_date: str = "",
    end_date: str = "",
    analysis_mode: str = "time_series",
    reducer: str = "mean",
    output_format: str = "csv",
    output_filename: str = "gee_result.csv",
    region_template: str = "ee.Geometry.Rectangle([120.9, 30.9, 122.1, 31.9])",
    zones_template: str = 'ee.FeatureCollection("FAO/GAUL/2015/level1").filterBounds(region)',
) -> str:
    if language == "python":
        script = _python_blueprint(
            dataset_id=dataset_id,
            band=band,
            start_date=start_date,
            end_date=end_date,
            analysis_mode=analysis_mode,
            reducer=reducer,
            output_format=output_format,
            output_filename=output_filename,
            region_template=region_template,
            zones_template=zones_template,
        )
    else:
        script = _javascript_blueprint(
            dataset_id=dataset_id,
            band=band,
            start_date=start_date,
            end_date=end_date,
            analysis_mode=analysis_mode,
            reducer=reducer,
            output_format=output_format,
            output_filename=output_filename,
            region_template=region_template,
            zones_template=zones_template,
        )

    payload = {
        "language": language,
        "dataset_id": dataset_id,
        "band": band,
        "analysis_mode": analysis_mode,
        "output_format": output_format,
        "output_filename": output_filename,
        "zones_template": zones_template if analysis_mode == "zonal_stats" else None,
        "script": script,
        "notes": [
            "For Python execution in this project, keep storage_manager path resolution.",
            "Validate with GeoCode_COT_Validation_tool block-by-block before final execution.",
            f"Use ee.Initialize(project='{REQUIRED_GEE_PROJECT}').",
        ],
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


class GEECatalogDiscoveryInput(BaseModel):
    query: str = Field(..., description="Dataset discovery query, e.g. VIIRS monthly nightlight.")
    max_results: int = Field(default=8, ge=1, le=20, description="Maximum number of candidates to return.")
    temporal_resolution: Optional[Literal["annual", "monthly", "daily"]] = Field(
        default=None,
        description="Optional filter hint for annual/monthly/daily use cases.",
    )


class GEEDatasetMetadataInput(BaseModel):
    dataset_id: str = Field(..., description="Exact Earth Engine dataset ID, e.g., NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG.")
    check_temporal: bool = Field(
        default=True,
        description="If true, try to infer temporal coverage for image collections.",
    )


class DatasetLatestAvailabilityInput(BaseModel):
    gee_dataset_ids: Optional[List[str]] = Field(
        default=None,
        description="Optional Earth Engine dataset ids to check for latest available date.",
    )
    laads_short_names: Optional[List[str]] = Field(
        default=None,
        description="Optional NASA LAADS/CMR short_name values to check for latest granule day, e.g. VNP46A2 or VJ102DNB.",
    )
    requested_end_date: Optional[str] = Field(
        default=None,
        description="Optional requested final observation date in YYYY-MM-DD, YYYY-MM, or YYYY format.",
    )
    bbox: Optional[str] = Field(
        default=None,
        description="Optional bbox for LAADS/CMR queries as minx,miny,maxx,maxy.",
    )
    lookback_days: int = Field(
        default=30,
        description="How many days back to search when checking LAADS/CMR latest availability.",
    )


def _extract_dataset_id_from_catalog_page(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        text = resp.text
    except Exception:
        return None

    patterns = [
        r"ee\.(?:ImageCollection|Image|FeatureCollection)\(\s*['\"]([^'\"]+)['\"]\s*\)",
        r"['\"]dataset_id['\"]\s*:\s*['\"]([^'\"]+)['\"]",
        r"\b(?:projects/[A-Za-z0-9._-]+/[A-Za-z0-9._\-/]+|[A-Z][A-Z0-9_]+(?:/[A-Za-z0-9._-]+){2,})\b",
    ]
    for pat in patterns:
        matches = re.findall(pat, text, flags=re.IGNORECASE)
        for m in matches:
            if "/" in m and not m.startswith("http"):
                return m
    return None


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _tokenize_query(query: str) -> List[str]:
    tokens = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) >= 2]
    return [t for t in tokens if t not in _COMMON_QUERY_STOPWORDS]


def _char_ngrams(text: str, n: int = 3) -> set:
    s = re.sub(r"[^a-z0-9]+", "", text.lower())
    if len(s) < n:
        return {s} if s else set()
    return {s[i : i + n] for i in range(len(s) - n + 1)}


def _jaccard_similarity(a: str, b: str) -> float:
    a_grams = _char_ngrams(a)
    b_grams = _char_ngrams(b)
    if not a_grams or not b_grams:
        return 0.0
    inter = len(a_grams & b_grams)
    union = len(a_grams | b_grams)
    return inter / union if union else 0.0


def _score_catalog_item(item: Dict, query: str) -> int:
    dataset_id = str(item.get("dataset_id") or "")
    title = str(item.get("title") or "")
    desc = str(item.get("description") or "")
    comment = str(item.get("raw_comment") or "")

    q_norm = _normalize_text(query)
    q_tokens = _tokenize_query(query)
    if not q_norm:
        return 0
    if not q_tokens:
        q_tokens = [t for t in re.findall(r"[a-z0-9]+", q_norm) if len(t) >= 2]
    if not q_tokens:
        return 0

    id_norm = _normalize_text(dataset_id)
    title_norm = _normalize_text(title)
    desc_norm = _normalize_text(desc)
    comment_norm = _normalize_text(comment)
    id_tokens = set(re.findall(r"[a-z0-9]+", id_norm))
    title_tokens = set(re.findall(r"[a-z0-9]+", title_norm))
    desc_tokens = set(re.findall(r"[a-z0-9]+", f"{desc_norm} {comment_norm}"))

    score = 0

    # Stage 1: strong exact/substring signals (mirrors typical catalog search expectations).
    if dataset_id and q_norm == id_norm:
        score += 240
    if dataset_id and q_norm in id_norm:
        score += 120
    if title and q_norm in title_norm:
        score += 80

    # Stage 2: weighted lexical matching across ID/title/description.
    covered = 0
    core_covered = 0
    for tok in q_tokens:
        tok_hit = False
        if tok in id_tokens:
            score += 24
            tok_hit = True
            core_covered += 1
        elif tok in id_norm:
            score += 10
            tok_hit = True

        if tok in title_tokens:
            score += 18
            tok_hit = True
            core_covered += 1
        elif tok in title_norm:
            score += 8
            tok_hit = True

        if tok in desc_tokens:
            score += 6
            tok_hit = True
        elif tok in desc_norm:
            score += 2
            tok_hit = True

        if tok_hit:
            covered += 1

    score += covered * 2
    if covered == len(q_tokens):
        score += 18
    if core_covered >= max(1, len(q_tokens) // 2):
        score += 8

    # Stage 3: robust fuzzy similarity for naming variants (no manual synonym dictionary).
    sim = max(
        _jaccard_similarity(query, dataset_id),
        _jaccard_similarity(query, title),
    )
    score += int(sim * 40)

    return score


def _clean_text(html_frag: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", html_frag)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _parse_catalog_blocks(html: str) -> List[Dict]:
    items: List[Dict] = []

    # Each dataset card is a <li class='ee-sample-image ...'> block.
    blocks = re.findall(
        r"<li class='ee-sample-image[\s\S]*?</li>",
        html,
        flags=re.IGNORECASE,
    )

    for b in blocks:
        href_match = re.search(r'href="(/earth-engine/datasets/catalog/[^"]+)"', b, flags=re.IGNORECASE)
        title_match = re.search(r"<h3[^>]*>([\s\S]*?)</h3>", b, flags=re.IGNORECASE)
        desc_match = re.search(
            r'<td class="ee-dataset-description-snippet">([\s\S]*?)</td>',
            b,
            flags=re.IGNORECASE,
        )
        comment_match = re.search(r"<!--([\s\S]*?)-->", b, flags=re.IGNORECASE)

        href = href_match.group(1) if href_match else None
        title = _clean_text(title_match.group(1)) if title_match else ""
        desc = _clean_text(desc_match.group(1)) if desc_match else ""
        comment = comment_match.group(1).strip() if comment_match else ""

        dataset_id = None
        if comment:
            id_match = re.search(
                r"(projects/[A-Za-z0-9._-]+/[A-Za-z0-9._\-/]+|[A-Z][A-Z0-9_]+(?:/[A-Za-z0-9._-]+){2,})",
                comment,
            )
            if id_match:
                dataset_id = id_match.group(1)

        if not href and not dataset_id and not title:
            continue

        items.append(
            {
                "dataset_id": dataset_id,
                "title": title,
                "description": desc,
                "catalog_url": f"https://developers.google.com{href}" if href else None,
                "raw_comment": comment,
            }
        )
    return items


def _load_catalog_items() -> List[Dict]:
    now = time.time()
    cached_items = _CATALOG_CACHE.get("items", [])
    fetched_at = float(_CATALOG_CACHE.get("fetched_at", 0.0) or 0.0)
    if cached_items and (now - fetched_at) < 3600:
        return cached_items  # 1-hour TTL

    html = requests.get(EE_CATALOG_PAGE, timeout=20).text
    items = _parse_catalog_blocks(html)
    _CATALOG_CACHE["items"] = items
    _CATALOG_CACHE["fetched_at"] = now
    return items


def _enrich_dataset_ids(candidates: List[Dict], max_to_resolve: int = 5) -> None:
    resolved = 0
    for rec in candidates:
        if resolved >= max_to_resolve:
            return

        dataset_id = rec.get("dataset_id")
        catalog_url = rec.get("catalog_url")
        if dataset_id or not catalog_url:
            continue

        if catalog_url in _DATASET_ID_CACHE:
            rec["dataset_id"] = _DATASET_ID_CACHE[catalog_url]
            continue

        inferred = _extract_dataset_id_from_catalog_page(catalog_url)
        if inferred:
            _DATASET_ID_CACHE[catalog_url] = inferred
            rec["dataset_id"] = inferred
            resolved += 1


def gee_catalog_discovery(
    query: str,
    max_results: int = 8,
    temporal_resolution: Optional[str] = None,
) -> str:
    # Official-catalog-page indexed retrieval with weighted lexical ranking.
    official_candidates: List[Dict] = []
    try:
        items = _load_catalog_items()
        scored: List[Tuple[int, Dict]] = []
        for it in items:
            score = _score_catalog_item(it, query)
            if score > 0:
                scored.append((score, it))
        scored.sort(key=lambda x: x[0], reverse=True)
        for score, it in scored[:max_results]:
            rec = dict(it)
            rec["match_score"] = score
            official_candidates.append(rec)
        _enrich_dataset_ids(official_candidates, max_to_resolve=min(5, max_results))
    except Exception as exc:  # noqa: BLE001
        payload = {
            "status": "error",
            "query": query,
            "official_catalog_page": EE_CATALOG_PAGE,
            "error": str(exc),
            "fallback": {
                "tool": "Tavily_search",
                "query_hint": f'site:developers.google.com/earth-engine/datasets "{query}"',
            },
        }
        return json.dumps(payload, indent=2, ensure_ascii=False)

    if temporal_resolution:
        # Optional temporal filtering only works for built-in known datasets.
        filtered = []
        for c in official_candidates:
            did = c.get("dataset_id")
            spec = DATASET_BY_ID.get(did) if did else None
            if spec is None or spec.temporal_resolution == temporal_resolution:
                filtered.append(c)
        official_candidates = filtered

    official_ids = [r.get("dataset_id") for r in official_candidates if r.get("dataset_id")]
    dataset_ids = []
    for did in official_ids:
        if did and did not in dataset_ids:
            dataset_ids.append(did)

    known_matches = []
    unknown_candidates = []
    for did in dataset_ids:
        spec = DATASET_BY_ID.get(did)
        if spec:
            known_matches.append(
                {
                    "dataset_id": spec.dataset_id,
                    "name": spec.name,
                    "temporal_resolution": spec.temporal_resolution,
                    "band": spec.band,
                    "supported_range": f"{spec.start_date.isoformat()} to {spec.end_date.isoformat()}",
                    "note": spec.note,
                }
            )
        else:
            unknown_candidates.append(did)

    if temporal_resolution:
        known_matches = [m for m in known_matches if m["temporal_resolution"] == temporal_resolution]

    payload = {
        "status": "ok",
        "query": query,
        "official_catalog_page": EE_CATALOG_PAGE,
        "official_candidates": official_candidates,
        "retrieval_backend": "official_catalog_page_weighted_index",
        "candidates": official_candidates,
        "known_matches": known_matches,
        "unknown_candidates": unknown_candidates,
        "next_action": [
            "If known_matches is non-empty, use selected dataset_id directly.",
            "If known_matches is empty, DO NOT conclude the dataset is unavailable. Check official_candidates first.",
            "Prefer official_candidates with higher match_score and non-empty dataset_id, then validate with GEE_dataset_metadata_tool.",
            "For unknown_candidates, call GEE_dataset_metadata_tool with chosen dataset_id.",
            "Use Tavily_search only as fallback for docs/examples, not as primary catalog truth.",
        ],
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


def _millis_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")
    except Exception:
        return None


def _temporal_resolution_for_dataset(known: Optional[DatasetSpec], curated: Optional[Dict]) -> Optional[str]:
    if known:
        return known.temporal_resolution
    if curated and curated.get("temporal_resolution"):
        return str(curated.get("temporal_resolution"))
    return None


def _latest_period_from_anchor(anchor_date: Optional[str], temporal_resolution: Optional[str]) -> Optional[str]:
    if not anchor_date:
        return None
    if temporal_resolution == "annual":
        return anchor_date[:4]
    if temporal_resolution == "monthly":
        return anchor_date[:7]
    return anchor_date


def _latest_date_semantics(temporal_resolution: Optional[str]) -> str:
    if temporal_resolution == "annual":
        return "period_start_anchor_for_annual_composite"
    if temporal_resolution == "monthly":
        return "period_start_anchor_for_monthly_composite"
    return "observation_date"


def gee_dataset_metadata(dataset_id: str, check_temporal: bool = True) -> str:
    known = DATASET_BY_ID.get(dataset_id)
    curated = _find_curated_dataset_entry(dataset_id)
    temporal_resolution = _temporal_resolution_for_dataset(known, curated)
    base_result = {
        "dataset_id": dataset_id,
        "status": "ok",
        "asset_type": None,
        "temporal_resolution": temporal_resolution,
        "band_names": [],
        "collection_size": None,
        "temporal_coverage": {"start": None, "end": None},
        "latest_available_date": None,
        "latest_available_period": None,
        "latest_date_semantics": _latest_date_semantics(temporal_resolution),
        "source": "earthengine-api",
    }
    if curated:
        base_result.update(
            {
                "asset_type": curated.get("asset_type"),
                "band_names": _curated_band_names(curated),
                "curated_dataset_key": curated.get("key"),
                "curated_category": curated.get("category"),
                "curated_use_when": curated.get("use_when", []),
                "curated_avoid_when": curated.get("avoid_when", []),
                "curated_scale_m": curated.get("scale_m"),
                "curated_primary_bands": curated.get("primary_bands", []),
                "curated_expected_temporal_coverage": curated.get("expected_temporal_coverage"),
                "source": "curated_registry+earthengine-api",
            }
        )
    if known:
        base_result.update(
            {
                "asset_type": "ImageCollection",
                "band_names": [known.band],
                "temporal_coverage": {
                    "start": known.start_date.isoformat(),
                    "end": known.end_date.isoformat(),
                },
                "latest_available_date": known.end_date.isoformat(),
                "known_dataset_note": known.note,
                "source": "built_in_catalog+earthengine-api",
            }
        )
        base_result["latest_available_period"] = _latest_period_from_anchor(
            known.end_date.isoformat(),
            temporal_resolution,
        )

    try:
        import ee  # type: ignore
    except Exception as exc:  # noqa: BLE001
        if known:
            base_result["status"] = "partial"
            base_result["source"] = "built_in_catalog"
            base_result["warning"] = f"earthengine-api import failed: {exc}"
            return json.dumps(base_result, indent=2, ensure_ascii=False)
        if curated:
            base_result["status"] = "partial"
            base_result["source"] = "curated_registry"
            base_result["warning"] = f"earthengine-api import failed: {exc}"
            return json.dumps(base_result, indent=2, ensure_ascii=False)
        return json.dumps(
            {"status": "error", "dataset_id": dataset_id, "error": f"earthengine-api import failed: {exc}"},
            indent=2,
            ensure_ascii=False,
        )

    try:
        ee.Initialize(project=REQUIRED_GEE_PROJECT)
    except Exception as exc:
        if known:
            base_result["status"] = "partial"
            base_result["source"] = "built_in_catalog"
            base_result["warning"] = f"ee.Initialize failed: {exc}"
            return json.dumps(base_result, indent=2, ensure_ascii=False)
        if curated:
            base_result["status"] = "partial"
            base_result["source"] = "curated_registry"
            base_result["warning"] = f"ee.Initialize failed: {exc}"
            return json.dumps(base_result, indent=2, ensure_ascii=False)

    result = dict(base_result)

    # 1) Try as ImageCollection
    try:
        col = ee.ImageCollection(dataset_id)
        size = col.size().getInfo()
        first = ee.Image(col.first())
        result["asset_type"] = "ImageCollection"
        result["collection_size"] = size
        result["band_names"] = first.bandNames().getInfo()
        if check_temporal:
            start_ms = col.aggregate_min("system:time_start").getInfo()
            end_ms = col.aggregate_max("system:time_start").getInfo()
            result["temporal_coverage"] = {
                "start": _millis_to_iso(start_ms),
                "end": _millis_to_iso(end_ms),
            }
            result["latest_available_date"] = result["temporal_coverage"]["end"]
            result["latest_available_period"] = _latest_period_from_anchor(
                result["latest_available_date"],
                result.get("temporal_resolution"),
            )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception:
        pass

    # 2) Try as Image
    try:
        img = ee.Image(dataset_id)
        result["asset_type"] = "Image"
        result["band_names"] = img.bandNames().getInfo()
        time_start = img.get("system:time_start").getInfo()
        if time_start is not None:
            result["latest_available_date"] = _millis_to_iso(time_start)
            result["latest_available_period"] = _latest_period_from_anchor(
                result["latest_available_date"],
                result.get("temporal_resolution"),
            )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception:
        pass

    # 3) Try as FeatureCollection
    try:
        fc = ee.FeatureCollection(dataset_id)
        result["asset_type"] = "FeatureCollection"
        result["collection_size"] = fc.size().getInfo()
        first = ee.Feature(fc.first()).toDictionary().getInfo()
        result["sample_properties"] = list(first.keys()) if isinstance(first, dict) else []
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001
        return json.dumps(
            {
                "status": "error",
                "dataset_id": dataset_id,
                "error": str(exc),
                "hint": "Use GEE_catalog_discovery_tool or Tavily_search to verify dataset id and catalog page first.",
            },
            indent=2,
            ensure_ascii=False,
        )


def _parse_bbox_text(raw: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    if not raw:
        return None
    parts = [p.strip() for p in str(raw).split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = (float(x) for x in parts)
    if maxx <= minx or maxy <= miny:
        raise ValueError("Invalid bbox: maxx>minx and maxy>miny required.")
    return minx, miny, maxx, maxy


def _compare_requested_end_date(
    latest_value: Optional[str],
    requested_end_date: Optional[str],
    temporal_resolution: Optional[str] = None,
) -> str:
    if not requested_end_date:
        return "not_requested"
    if not latest_value:
        return "unknown"
    try:
        latest = datetime.strptime(latest_value, "%Y-%m-%d").date()
    except Exception:
        return "unknown"
    try:
        raw = str(requested_end_date).strip()
        if temporal_resolution == "annual":
            requested_year = _parse_date(raw, "end").year
            return "available" if requested_year <= latest.year else "not_yet_available"
        if temporal_resolution == "monthly":
            requested_month = _parse_date(raw, "end")
            return (
                "available"
                if (requested_month.year, requested_month.month) <= (latest.year, latest.month)
                else "not_yet_available"
            )
        requested = _parse_date(raw, "end")
        return "available" if requested <= latest else "not_yet_available"
    except Exception:
        return "unknown"


def dataset_latest_availability(
    gee_dataset_ids: Optional[List[str]] = None,
    laads_short_names: Optional[List[str]] = None,
    requested_end_date: Optional[str] = None,
    bbox: Optional[str] = None,
    lookback_days: int = 30,
) -> str:
    checks: List[Dict[str, object]] = []

    for dataset_id in gee_dataset_ids or []:
        payload = json.loads(gee_dataset_metadata(dataset_id, check_temporal=True))
        payload["source"] = "gee"
        payload["coverage_status"] = _compare_requested_end_date(
            payload.get("latest_available_date"),
            requested_end_date,
            payload.get("temporal_resolution"),
        )
        checks.append(payload)

    if laads_short_names:
        from experiments.official_daily_ntl_fastpath.cmr_client import search_granules, select_latest_day_entries

        bbox_value = _parse_bbox_text(bbox)
        search_end = _today()
        search_start = search_end - timedelta(days=max(1, int(lookback_days)))
        for short_name in laads_short_names:
            granules = search_granules(
                short_name=str(short_name).strip(),
                start_date=search_start.isoformat(),
                end_date=search_end.isoformat(),
                bbox=bbox_value,
                page_size=200,
            )
            latest_day, entries = select_latest_day_entries(granules, night_only=False)
            checks.append(
                {
                    "source": "laads_cmr",
                    "short_name": short_name,
                    "status": "ok" if latest_day else "empty",
                    "temporal_resolution": "daily",
                    "search_window": {"start": search_start.isoformat(), "end": search_end.isoformat()},
                    "granule_count": len(granules),
                    "latest_available_date": latest_day,
                    "latest_available_period": latest_day,
                    "latest_date_semantics": "observation_date",
                    "latest_granule_ids": [g.producer_granule_id for g in entries[:5]],
                    "coverage_status": _compare_requested_end_date(latest_day, requested_end_date, "daily"),
                }
            )

    if not checks:
        return json.dumps(
            {
                "status": "error",
                "error": "Provide at least one gee_dataset_id or laads_short_name.",
            },
            indent=2,
            ensure_ascii=False,
        )

    statuses = [str(item.get("coverage_status")) for item in checks]
    if requested_end_date:
        if statuses and all(status == "available" for status in statuses):
            overall_status = "available"
        elif any(status == "available" for status in statuses):
            overall_status = "mixed"
        elif all(status == "not_yet_available" for status in statuses):
            overall_status = "not_yet_available"
        else:
            overall_status = "unknown"
    else:
        overall_status = "checked"

    payload = {
        "status": "ok",
        "requested_end_date": requested_end_date,
        "overall_status": overall_status,
        "checks": checks,
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


GEE_dataset_router_tool = StructuredTool.from_function(
    func=gee_dataset_router,
    name="GEE_dataset_router_tool",
    description=(
        "Route GEE tasks to the best dataset and execution mode. "
        "Validates temporal coverage and decides between direct download vs server-side processing."
    ),
    args_schema=GEEDatasetRouterInput,
)


GEE_script_blueprint_tool = StructuredTool.from_function(
    func=gee_script_blueprint,
    name="GEE_script_blueprint_tool",
    description=(
        "Generate GEE Python or JavaScript script blueprints for retrieval, analysis, and export workflows. "
        "Python templates are compatible with storage_manager and Geo-CodeCoT execution."
    ),
    args_schema=GEEScriptBlueprintInput,
)


GEE_catalog_discovery_tool = StructuredTool.from_function(
    func=gee_catalog_discovery,
    name="GEE_catalog_discovery_tool",
    description=(
        "Discover candidate GEE datasets from the Earth Engine catalog by natural-language query. "
        "Returns catalog URLs, known matches, and unknown candidates for metadata validation."
    ),
    args_schema=GEECatalogDiscoveryInput,
)


GEE_dataset_metadata_tool = StructuredTool.from_function(
    func=gee_dataset_metadata,
    name="GEE_dataset_metadata_tool",
    description=(
        "Validate metadata for any Earth Engine dataset id (ImageCollection/Image/FeatureCollection), "
        "including band names and temporal coverage when available."
    ),
    args_schema=GEEDatasetMetadataInput,
)


dataset_latest_availability_tool = StructuredTool.from_function(
    func=dataset_latest_availability,
    name="dataset_latest_availability_tool",
    description=(
        "Check latest data availability for one or more GEE datasets and/or NASA LAADS/CMR short_name products. "
        "Use before recent daily/event analysis to verify whether the requested end date is already available."
    ),
    args_schema=DatasetLatestAvailabilityInput,
)
