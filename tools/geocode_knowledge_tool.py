from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field


class GeoCodeKnowledgeInput(BaseModel):
    query: str = Field(..., description="Task description or coding question for geospatial analysis.")
    top_k: int = Field(default=3, ge=1, le=6, description="Number of recipes to return.")
    library_focus: Optional[str] = Field(
        default=None,
        description="Optional preferred library, e.g. gee, rasterio, geopandas, shapely.",
    )
    include_runtime: bool = Field(
        default=True,
        description=(
            "Whether to include curated runtime templates from "
            "RAG/code_guide/tools_latest_runtime_curated."
        ),
    )


ROOT_DIR = Path(__file__).resolve().parents[1]
CURATED_RUNTIME_DIR = ROOT_DIR / "RAG" / "code_guide" / "tools_latest_runtime_curated"
RUNTIME_MAX_POOL = 12
RUNTIME_CODE_MAX_CHARS = 2600


GEOCODE_RECIPES: List[Dict[str, Any]] = [
    {
        "id": "gee_annual_zonal_antl_fastpath",
        "title": "GEE annual district ANTL stats (Amap boundary + NPP-VIIRS-Like, direct-download fastpath)",
        "tags": [
            "gee",
            "ntl",
            "annual",
            "zonal_stats",
            "csv",
            "amap_boundary",
            "direct_download",
            "small_workload",
        ],
        "libraries": ["earthengine-api", "geopandas", "pandas"],
        "source": "static_recipe",
        "why_it_works": (
            "Uses Amap-derived district boundary from local workspace and computes zonal statistics "
            "on one annual image, which is efficient for small workloads (<=6 images)."
        ),
        "code": """
import ee
import geopandas as gpd
import pandas as pd
import json
from storage_manager import storage_manager

project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

boundary_path = storage_manager.resolve_input_path('shanghai_districts_boundary.shp')
gdf = gpd.read_file(boundary_path)
if gdf.crs and gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(epsg=4326)
fc = ee.FeatureCollection(json.loads(gdf.to_json()))

img = (
    ee.ImageCollection('projects/sat-io/open-datasets/npp-viirs-ntl')
    .filterDate('2020-01-01', '2020-12-31')
    .select('b1')
    .first()
)

stats_fc = img.reduceRegions(
    collection=fc,
    reducer=ee.Reducer.mean().setOutputs(['ANTL']),
    scale=500,
    maxPixelsPerRegion=1e13,
    tileScale=4,
)

rows = [f['properties'] for f in stats_fc.getInfo()['features']]
out_csv = storage_manager.resolve_output_path('shanghai_districts_antl_2020.csv')
pd.DataFrame(rows).to_csv(out_csv, index=False)
print(out_csv)
""".strip(),
        "references": [
            "https://developers.google.com/earth-engine/apidocs/ee-image-reduceregions",
            "https://developers.google.com/earth-engine/guides/client_server",
        ],
    },
    {
        "id": "gee_annual_zonal_antl_legacy",
        "title": "GEE annual ANTL/TNTL zonal statistics by districts (legacy asset-boundary example)",
        "tags": ["gee", "ntl", "annual", "zonal_stats", "csv", "legacy"],
        "libraries": ["earthengine-api", "pandas"],
        "source": "static_recipe",
        "why_it_works": "Keeps aggregation server-side in Earth Engine and exports only tabular results.",
        "code": """
import ee
import pandas as pd
from storage_manager import storage_manager

project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

year = 2022
region_name = 'Shanghai'
city_fc = ee.FeatureCollection('projects/empyrean-caster-430308-m2/assets/city')
region = city_fc.filter(ee.Filter.eq('name', region_name))

ntl = (
    ee.ImageCollection('NOAA/VIIRS/DNB/ANNUAL_V22')
    .filterDate(f'{year}-01-01', f'{year + 1}-01-01')
    .select('average')
    .mean()
)

stats_fc = ntl.reduceRegions(
    collection=region,
    reducer=ee.Reducer.mean().combine(ee.Reducer.sum(), sharedInputs=True),
    scale=500,
    maxPixelsPerRegion=1e13,
)

rows = []
for ft in stats_fc.getInfo()['features']:
    p = ft['properties']
    rows.append({
        'Region': p.get('name', 'unknown'),
        'Year': year,
        'ANTL': p.get('mean'),
        'TNTL': p.get('sum'),
    })

out_csv = storage_manager.resolve_output_path(f'shanghai_antl_tntl_{year}.csv')
pd.DataFrame(rows).to_csv(out_csv, index=False)
print(out_csv)
""".strip(),
        "references": [
            "https://developers.google.com/earth-engine/guides/reducers_reduce_region",
            "https://developers.google.com/earth-engine/apidocs/ee-image-reduceregions",
        ],
    },
    {
        "id": "gee_daily_series_server_side",
        "title": "GEE long daily time-series summary (server-side)",
        "tags": ["gee", "daily", "time_series", "server_side", "vnp46a2"],
        "libraries": ["earthengine-api", "pandas"],
        "source": "static_recipe",
        "why_it_works": "Avoids downloading hundreds of TIFFs and computes ANTL per date on the cloud.",
        "code": """
import ee
import pandas as pd
from storage_manager import storage_manager

project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

region = ee.FeatureCollection('FAO/GAUL/2015/level1').filter(
    ee.Filter.eq('ADM1_NAME', 'Shanghai')
)
geom = region.geometry()

col = (
    ee.ImageCollection('NASA/VIIRS/002/VNP46A2')
    .filterDate('2024-01-01', '2025-01-01')
    .select('Gap_Filled_DNB_BRDF_Corrected_NTL')
)

def per_image_stat(img):
    antl = img.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
    ).get('Gap_Filled_DNB_BRDF_Corrected_NTL')
    return ee.Feature(None, {
        'date': img.date().format('YYYY-MM-dd'),
        'ANTL': antl,
    })

fc = ee.FeatureCollection(col.map(per_image_stat))
records = [f['properties'] for f in fc.getInfo()['features']]

df = pd.DataFrame(records).sort_values('date')
out_csv = storage_manager.resolve_output_path('shanghai_daily_antl_2024.csv')
df.to_csv(out_csv, index=False)
print(out_csv)
""".strip(),
        "references": [
            "https://developers.google.com/earth-engine/guides/client_server",
            "https://developers.google.com/earth-engine/datasets/catalog/NASA_VIIRS_002_VNP46A2",
        ],
    },
    {
        "id": "local_raster_zonal_stats",
        "title": "Rasterio + GeoPandas zonal stats for ANTL/TNTL/LArea",
        "tags": ["rasterio", "geopandas", "zonal_stats", "ntl", "csv"],
        "libraries": ["rasterio", "geopandas", "numpy", "pandas"],
        "source": "static_recipe",
        "why_it_works": "Aligns CRS before masking and computes metrics per polygon with clear output schema.",
        "code": """
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from storage_manager import storage_manager

raster_path = storage_manager.resolve_input_path('shanghai_2022_ntl.tif')
vector_path = storage_manager.resolve_input_path('shanghai_districts.shp')
out_csv = storage_manager.resolve_output_path('shanghai_ntl_stats_2022.csv')

with rasterio.open(raster_path) as src:
    arr = src.read(1)
    nodata = src.nodata
    gdf = gpd.read_file(vector_path).to_crs(src.crs)

    pixel_area = abs(src.transform.a * src.transform.e)
    rows = []
    for _, row in gdf.iterrows():
        geom = [row.geometry]
        mask = geometry_mask(geom, transform=src.transform, invert=True, out_shape=arr.shape)
        vals = arr[mask]
        if nodata is not None:
            vals = vals[vals != nodata]
        vals = vals[np.isfinite(vals)]

        an = float(vals.mean()) if vals.size else np.nan
        tn = float(vals.sum()) if vals.size else np.nan
        la = float(vals.size * pixel_area) if vals.size else 0.0

        rows.append({
            'Region': row.get('name', 'unknown'),
            'ANTL': an,
            'TNTL': tn,
            'LArea': la,
        })

pd.DataFrame(rows).to_csv(out_csv, index=False)
print(out_csv)
""".strip(),
        "references": [
            "https://rasterio.readthedocs.io/en/stable/topics/features.html",
            "https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_crs.html",
        ],
    },
    {
        "id": "raster_alignment_reproject",
        "title": "Raster reprojection and grid alignment with Rasterio",
        "tags": ["rasterio", "reproject", "resample", "alignment"],
        "libraries": ["rasterio"],
        "source": "static_recipe",
        "why_it_works": "Produces an output raster aligned to a reference grid, reducing downstream mismatch errors.",
        "code": """
import rasterio
from rasterio.warp import reproject, Resampling
from storage_manager import storage_manager

src_path = storage_manager.resolve_input_path('ntl_source.tif')
ref_path = storage_manager.resolve_input_path('ntl_reference.tif')
out_path = storage_manager.resolve_output_path('ntl_source_aligned.tif')

with rasterio.open(src_path) as src, rasterio.open(ref_path) as ref:
    profile = src.profile.copy()
    profile.update({
        'crs': ref.crs,
        'transform': ref.transform,
        'width': ref.width,
        'height': ref.height,
    })

    with rasterio.open(out_path, 'w', **profile) as dst:
        for band_id in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, band_id),
                destination=rasterio.band(dst, band_id),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=ref.transform,
                dst_crs=ref.crs,
                resampling=Resampling.bilinear,
            )
print(out_path)
""".strip(),
        "references": [
            "https://rasterio.readthedocs.io/en/stable/topics/reproject.html",
        ],
    },
    {
        "id": "geopandas_spatial_join_poi",
        "title": "GeoPandas spatial join: POI count by district",
        "tags": ["geopandas", "spatial_join", "poi", "vector"],
        "libraries": ["geopandas", "pandas"],
        "source": "static_recipe",
        "why_it_works": "Uses explicit predicate join and outputs transparent district-level counts.",
        "code": """
import geopandas as gpd
import pandas as pd
from storage_manager import storage_manager

districts_path = storage_manager.resolve_input_path('shanghai_districts.shp')
poi_path = storage_manager.resolve_input_path('poi_points.geojson')
out_csv = storage_manager.resolve_output_path('poi_count_by_district.csv')

districts = gpd.read_file(districts_path)
pois = gpd.read_file(poi_path).to_crs(districts.crs)

joined = gpd.sjoin(pois, districts, how='inner', predicate='within')
result = joined.groupby('name').size().reset_index(name='POI_Count')
result.rename(columns={'name': 'Region'}, inplace=True)
result.to_csv(out_csv, index=False)
print(out_csv)
""".strip(),
        "references": [
            "https://geopandas.org/en/stable/docs/reference/api/geopandas.sjoin.html",
        ],
    },
    {
        "id": "shapely_fix_geometry",
        "title": "Repair invalid geometry before dissolve/overlay",
        "tags": ["shapely", "geopandas", "geometry", "topology"],
        "libraries": ["geopandas", "shapely"],
        "source": "static_recipe",
        "why_it_works": "Repairs invalid polygons to prevent topology exceptions during dissolve/overlay.",
        "code": """
import geopandas as gpd
from shapely import make_valid
from storage_manager import storage_manager

in_path = storage_manager.resolve_input_path('districts_raw.shp')
out_path = storage_manager.resolve_output_path('districts_valid.shp')

gdf = gpd.read_file(in_path)
gdf['geometry'] = gdf.geometry.apply(make_valid)
gdf = gdf[gdf.geometry.notnull()]
gdf = gdf[gdf.is_valid]
gdf.to_file(out_path)
print(out_path)
""".strip(),
        "references": [
            "https://shapely.readthedocs.io/en/2.1.1/reference/shapely.make_valid.html",
            "https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.overlay.html",
        ],
    },
]


def _compact_code(code: str, max_chars: int = RUNTIME_CODE_MAX_CHARS) -> tuple[str, bool]:
    text = code.strip()
    if len(text) <= max_chars:
        return text, False
    clipped = text[:max_chars].rstrip()
    return (
        clipped
        + "\n# ... truncated runtime template; use full_code_path to open the complete script.",
        True,
    )


def _runtime_tags(file_name: str, code: str) -> List[str]:
    low = f"{file_name}\n{code}".lower()
    tags: set[str] = {"runtime", "python"}
    patterns = {
        "gee": ("ee.initialize", "earth engine", "reduceregions", "imagecollection("),
        "ntl": ("ntl", "antl", "vnp46", "viirs"),
        "zonal_stats": ("reduceregions(", "zonal"),
        "gdp": ("gdp", "regression"),
        "disaster": ("earthquake", "impact", "recovery", "blackout", "flood", "wildfire"),
        "trend": ("trend", "slope", "cagr", "growth"),
    }
    for tag, keys in patterns.items():
        if any(k in low for k in keys):
            tags.add(tag)
    return sorted(tags)


@lru_cache(maxsize=1)
def _load_runtime_recipes() -> List[Dict[str, Any]]:
    if not CURATED_RUNTIME_DIR.exists():
        return []
    py_files = sorted(
        CURATED_RUNTIME_DIR.glob("*.py"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:RUNTIME_MAX_POOL]

    recipes: List[Dict[str, Any]] = []
    for py in py_files:
        try:
            code = py.read_text(encoding="utf-8")
        except Exception:
            continue
        compact_code, truncated = _compact_code(code)
        meta_path = py.with_suffix(".meta.json")
        summary = ""
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                summary = str(meta.get("stdout_excerpt", ""))[:300]
            except Exception:
                summary = ""

        recipes.append(
            {
                "id": f"runtime_{py.stem}",
                "title": f"Runtime template: {py.name}",
                "tags": _runtime_tags(py.name, code),
                "libraries": ["earthengine-api", "geopandas", "rasterio", "pandas"],
                "source": "runtime_curated",
                "why_it_works": (
                    "Successfully executed runtime script archived from recent project runs."
                    + (f" Excerpt: {summary}" if summary else "")
                ),
                "code": compact_code,
                "full_code_path": str(py),
                "code_truncated": truncated,
                "references": [],
            }
        )
    return recipes


def _normalize_tokens(text: str) -> List[str]:
    text = text.lower()
    zh_map = {
        "分区统计": "zonal_stats",
        "分区": "zonal_stats",
        "年": "annual",
        "月": "monthly",
        "日": "daily",
        "重投影": "reproject",
        "配准": "alignment",
        "空间连接": "spatial_join",
        "拓扑": "geometry",
        "无效几何": "geometry",
        "夜光": "ntl",
        "地球引擎": "gee",
    }
    expanded = text
    for zh, en in zh_map.items():
        if zh in text:
            expanded += f" {en}"
    return re.findall(r"[a-zA-Z_]+", expanded)


def _score_recipe(recipe: Dict[str, Any], tokens: List[str], library_focus: Optional[str]) -> int:
    score = 0
    searchable = " ".join(
        [
            recipe.get("id", ""),
            recipe.get("title", ""),
            " ".join(recipe.get("tags", [])),
            " ".join(recipe.get("libraries", [])),
            recipe.get("why_it_works", ""),
        ]
    ).lower()

    for tok in tokens:
        if tok in searchable:
            score += 3

    if library_focus:
        lf = library_focus.lower()
        if lf in searchable:
            score += 8

    if recipe.get("source") == "runtime_curated":
        score += 2

    return score


def retrieve_geocode_knowledge(
    query: str,
    top_k: int = 3,
    library_focus: Optional[str] = None,
    include_runtime: bool = True,
) -> str:
    tokens = _normalize_tokens(query)
    recipe_pool: List[Dict[str, Any]] = list(GEOCODE_RECIPES)
    runtime_count = 0
    if include_runtime:
        runtime = _load_runtime_recipes()
        runtime_count = len(runtime)
        recipe_pool.extend(runtime)

    ranked = sorted(
        recipe_pool,
        key=lambda r: _score_recipe(r, tokens, library_focus),
        reverse=True,
    )

    selected = ranked[:top_k]
    selected_runtime = sum(1 for item in selected if item.get("source") == "runtime_curated")
    payload = {
        "query": query,
        "library_focus": library_focus,
        "top_k": top_k,
        "include_runtime": include_runtime,
        "recipe_pool": {
            "static_count": len(GEOCODE_RECIPES),
            "runtime_curated_count": runtime_count,
            "selected_runtime_count": selected_runtime,
        },
        "matched_recipes": selected,
        "usage_note": (
            "Adapt recipe filenames to the current workspace and always resolve file paths via storage_manager. "
            "For long daily time-series, prioritize GEE server-side aggregation and export only final tables. "
            "For zonal_stats with image_count<=6 (daily/monthly/annual), prefer direct_download workflow."
        ),
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


GeoCode_Knowledge_Recipes_tool = StructuredTool.from_function(
    func=retrieve_geocode_knowledge,
    name="GeoCode_Knowledge_Recipes_tool",
    description=(
        "Retrieve project-aligned geospatial coding recipes for GEE, rasterio, geopandas, and shapely. "
        "Returns runnable template code snippets and official API references for NTL workflows."
    ),
    args_schema=GeoCodeKnowledgeInput,
)
