# Official Earth Engine Python API Patterns for 地缘环境智能计算平台

This reference distills stable patterns from the official Earth Engine Python client and documentation. Use it to write compact, table-oriented NTL scripts; do not paste broad official examples into generated code.

Primary sources:

- Earth Engine Python client repo: https://github.com/google/earthengine-api
- Authentication guide: https://developers.google.com/earth-engine/guides/auth
- Client/server guide: https://developers.google.com/earth-engine/guides/client_server
- API reference: https://developers.google.com/earth-engine/api_docs

Local source snapshot used for this reference:

- `_external_refs/earthengine-api`
- Commit: `580e4ca batch.py: Add type annotations`

## Initialization

Use an explicit Cloud project from runtime configuration:

```python
import os
import ee

project_id = os.environ.get("GEE_DEFAULT_PROJECT_ID")
if not project_id:
    raise ValueError("Set GEE_DEFAULT_PROJECT_ID before running this script.")
ee.Initialize(project=project_id)
```

Why:

- The Python client accepts `ee.Initialize(project=...)`.
- Missing or unauthorized quota projects can surface later on the first server call, not only during initialization.
- For 地缘环境智能计算平台, `USER_PROJECT_DENIED`, disabled API, missing IAM, or expired auth must stop the run and be reported instead of changing the dataset or algorithm.

## Date Filtering

Use half-open date ranges: start inclusive, end exclusive.

```python
collection = (
    ee.ImageCollection("projects/sat-io/open-datasets/npp-viirs-ntl")
    .filterDate("2020-01-01", "2021-01-01")
    .select("b1")
)
```

For one daily image after the product/index date has already been chosen:

```python
daily = (
    ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
    .filterDate("2025-04-01", "2025-04-02")
    .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
)
```

Avoid `"2020-01-01"` to `"2020-12-31"` for annual composites unless the dataset semantics explicitly require it; use the next day/year as the exclusive end.

## Collection Mapping

Use `ImageCollection.map()` to describe server-side work. The mapped Python function is captured once as an Earth Engine expression, so it must not depend on mutable client state, local file writes, print statements, or imperative side effects.

Good:

```python
def per_image(image):
    value = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=500,
        maxPixels=1e13,
        tileScale=4,
        bestEffort=True,
    ).get("b1")
    return ee.Feature(None, {
        "year": image.date().format("YYYY"),
        "ntl_mean": value,
    })

table = ee.FeatureCollection(collection.map(per_image))
```

Avoid:

- Calling `getInfo()` inside a mapped function.
- Appending to a Python list inside a mapped function.
- Reading local files inside a mapped function.

## `reduceRegion` for One Geometry

Use for one city/province/country geometry or a single buffer:

```python
stats = image.reduceRegion(
    reducer=ee.Reducer.mean(),
    geometry=region,
    scale=500,
    maxPixels=1e13,
    tileScale=4,
    bestEffort=True,
)
mean_value = stats.get("b1")
```

Notes:

- Set `scale` explicitly for NTL work.
- Use `tileScale=2`, `4`, or `8` when aggregation memory fails.
- `bestEffort=True` is acceptable for one-geometry summaries but should be documented because it may change effective scale.

## `reduceRegions` for Many Features

Use for province, district, grid, buffer, or other multi-feature zonal statistics:

```python
stats = image.rename("ntl_value").reduceRegions(
    collection=zones,
    reducer=ee.Reducer.mean(),
    scale=500,
    tileScale=4,
    maxPixelsPerRegion=1e13,
)
```

Normalize output before bringing compact results to Python:

```python
def normalize(feature):
    return ee.Feature(None, {
        "region_name": feature.get("name"),
        "ntl_mean": feature.get("mean"),
    })

table = stats.map(normalize)
rows = [f["properties"] for f in table.getInfo()["features"]]
```

Use `reduceRegions` for country/all-province rankings. Do not download a country GeoTIFF and then run local zonal statistics when the user only requested a table.

## Local Boundaries to Earth Engine

For small uploaded shapefiles:

```python
import json
import geopandas as gpd

gdf = gpd.read_file(boundary_path)
if gdf.crs is None:
    raise ValueError("Boundary CRS is missing.")
gdf = gdf.to_crs("EPSG:4326")
payload = json.loads(gdf.to_json())
features = [
    ee.Feature(ee.Geometry(item["geometry"]), item.get("properties", {}))
    for item in payload["features"]
]
zones = ee.FeatureCollection(features)
```

Keep this for small feature sets. For large administrative datasets, prefer cloud-hosted FeatureCollections.

## Compact Client Transfer

`getInfo()` is acceptable only after reducing to a compact table:

```python
payload = table.getInfo()
rows = [item.get("properties", {}) for item in payload.get("features", [])]
```

Do not call `getInfo()` on full rasters, large collections, or per-pixel arrays. If a result table may be large, use Earth Engine table export instead of synchronous `getInfo()`.

## Table Export Pattern

When synchronous transfer is too large or slow:

```python
task = ee.batch.Export.table.toDrive(
    collection=table,
    description="ntl_zonal_stats",
    fileFormat="CSV",
)
task.start()
print(f"export_task_id={task.id}")
```

For 地缘环境智能计算平台 interactive runs, prefer synchronous compact CSV writes when the expected row count is small. Use async export only when the table is too large for a responsive chat turn.

## Standard NTL Dataset Patterns

Annual NPP-VIIRS-like:

```python
image = (
    ee.ImageCollection("projects/sat-io/open-datasets/npp-viirs-ntl")
    .filterDate("2020-01-01", "2021-01-01")
    .select("b1")
    .mean()
)
```

Daily VNP46A2:

```python
image = (
    ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
    .filterDate("2025-04-01", "2025-04-02")
    .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
    .mean()
)
```

Monthly VIIRS stray-light-corrected:

```python
monthly = (
    ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG")
    .filterDate("2020-01-01", "2021-01-01")
    .select("avg_rad")
)
```

Always verify actual band names against the chosen dataset before execution.

## Failure Semantics

Stop and report instead of silently changing methods for:

- `USER_PROJECT_DENIED`
- missing `serviceusage.serviceUsageConsumer`
- Earth Engine API disabled
- expired or missing credentials
- empty image collection for a covered date range
- missing band
- empty boundary FeatureCollection
- `Total request size ... must be less than or equal to 50331648` from direct download

For request-size failures, switch to table-oriented GEE server-side reductions when the user asked for statistics. For small-country or small-AOI GeoTIFF requests, direct download may still be attempted first and judged by actual tool result.

