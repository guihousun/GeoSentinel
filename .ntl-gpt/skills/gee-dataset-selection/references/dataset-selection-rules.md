# Dataset Selection Rules

Use the smallest product that directly matches the user's temporal and analytical intent.

## NTL Product Choice

| Task intent | First choice | Use when | Avoid |
| --- | --- | --- | --- |
| Long-term annual trend, 2000 onward | `projects/sat-io/open-datasets/npp-viirs-ntl` band `b1` | annual mean/ranking/trend, DMSP-VIIRS consistency matters | daily products for annual summaries |
| Official VIIRS annual composites, 2022 onward | `NOAA/VIIRS/DNB/ANNUAL_V22` band `average` | user asks for NOAA/EOG annual VIIRS for 2022+ | using V22 for 2013-2021; live metadata on 2026-04-23 showed V22 indexes `20220101` through `20250101` |
| Official VIIRS annual composites, 2013-2021 | `NOAA/VIIRS/DNB/ANNUAL_V21` band `average` | user asks for NOAA/EOG annual VIIRS before V22 coverage | pre-2013 studies; use DMSP or harmonized products |
| Monthly VIIRS dynamics | `NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG` band `avg_rad` | monthly trend, seasonal comparison, stray-light-corrected product preferred | estimating monthly values from daily images unless necessary |
| Daily event impact | `NASA/VIIRS/002/VNP46A2` band `Gap_Filled_DNB_BRDF_Corrected_NTL` | earthquake, flood, conflict, outage, first-night analysis | same-day event assumptions; check overpass/date logic and live product latency before promising recent dates |
| At-sensor daily radiance | `NOAA/VIIRS/001/VNP46A1` band `DNB_At_Sensor_Radiance_500m` | rawer historical daily DNB radiance or historical UTC_Time verification is explicitly required and GEE coverage includes the target date | default daily ANTL work where VNP46A2 is better; recent UTC boundary verification beyond GEE VNP46A1 coverage |
| Historical pre-2012 NTL | `NOAA/DMSP-OLS/NIGHTTIME_LIGHTS` band `stable_lights` or `avg_vis` | DMSP-era studies | mixing with VIIRS without harmonization |

## Boundary Choice

| Region need | First choice | Notes |
| --- | --- | --- |
| Global country/province/city/prefecture boundary in GEE | `WM/geoLab/geoBoundaries/600/ADM0-ADM2` | Live metadata on 2026-04-23 found ADM0-ADM2 available; ADM3/ADM4 assets were not found. Filter by `shapeGroup` country code and `shapeName`; preferred outside China. |
| China province/city/county inside existing tools | project assets under `projects/empyrean-caster-430308-m2/assets/` | Use only if names/properties are clean for the output language; validate first. |
| Legacy global administrative fallback | `FAO/GAUL/2015/level0-2` | Useful fallback when geoBoundaries filtering fails; names may differ, e.g. `Shanghai Shi`. |
| Simple country mask | `USDOS/LSIB_SIMPLE/2017` | Country-level only. |

## Auxiliary Data Choice

| Task intent | First choice | Notes |
| --- | --- | --- |
| Electricity access / population exposed to lights | LandScan or WorldPop | Task-tested on 2026-04-23. LandScan uses band `b1`, observed 2000-2024, nominal ~928m, ambient population. WorldPop uses band `population`, observed 2000-2020, nominal ~93m, residential population. |
| Coarse global population time series | GPW or GHSL population | Task-tested on 2026-04-23. GPW uses `population_count`, 2000/2005/2010/2015/2020, nominal ~928m. GHSL uses `population_count`, 1975-2030 5-year grids, nominal 100m; distinguish observed vs projected years. |
| Vegetation correction / EANTLI / ecological covariates | MODIS `MOD13A1`/`MOD13A2` NDVI/EVI | Apply scale factor `0.0001`. |
| Land-cover mask / urban extraction support | `MODIS/061/MCD12Q1` or `ESA/WorldCover/v200` | MODIS is annual 500m. WorldCover v200 is task-tested with band `Map`, year 2021 only, nominal ~10m. |
| Topographic control | `USGS/SRTMGL1_003` or `NASA/NASADEM_HGT/001` | Use band `elevation`; reproject/aggregate before joining with NTL. |
| Fire context | `FIRMS` or `MODIS/061/MCD64A1` | Task-tested on 2026-04-23. FIRMS bands include `T21`, `confidence`, `line_number`, observed 2000-11-01 to 2026-04-22, nominal ~927m. MCD64A1 uses `BurnDate` for burned area. |
| Flood context | `GLOBAL_FLOOD_DB/MODIS_EVENTS/V1` | Task-tested on 2026-04-23. Bands include `flooded`, `duration`, `clear_views`, `clear_perc`, `jrc_perm_water`, observed 2000-02-17 to 2018-12-05, nominal 250m. |

LandScan, GHSL, WorldPop, WorldCover, FIRMS, and Flood DB now have at least one live metadata check and one light smoke test. Still validate bands, scale, date coverage, and reducer behavior for the task's exact AOI/date before final execution.

## Execution Choice

- If the user asks for a GeoTIFF and the AOI/date range is small, direct download is allowed.
- If the user asks for statistics/ranking/comparison over many features, use GEE server-side reductions and return a table.
- If `NTL_download_tool` returns a GEE request-size/export error, switch to server-side table workflows for statistical tasks.
- Never compute annual/monthly statistics by downloading long daily image series.

## Validation

Before finalizing a GEE execution plan:

- Verify `dataset_id` exists with `GEE_dataset_metadata_tool`.
- Verify selected `band` is in the returned band list.
- Verify date coverage when the product has `system:time_start`.
- For daily event/recent-date work, verify latest availability before execution:
  - GEE collections: inspect live latest `system:time_start`, not only documented coverage or expected latency.
  - LAADS/CMR products: query latest granule day for the relevant `short_name` and AOI/date range.
  - If the requested date is later than the latest available date, return a latency/coverage decision instead of treating it as analytical no-data.
- For FeatureCollections, inspect sample property names before filtering.
- After a real task uses an auxiliary dataset, update the registry or record a maintenance candidate with actual band names, scale factors, coverage, reducer behavior, and failure points.

## Latest-Availability Gate

Use this gate before daily VNP46/VJ146/VJ102/VJ103 event analysis, recent monthly composites, or any task near the current date.

1. Determine the requested final observation date after timezone/date-boundary handling.
2. Check the intended execution source:
   - Preferred unified path: `dataset_latest_availability_tool`.
   - `GEE`: use `dataset_latest_availability_tool` or `GEE_dataset_metadata_tool`.
   - `LAADS/CMR`: use `dataset_latest_availability_tool` or `scripts/check_latest_availability.py --laads-short-name <short_name> [--bbox ...]`.
3. Compare `requested_end_date <= latest_available_date`.
   - For annual/monthly GEE products, do not read the anchor date literally:
     `2024-01-01` on an annual collection means the latest available period is `2024`,
     and `2026-03-01` on a monthly collection means the latest available period is `2026-03`.
   - Prefer `latest_available_period` when reasoning about annual/monthly availability.
4. If false, return a clear decision:
   - `not_yet_available`: product has not updated to the needed date.
   - `fallback_candidate`: another product/source appears available and is methodologically acceptable.
   - `wait_or_adjust_window`: wait for ingestion or use the latest available window with an explicit caveat.

Do not silently substitute GEE and LAADS sources. Report source, product short_name/dataset_id, latest date, and decision.

For recent daily first-night UTC boundary checks:
- GEE `NASA/VIIRS/002/VNP46A2` is the default GEE daily ANTL source but does not expose pixel-level `UTC_Time`.
- GEE `NOAA/VIIRS/001/VNP46A1` exposes `UTC_Time`, but it must pass live coverage checks for the target date.
- If GEE VNP46A1 does not cover the target date, use LAADS/CMR granule timing or official metadata to decide UTC file dates.
