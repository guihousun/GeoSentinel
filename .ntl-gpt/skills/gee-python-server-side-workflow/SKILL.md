---
name: gee-python-server-side-workflow
description: Use when writing or validating GEE Python scripts for NTL analysis, especially server-side zonal statistics, country/multi-province summaries, long time series, event impact, reduceRegion/reduceRegions, and export/table outputs.
metadata:
  schema: "ntl.gee_python.workflow.v1"
  template: "/skills/gee-python-server-side-workflow/references/gee_python_server_side_template.py"
---

# GEE Python Server-Side Workflow

Use this skill when the task needs a runnable Earth Engine Python script rather than a local raster workflow.

## Decision Gate

Choose GEE Python server-side execution when:

- The user asks for statistics, ranking, comparison, time series, or impact assessment over a country, all provinces, multiple provinces, or many features.
- The requested raster download is likely too large, or `NTL_download_tool` returned request-size/export failure or empty `output_files`.
- The task needs long daily/monthly series aggregation.
- The user explicitly asks for GEE Python API execution or cloud-side computation.

Do not use GEE Python server-side execution just because the region is a country. Small-country file downloads are allowed when the user wants a GeoTIFF and the export succeeds.

## Required Flow

1. Define `NTL_SCRIPT_CONTRACT` before code generation.
2. Select dataset, band, scale, and auxiliary layers using `/skills/gee-dataset-selection/` when not already fixed by the handoff.
3. Initialize Earth Engine with the runtime project from Engineer handoff:
   `ee.Initialize(project="<GEE_DEFAULT_PROJECT_ID>")`.
4. Load cloud-hosted imagery and cloud-hosted boundaries.
5. Keep computation server-side:
   - use `ee.Image.reduceRegions()` for feature collections,
   - use `ee.Image.reduceRegion()` for one geometry,
   - avoid per-feature client-side loops.
6. Return/export compact tables, not country-scale GeoTIFFs, for statistics/ranking tasks.
7. Write final outputs to workspace `outputs/` via `storage_manager.resolve_output_path(...)` when running inside this project.
8. If using `Export.table` or `Export.image`, manage the Earth Engine task lifecycle explicitly: start, poll/list status, inspect errors, and download only completed outputs into workspace `outputs/`.

## Scale Strategy

| User intent | Recommended mode | Avoid |
| --- | --- | --- |
| Download one small AOI GeoTIFF | `direct_download` first | assuming all country downloads fail |
| City/province local stats with existing files | local tool or local script | unnecessary GEE rewrite |
| Country/all-province/multi-province stats | GEE server-side table | country GeoTIFF + bulk shp + local zonal stats |
| Long daily/monthly aggregation | GEE server-side reduction/export | downloading hundreds of rasters |
| Event impact with buffers/windows | GEE server-side script | ad-hoc bbox or empty same-day `filterDate` |

## Script Contract Fields

The Engineer handoff must include:

- `objective`
- `gee_project_id`
- `dataset_id`, `band`, date range, reducer, scale
- boundary source or AOI source
- expected output filename(s)
- validation checks
- failure gates for GEE auth/IAM/API/quota

## Failure Gates

Stop and report `needs_engineer_decision` for:

- `USER_PROJECT_DENIED`
- missing `serviceusage.serviceUsageConsumer`
- Earth Engine authentication failure
- API disabled / quota denial / project not enabled
- missing dataset/band
- empty image collection when the requested date should be covered
- empty feature collection for the intended boundary source

Do not fix these by changing datasets, changing algorithms, or silently using a bbox.

## Recoverable GEE Failures

Attempt one bounded repair before escalation for:

- memory or aggregation timeout: retry with `tileScale=2`, then `4`, `8`, or `16`; do not change `scale` unless the script contract allows a coarser statistic.
- large result or client timeout from `getInfo()`: switch to `Export.table` and download the completed CSV/table artifact.
- request-size/export URL failures from image download: switch statistical tasks to server-side table outputs.
- empty output after export: inspect task error and expected row/pixel count before claiming success.

Treat `getInfo()` as a small-result inspection tool only. It is acceptable for `bandNames()`, `collection.size()`, small metadata, and small sanity-check tables. Do not use it for long time series, many-feature results, or large image-derived tables.

## Template

For code generation, read:

- `/skills/gee-python-server-side-workflow/references/gee_python_server_side_template.py`

Adapt the template to the task; do not copy placeholders blindly.

## References

Load only the reference that matches the task:

- `/skills/gee-python-server-side-workflow/references/case-index.json` for choosing a real project case pattern.
- `/skills/gee-python-server-side-workflow/references/cases/china_province_annual_reduceRegions.py` for country/all-province annual ranking.
- `/skills/gee-python-server-side-workflow/references/cases/local_shapefile_zonal_reduceRegions.py` for a small local boundary shapefile uploaded by the user.
- `/skills/gee-python-server-side-workflow/references/cases/single_region_annual_timeseries.py` for annual mean time series over one region.
- `/skills/gee-python-server-side-workflow/references/cases/event_buffer_daily_antl.py` for daily event impact with buffers and first-night logic.
- `/skills/gee-python-server-side-workflow/references/official-api-patterns.md` for stable Earth Engine Python API patterns used by 地缘环境智能计算平台.
- `/skills/gee-python-server-side-workflow/references/export-task-lifecycle.md` when a script starts, monitors, cancels, or diagnoses Earth Engine export tasks.
- `/skills/gee-python-server-side-workflow/references/drive-export-download.md` when an Earth Engine Drive export must be retrieved into the current thread workspace.
- `/skills/gee-dataset-selection/` for dataset metadata, band meanings, date coverage, scale, and parameter semantics.

