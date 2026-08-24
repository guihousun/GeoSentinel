---
name: gee-dataset-selection
description: Use when choosing Earth Engine datasets, bands, date ranges, scale, reducers, or auxiliary data for NTL-GPT tasks; includes curated NTL, boundary, population, vegetation, land-cover, DEM, and event-context dataset metadata and selection rules.
metadata:
  schema: "ntl.gee_dataset_selection.v1"
  registry: "/skills/gee-dataset-selection/references/gee-dataset-registry.json"
---

# GEE Dataset Selection

Use this skill before writing a GEE plan or script when the task depends on dataset choice, band choice, temporal coverage, spatial scale, or auxiliary layers.

## Operating Rule

1. Read `/skills/gee-dataset-selection/references/dataset-selection-rules.md`.
2. Read `/skills/gee-dataset-selection/references/gee-dataset-registry.json` only for the dataset family needed by the task.
3. Call `GEE_request_plan_tool` to preserve explicit ids, combine curated and official-catalog candidates, and determine the execution mode.
4. Before event/daily/recent-date work, run the latest-availability gate:
   - Preferred: call `dataset_latest_availability_tool`.
   - GEE path: validate selected `dataset_id`, bands, and latest `system:time_start` with `dataset_latest_availability_tool` or `GEE_dataset_metadata_tool`.
   - NASA LAADS path: validate the LAADS/CMR short_name has granules through the requested date with `dataset_latest_availability_tool` or the official pipeline query step.
5. Validate the selected `dataset_id` and bands with `GEE_dataset_metadata_tool` when the unified plan is not already live-verified.
6. If the selected dataset is unsupported or not yet updated for the requested date, choose a documented alternative, adjust the analysis window with user-visible justification, or return a clear coverage/latency error.
7. Treat dataset metadata as evidence-scoped: prefer entries with `validation_status: task_tested`, but still validate live band/date coverage for the exact task.

## Registry Scope

The registry is curated for NTL-GPT, not a full Earth Engine catalog. It covers:

- NTL products: annual, monthly, daily, legacy DMSP.
- Administrative boundaries: geoBoundaries, project China assets, GAUL/LSIB fallback.
- Population/electrification support: LandScan, WorldPop, GPW, GHSL population.
- Optical/thermal imagery: Sentinel-2 SR Harmonized with Cloud Score+, Landsat 8/9 Collection 2 Level 2.
- Environmental covariates: MODIS NDVI/EVI/NPP, land cover, WorldCover, DEM.
- Event context: fire and flood context layers.

Auxiliary entries such as LandScan, GHSL, WorldPop, WorldCover, FIRMS, and Global Flood Database were live metadata checked and smoke tested on 2026-04-23; use their registry `live_validation` notes, and re-check exact AOI/date coverage before execution.

## Contract

Every GEE handoff should state:

- selected `dataset_id`
- selected `bands`
- `date_range` with end-exclusive convention for scripts
- `scale_m`
- reducer and aggregation parameters
- why this dataset fits the task
- known caveats and fallback dataset
- validated processing/QA preset when scientific scaling or masking is required

For common GEE parameter meanings, read:

- `/skills/gee-dataset-selection/references/gee-parameter-glossary.md`

For recent-event availability checks, run or adapt:

- `dataset_latest_availability_tool`
- `/skills/gee-dataset-selection/scripts/check_latest_availability.py`

Minimum output required before execution:

- `gee.latest_date` for the selected GEE collection, when using GEE.
- `gee.latest_period` for annual/monthly products. Treat `latest_date` there as the
  period anchor (`2024-01-01` means the 2024 annual composite, not "only through
  January 1, 2024").
- `laads.latest_day` for the selected LAADS/CMR short_name, when using official NASA granules.
- `requested_end_date`.
- `coverage_status`: `available`, `not_yet_available`, `unknown`, or `not_applicable`.
- `decision`: proceed, use fallback product, shrink/shift window, or report latency.

For registry maintenance after real tasks, read:

- `/skills/gee-dataset-selection/references/registry-maintenance.md`
