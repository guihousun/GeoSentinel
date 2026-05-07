---
name: gee-routing-blueprint-strategy
description: "Use for Data_Searcher GEE route decisions: direct_download vs gee_server_side, metadata/catalog checks, boundary retrieval need, image-count coverage, and failed-download semantics."
---

# GEE Routing / Blueprint Strategy

This skill is for **Data_Searcher route planning**, not for writing runnable GEE Python scripts. If a runnable script is needed, also read `/skills/gee-python-server-side-workflow/`.

## Use When

- The task involves GEE data retrieval or GEE execution planning.
- Data_Searcher must choose between `direct_download` and `gee_server_side`.
- The task may need `GEE_dataset_router_tool`, `GEE_script_blueprint_tool`, `GEE_dataset_metadata_tool`, or `GEE_catalog_discovery_tool`.
- A download result must be interpreted as complete, partial, or failed.
- Dataset, band, scale, or auxiliary-layer choice is part of the routing decision. In that case, also read `/skills/gee-dataset-selection/`.

## Decision Order

1. If the task is pure local-file analysis with explicit existing filenames and no GEE retrieval, skip this skill.
2. If dataset choice is non-trivial, read `/skills/gee-dataset-selection/` before calling tools.
3. Before promising recent daily/monthly coverage, call `dataset_latest_availability_tool`.
   - For annual/monthly products, interpret anchor dates using `latest_available_period`.
   - For daily products, interpret freshness using `latest_available_date`.
4. Call `GEE_dataset_router_tool` for GEE retrieval/planning tasks.
5. Use `direct_download` only for file-focused lightweight requests:
   - daily `<=14` images,
   - monthly `<=12` images,
   - annual `<=12` images.
6. Use `gee_server_side` for:
   - statistics/ranking/comparison over a country, all provinces, multiple provinces, or many features,
   - long daily/monthly time series,
   - explicit GEE Python API / cloud-side analysis requests,
   - export-size/request-size failure from direct download.
7. For unknown dataset mapping, call:
   `GEE_catalog_discovery_tool -> GEE_dataset_metadata_tool`.

## Boundary Retrieval

Do not retrieve boundary artifacts by default for simple successful downloads.

Retrieve or verify boundary only when:

- the user explicitly requests boundary file/metadata,
- analysis/clip/zonal statistics requires boundary validation,
- direct download reports ambiguous or missing region,
- non-China boundary artifacts are needed via geoBoundaries.

## Completion Gate

- Use router `estimated_image_count` as expected count.
- Treat `NTL_download_tool.output_files` as the source of truth for file coverage.
- `status=error`, non-empty `error`, or empty `output_files` means the download failed.
- If output count is lower than expected, complete missing years/months before returning.
- Return one final structured retrieval contract; do not loop with repeated completion messages.

## Guardrails

- Small-country GeoTIFF download is allowed when the user wants a file and export succeeds.
- Do not use country-scale GeoTIFF download as the primary path for national/multi-province statistics.
- Do not replace named regions with invented bbox.
- Do not compute annual/monthly statistics by downloading long daily series locally.
- Keep this skill focused on route decisions; use `/skills/gee-python-server-side-workflow/` for script construction.
