---
name: gee-routing-blueprint-strategy
description: "Use for unified NTL/general-GEE planning, validated direct download, server reduction, batch export, official Earthdata routing, and execution failure semantics."
---

# Unified GEE Routing / Blueprint Strategy

This skill governs Data_Searcher route planning. Use `/skills/gee-python-server-side-workflow/` when the selected plan needs runnable server-side code.

## Required Decision Order

1. Skip GEE planning only for pure local-file analysis with explicit existing files.
2. Call `GEE_request_plan_tool` with all known dataset, bands, dates, AOI, output kind, analysis kind, scale, destination, and provenance requirements.
3. Preserve explicit dataset ids. Never replace an invalid or unavailable explicit id with a default NTL product.
4. Execute only the returned `ntl.gee.plan.v1` mode:
   - `direct_local`: NTL uses `NTL_download_tool`; general GEE uses `GEE_raster_download_tool`.
   - `server_reduce`: create a server-side reducer/table blueprint; do not download source rasters.
   - `batch_export`: submit with `GEE_batch_export_tool`, persist its `/memories/gee_exports/...json` manifest, and inspect it with `GEE_export_status_tool`; cancel only with explicit user intent.
   - `official_earthdata`: use the audited VNP46A1/VNP46A2 official path.
   - `needs_input`: stop and return exact missing or invalid fields.
5. Before recent daily/monthly NTL execution, call `dataset_latest_availability_tool`.

`GEE_dataset_router_tool` is a compatibility adapter for old annual/monthly/daily handoffs. Do not use it as the primary planner.

## Routing Evidence

Global fixed image-count thresholds are prohibited. Routing must consider:

- output kind and analysis kind,
- AOI area or bbox,
- nominal scale,
- requested band count,
- estimated output pixels and bytes,
- source image count,
- destination,
- live asset/band validation,
- workspace quota and synchronous export limits.

Statistics, rankings, zonal summaries, and long time-series normally use `server_reduce`. Large/high-resolution rasters use `batch_export`. Small validated rasters may use `direct_local`.

## Catalog and Metadata

- Explicit id: validate it directly.
- No explicit id: use unified curated + official-catalog discovery.
- Validate the strongest candidates through live metadata before execution.
- Use `GEE_catalog_discovery_tool` for deeper inspection only when the unified planner has insufficient candidates.
- Community assets require provider/license/provenance warnings.

## Boundary and Storage

- Never invent a bbox for a named area.
- General direct-local execution currently requires a verified bbox.
- Use cloud boundaries for large-area server-side statistics.
- Write retrieved files into current-thread `inputs`; `/shared` is read-only.

## Completion

- A valid plan is `planned`, not `completed`.
- Completion requires successful tool status and real non-empty artifact paths.
- Estimated image counts are workload hints, not proof of expected file count.
- On direct export size failure, use the plan fallback chain instead of retrying the same oversized request.
