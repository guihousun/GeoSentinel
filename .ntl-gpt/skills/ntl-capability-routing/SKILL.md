---
name: ntl-capability-routing
description: Choose the lean NTL-GPT capability path before selecting tools. Use for tool pressure reduction, agent delegation, national-scale statistics, GEE routing, local-file analysis, and specialty NTL workflows.
---

# NTL Capability Routing

Use this skill before selecting direct tools for any non-trivial NTL task.

## Purpose

Keep tool selection clear by separating:

- **Core tools**: stable execution interfaces that perform work or return machine-readable status.
- **Router/registry tools**: lightweight tools that recommend skills, agents, and execution modes.
- **Skills**: workflow rules, examples, failure recovery, tool-selection knowledge, and code templates.

## Required First Step

For complex, ambiguous, or multi-step user requests, read:

- `/skills/ntl-capability-routing/references/tool-capability-index.json`

Use that index as a compact map of tool ownership, direct exposure, and migration status. Do not treat it as an executable router; the current agent remains responsible for choosing the route from task evidence and matched workflow skills.

## Routing Rules

- **Retrieval/download only**: delegate to `Data_Searcher`; call `GEE_request_plan_tool` first, then use `NTL_download_tool` for stable NTL direct-local plans, `GEE_raster_download_tool` for validated general-GEE direct-local plans, or `GEE_batch_export_tool` plus `GEE_export_status_tool` for large asynchronous exports.
- **Named China administrative AOI**: when no verified boundary input exists, delegate to `Data_Searcher` and use `get_administrative_division_data` before GAUL, geoBoundaries, or ad hoc GEE boundary searches. Verify the returned administrative level and WGS84 artifact.
- **Country or multi-province statistics/ranking**: use GEE server-side `ee.Image.reduceRegions()` and return/export a table. Do not download a country-scale GeoTIFF or bulk shapefiles as the primary path.
- **Single-city-or-smaller zonal statistics/ranking**: this is L2 when existing tools cover the metric. If inputs are missing, retrieve the annual/local GeoTIFF and the multi-feature administrative boundary in one Data_Searcher handoff, then use `NTL_raster_statistics` (for mean light, `selected_indices=["ANTL"]`) and rank the returned CSV. Do not write a GEE/Python script merely because the inputs had to be retrieved first.
- **Local GeoTIFF + boundary statistics**: use `NTL_raster_statistics` when files exist and the spatial scope is not national-scale. Use `geodata_inspector_tool` or `geodata_quick_check_tool` for field/CRS checks; do not create an inspection script.
- **Custom/event/code tasks**: Engineer designs `ntl.script.contract.v2`, saves, preflights, executes, and validates the script directly. Invoke `Code_Assistant` only when the user requests verification or Engineer explicitly requests independent full review.
- **Rare specialty operations**: first read the relevant workflow skill and the capability index; do not assume the Engineer has every specialty tool directly exposed.

## Failure Semantics

- Treat `status: error`, non-empty `error`, or empty `output_files` from a download tool as failure.
- If a GEE request-size/export limit appears, switch to server-side GEE planning.
- If a compatibility router recommends a path that conflicts with `ntl.gee.plan.v1`, use the unified plan and record the conflict.

## Context Policy

Do not copy long tool manuals into prompts. Put method details in skills and keep direct tool descriptions short. A tool listed only in this skill is documentation, not runtime permission; agents can directly call only tools exposed in their current tool list.
