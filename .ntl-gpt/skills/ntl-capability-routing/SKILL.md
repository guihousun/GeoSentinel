---
name: ntl-capability-routing
description: Choose the lean 地缘环境智能计算平台 capability path before selecting tools. Use for tool pressure reduction, agent delegation, national-scale statistics, GEE routing, local-file analysis, and specialty NTL workflows.
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

- **Retrieval/download only**: delegate to `Data_Searcher`; use `GEE_dataset_router_tool` and `NTL_download_tool` when lightweight.
- **Country or multi-province statistics/ranking**: use GEE server-side `ee.Image.reduceRegions()` and return/export a table. Do not download a country-scale GeoTIFF or bulk shapefiles as the primary path.
- **Local GeoTIFF + boundary statistics**: use `NTL_raster_statistics` only when files already exist and the spatial scope is not national-scale.
- **Custom/event/code tasks**: Engineer designs `ntl.script.contract.v1`; `Code_Assistant` executes via saved script and validation tools.
- **Rare specialty operations**: first read the relevant workflow skill and the capability index; do not assume the Engineer has every specialty tool directly exposed.

## Failure Semantics

- Treat `status: error`, non-empty `error`, or empty `output_files` from a download tool as failure.
- If a GEE request-size/export limit appears, switch to server-side GEE planning.
- If a router recommends a prohibited path, override older workflow templates that suggest that path.

## Context Policy

Do not copy long tool manuals into prompts. Put method details in skills and keep direct tool descriptions short. A tool listed only in this skill is documentation, not runtime permission; agents can directly call only tools exposed in their current tool list.

