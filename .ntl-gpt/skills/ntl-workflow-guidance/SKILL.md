---
name: ntl-workflow-guidance
description: Preferred workflow-selection skill for 地缘环境智能计算平台 tasks. Use first for NTL retrieval, statistics, trend, event impact, urban extraction, regression/indicator, and validation workflows before calling Knowledge_Base_Searcher.
allowed-tools: "NTL_Solution_Knowledge"
metadata:
  schema: "ntl.workflow.intent.router.v1"
  router_index: "/skills/ntl-workflow-guidance/references/router_index.json"
  workflows_root: "/skills/ntl-workflow-guidance/references/workflows"
  code_root: "/skills/ntl-workflow-guidance/references/code"
---

# NTL Workflow Guidance

Use this skill to select a reusable workflow template. It is the first stop for standard NTL tasks; use `Knowledge_Base_Searcher` only when no matching workflow exists or methodology research is genuinely needed.

## Read Order

1. Read `/skills/ntl-workflow-guidance/references/router_index.json`.
2. Choose one `intent_id`.
3. Read only the mapped file under `/skills/ntl-workflow-guidance/references/workflows/<intent_id>.json`.
4. Select the best task or compose a small sequence from two tasks.
5. Return a workflow contract; do not execute tools from inside this skill.

Do not full-scan all workflow JSON files unless the router index is missing or corrupted.

## Intent Coverage

- `data_retrieval_preprocessing`
- `statistical_analysis`
- `trend_change_detection`
- `event_impact_assessment`
- `urban_extraction_structure`
- `regression_indicator_estimation`
- `quality_validation_misc`

## Matching Rules

- Exact task ID/name match: confidence `0.90-1.00`.
- Strong semantic match: confidence `0.70-0.89`.
- Usable but adapted template: confidence `0.40-0.69`.
- No useful match: return `status: "no_match"` and then consider `Knowledge_Base_Searcher`.

Before escalating, check whether the user request can be composed from existing retrieval + analysis workflows.

## Template Adaptation Gate

Treat workflow JSON entries as templates, not scripts.

Before returning a workflow, classify spatial scope:

- `single_city_or_smaller`
- `single_province`
- `country_or_multi_province`

If a matched workflow uses `NTL_download_tool` + local boundary download + `NTL_raster_statistics`, but the user asks for country-scale, all-province, multi-province, or province-level ranking/statistics:

- Do not recommend country-scale GeoTIFF download as the primary path.
- Do not recommend bulk provincial shapefile download for local zonal statistics.
- Require GEE server-side `ee.Image.reduceRegions()` over a cloud-hosted administrative `FeatureCollection`.
- Return/export only a CSV/table result.
- Include `adaptation_reason` and `replaced_steps` in the workflow contract.

Small-country file retrieval is different: if the user asks to download a GeoTIFF for a small country or AOI, direct download may be attempted. Switch to server-side only after actual export-size/output failure.

## Workflow Contract

Return a compact object with:

```json
{
  "schema": "ntl.workflow.contract.v1",
  "status": "matched|adapted|composed|no_match",
  "intent_id": "...",
  "task_id": "...",
  "task_name": "...",
  "confidence": 0.0,
  "task_level": "L1|L2|L3",
  "recommended_agent_sequence": ["Data_Searcher", "Code_Assistant"],
  "steps": [],
  "required_tools": [],
  "prohibited_paths": [],
  "adaptation_reason": null,
  "source_workflow_file": "/skills/ntl-workflow-guidance/references/workflows/<intent_id>.json"
}
```

## Coordination With Other Skills

- Use `/skills/ntl-capability-routing/` to understand runtime tool ownership and delegation.
- Use `/skills/gee-routing-blueprint-strategy/` for Data_Searcher direct download vs server-side GEE route decisions.
- Use `/skills/gee-python-server-side-workflow/` when the selected workflow needs a runnable GEE Python server-side script.
- Use `/skills/gee-ntl-date-boundary-handling/` only for daily/event windows, first-night logic, timezone, or event AOI details.
- Use `/skills/code-generation-execution-loop/` when a saved script will be validated/executed.
- Use `/skills/workflow-self-evolution/` only after task completion and user approval.

## Evolution Boundary

This skill may identify that a workflow should be improved, but normal task execution is read-only. Formal workflow updates must follow `/skills/workflow-self-evolution/`.

