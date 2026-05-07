---
name: conflict-ntl-workflow
description: Use for conflict, war, strike, outage, infrastructure attack, refinery, port, airport, power-grid, thermal-anomaly, or other conflict-related event chains that need nighttime-light verification in 地缘环境智能计算平台.
metadata:
  schema: "conflict_ntl.workflow.skill.v1"
  data_sources: "/skills/conflict-ntl-workflow/references/data_source_inventory.md"
  screening_rules: "/skills/conflict-ntl-workflow/references/event_screening_criteria.md"
  output_contracts: "/skills/conflict-ntl-workflow/references/output_contracts.json"
---

# ConflictNTL Agent System

ConflictNTL is an agent-system capability for conflict tracking and nighttime-light analysis. It converts conflict-related event leads into reproducible nighttime-light verification tasks, but it is not proof that an event occurred or that a reported attack caused a measured anomaly.

## Use When

Use this skill for:

- conflict, war, airstrike, missile, drone, explosion, attack, retaliation, or shelling prompts;
- fixed or interpretable targets such as refinery, fuel depot, LNG/gas facility, power plant, substation, port, airport, airbase, bridge, industrial zone, nuclear site, or urban infrastructure;
- outage or thermal-anomaly leads that may indicate conflict-related infrastructure impact.

Do not use this skill as the primary route for natural-disaster chains such as earthquake, flood, wildfire, hurricane, or typhoon unless the user explicitly frames the event as conflict-related.

## Read Order

1. Read `/skills/conflict-ntl-workflow/references/data_source_inventory.md`.
2. Read `/skills/conflict-ntl-workflow/references/event_screening_criteria.md`.
3. Read `/skills/conflict-ntl-workflow/references/output_contracts.json`.
4. If the user has not supplied an event CSV/JSON, call `conflict_ntl_fetch_isw_events_tool` to pull ISW/CTP StoryMap points first.
5. If tools are available, call `conflict_ntl_agent_system_tool` for end-to-end staging; use lower-level screening/AOI tools for partial or debugging runs.

## Agent System Chain

1. `ConflictNTL-Commander`: coordinate the user request, run stages, and keep the report auditable.
2. `Conflict-Searcher`: track event feeds, normalize fields, and apply Round 1 traceability plus Round 2 NTL applicability screening.
3. `Data-Searcher`: build buffer/admin AOIs, aggregate same-day units, and prepare NTL data handoff contracts.
4. `Conflict-Analyst`: run or stage multi-buffer VNP46A2 analysis, label caveats, and write non-attribution interpretations.

Use lower-level stage rules when needed:

1. `Candidate geometry`: prefer event feeds with date and coordinates, especially ISW/CTP ArcGIS StoryMap, ACLED, or UCDP.
2. `Source hardening`: attach official, operator, wire-service, or major-media sources before treating an event as confirmed.
3. `Screening`: apply Round 1 traceability scoring and Round 2 NTL applicability screening.
4. `AOI generation`: exact/general-neighborhood points get 2 km and 5 km buffers by default; coarse records go to administrative AOI handling.
5. `Aggregation`: merge same-day overlapping buffers by radius and same-day matching administrative units before requesting NTL analysis.
6. `NTL verification`: use VIIRS daily products and strict date-boundary handling; report changes as candidate evidence, not as automatic attribution.

## Non-Attribution Rule

Nighttime-light changes do not prove conflict damage by themselves. A ConflictNTL result may support verification or impact assessment only when event timing, location, source confidence, controls, and remote-sensing quality checks are documented.

## Output Expectations

Return or write structured artifacts with these schemas:

- `conflict_ntl.event_screening.v1`
- `conflict_ntl.analysis_units.v1`
- `conflict_ntl.task_queue.v1`
- `conflict_ntl.isw_event_fetch.v1`
- `conflict_ntl.case_report.v1`
- `conflict_ntl.agent_system_run.v1`

Generated files must stay under the current thread workspace outputs. Do not write into `/shared`, `base_data`, or project-root docs from runtime tasks.

