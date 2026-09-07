# GeoSentinel Runtime Memory

This file is the small, versioned startup memory for the Deep Agents runtime.
It is reference context, not a replacement for the active role prompts,
typed contracts, or role-scoped Skills.

## Four-role routing

- `NTL_Engineer` owns task truth, planning, conditional routing, ordinary
  coding/execution, package acceptance, and final EvidenceReport synthesis.
- `NTL_Data_Searcher`（数据助手）owns event feeds, boundaries, remote sensing,
  Earthdata/GEE, climate, population, infrastructure and socioeconomic sources,
  plus product/date/AOI resolution, acquisition, QA and provenance.
- `NTL_Analyst`（分析助手）owns specialized spatial, temporal, raster/vector,
  remote-sensing, event-impact and socioeconomic analysis. Nighttime-light is
  one strong evidence family rather than the platform identity.
- `NTL_Event_Tracker`（事件助手）owns source-bounded conflict, disaster, outage,
  accident, policy and logistics facts, timelines, as-of time and source conflicts.

Route by domain rather than apparent complexity. Complex but general-purpose
tabular statistics, generic GIS, deterministic file processing, plotting, and
report synthesis may remain with Engineer. Missing observations go to Data
Searcher; specialized geoenvironmental methods and interpretation go to Analyst; event-source work
goes to Event Tracker. Specialists do not call one another.

Correction operations are opt-in. Keep standard product values unless the user
explicitly requests a specific correction or explicitly selects a method whose
required workflow includes it. Do not infer angle, seasonal, radiometric,
cross-sensor, atmospheric, geometric, or other correction from a model plan.

Engineer and Analyst share a contract-checked local script runner. Engineer
may use it for ordinary deterministic work; Analyst uses it for assigned
nighttime-light science. Scripts require `ntl.script.contract.v2`, one primary
execution, and one final task-relevant validation. A persisted package handle
is authoritative: reuse it and do not probe with new IDs or repeat unchanged
validation. This startup memory is read-only during benchmark runs; workflow
changes belong in versioned prompts, Skills, and code.
