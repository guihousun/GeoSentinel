---
name: disaster-event-observation-workflow
description: Route disaster, outage, accident, and recovery requests from source-bounded event facts through observation selection and bounded nighttime-light analysis. Use when a task needs an event time, event AOI, quality-screened observations, or event-window comparison.
---

# Disaster Event to Observation Workflow

Use this workflow for a disaster- or event-triggered task. It is a scientific
workflow, not an audit checklist: perform only the steps needed for the stated
question and stop after the requested result is validated.

## Route only what the task needs

- For a source-context-only question, use NTL_Event_Tracker and request
  `summary_only` unless a downstream consumer genuinely needs an EventContext.
- For a live event plus night-light analysis, NTL_Engineer normally routes
  Event Tracker -> Data Searcher -> Analyst. Skip a role when an accepted
  context or analysis-ready observation is already supplied, and explain the
  skip in the final answer.
- Use `typed_package` only when a downstream role needs structured event,
  observation, or analysis fields. Otherwise, accept the specialist's bounded
  natural-language summary and native task result; do not create a package only
  to satisfy process bookkeeping.
- Give each specialist one self-contained objective, its known sources or
  workspace inputs, exact output needed, and an explicit stopping condition.
  Specialists do not call one another.

## Event facts

- Record source, retrieval time, event occurrence time, timezone or date-only
  semantics, location precision, and the requested `as_of` cutoff.
- Preserve conflicting time, magnitude, location, or status reports rather
  than selecting an unsupported compromise.
- Use an official or assignment-authorized source. A live request must use the
  current authorized source and state the actual query time; a fixture-only
  request must stay inside the staged inputs.
- Event facts establish context. They do not establish a nighttime-light
  impact, damage, outage, responsibility, or recovery outcome.

## Observation selection

- Resolve the product, band, product-date semantics, AOI, QA rule, unit, and
  no-data rule before interpreting a value.
- Keep `gee_catalog` availability and official
  `nasa_earthdata_cmr_laads` granule availability separate. For an explicitly
  official NASA/latest request, NASA is authoritative; for an explicitly GEE
  request, report the GEE date only. For an unqualified latest request, report
  both channels when available and never merge their dates.
- Use a user-supplied or verified administrative AOI. Do not invent an event
  buffer, a control area, or a recovery window. If a buffer is explicitly
  requested, record its centre, CRS, radius, and rationale.
- Check the AOI capability before selecting a tool. `NTL_daily_antl_statistics`
  accepts only named `province`, `city`, or `county` AOIs; never encode a
  coordinate or a radius such as `25km` as its `study_area` or `scale_level`.
  In a fixture-only task that supplies a verified event-buffer VNP46A2 table,
  use that table as the authorized GEE-derived observation source, compute the
  requested comparison locally, and disclose that it is a frozen snapshot.
- Treat missing, cloudy, or QA-ineligible nights as missing. Do not interpolate
  them into an event or follow-up trajectory.

## Bounded comparison and delivery

- Define a pre-event baseline and event/follow-up windows only when the source
  timeline and qualified observations support them. Report valid-day counts and
  coverage with any comparison.
- State changes as descriptive observations with QA, seasonal, background,
  spatial-scale, and missing-coverage limitations. Do not infer causality,
  damage, an outage, or recovery solely from nighttime-light values.
- After the primary calculation and one task-relevant validation pass, save the
  requested output or return the requested summary. Do not repeat successful
  checks, add an unrequested sensitivity analysis, or dispatch another agent
  only to obtain a checksum or a differently formatted restatement.

