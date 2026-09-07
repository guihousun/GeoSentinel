---
name: ntl-statistics-and-time-series
description: Execute task-specific nighttime-light statistics, time-series, trend, anomaly, and comparison workflows on accepted observations.
---

# NTL Statistics and Time Series

- Consume either accepted ObservationPackage artifacts with explicit product, units, dates, QA, grid, and valid-pixel semantics, or checksum-bound staged inputs explicitly authorized by an accepted TaskPlan with `observation_required=false`. Never reinterpret a staged fixture as a live observation.
- Define the assigned statistic, temporal aggregation, baseline, comparison, missing-data rule, and uncertainty before execution.
- Check sample support, NoData, edge effects, numerical range, and sensitivity to low baselines.
- Save machine-readable tables and plots with parameters and checksums.
- Report association and observation; do not infer causal mechanisms from radiance alone.

## Stable registered-tool default-first policy

- For an allowlisted method tool with a validated default contract, provide only
  required inputs plus fields explicitly required by the user or accepted
  TaskPlan. Leave optional method, preprocessing, threshold, reducer, or
  formatting parameters unset so stable defaults apply. Do not guess, restate,
  or tune every default parameter.
- Override a default only for an explicit user, TaskPlan, or immutable
  product/method-contract requirement, or a genuinely unresolved schema-required
  scientific input. Do not reconstruct defaults from memory or tune them toward
  an expected result.
- Treat tool-returned `resolved_parameters` (or an equivalent structured
  actual-parameter record) as the execution evidence for the AnalysisPackage and
  validation. Check it against explicit contract fields; do not treat a planned
  or omitted default as evidence.

## Registered-method rules

- For a staged local raster-plus-boundary request for a standard zonal
  nighttime-light metric exposed by `NTL_raster_statistics` through
  `selected_indices`, call `NTL_raster_statistics` first. Preserve its
  source-grid, pixel-centre, NoData, and area defaults; do not substitute an
  ad hoc reprojection, resampling, rasterization, or area-calculation script.
  Custom code may only format, map, or validate the tool-produced result.
- For a multiannual raster anomaly task, call `Detect_NTL_anomaly` before
  drafting custom code. Pass the chronologically ordered rasters, use the
  stated target (or the latest raster when none is stated), and pass the
  declared AOI boundary as `vector_file` when one is staged. Its canonical
  contract is a baseline **population** standard deviation (`ddof=0`), a
  positive anomaly only when `z > threshold` (not `>=` and not `|z|`), and a
  common-valid support shared by every baseline and target raster. Pixels with
  zero baseline standard deviation cannot become anomalies; report them
  separately rather than manufacturing an epsilon-denominator z-score.
- For a pixel-wise slope/trend task on chronological rasters, call
  `Analyze_NTL_trend` first. Its canonical outputs are the Theil-Sen median
  pairwise slope and the two-sided Kendall tau-b p-value. Do not replace them
  with an OLS slope, a t-test, or a regression-line summary; custom code may
  only tabulate or visualize the tool's outputs.
- For a daily target/reference event-window comparison, construct each window
  from the calendar-date intersection whose target and reference observations
  both pass the same declared validity/QA rule. Compute both city means from
  that same matched-valid-day support, report each city's own valid-day count
  separately from the matched count, and do not mix unmatched daily supports.

