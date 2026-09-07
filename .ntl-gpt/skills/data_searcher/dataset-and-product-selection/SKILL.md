---
name: dataset-and-product-selection
description: Select and validate the nighttime-light or auxiliary product, band, resolution, and source required by an accepted assignment.
---

# Dataset and Product Selection

- Preserve every explicit dataset ID and band unless NTL_Engineer approves a documented fallback.
- Validate asset type, band semantics, spatial resolution, units, temporal coverage, version, and source provenance before execution.
- Use `GEE_request_plan_tool` first for Earth Engine retrieval and follow its returned execution mode.
- Treat the Earth Engine runtime/billing project and authentication state as system-managed runtime context. Do not pass that billing project in model-facing tool inputs, do not start interactive OAuth, and preserve the classified runtime failure when initialization fails. Dataset and source/output asset IDs remain part of the scientific resource contract.
- Prefer official or validated sources and label community catalogs with provider, license, and warnings.
- Return `OBSERVATION_NOT_AVAILABLE` rather than substituting a scientifically different product silently.

## Stable registered-tool default-first policy

- For an allowlisted planner, acquisition, boundary, or preprocessing tool with
  a validated default contract, provide only required inputs plus fields
  explicitly required by the user or accepted TaskPlan. Leave optional product,
  processing, scale, reducer, or formatting parameters unset so stable defaults
  apply. Do not guess, restate, or tune every default parameter.
- Override a default only for an explicit user, TaskPlan, or immutable product
  contract requirement, or a genuinely unresolved schema-required scientific
  input. Do not reconstruct defaults from memory or tune them toward an expected
  result.
- Treat tool-returned `resolved_parameters` (or an equivalent structured
  actual-parameter record) as provenance and validation evidence. Check it
  against explicit contract fields; do not treat a planned or omitted default as
  execution evidence.

## Immutable product contract

- Before planning or downloading, make a compact **product-segment ledger**. Each
  segment records the requested product/dataset ID, version, band or semantic
  field, sensor family, inclusive time range, and intended output. Derive it
  only from the user request and the accepted TaskPlan; do not infer it from a
  benchmark case, a location, or a desired result.
- An explicit dataset ID, product/version, band, semantic field, or time segment
  is immutable. A familiar product nickname is not permission to replace that
  field with a different one.
- Bands with different meanings remain different products for this purpose. For
  example, DMSP--OLS `avg_vis` and `stable_lights` are not interchangeable: an
  explicit `avg_vis` request must reach the planner, acquisition call, and final
  provenance as `avg_vis`; never relabel it or substitute `stable_lights`.
- If the request names a product but not a band, do not manufacture a band from
  prose such as "stable" or "annual". Use the selected product profile's
  validated default and report that exact returned band. If no validated default
  exists, ask NTL_Engineer to clarify rather than choosing a nearby semantic.
- A request spanning sensor/product eras is a list of independent segments, not
  one blended annual series. Validate the product, band, and requested years for
  every segment before acquisition; never move years across DMSP/VIIRS (or any
  other) product boundary just to make coverage continuous.
- If the delegated TaskPlan and user-request product ledger disagree, or a
  selected plan cannot preserve a ledger field, stop with
  `PRODUCT_CONTRACT_CONFLICT` (or `OBSERVATION_NOT_AVAILABLE` for true absence),
  state the requested versus proposed field, and request an Engineer revision.
  Do not quietly retry a substitute collection, band, sensor, or year range.

