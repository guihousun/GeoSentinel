# Dataset Registry Maintenance

Use this reference after a real GEE task exercises a dataset whose registry entry is provisional or incomplete.

## Confidence Levels

Use these meanings when reviewing or updating registry entries:

- `curated`: added from documentation or prior knowledge, but not yet live-tested in 地缘环境智能计算平台.
- `live_metadata_checked`: dataset exists and band list/date metadata were checked with GEE tools.
- `task_tested`: used in a real 地缘环境智能计算平台 task with successful output validation.
- `low_confidence`: metadata was ambiguous, the task failed, or the product behaved differently than expected.

Auxiliary datasets start as `curated` unless task evidence says otherwise. This especially applies to:

- LandScan
- GHSL
- WorldPop
- ESA WorldCover
- FIRMS
- Global Flood Database

## Fields To Confirm

When a dataset is used in a real task, capture:

- exact `dataset_id`
- asset type: `Image`, `ImageCollection`, or `FeatureCollection`
- actual band names and meanings
- scale or nominal resolution from live metadata
- scale factor and unit, if any
- temporal coverage and product latency, if time-indexed
- reducer behavior: expected output field names, masks, null handling
- common failure points: empty collection, missing band, permission, memory, geometry, export limits
- preferred fallback dataset or workaround

## Update Rule

Only promote an auxiliary dataset to a high-confidence recommendation after both are true:

- live metadata was checked for the selected band and date range;
- at least one real task produced a validated output using that dataset.

If the task exposed a failure but no validated output, keep the entry provisional and record the failure point for future planning.

## Maintenance Candidate

If direct registry editing is not appropriate during the task, record a candidate note in the task summary or evolution candidate log:

```json
{
  "schema": "ntl.gee_dataset_registry.candidate.v1",
  "dataset_key": "worldpop_population",
  "dataset_id": "WorldPop/GP/100m/pop",
  "evidence_task": "short_task_id",
  "verified_bands": ["population"],
  "verified_scale_m": 100,
  "verified_coverage": "observed date/year range",
  "failure_points": [],
  "confidence_recommendation": "task_tested"
}
```

Do not overwrite known-good NTL dataset entries while updating auxiliary data.


