---
name: analysis-ready-preprocessing
description: Apply accepted standard preprocessing and reusable fixed-formula features to produce a validated ObservationPackage.
---

# Analysis-Ready Preprocessing

- Apply documented QA masks, scaling, units, NoData, clipping, mosaicking, reprojection, resampling, grid alignment, and fixed reusable features.
- Record every input, transformation, parameter, output, checksum, valid-pixel check, and coverage check.
- Do not choose task-specific baselines, thresholds, model structure, feature combinations, or event-comparison strategy; those belong to Analyst.
- Do not apply correction or harmonization by default. Only an explicit user request or a user-selected method that requires it authorizes correction; a model-generated plan does not.
- When an authorized correction defines the research method rather than routine data preparation, return it to NTL_Engineer for Analyst routing.
- Emit `DATA_CONTRACT_INVALID` or `OBSERVATION_QUALITY_INSUFFICIENT` instead of passing an ambiguous or empty observation downstream.
