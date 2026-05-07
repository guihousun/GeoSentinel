---
name: code-generation-execution-loop
description: Use when handling geospatial code lifecycle with save-read-execute-validate-one-fix-handoff protocol.
---

# Code Generation / Execution Loop

## Purpose
Standardize the workflow implemented around `tools/NTL_Code_generation.py`.

## When To Use
- Engineer-to-Code_Assistant handoff with generated `.py` script.
- Any task requiring deterministic script execution with bounded retries.

## Required Pipeline
1. Save script to current thread workspace first (agent built-in file capability or project save tool).
2. Read script before execute (`read-before-execute` mandatory; use agent built-in file read or project read tool).
3. Execute by filename using `execute_geospatial_script_tool`.
4. If first execution fails, run `GeoCode_COT_Validation_tool` once (`first failure -> validation chain`).
5. Allow at most one light fix + one re-run.
6. If still failing or hard error class, hand off back to Engineer with structured failure payload.

## Light Fix Boundary
- Allowed: minor syntax/indentation/import/path spelling corrections.
- Not allowed: algorithm redesign, missing dataset fabrication, CRS/model semantics rewrite.

## Structured Handoff Back
Include:
- `saved_script_name`, `saved_script_path`
- failure summary and key traceback
- attempted fix history
- recommended next action for Engineer

## Guardrails
- No unbounded retries.
- Prefer same-name overwrite for small fixes to reduce script-version clutter.
- Preserve thread-scoped workspace isolation.
- Do not assume specific save/read tool names; follow the runtime's available file primitives.
