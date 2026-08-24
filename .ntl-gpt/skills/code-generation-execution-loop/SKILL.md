---
name: code-generation-execution-loop
description: Use when handling Engineer-owned geospatial scripts with contract-v2 preflight, direct execution, optional independent review, output validation, and bounded repair.
---

# Code Generation / Execution Loop

## Purpose
Standardize the workflow implemented around `tools/NTL_Code_generation.py`.

## When To Use
- Any Engineer-authored custom `.py` script.
- An explicitly requested Code_Assistant independent review.
- Any task requiring deterministic script execution with bounded retries.

Do not use this skill for L1/L2 tasks covered by built-in tools or a built-in multi-tool chain. Before entering this loop, Engineer must record `BUILTIN_TOOL_GAP` with the exact capability that existing tools cannot provide.

## Required Pipeline
1. Engineer creates a literal `NTL_SCRIPT_CONTRACT` with `schema: ntl.script.contract.v2`. Contract v1 is unsupported.
2. Save the complete script under current thread `outputs/` and read it before execution.
3. Engineer executes by filename using `execute_geospatial_script_tool`; static contract validation and preflight are always mandatory.
4. Follow `execution.test_strategy`: default `auto`; sample only for large/slow/quota-heavy, novel, difficult-to-validate, or diagnostic cases.
5. Verify `contract_output_audit`, `artifact_audit`, and the generated execution manifest before reporting success.
6. A failure does not automatically invoke Code_Assistant. Engineer decides whether independent review is useful.
7. Invoke Code_Assistant only when the user explicitly requests review/verification or Engineer sends `review_requested: true` with a non-empty `review_reason`.
8. When review is requested, Code_Assistant performs contract review, static preflight, strategy-controlled sample testing, full execution, and output validation.
9. Allow at most one light fix + one re-run; record it as one `execution.repair_history` item with non-empty `reason`, `before`, and `after` strings.
10. Return unresolved scientific/data decisions to Engineer. Code_Assistant cannot ask the user directly.

## Contract V2 Execution Defaults

- `mode: execute`; use `plan_only` to suppress runtime execution.
- `overwrite_policy: version`; only Engineer may deliberately select `replace`.
- `test_strategy: auto`; accepted values are `auto|none|sample|required`.
- `network_scope`: list only services the script actually needs.
- No `authorized` field is required. A valid Engineer-authored v2 contract is executable by default.

## Light Fix Boundary
- Allowed: minor syntax/indentation/import/path spelling corrections.
- Not allowed: algorithm redesign, missing dataset fabrication, CRS/model semantics rewrite.
- Never change formulas, date windows, thresholds, scale, datasets/bands, CRS/resampling/nodata semantics, missing-data strategy, boundaries, acceptance conditions, or output scope as a light fix.

## Structured Handoff Back
Include:
- `saved_script_name`, `saved_script_path`
- failure summary and key traceback
- attempted fix history
- recommended next action for Engineer

## Guardrails
- No unbounded retries.
- If a future import is needed, place it before `NTL_SCRIPT_CONTRACT`; future imports cannot appear after an assignment.
- The execution sandbox does not define `__file__`. Use relative `inputs/...` and `outputs/...` paths or one `storage_manager.resolve_*` call, and never derive paths from `Path(__file__)`.
- Default to versioned outputs and manifests. Replace only when Engineer explicitly chooses it.
- Preserve thread-scoped workspace isolation.
- Successful scripts remain task artifacts and are not automatically promoted into Tool/MCP registrations.
- Failed scripts, logs, and manifests belong under `memory/failed_runs/`, not formal outputs.
- Do not assume specific save/read tool names; follow the runtime's available file primitives.
