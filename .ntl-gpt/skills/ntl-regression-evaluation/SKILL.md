---
name: ntl-regression-evaluation
description: Use after changing 地缘环境智能计算平台 skills, prompts, GEE routing, dataset selection, date handling, or execution tools; provides regression scenarios that catch known routing, dataset, first-night, file-management, and server-side computation failures.
metadata:
  schema: "ntl.regression_evaluation.v1"
  checklist: "/skills/ntl-regression-evaluation/references/regression-checklist.json"
---

# NTL Regression Evaluation

Use this skill before finishing changes to:

- agent prompts,
- GEE dataset/routing/date skills,
- tool descriptions or tool behavior,
- code execution/file-management tools,
- workflow JSON files.

## Required Process

1. Read `/skills/ntl-regression-evaluation/references/regression-checklist.json`.
2. Select the smallest relevant subset based on touched area.
3. For each selected case, verify expected route and explicit prohibited route.
4. If code changed, run the smallest compile/direct invocation checks.
5. Report cases as `pass`, `not_run`, or `fail` with reason.

## Case Selection

- GEE routing or workflow change: run routing cases.
- Dataset registry or metadata change: run dataset-selection cases.
- Date/event change: run first-night/timezone cases.
- Code execution/tool change: run file-management and execution cases.
- Streamlit/UI change: run only UI lifecycle cases plus any affected routing case.

## Minimum Output

Return a compact table:

| case_id | status | evidence |
| --- | --- | --- |

Never claim a regression case passed unless the expected route/result was actually inspected or executed.

