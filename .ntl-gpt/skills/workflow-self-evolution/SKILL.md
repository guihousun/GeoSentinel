---
name: workflow-self-evolution
description: Use after an NTL task finishes and the user explicitly agrees to record learning. Provides a conservative, file-based protocol for failure classification, candidate logging, and workflow updates without fake imports or automatic mutation.
metadata:
  schema: "ntl.workflow.self_evolution.v1"
  user_gated: "true"
---

# Workflow Self-Evolution

This skill is a **file-based protocol**, not a Python module. Never import `skills.workflow_self_evolution`.

Use it only after a task has produced a terminal result and the user has agreed to run learning/evolution updates.

Before writing a reusable learning from routing, dataset, date, or execution behavior, check `/skills/ntl-regression-evaluation/` for an existing case or add a candidate case through the user-gated workflow.

## Stable Policy

- Default run mode is read-only: do not mutate workflow files during normal task execution.
- Ask the user before any learning writeback.
- `Code_Assistant` may propose workflow changes but must not edit skill/workflow files.
- `NTL_Engineer` is the only role that may apply formal workflow mutations.
- External/API/auth failures are not code-method failures unless the same pattern recurs and the fix is a reusable workflow guardrail.

## Failure Classification

Classify terminal failures into one category:

| Category | Examples | Learn? |
| --- | --- | --- |
| `systemic` | wrong tool contract, invalid parameter schema, repeatable workflow logic bug | yes |
| `recurring` | same workflow/signature fails at least 3 times in 7 days | yes, with review |
| `transient` | timeout, temporary network/API outage | no |
| `user_input` | missing upload, invalid user-supplied file/region/date | no |
| `external` | GEE IAM/quota/API provider denial | monitor; learn only if reusable guardrail is needed |

## GEE Failure Mapping

Use this mapping when a terminal failure came from Google Earth Engine:

| Failure signal | Category | Learning action |
| --- | --- | --- |
| `USER_PROJECT_DENIED`, missing `serviceusage.serviceUsageConsumer`, API disabled, auth failure, quota denial | `external` | Do not mutate workflows; report configuration/IAM action. Learn only if routing repeatedly retries instead of stopping. |
| Dataset exists but selected band is missing, reducer output field is wrong, expected date coverage is mis-specified | `systemic` | Learn immediately; update dataset-selection or script-contract guidance after validation. |
| Empty image collection for unsupported date range | `user_input` or `systemic` | User input if request is outside documented coverage; systemic if the planner selected the wrong dataset/date window. |
| Empty feature collection from boundary filter | `systemic` | Learn if boundary source/filter was wrong; otherwise ask user for a clearer AOI. |
| Memory limit, aggregation timeout, large `getInfo()` result, request-size export failure | `transient` or `systemic` | First apply bounded retry/server-side export rules; learn only if the workflow pattern caused repeated large client-side or raster-download attempts. |
| Completed export missing from workspace `outputs/` | `systemic` | Learn if task lifecycle or Drive download gate was skipped. |

## Write Targets

- Candidate failures: `/skills/ntl-workflow-guidance/references/evolution_candidates.jsonl`
- Formal workflow changes: `/skills/ntl-workflow-guidance/references/workflows/<intent_id>.json`
- Formal mutation log: `/skills/ntl-workflow-guidance/references/evolution_log.jsonl`
- Optional metrics/failure logs:
  - `/skills/workflow-self-evolution/references/metrics.json`
  - `/skills/workflow-self-evolution/references/failure_log.jsonl`
  - `/skills/workflow-self-evolution/references/learning_log.jsonl`

## Candidate Record

For failures that might be useful later, append one JSONL object:

```json
{
  "schema": "ntl.workflow.evolution.candidate.v1",
  "timestamp": "ISO-8601",
  "intent_id": "statistical_analysis",
  "task_signature": "short stable description",
  "failure_category": "systemic|recurring|transient|user_input|external",
  "error_signature": "short normalized error",
  "evidence": {
    "script_name": "optional.py",
    "tool": "optional_tool_name",
    "artifact_audit_pass": false
  },
  "recommended_action": "none|patch_existing|append_new|monitor",
  "reason": "why this should or should not become a workflow update"
}
```

## Formal Update Gate

Apply a formal workflow change only when all are true:

- User approved evolution for this run.
- The change is capability-level, not a one-case patch.
- The target workflow JSON remains valid.
- The change does not introduce tools that are not available to the owning agent.
- The workflow item records `_evolution` with reason, evidence, and date.

## Validation

After any formal update:

1. Parse the changed JSON files.
2. Confirm referenced tools exist in the actual runtime groups.
3. Confirm related prompt/skill rules do not contradict the workflow.
4. If validation fails, revert only the attempted evolution change and keep the candidate record.
