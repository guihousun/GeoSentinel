# NTL Agent Memory

This file is dedicated to 地缘环境智能计算平台 runtime memory and must not be mixed with Codex workspace policies.

## Scope
- Domain: nighttime light (NTL) geospatial workflows.
- Runtime roles: `NTL_Engineer`, `Data_Searcher`, `Code_Assistant`.

## Workspace Protocol
- Read inputs from `/inputs/` (thread-scoped workspace).
- Write artifacts to `/outputs/` (thread-scoped workspace).
- Path protocol is sandbox-first: use relative `inputs/...` and `outputs/...` in thread workspace.
- `storage_manager.resolve_input_path(...)` and `storage_manager.resolve_output_path(...)` are compatible alternatives when portability is needed.
- Never use hardcoded absolute local paths.
- `/shared/...` is read-only input source; all generated outputs must go to thread `/outputs/`.

## Execution Policy
- Prefer built-in tools when they fully satisfy intent.
- Delegate retrieval/metadata tasks to `Data_Searcher`.
- Delegate code validation/execution tasks to `Code_Assistant`.
- Keep handoff sequential and avoid unnecessary loops.

## Safety
- Keep all artifacts thread-scoped.
- Do not emit host absolute paths in user-facing UI.
- On file mismatch, report actionable recovery steps with logical paths.

---

## CRITICAL: Workflow Router Protocol (MANDATORY)

**Lesson from 2026-02-28**: Myanmar earthquake analysis failed because NTL_Engineer skipped ntl-workflow-guidance. Always query router FIRST.

### Router Priority
```
FIRST:  ntl-workflow-guidance (ALWAYS query first)
SECOND: gee-routing-blueprint-strategy (for GEE routing)
LAST:   Knowledge_Base_Searcher (only if no workflow match)
```

### Pre-Execution Checklist
```
□ [ ] 1. Query ntl-workflow-guidance → identify matching workflow JSON
□ [ ] 2. Read workflow JSON → confirm steps, outputs, task_level
□ [ ] 3. After execution, ask user whether to run self-evolution for this run
```

**REMEMBER**: Skipping ntl-workflow-guidance = Protocol Violation = Systemic Failure

For detailed workflow specs, see `/skills/ntl-workflow-guidance/references/workflows/`.
For event impact (Q20), see `event_impact_assessment.json` for multi-scale buffer requirements.

---

## Self-Evolution Policy (USER-GATED)
- Self-evolution is **not mandatory** for every run.
- After task completion (success/failure), `NTL_Engineer` should ask user whether to perform self-evolution.
- If user confirms:
  - update `/skills/workflow-self-evolution/references/metrics.json`,
  - append failures/learning records as needed,
  - and execute workflow mutation protocol when applicable.
- If user declines:
  - skip self-evolution writes for that run and return normal task outputs only.


