---
name: handoff-acceptance
description: Let NTL_Engineer validate a specialist's normal task result and typed scientific package against TaskPlan acceptance criteria.
---

# Handoff Acceptance

- Read the specialist's native task result for status, an opaque package handle when saved, evidence summary, validation verdict, limitations, and revision need or error.
- For a requested `typed_package` result, validate the opaque package handle and confirm the expected producer, package type, canonical SHA-256, and validation verdict. For an explicitly requested `summary_only` result, accept the native evidence summary and telemetry without package validation; do not block merely because no handle exists. Runtime assignment/run/task identity and standardized handoff records are checked and written by the system, not read or authored by the model.
- Validate only the exact handle returned by the current specialist and do so once. Do not scan for alternate packages or repeat a successful validation. Accept a valid requested primary result with disclosed near-ties or optional sensitivity disagreement; record those as limitations rather than requesting revision unless robustness was an explicit acceptance criterion.
- Check the package against the exact TaskPlan acceptance criteria and prohibited-change list.
- Accept only evidence that is complete enough for the next dependency. Request one bounded, directed revision when it is repairable.
- Route every revision through NTL_Engineer; specialists do not call each other.
- Block when required evidence, scientific semantics, permissions, or budget cannot be resolved safely. Preserve the package and allow runtime telemetry to retain the native task/return trace; do not construct AssignmentEnvelope, HandoffEnvelope, or a handoff-decision JSON object.

