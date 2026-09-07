---
name: provenance-and-evidence-boundary
description: Preserve source provenance, temporal scope, uncertainty, and non-attribution boundaries across GeoSentinel roles.
---

# Provenance and Evidence Boundary

- Record the source/product identity, observation or publication time, retrieval `as_of`, and processing version.
- For each local input/output artifact, the model declares only its workspace-relative path, semantic role, and media type when known. The typed save layer binds its actual SHA-256 and byte count from the task workspace; the model must not compute, guess, copy, null-fill, or placeholder-fill those system-owned fields.
- Missing model-side checksum tooling is not missing scientific evidence and is never, by itself, a reason to block, fail, or request another delegation.
- For a named sensor-specific index, published threshold/classification, or other explicitly named method, first inspect the matching registered dedicated tool when one is available and use its documented formula, threshold, reducer, and units as the primary implementation. Generic code may fill an unmet output or validation gap, but must not silently replace that method with a guessed formula, an alternate proxy, or a data-driven selection rule.
- Separate observed values from interpretation. Nighttime-light change is not by itself proof of cause, damage, outage, responsibility, or recovery.
- Preserve missing coverage, conflicts, quality limitations, fallback use, and alternative explanations.
- Do not read benchmark Gold or evaluator feedback during the tested run, and do not let post-run evaluation repair the result.
- State only what the recorded evidence and accepted parent contracts support.
- Use minimum-sufficient validation: after the last mutation, perform one batched, task-relevant check of the requested outputs, then stop checking and advance to the required typed save or route action. Do not repeat a successful read, inspection, contract validation, or route transition on unchanged state, and do not create optional diagnostics or alternate-method outputs solely for reassurance. A prose-only model turn ends the run, so do not announce a future action while a required tool call remains available.
- One primary execution and at most one concrete non-semantic repair are allowed. If the repair still fails, return an explicit limited or failed result; do not start a third implementation. A sensitivity disagreement outside the TaskPlan is reported as a limitation, not used to invalidate an otherwise correct requested result.

