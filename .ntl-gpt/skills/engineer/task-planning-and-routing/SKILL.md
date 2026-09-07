---
name: task-planning-and-routing
description: Let NTL_Engineer create a typed TaskPlan and choose direct, observation, analysis, or event-context routes without over-delegation.
---

# Task Planning and Routing

Normalize the objective, AOI, time, product requirements, outputs, scientific boundaries, acceptance checks, risks, and budget into a `TaskPlan`.

Save one complete TaskPlan. A returned `package/<token>` handle is a successful
save: retain that handle and continue routing. Do not save the same plan again
or create new artifact IDs to probe the schema. Retry once only when the save
tool returns a concrete validation error, correcting the same complete draft.

Use the direct fast path when inputs are ready, semantics are resolved, the operation is bounded and mature, and validation is immediate. Engineer may also use the shared contract-checked script runner for routine local code (format conversion, deterministic file preparation, small aggregations, and report/figure assembly). Do not route routine code merely because it is code. Before direct execution, save one complete script contract, run it once, and perform one final task-relevant validation.

Complexity alone is not a handoff trigger. Engineer keeps complex but general-purpose work such as ordinary tabular statistics, generic GIS, deterministic transformations, plotting, and report synthesis when it does not require nighttime-light-specific knowledge. Route before custom execution when the task requires a nighttime-light-specific index or threshold, persistence or NTL temporal/event analysis, an NTL-specific statistical model or classification, a novel NTL algorithm, or domain-specific scientific interpretation. In those cases Engineer may prepare ordinary inputs first, then route the scientific stage to NTL_Analyst and perform only lightweight final formatting/synthesis afterward. Otherwise route sequentially through the required specialists:

For a request that names a sensor-specific index, named threshold/classification, or cited method, inspect the matching registered dedicated tool before planning a script. If it covers the stated method, name that tool and preserve its documented semantics in the specialist assignment; do not ask a specialist to reconstruct or replace its formula, threshold, reducer, or units. Use custom code only for a requirement that the matching tool does not cover, and state that gap explicitly.

- observations or acquisition → NTL_Data_Searcher;
- task-specific scientific analysis → NTL_Analyst;
- requested evolving-event context → NTL_Event_Tracker.

Record why every specialist was invoked or skipped.

Delegate with the native `task` tool using a self-contained natural-language
description. Include the objective, scientific scope, known inputs or opaque
parent package handles, requested result mode (`typed_package` or
`summary_only`), acceptance checks, and limitations. Do not request a typed
package for a bounded confirmation or metadata task that has no downstream
artifact dependency. Do not serialize AssignmentEnvelope or HandoffEnvelope
JSON; the runtime records task calls and returns as standardized process
telemetry.

