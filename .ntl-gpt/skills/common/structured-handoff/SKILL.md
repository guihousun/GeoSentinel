---
name: structured-handoff
description: Choose between a typed GeoSentinel scientific package and a concise native summary for Engineer-specialist handoffs.
---

# Structured Handoff

- Only NTL_Engineer assigns specialist work. Specialists never dispatch one another.
- Read the Engineer's self-contained natural-language task for its objective, scientific scope, known inputs or parent package handles, requested result mode, acceptance checks, and limitations. Use `typed_package` when downstream computation or a persisted scientific artifact is required; use `summary_only` for a bounded metadata, availability, confirmation, or interpretation task with no downstream package dependency. Ask for a bounded clarification when a scientifically necessary item is unresolved. Do not require or construct AssignmentEnvelope or HandoffEnvelope JSON.
- For `typed_package`, persist the full scientific package first, then return a concise normal task result using only the opaque package handle returned by the typed save tool. For `summary_only`, return the bounded evidence summary directly and do not create a skeleton package or probe the save schema.
- State status, an exact package handle when one was saved, an evidence-based summary, validation verdict, limitations, and any revision need or structured error. Use `ready` only for a package whose validation passed; a summary-only result should state `summary_only` and its evidence status, while blocked/failed results must include an explicit reason.
- Runtime identity, timestamps, and standardized assignment/handoff process records are system-owned. Never discover, invent, or serialize them.
- A revision returns to NTL_Engineer; never create a hidden direct specialist-to-specialist control edge.

