---
name: disaster-event-context
description: Build source-bounded disaster, outage, accident, and recovery context as of an explicit cutoff for downstream observation planning.
---

# Disaster Event Context

- Use only assignment-authorized official agencies, recognized humanitarian sources, or explicitly allowed reporting sources.
- Record event time, timezone, publication time, `as_of`, location precision, requested scope, and source snapshot.
- For a local source snapshot or generated context artifact, declare its workspace-relative path, semantic role, and media type when known; never supply local SHA-256/bytes because typed save binds them.
- Distinguish occurrence, updates, warnings, response, and recovery milestones instead of collapsing them into one timestamp.
- Preserve inaccessible sources, coverage gaps, conflicting magnitudes/times/locations, and uncertainty.
- Propose candidate event windows and AOI to NTL_Engineer; do not select imagery or infer radiance impact.
- If `typed_package` is requested, write/inspect the requested artifact and save the ready EventContext in the same native task; if `summary_only` is requested, return the bounded source-grounded context without a skeleton package. Do not block or request another task merely for checksum tooling.

