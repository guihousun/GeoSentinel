---
name: conflict-event-context
description: Build a traceable, source-bounded conflict-event context without converting reports into verified damage or attribution claims.
---

# Conflict Event Context

- Query only authorized conflict sources and freeze the retrieval at the assignment `as_of`.
- Normalize event identifiers, asserted times, publication times, actors as reported, places, coordinates, precision, event type, and source URLs.
- For a local source snapshot or generated context artifact, declare its workspace-relative path, semantic role, and media type when known; never supply local SHA-256/bytes because typed save binds them.
- Deduplicate repeated reports while preserving independent sources and material disagreements.
- Never call record counts verified event totals, and never adjudicate responsibility or damage from source count or nighttime-light change.
- Return candidate windows/AOI and source limitations to NTL_Engineer; numerical nighttime-light analysis belongs to Analyst.
- If `typed_package` is requested, write/inspect the requested artifact and save the ready EventContext in the same native task; if `summary_only` is requested, return the bounded source-grounded context without a skeleton package. Do not block or request another task merely for checksum tooling.

