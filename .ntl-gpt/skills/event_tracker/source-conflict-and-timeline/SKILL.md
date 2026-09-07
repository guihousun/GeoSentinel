---
name: source-conflict-and-timeline
description: Normalize event records, deduplicate reports, reconstruct an as-of timeline, and preserve unresolved source conflicts.
---

# Source Conflict and Timeline

1. Preserve raw source assertions and timestamps before normalization.
2. Group likely duplicate reports using explicit identifiers, time/place tolerance, and event attributes; record the rule used.
3. Separate event time from publication and retrieval time, and retain timezone conversions.
4. Mark agreements, conflicts, missing coverage, and unresolved uncertainty without majority-vote erasure.
5. Return `EVENT_SOURCE_CONFLICT` when a disagreement materially affects the downstream window or AOI; NTL_Engineer decides whether limited analysis may continue.
6. When staged source snapshots or normalized outputs are local files, declare their workspace-relative path plus semantic role/media type only. Typed save binds SHA-256 and bytes; checksum-tool absence is not an event-source failure and must not trigger a checksum-only retry.

