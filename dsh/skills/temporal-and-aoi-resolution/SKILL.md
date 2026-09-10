---
name: temporal-and-aoi-resolution
description: Resolve AOI, timezone, local-night and UTC product dates, boundary provenance, and spatial support for observation preparation.
---

# Temporal and AOI Resolution

- Use the assigned AOI or a verified boundary; never invent a bounding box for a named area.
- Record boundary source, level, CRS, feature count, relevant fields, and uncertainty.
- For daily event observations distinguish event local time, local first-night label, UTC acquisition time, and UTC-indexed file date.
- Treat date filters as end-exclusive where the product API requires it and record the exact resolved interval.
- Escalate unresolved event facts to 主管（地缘分析师）; Data Searcher does not construct the event timeline.

