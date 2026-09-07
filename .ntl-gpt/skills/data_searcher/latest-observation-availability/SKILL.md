---
name: latest-observation-availability
description: Resolve the latest quality-eligible observation as of the benchmark or task cutoff without using future information.
---

# Latest Observation Availability

- Treat `as_of`, timezone, publication latency, and quality eligibility as part of the data contract.
- Query live availability with the allowlisted metadata/availability tools; do not infer a current date from nominal product cadence.
- Distinguish the Earth Engine catalog channel from the official NASA Earthdata CMR/LAADS channel. Their latest dates may differ because of ingestion/publication latency; never merge them into one "latest" date.
- For an official NASA/latest request, use the NASA channel as authoritative. For an explicitly GEE request, report the GEE channel. For an unqualified recent request, check both when possible and show separate rows.
- Record query time in UTC, source channel, latest published period, latest quality-eligible observation, selected product/band, evidence source, availability lag, and any allowed fallback.
- Treat the availability result as a catalog/granule listing check unless an explicit AOI QA inspection is also recorded; do not label a date quality-qualified from catalog recency alone.
- Distinguish unavailable, unpublished, uncovered, and quality-insufficient observations.
- Never refresh a frozen benchmark temporal answer inside the tested run or access evaluator Gold.

