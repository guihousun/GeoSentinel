# ConflictNTL Data Source Inventory

Last updated: 2026-05-04

Purpose: record source roles for conflict-event retrieval, infrastructure impact triage, thermal anomaly checks, and nighttime-light verification. This inventory is scoped to conflict chains and does not replace natural-disaster workflows.

## Source Tiers

| Tier | Role | Typical sources | Direct NTL task? |
|---|---|---|---|
| T1 geometry-event source | Date and point geometry | ISW/CTP ArcGIS StoryMap, ACLED, UCDP GED | Candidate AOI only; still verify |
| T2 infrastructure/thermal signal | Outage, fire, or infrastructure anomaly | NASA FIRMS, Cloudflare Radar, maritime/aviation feeds | Triage signal only |
| T3 news discovery | Lead and source URL discovery | GDELT DOC, GDELT BigQuery, RSS/news search | No |
| T4 high-confidence verification | Fact confirmation and narrative checks | official statements, Reuters, AP, BBC, IAEA, CENTCOM, operators | Supports claims, often lacks coordinates |
| T5 remote-sensing evidence | Independent impact evidence | VIIRS daily NTL, FIRMS, Sentinel/Landsat/SAR | Verification layer |

## Core Event Geometry Sources

### ISW / CTP ArcGIS StoryMap

- StoryMap item metadata: `https://www.arcgis.com/sharing/rest/content/items/089bc1a2fe684405a67d67f13bd31324?f=json`
- Best fields: `event_date_utc`, `latitude`, `longitude`, `event_type`, `site_type`, `site_subtype`, `city`, `country`, `coord_type`, `source_1`, `source_2`, `sources`.
- Strength: strong date and point geometry for strike and retaliation records.
- Weakness: editorial/geolocation product; embedded sources or independent reporting are still needed before paper-grade claims.
- Operational note: compare ArcGIS `modified` time and visible coverage labels before assuming same-day completeness.

### ACLED

- Best fields: event date, event type, actors, fatalities, admin areas, latitude/longitude, source scale.
- Strength: structured conflict-event dataset with coordinates.
- Weakness: access tier, licensing, and API limits matter.
- Recommended role: independent corroboration and broader conflict coverage outside a StoryMap scope.

### UCDP GED

- Best fields: event date, latitude/longitude, conflict actors, deaths, event type.
- Strength: high academic credibility.
- Weakness: less useful for immediate nowcasting.
- Recommended role: retrospective validation after the event window stabilizes.

## Infrastructure And Discovery Sources

### NASA FIRMS

- Role: thermal corroboration for refinery fires, port fires, explosion aftermath, and persistent flaring changes.
- Caveat: thermal anomaly is not confirmed damage; clouds, overpass timing, false positives, and industrial heat sources matter.

### Cloudflare Radar Internet Outages

- Role: triage signal for communications or infrastructure disruption.
- Caveat: usually not point geometry; internet outage does not equal nighttime-light outage.

### GDELT DOC / BigQuery

- Role: broad lead and source URL discovery.
- Caveat: news geography is not reliable point geometry; do not treat GDELT hits as event confirmation.

## Nighttime Light And Remote-Sensing Evidence

- Primary NTL products: VIIRS daily products such as `VNP46A1`, `VNP46A2`, and official VJ/DNB workflows when available.
- Required checks: product availability, quality flags, cloud/snow/lunar/stray-light handling, UTC/local acquisition date boundary, and suitable baseline/control periods.
- Interpretation limit: NTL evidence supports verification or impact estimation; it does not replace event-source confirmation.

## Maintenance Rules

- Record `retrieved_at_utc`, `source_modified_utc`, and source URLs for every automated pull.
- Keep lead sources, primary confirmation sources, and secondary sources in separate fields.
- Mark source lag explicitly.
- Deduplicate and spatially cluster dense points before generating NTL tasks.
- For paper claims, require at least one geometry source plus one independent confirmation source, or one high-confidence official/operator source plus remote-sensing evidence.
