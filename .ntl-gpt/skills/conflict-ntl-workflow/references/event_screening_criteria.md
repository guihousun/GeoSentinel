# ConflictNTL Event Screening Criteria

Last updated: 2026-05-04

This rule set screens whether conflict-event records can enter nighttime-light verification. It does not confirm event truth and does not attribute nighttime-light change to conflict by itself.

## Round 1: Traceability Screening

Round 1 asks whether a record can support downstream AOI construction and VIIRS retrieval.

Pass rule:

```text
time_score >= 10 AND coord_score >= 8 AND source_score >= 3 AND round1_score >= 25
```

### Time Score

| Condition | time_quality | Score |
|---|---|---:|
| `event_date_utc`, `event_date`, or `date` exists | `event_date_available` | 20 |
| only `post_date_utc` or `publication_date_utc` exists | `fallback_date_available` | 10 |
| no parseable date | `missing_date` | 0 |

### Coordinate Score

| Condition | coord_quality | Score | AOI policy |
|---|---|---:|---|
| lat/lon and `coord_type=exact` | `exact` | 25 | 2 km and 5 km buffers |
| lat/lon and `coord_type=general neighborhood` | `general neighborhood` | 15 | 2 km and 5 km buffers |
| lat/lon and `coord_type=pov` | `pov` | 15 | smallest usable admin AOI |
| lat/lon and `coord_type=general town` | `general_town` | 10 | smallest usable admin AOI |
| lat/lon with unknown precision | `coordinate_precision_unknown` | 8 | admin AOI by default |
| missing lat/lon | `missing_coordinates` | 0 | geocode or archive |

### Source Score

| Condition | source_quality | Score |
|---|---|---:|
| at least one strong source domain | `strong` | 15 |
| map/facility reference plus other source leads | `reference_plus_leads` | 8 |
| at least three sources with social/news leads | `social_multi_lead` | 5 |
| at least one weak lead | `weak_lead` | 3 |
| no source link | `missing_sources` | 0 |

Strong sources include official/operator sources and major verification or wire/media sources such as Reuters, AP, BBC, IAEA, CENTCOM, UN, Bloomberg, FT, NYTimes, WSJ, CNN, Al Jazeera, Bellingcat, and GeoConfirmed.

Weak leads include isolated X/Twitter, Telegram, YouTube, single local-media leads, Wikipedia timelines, or GDELT discovery hits. Weak leads can keep a record in the candidate queue, but they are not confirmation.

### Round 1 Status

| Status | Rule |
|---|---|
| `event_candidate` | pass rule satisfied |
| `needs_geocoding` | date exists but coordinate score is too low |
| `needs_source_hardening` | date and coordinates exist but source score is too low |
| `archive_only` | date missing or total score too low |

## Round 2: NTL Applicability

Round 2 only applies after Round 1. It asks whether the target can plausibly produce a nighttime-light signal.

| Label | Rule |
|---|---|
| `ntl_applicable` | fixed or interpretable ground targets such as refinery, oil terminal, oil infrastructure, fuel depot, LNG/gas facility, power/substation, port, airport, airbase, launch site, military base, naval base, missile base, HQ, internal-security facility, police/IRGC/Basij site, political/administrative facility, industrial zone, nuclear site, bridge, road, railway, transit node, or urban fixed target |
| `ntl_uncertain` | unknown target, no fixed target, air-defense-only activity, evacuation notice, clash/crossfire with no identifiable target, or explosion report with no interpretable ground target |
| `out_of_scope_non_conflict` | natural-disaster or non-conflict event class |

Final queue rule:

```text
round1_event_candidate_status == event_candidate AND ntl_relevance_level == ntl_applicable
```

## AOI Rule

| Coordinate / target case | AOI output |
|---|---|
| `exact` or `general neighborhood` point | 2 km and 5 km buffers |
| `pov`, `general town`, or unknown precision | smallest practical town, municipality, district, or other admin AOI |
| missing coordinates | geocode first; if only place name is available, use an admin AOI |

## Same-Day Aggregation Rule

| Input AOI | Aggregation rule | Output unit |
|---|---|---|
| 2 km / 5 km buffer AOI | same day, same radius, intersection area / min area >= 0.6 | `buffer_overlap_day` |
| administrative AOI | same day and same `admin_iso3 + admin_level + admin_id` | `admin_day` |

Keep singleton AOIs as valid analysis units. The goal is to reduce redundant NTL requests, not to prove that clustered points are the same facility.

## VIIRS Time Handling

VIIRS nighttime acquisition is usually near local early morning but should not be treated as a fixed 02:00 local time. For event-to-first-night matching, record local event time, candidate local overpass window, UTC acquisition date, and the UTC-indexed product/file date. Use product metadata or pixel-level timing when date boundaries matter.
