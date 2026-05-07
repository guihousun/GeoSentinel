---
name: gee-ntl-date-boundary-handling
description: "Use for daily/event NTL timing and AOI handling: VIIRS first-night selection, filterDate end-exclusive windows, timezone conversion, event buffers, and no-data guards."
---

# GEE NTL Date & Boundary Handling

## Purpose
Prevent common failure modes in GEE nighttime light analysis:
- Wrong date window due to `filterDate` end-date exclusivity.
- Missing first-night image after daytime events.
- Conflating the local first-night label with the UTC-indexed image/file date.
- AOI mismatch from unverified bbox.
- Unstable reduction from missing `scale/maxPixels/tileScale`.

Core distinction:
- `local_first_night_date`: the local calendar night used for interpretation.
- `utc_file_date` / UTC query date: the date used by UTC-indexed products, GEE
  `system:time_start`, or official daily files after converting the local
  acquisition time to UTC.
- `local_acquisition_time` is not fixed. Treat 00:30-02:30 local as a typical
  candidate range unless product metadata or pixel-level UTC time confirms a
  narrower value.

## When To Use
Use this skill when task includes any of:
- `VNP46A2` / `VIIRS` daily analysis.
- Event impact assessment (`pre-event`, `first-night`, `post-event`).
- Buffer-based ANTL statistics around epicenter.
- Timezone-sensitive event timing or same-day/no-data risks.

Do not trigger this skill for ordinary annual/monthly province statistics unless the task also involves event windows, first-night logic, daily imagery, or uncertain AOI timing.

## Non-Negotiable Rules
1. `filterDate(start, end)` is end-exclusive.
2. For single-day query, always use `end = start + 1 day`.
3. If event occurs after local VIIRS overpass/acquisition (often within ~00:30-02:30 local), `local_first_night_date = D+1`.
4. For official daily products/files indexed by UTC acquisition or UTC file date, convert the selected local first-night acquisition time to UTC before choosing the file/date.
5. If the UTC date decision is near midnight or otherwise ambiguous, verify using pixel-level `UTC_Time` only when the relevant source covers the target date. For recent dates beyond GEE VNP46A1 coverage, use LAADS/CMR granule timing or official product metadata before final date selection.
6. Prefer confirmed administrative boundary; avoid ad-hoc bbox for named regions.
7. Always set `scale` and `maxPixels` in event reductions.
8. For large event AOI, add `bestEffort=True` or `tileScale>1`.

## Execution Checklist
1. Confirm event local time and timezone.
2. Determine local first-night date using overpass rule.
3. Determine product date convention:
   - GEE daily `filterDate` over `ImageCollection` usually uses UTC `system:time_start`; use explicit one-day windows and inspect image dates when ambiguity matters.
   - Official VJ/DNB daily files may be indexed by UTC acquisition/file date, not local night date.
   - Convert local first-night acquisition time to UTC before selecting UTC-indexed files.
   - Do not assume a fixed 02:00 local overpass. Use a candidate range such as 00:30-02:30 local, then narrow it with `UTC_Time`/official metadata when the date boundary matters.
4. Before committing to a recent event window, run the latest-availability gate:
   - GEE: confirm the collection has updated through the required UTC date.
   - LAADS/CMR: confirm the short_name has granules through the required date.
   - If not updated, return a latency decision instead of analytical no-data.
5. Build period windows:
   - baseline: event-14d to event-7d
   - first-night local concept: D+1 when the event is after the local overpass
   - first-night product query: use the product's date convention. For UTC-indexed files/collections, query the UTC file date derived from the local acquisition time, not the local calendar date.
   - recovery: event+7d to event+14d
6. Use inclusive-end helper for all date windows.
7. Use validated AOI (admin boundary / confirmed geometry).
8. Run reductions with explicit safety parameters.
9. Return structured logs for image counts and no-data periods.

## Ambiguity Verification Flow
Use this when the local-night label and UTC-indexed file date might diverge:

1. Confirm event UTC time, local timezone, and local first-night label.
2. Build a candidate local acquisition window, typically about `00:30-02:30` local unless product metadata suggests otherwise.
3. Convert that local window to UTC and check whether it crosses a UTC date boundary.
4. If the UTC date choice affects which file/image will be queried, verify with one of:
   - pixel-level `UTC_Time` from `NOAA/VIIRS/001/VNP46A1`, only when GEE VNP46A1 covers the target UTC date,
   - official product metadata / granule timestamps,
   - official LAADS/CMR granule timing.
5. Record both:
   - `local_first_night_date`
   - `utc_file_date`
6. If verification cannot disambiguate the date safely, stop and return `needs_verification` instead of inventing an exact time.

## Reusable Snippets

### 1) Inclusive date helper
```python
from datetime import datetime, timedelta

def to_exclusive_end(date_str: str) -> str:
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

# Example: single day 2025-03-29
# filterDate("2025-03-29", to_exclusive_end("2025-03-29"))
```

### 2) Timezone-aware first-night calculation
```python
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python < 3.9

def get_first_night_date(event_date_str: str, event_time_local: str, 
                         timezone_str: str, overpass_hour: int = 1, overpass_minute: int = 30):
    """
    Determine the local first-night label based on VIIRS overpass timing.
    
    VIIRS overpass time: ~01:30 local time (varies by latitude)
    
    Rule: If event occurs AFTER local overpass on day D, local first-night = D+1
          If event occurs BEFORE local overpass on day D, local first-night = D
    
    Parameters:
    - event_date_str: 'YYYY-MM-DD'
    - event_time_local: 'HH:MM' (24-hour format, local time)
    - timezone_str: e.g., 'Asia/Shanghai', 'America/New_York'
    - overpass_hour, overpass_minute: VIIRS overpass time (default 01:30)
    
    Returns:
    - local_first_night_date: 'YYYY-MM-DD'
    """
    tz = ZoneInfo(timezone_str)
    event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
    event_time = datetime.strptime(event_time_local, "%H:%M").time()
    
    # Event datetime in local timezone
    event_dt = datetime.combine(event_date, event_time, tzinfo=tz)
    
    # Overpass time on the same calendar day
    overpass_dt = datetime.combine(event_date, 
                                    datetime.strptime(f"{overpass_hour:02d}:{overpass_minute:02d}", "%H:%M").time(),
                                    tzinfo=tz)
    
    # If event is after overpass, local first-night is next day
    if event_dt > overpass_dt:
        first_night = event_date + timedelta(days=1)
    else:
        first_night = event_date
    
    return first_night.strftime("%Y-%m-%d")

# Example: Myanmar earthquake 2025-03-28 12:50 (Asia/Yangon)
# overpass/acquisition is before the noon event -> after overpass
# local_first_night_date = 2025-03-29
# first_night = get_first_night_date("2025-03-28", "12:50", "Asia/Yangon")
# print(first_night)  # Output: 2025-03-29
```

### 3) Timezone conversion helper
```python
from datetime import datetime
from zoneinfo import ZoneInfo

def convert_timezone(datetime_str: str, from_tz: str, to_tz: str, 
                     fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Convert datetime from one timezone to another.
    
    Parameters:
    - datetime_str: datetime string in from_tz
    - from_tz: source timezone (e.g., 'Asia/Shanghai')
    - to_tz: target timezone (e.g., 'UTC')
    - fmt: datetime format string
    
    Returns:
    - Converted datetime string in to_tz
    """
    dt_naive = datetime.strptime(datetime_str, fmt)
    dt_from = dt_naive.replace(tzinfo=ZoneInfo(from_tz))
    dt_to = dt_from.astimezone(ZoneInfo(to_tz))
    return dt_to.strftime(fmt)

# Example: Convert local event time to UTC for GEE filter
# utc_time = convert_timezone("2025-03-29 15:30:00", "Asia/Yangon", "UTC")
```

### 4) Local first-night to UTC product date
```python
from datetime import datetime, time
from zoneinfo import ZoneInfo

def local_night_acquisition_to_utc(first_night_date: str, timezone_str: str,
                                   local_hour: int = 2, local_minute: int = 0):
    """
    Convert local first-night acquisition time to UTC.

    Use this when a daily product/file is indexed by UTC acquisition date rather
    than local night date. The local night date and UTC file date can differ.
    """
    local_date = datetime.strptime(first_night_date, "%Y-%m-%d").date()
    local_dt = datetime.combine(local_date, time(local_hour, local_minute), tzinfo=ZoneInfo(timezone_str))
    utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    return {
        "local_datetime": local_dt.isoformat(),
        "utc_datetime": utc_dt.isoformat(),
        "utc_file_date": utc_dt.strftime("%Y-%m-%d"),
    }

# Iran example from prior 地缘环境智能计算平台 validation:
# Local first-night around 02-29 00:30-02:30 in Iran can map to UTC 02-28 late evening
# depending on the exact local offset/acquisition time.
# Therefore, if official daily files are UTC-indexed, select the UTC 02-28 file,
# not blindly the local 02-29 file.
#
# Myanmar earthquake example:
# Event: 2025-03-28 06:20 UTC = 2025-03-28 12:50 MMT.
# The next local nighttime acquisition may fall in a range such as
# 2025-03-29 00:30-02:30 MMT, which maps to 2025-03-28 18:00-20:00 UTC.
# Therefore, for UTC-indexed daily products/files, the first-night image/file
# date is still 2025-03-28, not 2025-03-29; verify the exact time when needed.
# For recent events beyond GEE VNP46A1 coverage, use LAADS/CMR or official
# granule metadata instead of GEE VNP46A1.UTC_Time.
```

### 5) Pixel-level UTC time verification
```python
def utc_time_minmax_from_vnp46a1(date_utc: str, geom):
    """
    Inspect pixel-level UTC acquisition time when date-boundary ambiguity matters.

    In the public GEE catalog, NOAA/VIIRS/001/VNP46A1 exposes `UTC_Time`,
    but only use it when VNP46A1 covers the target date. NASA/VIIRS/002/VNP46A2
    does not expose this band, so VNP46A2 cannot be used directly for pixel-level
    UTC min/max validation. For recent dates beyond GEE VNP46A1 coverage, use
    LAADS/CMR granule timing or official metadata.
    """
    end_utc = to_exclusive_end(date_utc)
    img = (
        ee.ImageCollection("NOAA/VIIRS/001/VNP46A1")
        .filterDate(date_utc, end_utc)
        .filterBounds(geom)
        .first()
    )
    return img.select("UTC_Time").reduceRegion(
        reducer=ee.Reducer.minMax(),
        geometry=geom,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
        tileScale=4,
    )

# Smoke-test result from 地缘环境智能计算平台 local GEE on Myanmar epicenter 100 km:
# VNP46A1 2023-03-28 UTC_Time_min/max ~= 18.42..20.09 UTC hours,
# corresponding to about 00:55..02:35 MMT on the following local night.
```

### 6) Safe collection builder
```python
def load_vnp46a2(start_date: str, end_date_inclusive: str, geom):
    end_exclusive = to_exclusive_end(end_date_inclusive)
    return (
        ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
        .filterDate(start_date, end_exclusive)
        .filterBounds(geom)
        .select("Gap_Filled_DNB_BRDF_Corrected_NTL")
    )
```

### 7) Robust reduction
```python
def mean_antl(image, geom):
    stats = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
        tileScale=2,
    )
    return stats.get("Gap_Filled_DNB_BRDF_Corrected_NTL")
```

### 8) No-data guard
```python
collection = load_vnp46a2(start_date, end_date, geom)
count = collection.size().getInfo()
if count == 0:
    return {"status": "no_data", "image_count": 0}
```

## Boundary Strategy
- Preferred: confirmed administrative geometry from retrieval stage.
- Allowed fallback: epicenter buffers for impact analysis.
- Avoid: unnamed hardcoded bbox for named-city/province tasks.
- If AOI uncertainty exists, request boundary confirmation instead of guessing.

## Output Contract (Recommended)
Return at least:
- `period_name`, `start_date`, `end_date`, `image_count`, `status`
- `local_first_night_date`, `local_acquisition_time`, `utc_acquisition_time`, `utc_file_date` when product/date convention is UTC-sensitive
- `buffer_name` or `region_id`
- `antl_mean` (and optional `antl_std`)
- `notes` (first-night rule, timezone decision, boundary source)

## Case-Derived Guidance
From successful Myanmar impact runs in this workspace:
- First-night selection must be explicit and justified by local overpass timing.
- The Myanmar 2025 earthquake case has two dates that must not be merged:
  local first-night acquisition may fall within about 2025-03-29 00:30-02:30 MMT,
  while the corresponding UTC acquisition/file date remains 2025-03-28.
- If the product is UTC-indexed, query/select 2025-03-28 for that first-night
  image. Record the local night date separately for interpretation.
- When exact overpass timing affects the date decision, inspect pixel-level
  `UTC_Time` from VNP46A1/source products only when coverage includes the target
  date; otherwise use LAADS/CMR granule timing or official metadata. Do not
  invent an exact 02:00 local acquisition time.
- Buffer-based ANTL (25/50/100 km) is robust when admin boundaries are unavailable.
- `scale=500 + maxPixels + tileScale` significantly reduces reduction failures.

From Iran official VJ/DNB validation:
- "First night" is a local-night concept, but official files can be UTC-indexed.
- Iran local first-night around 02-29 00:30-02:30 can correspond to UTC 02-28 late evening.
- Select files by the product's date convention after timezone conversion, not by local calendar date alone.

## Anti-Patterns
- `filterDate("2025-03-29", "2025-03-29")` (always empty).
- Selecting UTC-indexed official daily files solely by local first-night calendar date.
- Running long loops with excessive `getInfo()` calls where server-side map/reduce is possible.
- Unbounded retries on no-data periods without window adjustment.
- Writing outputs via hardcoded absolute paths.

