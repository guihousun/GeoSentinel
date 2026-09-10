# 可复用片段（按需取用）

这些片段是实现参考，不是必须照抄的流程。按任务需要选用，确认参数后再执行。

## 1) 结束端排他的日期助手

```python
from datetime import datetime, timedelta

def to_exclusive_end(date_str: str) -> str:
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

# 例：单日 2025-03-29
# filterDate("2025-03-29", to_exclusive_end("2025-03-29"))
```

## 2) 时区感知的首夜日期

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+

def get_first_night_date(event_date_str: str, event_time_local: str,
                         timezone_str: str, overpass_hour: int = 1, overpass_minute: int = 30):
    """事件发生在当地过境时刻之后 -> 当地首夜为次日；之前 -> 当日。"""
    tz = ZoneInfo(timezone_str)
    event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
    event_time = datetime.strptime(event_time_local, "%H:%M").time()
    event_dt = datetime.combine(event_date, event_time, tzinfo=tz)
    overpass_dt = datetime.combine(event_date,
                                   datetime.strptime(f"{overpass_hour:02d}:{overpass_minute:02d}", "%H:%M").time(),
                                   tzinfo=tz)
    first_night = event_date + timedelta(days=1) if event_dt > overpass_dt else event_date
    return first_night.strftime("%Y-%m-%d")

# 例：缅甸地震 2025-03-28 12:50 (Asia/Yangon) -> 2025-03-29
```

## 3) 时区换算

```python
from datetime import datetime
from zoneinfo import ZoneInfo

def convert_timezone(datetime_str: str, from_tz: str, to_tz: str,
                     fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    dt_from = datetime.strptime(datetime_str, fmt).replace(tzinfo=ZoneInfo(from_tz))
    return dt_from.astimezone(ZoneInfo(to_tz)).strftime(fmt)
```

## 4) 当地首夜 -> UTC 产品日期

```python
from datetime import datetime, time
from zoneinfo import ZoneInfo

def local_night_acquisition_to_utc(first_night_date: str, timezone_str: str,
                                   local_hour: int = 2, local_minute: int = 0):
    local_dt = datetime.combine(datetime.strptime(first_night_date, "%Y-%m-%d").date(),
                                time(local_hour, local_minute), tzinfo=ZoneInfo(timezone_str))
    utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    return {"local_datetime": local_dt.isoformat(),
            "utc_datetime": utc_dt.isoformat(),
            "utc_file_date": utc_dt.strftime("%Y-%m-%d")}

# 伊朗：当地 02-29 00:30-02:30 可对应 UTC 02-28 深夜，UTC 索引文件应取 02-28。
# 缅甸：事件 2025-03-28 06:20 UTC，首夜采集约 2025-03-29 00:30-02:30 MMT
#       = 2025-03-28 18:00-20:00 UTC，UTC 索引文件日期仍是 2025-03-28。
```

## 5) 像元级 UTC 时间核验

```python
def utc_time_minmax_from_vnp46a1(date_utc: str, geom):
    """仅在 VNP46A1 覆盖目标日期时使用；VNP46A2 不含 UTC_Time 波段。"""
    end_utc = to_exclusive_end(date_utc)
    img = (ee.ImageCollection("NOAA/VIIRS/001/VNP46A1")
           .filterDate(date_utc, end_utc)
           .filterBounds(geom)
           .first())
    return img.select("UTC_Time").reduceRegion(
        reducer=ee.Reducer.minMax(), geometry=geom, scale=500,
        maxPixels=1e13, bestEffort=True, tileScale=4)
```

## 6) 安全集合构建

```python
def load_vnp46a2(start_date: str, end_date_inclusive: str, geom):
    return (ee.ImageCollection("NASA/VIIRS/002/VNP46A2")
            .filterDate(start_date, to_exclusive_end(end_date_inclusive))
            .filterBounds(geom)
            .select("Gap_Filled_DNB_BRDF_Corrected_NTL"))
```

## 7) 稳健的 reduce

```python
def mean_antl(image, geom):
    return image.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=geom, scale=500,
        maxPixels=1e13, bestEffort=True, tileScale=2,
    ).get("Gap_Filled_DNB_BRDF_Corrected_NTL")
```

## 8) 无数据保护

```python
collection = load_vnp46a2(start_date, end_date, geom)
if collection.size().getInfo() == 0:
    return {"status": "no_data", "image_count": 0}
```
