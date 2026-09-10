from __future__ import annotations

import hashlib
import html
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree

import certifi
import requests


REQUEST_TIMEOUT_SECONDS = 22
MAX_EVENTS_PER_SOURCE = 10
USER_AGENT = "GeoIntelligenceMonitor/0.1 (+local-research-platform)"


def _now() -> int:
    return int(time.time())


def _truthy(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    return default if not value else value in {"1", "true", "yes", "on"}


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.getenv(name, default) or default))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _eonet_curl_fallback(
    url: str,
    *,
    params: dict[str, Any] | None,
    request_headers: dict[str, str],
    timeout_seconds: int,
) -> requests.Response:
    prepared_url = requests.Request("GET", url, params=params).prepare().url
    if not prepared_url or not prepared_url.startswith("https://eonet.gsfc.nasa.gov/"):
        raise ValueError("curl fallback is restricted to the NASA EONET HTTPS origin")
    if any(name.lower() in {"authorization", "cookie", "proxy-authorization"} for name in request_headers):
        raise ValueError("curl fallback does not accept sensitive request headers")

    command = [
        "curl.exe",
        "--silent",
        "--show-error",
        "--location",
        "--http1.1",
        "--max-time",
        str(timeout_seconds),
        "--user-agent",
        request_headers["User-Agent"],
        "--header",
        f"Accept: {request_headers['Accept']}",
        "--write-out",
        "\n%{http_code}",
        prepared_url,
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        timeout=timeout_seconds + 5,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise requests.exceptions.SSLError(f"NASA EONET curl fallback failed: {message[:180]}")
    body, separator, status = completed.stdout.rpartition(b"\n")
    if not separator or not status.isdigit():
        raise requests.exceptions.ConnectionError("NASA EONET curl fallback returned no HTTP status")

    response = requests.Response()
    response.status_code = int(status)
    response._content = body
    response.url = prepared_url
    response.encoding = "utf-8"
    response.headers["Content-Type"] = "application/json"
    response.headers["X-GeoSentinel-Transport"] = "curl-http1.1"
    return response


def _request(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> requests.Response:
    request_headers = {"User-Agent": USER_AGENT, "Accept": "application/json, application/xml, text/xml;q=0.9, */*;q=0.5"}
    request_headers.update(headers or {})
    timeout_seconds = _bounded_int("NTL_MONITOR_REQUEST_TIMEOUT_S", REQUEST_TIMEOUT_SECONDS, 5, 60)
    try:
        return requests.get(
            url,
            params=params,
            timeout=timeout_seconds,
            verify=certifi.where(),
            headers=request_headers,
        )
    except requests.exceptions.SSLError as exc:
        if not url.startswith("https://eonet.gsfc.nasa.gov/") or "UNEXPECTED_EOF" not in str(exc).upper():
            raise
        return _eonet_curl_fallback(
            url,
            params=params,
            request_headers=request_headers,
            timeout_seconds=timeout_seconds,
        )


def _clean_text(value: Any, limit: int = 520) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(re.sub(r"\s+", " ", text)).strip()
    return text[:limit]


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _timestamp(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return _now()
    for parser in (
        lambda: parsedate_to_datetime(text),
        lambda: datetime.fromisoformat(text.replace("Z", "+00:00")),
    ):
        try:
            parsed = parser()
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except Exception:
            continue
    return _now()


def _local_name(tag: str) -> str:
    return str(tag or "").rsplit("}", 1)[-1].lower()


def _item_text(item: ElementTree.Element, name: str) -> str:
    for node in item.iter():
        if _local_name(node.tag) == name.lower() and node.text:
            return _clean_text(node.text)
    return ""


def _geometry_point(value: Any) -> tuple[float | None, float | None]:
    if not isinstance(value, list):
        return None, None
    if len(value) >= 2 and isinstance(value[0], (int, float)) and isinstance(value[1], (int, float)):
        return _safe_float(value[0]), _safe_float(value[1])
    for child in value:
        longitude, latitude = _geometry_point(child)
        if longitude is not None and latitude is not None:
            return longitude, latitude
    return None, None


def _severity_from_alert(alert: str) -> str:
    level = str(alert or "").strip().lower()
    if level in {"red", "high", "critical"}:
        return "high"
    if level in {"orange", "medium", "moderate"}:
        return "medium"
    return "low"


def _severity_from_title(title: str) -> str:
    text = str(title or "").lower()
    if re.search(r"\b(war|attack|strike|blockade|invasion|missile|ceasefire|sanction)\b|冲突|袭击|封锁|制裁|战争", text):
        return "high"
    if re.search(r"\b(protest|election|border|security|military|displacement)\b|抗议|边境|安全|军事|流离失所", text):
        return "medium"
    return "low"


def _candidate(
    *,
    source_name: str,
    source_event_id: str,
    source_url: str,
    title: str,
    summary: str,
    event_type: str,
    severity: str,
    published_at: int,
    longitude: float | None = None,
    latitude: float | None = None,
    location_name: str = "",
    country: str = "",
    raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stable = "|".join([source_name, source_event_id or source_url or title, str(int(published_at or _now()) // 3600)])
    dedup_key = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:40]
    return {
        "candidate_id": f"candidate_{dedup_key[:16]}",
        "event_id": f"monitor_{dedup_key[:20]}",
        "dedup_key": dedup_key,
        "source_name": source_name,
        "source_event_id": str(source_event_id or ""),
        "source_url": str(source_url or ""),
        "title": _clean_text(title, 300) or "未命名公开事件",
        "summary": _clean_text(summary, 520),
        "event_type": str(event_type or "other"),
        "severity": severity if severity in {"high", "medium", "low"} else "low",
        "published_at": int(published_at or _now()),
        "longitude": longitude,
        "latitude": latitude,
        "location_name": _clean_text(location_name, 160),
        "country": _clean_text(country, 120),
        "source_refs": [{"name": source_name, "url": str(source_url or ""), "source_event_id": str(source_event_id or "")}],
        "raw": raw or {},
    }


def fetch_gdacs_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_name = "GDACS"
    response = _request("https://www.gdacs.org/xml/rss.xml")
    if response.status_code != 200:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "GDACS RSS 请求未成功。"}
    root = ElementTree.fromstring(response.content)
    max_items = limit or _bounded_int("NTL_MONITOR_MAX_EVENTS_PER_SOURCE", MAX_EVENTS_PER_SOURCE, 1, 40)
    candidates: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:max_items]:
        title = _item_text(item, "title")
        link = _item_text(item, "link")
        alert = _item_text(item, "alertlevel")
        event_type = _item_text(item, "eventtype") or "hazard"
        country = _item_text(item, "country")
        candidates.append(
            _candidate(
                source_name=source_name,
                source_event_id=_item_text(item, "eventid") or link or title,
                source_url=link,
                title=title,
                summary=_item_text(item, "description"),
                event_type=event_type.lower(),
                severity=_severity_from_alert(alert),
                published_at=_timestamp(_item_text(item, "pubdate")),
                longitude=_safe_float(_item_text(item, "long")),
                latitude=_safe_float(_item_text(item, "lat")),
                location_name=country,
                country=country,
                raw={"alert_level": alert, "event_type": event_type},
            )
        )
    return candidates, {"source": source_name, "status": "ok", "http_status": response.status_code, "count": len(candidates), "message": "全球灾害预警与协调系统。"}


def fetch_eonet_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_name = "NASA EONET"
    max_items = limit or _bounded_int("NTL_MONITOR_MAX_EVENTS_PER_SOURCE", MAX_EVENTS_PER_SOURCE, 1, 40)
    response = _request("https://eonet.gsfc.nasa.gov/api/v3/events", params={"status": "open", "limit": max_items})
    if response.status_code != 200:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "NASA EONET 请求未成功。"}
    payload = response.json()
    candidates: list[dict[str, Any]] = []
    for event in list(payload.get("events") or [])[:max_items]:
        geometries = list(event.get("geometry") or [])
        geometry = geometries[-1] if geometries else {}
        longitude, latitude = _geometry_point(geometry.get("coordinates"))
        categories = [str(value.get("title") or value.get("id") or "") for value in (event.get("categories") or [])]
        category = categories[0] if categories else "natural_event"
        sources = list(event.get("sources") or [])
        source_url = str((sources[0] if sources else {}).get("url") or event.get("link") or "")
        candidates.append(
            _candidate(
                source_name=source_name,
                source_event_id=str(event.get("id") or event.get("title") or ""),
                source_url=source_url,
                title=str(event.get("title") or ""),
                summary=str(event.get("description") or ""),
                event_type=category.lower().replace(" ", "_"),
                severity="medium" if category.lower() in {"wildfires", "severe storms", "floods", "volcanoes"} else "low",
                published_at=_timestamp(geometry.get("date") or event.get("closed") or event.get("created")),
                longitude=longitude,
                latitude=latitude,
                location_name=str(event.get("title") or ""),
                raw={"categories": categories, "geometry_date": geometry.get("date")},
            )
        )
    transport = response.headers.get("X-GeoSentinel-Transport", "requests")
    return candidates, {"source": source_name, "status": "ok", "http_status": response.status_code, "count": len(candidates), "message": "NASA 自然事件观测目录。", "transport": transport}


def _acled_token() -> str:
    token = str(os.getenv("NTL_MONITOR_ACLED_ACCESS_TOKEN", "") or "").strip()
    if token:
        return token
    username = str(os.getenv("NTL_MONITOR_ACLED_USERNAME", "") or "").strip()
    password = str(os.getenv("NTL_MONITOR_ACLED_PASSWORD", "") or "").strip()
    if not username or not password:
        return ""
    response = requests.post(
        "https://acleddata.com/oauth/token",
        data={"username": username, "password": password, "grant_type": "password", "client_id": "acled"},
        timeout=_bounded_int("NTL_MONITOR_REQUEST_TIMEOUT_S", REQUEST_TIMEOUT_SECONDS, 5, 60),
        verify=certifi.where(),
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    if response.status_code != 200:
        return ""
    try:
        return str(response.json().get("access_token") or "").strip()
    except ValueError:
        return ""


def fetch_acled_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Optional conflict/event source; skipped unless the deployment supplies OAuth credentials."""
    source_name = "ACLED"
    if not _truthy("NTL_MONITOR_ACLED_ENABLED", False):
        return [], {"source": source_name, "status": "disabled", "count": 0, "message": "未启用 ACLED 冲突事件源。"}
    token = _acled_token()
    if not token:
        return [], {"source": source_name, "status": "unconfigured", "count": 0, "message": "缺少 ACLED OAuth 访问凭据。"}
    max_items = limit or _bounded_int("NTL_MONITOR_MAX_EVENTS_PER_SOURCE", MAX_EVENTS_PER_SOURCE, 1, 40)
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=_bounded_int("NTL_MONITOR_ACLED_LOOKBACK_DAYS", 7, 1, 30))
    fields = "event_id_cnty|event_date|event_type|sub_event_type|country|admin1|location|latitude|longitude|fatalities|notes|source|source_scale"
    response = _request(
        "https://acleddata.com/api/acled/read",
        params={
            "limit": max_items,
            "event_date": f"{start.isoformat()}|{today.isoformat()}",
            "event_date_where": "BETWEEN",
            "fields": fields,
        },
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    if response.status_code != 200:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "ACLED 请求未成功。"}
    try:
        payload = response.json()
    except ValueError:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "ACLED 返回了非 JSON 响应。"}
    records = list(payload.get("data") or [])
    candidates: list[dict[str, Any]] = []
    for record in records[:max_items]:
        fatalities = int(float(record.get("fatalities") or 0))
        event_type = str(record.get("event_type") or "conflict_event")
        severity = "high" if fatalities >= 10 else "medium" if fatalities > 0 or event_type in {"Battles", "Explosions/Remote violence"} else "low"
        location = " · ".join(part for part in (str(record.get("location") or "").strip(), str(record.get("admin1") or "").strip()) if part)
        title = " · ".join(part for part in (event_type, str(record.get("sub_event_type") or "").strip(), location or str(record.get("country") or "").strip()) if part)
        candidates.append(
            _candidate(
                source_name=source_name,
                source_event_id=str(record.get("event_id_cnty") or ""),
                source_url="https://acleddata.com/dashboard/",
                title=title,
                summary=_clean_text(record.get("notes") or record.get("source") or ""),
                event_type="conflict",
                severity=severity,
                published_at=_timestamp(record.get("event_date")),
                longitude=_safe_float(record.get("longitude")),
                latitude=_safe_float(record.get("latitude")),
                location_name=location,
                country=str(record.get("country") or ""),
                raw={"fatalities": fatalities, "event_type": event_type, "source_scale": record.get("source_scale")},
            )
        )
    return candidates, {"source": source_name, "status": "ok", "http_status": response.status_code, "count": len(candidates), "message": "可选的冲突与政治暴力事件数据源。"}


def fetch_gdelt_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_name = "GDELT"
    if not _truthy("NTL_MONITOR_GDELT_ENABLED", True):
        return [], {"source": source_name, "status": "disabled", "count": 0, "message": "已通过配置关闭。"}
    max_items = limit or _bounded_int("NTL_MONITOR_MAX_EVENTS_PER_SOURCE", MAX_EVENTS_PER_SOURCE, 1, 40)
    query = str(os.getenv("NTL_MONITOR_GDELT_QUERY", "conflict OR protest OR sanctions OR blockade") or "").strip()
    response = _request(
        "https://api.gdeltproject.org/api/v2/doc/doc",
        params={"query": query, "mode": "artlist", "format": "json", "maxrecords": max_items, "timespan": "24h"},
    )
    if response.status_code == 429:
        return [], {"source": source_name, "status": "rate_limited", "http_status": 429, "count": 0, "message": "GDELT 已限流；下一轮按 30 分钟窗口重试。"}
    if response.status_code != 200:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "GDELT 请求未成功。"}
    try:
        payload = response.json()
    except json.JSONDecodeError:
        body = response.text.lower()
        if "please limit requests" in body or "rate limit" in body or "too many" in body:
            return [], {"source": source_name, "status": "rate_limited", "http_status": response.status_code, "count": 0, "message": "GDELT 返回限流提示；下一轮按 30 分钟窗口重试。"}
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "GDELT 返回了非 JSON 响应。"}
    candidates: list[dict[str, Any]] = []
    for article in list(payload.get("articles") or [])[:max_items]:
        title = str(article.get("title") or "")
        url = str(article.get("url") or "")
        candidates.append(
            _candidate(
                source_name=source_name,
                source_event_id=url or title,
                source_url=url,
                title=title,
                summary=(str(article.get("domain") or "") + (f" · {article.get('sourcecountry')}" if article.get("sourcecountry") else "")),
                event_type="geopolitical",
                severity=_severity_from_title(title),
                published_at=_timestamp(article.get("seendate")),
                country=str(article.get("sourcecountry") or ""),
                raw={"domain": article.get("domain"), "language": article.get("language"), "sourcecountry": article.get("sourcecountry")},
            )
        )
    return candidates, {"source": source_name, "status": "ok", "http_status": response.status_code, "count": len(candidates), "message": "公开新闻事件索引；仅作为线索，不替代原始证据核验。"}


# Public news feeds with no credential. They carry no coordinates, so entries
# stay list-only leads; the publisher's country is never treated as the event
# location. Feeds are filtered to geopolitics/conflict/disaster vocabulary so
# the channel does not fill the shared queue with sport and culture.
NEWS_FEEDS: tuple[tuple[str, str], ...] = (
    ("UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("RFE/RL", "https://www.rferl.org/api/zrqiteuuir"),
    ("DW", "https://rss.dw.com/rdf/rss-en-world"),
    ("Times of Israel", "https://www.timesofisrael.com/feed/"),
    ("Anadolu", "https://www.aa.com.tr/en/rss/default?cat=guncel"),
    ("TASS", "https://tass.com/rss/v2.xml"),
    ("Nikkei Asia", "https://asia.nikkei.com/rss/feed/nar"),
    ("SCMP", "https://www.scmp.com/rss/91/feed"),
    ("CNA", "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml"),
    ("Africanews", "https://www.africanews.com/feed/rss"),
    ("Crisis Group", "https://www.crisisgroup.org/rss.xml"),
)
NEWS_KEYWORDS = re.compile(
    r"\b("
    r"sanction|sanctions|conflict|war|warfare|strike|strikes|airstrike|missile|drone|shelling|offensive|"
    r"ceasefire|truce|treaty|border|invasion|occupation|annex|military|troops|army|navy|air force|"
    r"coup|junta|militant|insurgent|terror|attack|bombing|hostage|kidnap|massacre|atrocit|genocide|"
    r"protest|uprising|unrest|crackdown|martial law|curfew|blockade|embargo|mobiliz|escalat|"
    r"refugee|displace|humanitarian|famine|aid convoy|peacekeep|"
    r"nuclear|ballistic|cyberattack|espionage|spy|expel|ambassador|"
    r"earthquake|flood|wildfire|cyclone|typhoon|drought|volcano|tsunami|landslide|outbreak|epidemic"
    r")\b",
    re.IGNORECASE,
)
# Political-process words alone are too weak (they match sport, business and
# culture stories); they only count when at least two of them co-occur.
NEWS_WEAK_KEYWORDS = re.compile(
    r"\b(election|referendum|parliament|president|prime minister|regime|summit|talks|negotiat|"
    r"united nations|nato|european union|g7|brics|diplomat)\b",
    re.IGNORECASE,
)


def _news_relevant(title: str) -> bool:
    if NEWS_KEYWORDS.search(title):
        return True
    return len(set(match.group(0).lower() for match in NEWS_WEAK_KEYWORDS.finditer(title))) >= 2


def _rss_items(root: ElementTree.Element) -> list[ElementTree.Element]:
    """RSS 2.0 and RDF/XML both use `item`, but RDF puts it in a namespace."""
    return [node for node in root.iter() if _local_name(node.tag) == "item"]


def fetch_news_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """International news leads from public feeds: geopolitics, conflict, disaster."""
    source_name = "国际新闻"
    if not _truthy("NTL_MONITOR_NEWS_ENABLED", True):
        return [], {"source": source_name, "status": "disabled", "count": 0, "message": "已通过配置关闭国际新闻源。"}
    per_feed = max(1, min(5, _bounded_int("NTL_MONITOR_NEWS_PER_FEED", 2, 1, 5)))
    total_cap = limit or _bounded_int("NTL_MONITOR_NEWS_MAX", 20, 1, 40)
    feeds = NEWS_FEEDS
    configured = str(os.getenv("NTL_MONITOR_NEWS_FEEDS", "") or "").strip()
    if configured:
        parsed: list[tuple[str, str]] = []
        for entry in configured.split(","):
            name, _, url = entry.partition("=")
            if name.strip() and url.strip().startswith("http"):
                parsed.append((name.strip()[:40], url.strip()))
        if parsed:
            feeds = tuple(parsed)
    candidates: list[dict[str, Any]] = []
    ok_feeds = 0
    failures: list[str] = []
    # Fetch every feed, then take items round-robin so one prolific outlet
    # cannot fill the whole channel and regional coverage stays balanced.
    pools: list[tuple[str, list[dict[str, Any]]]] = []
    for name, url in feeds:
        try:
            response = _request(url)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{name}:{type(exc).__name__}")
            continue
        if response.status_code != 200:
            failures.append(f"{name}:{response.status_code}")
            continue
        try:
            root = ElementTree.fromstring(response.content)
        except ElementTree.ParseError:
            failures.append(f"{name}:xml")
            continue
        ok_feeds += 1
        pool: list[dict[str, Any]] = []
        for item in _rss_items(root):
            if len(pool) >= per_feed:
                break
            title = _item_text(item, "title")
            link = _item_text(item, "link")
            if not title or not link or not _news_relevant(title):
                continue
            pool.append(
                _candidate(
                    source_name=source_name,
                    source_event_id=link,
                    source_url=link,
                    title=title,
                    summary=f"{name} 公开新闻条目；无坐标，需结合原始报道与官方来源核验。",
                    event_type="geopolitical",
                    severity=_severity_from_title(title),
                    published_at=_timestamp(_item_text(item, "pubdate") or _item_text(item, "date")),
                    raw={"feed": name, "description": _item_text(item, "description")[:400]},
                )
            )
        if pool:
            pools.append((name, pool))
    for index in range(per_feed):
        for _, pool in pools:
            if len(candidates) >= total_cap:
                break
            if index < len(pool):
                candidates.append(pool[index])
        if len(candidates) >= total_cap:
            break
    if not ok_feeds:
        return [], {
            "source": source_name,
            "status": "error",
            "count": 0,
            "message": "国际新闻源本轮均不可用：" + (", ".join(failures[:4]) or "无响应"),
        }
    status = "ok" if not failures else "degraded"
    return candidates, {
        "source": source_name,
        "status": status,
        "count": len(candidates),
        "message": f"{ok_feeds}/{len(feeds)} 个公开新闻源可用；仅作为线索，无坐标。"
        + (f" 失败：{', '.join(failures[:4])}" if failures else ""),
    }


def fetch_emsc_events(limit: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """EMSC seismic portal FDSN feed: public GeoJSON, no credential, global recent events."""
    source_name = "EMSC"
    max_items = limit or _bounded_int("NTL_MONITOR_MAX_EVENTS_PER_SOURCE", MAX_EVENTS_PER_SOURCE, 1, 40)
    response = _request(
        "https://www.seismicportal.eu/fdsnws/event/1/query",
        params={"format": "json", "limit": max_items, "orderby": "time"},
    )
    if response.status_code != 200:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "EMSC 地震目录请求未成功。"}
    try:
        payload = response.json()
    except ValueError:
        return [], {"source": source_name, "status": "error", "http_status": response.status_code, "count": 0, "message": "EMSC 返回了非 JSON 响应。"}
    candidates: list[dict[str, Any]] = []
    for feature in list(payload.get("features") or [])[:max_items]:
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = list(geometry.get("coordinates") or [])
        magnitude = _safe_float(properties.get("mag"))
        region = _clean_text(properties.get("flynn_region"), 120)
        candidates.append(
            _candidate(
                source_name=source_name,
                source_event_id=str(properties.get("unid") or feature.get("id") or ""),
                source_url=f"https://www.seismicportal.eu/eventdetails.html?unid={properties.get('unid')}" if properties.get("unid") else "",
                title=f"M{magnitude:.1f} 地震 · {region}" if magnitude is not None else f"地震 · {region}",
                summary="EMSC 地震目录条目；震级与位置为目录值，影响与损失需另行核实。",
                event_type="earthquake",
                severity="high" if (magnitude or 0) >= 6 else "medium" if (magnitude or 0) >= 5 else "low",
                published_at=_timestamp(properties.get("time")),
                longitude=_safe_float(coordinates[0]) if len(coordinates) >= 2 else None,
                latitude=_safe_float(coordinates[1]) if len(coordinates) >= 2 else None,
                location_name=region,
                raw={"magtype": properties.get("magtype"), "depth": properties.get("depth"), "auth": properties.get("auth")},
            )
        )
    return candidates, {"source": source_name, "status": "ok", "http_status": response.status_code, "count": len(candidates), "message": "EMSC 全球近实时地震目录。"}


def collect_monitor_candidates() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []
    for collector in (fetch_gdacs_events, fetch_eonet_events, fetch_emsc_events, fetch_news_events, fetch_acled_events, fetch_gdelt_events):
        try:
            items, status = collector()
        except Exception as exc:  # noqa: BLE001
            items, status = [], {"source": collector.__name__, "status": "error", "count": 0, "message": f"{type(exc).__name__}: {str(exc)[:180]}"}
        candidates.extend(items)
        statuses.append(status)
    unique: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        unique.setdefault(str(candidate.get("dedup_key") or ""), candidate)
    return list(unique.values()), statuses
