from __future__ import annotations

import csv
import json
import math
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager


ISW_STORYMAP_ITEM_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/"
    "089bc1a2fe684405a67d67f13bd31324?f=json"
)
ISW_STORYMAP_URL = "https://storymaps.arcgis.com/stories/089bc1a2fe684405a67d67f13bd31324"

DEFAULT_ISW_STORYMAP_LAYERS = [
    {
        "name": "combined_force_strikes_on_iran_2026",
        "label": "View Combined Force Strikes on Iran 2026",
        "url": "https://services5.arcgis.com/SaBe5HMtmnbqSWlu/arcgis/rest/services/MDS_CF_Strikes_on_Iran_2026_view/FeatureServer/0",
        "event_family": "us_israel_combined_force_strike",
    },
    {
        "name": "iran_axis_retaliatory_strikes_2026",
        "label": "View Iran Axis Retaliatory Strikes 2026",
        "url": "https://services5.arcgis.com/SaBe5HMtmnbqSWlu/arcgis/rest/services/View_Iran_Axis_Retaliatory_Strikes_2026/FeatureServer/0",
        "event_family": "iran_axis_retaliatory_strike",
    },
]

ISW_EVENT_FIELDS = [
    "source_storymap_url",
    "source_layer",
    "source_layer_url",
    "event_family",
    "objectid",
    "event_id",
    "event_date_utc",
    "post_date_utc",
    "publication_date_utc",
    "time_utc",
    "time_raw",
    "event_type",
    "confirmed",
    "struck",
    "actor",
    "side",
    "subject",
    "site_type",
    "site_subtype",
    "city",
    "province",
    "country",
    "latitude",
    "longitude",
    "coord_type",
    "source_1",
    "source_2",
    "sources",
]

STRONG_SOURCE_DOMAINS = {
    "reuters.com",
    "apnews.com",
    "bbc.com",
    "pbs.org",
    "ft.com",
    "bloomberg.com",
    "nytimes.com",
    "wsj.com",
    "cnn.com",
    "aljazeera.com",
    "iaea.org",
    "centcom.mil",
    "whitehouse.gov",
    "defense.gov",
    "un.org",
    "reliefweb.int",
    "bellingcat.com",
    "geoconfirmed.org",
}

WEAK_SOURCE_MARKERS = {
    "x.com",
    "twitter.com",
    "t.me",
    "telegram",
    "youtube.com",
    "youtu.be",
    "wikipedia.org",
    "gdelt",
}

CONFLICT_TERMS = {
    "airstrike",
    "strike",
    "missile",
    "rocket",
    "drone",
    "war",
    "conflict",
    "explosion",
    "attack",
    "retaliatory",
    "military",
    "shelling",
    "bombardment",
    "air defense",
}

NON_CONFLICT_TERMS = {"flood", "wildfire", "earthquake", "hurricane", "typhoon", "storm"}

UNCERTAIN_TARGET_TERMS = {
    "unknown",
    "air defense",
    "interception",
    "intercept",
    "evac notice",
    "evacuation notice",
    "clash",
    "crossfire",
}

FIXED_TARGET_TERMS = {
    "refinery",
    "oil terminal",
    "oil infrastructure",
    "fuel depot",
    "lng",
    "gas facility",
    "power",
    "substation",
    "port",
    "airport",
    "airbase",
    "air base",
    "base",
    "naval base",
    "missile base",
    "launch site",
    "hq",
    "industrial",
    "nuclear",
    "military base",
    "military",
    "internal security",
    "police",
    "irgc",
    "basij",
    "government",
    "administrative",
    "political",
    "bridge",
    "road",
    "road infrastructure",
    "railway",
    "railway infrastructure",
    "transit",
    "urban fixed target",
    "steel",
    "desalination",
    "utility",
    "grid",
    "building",
}


class ConflictNTLScreenEventsInput(BaseModel):
    events_path: str = Field(..., description="CSV or JSON event table under inputs/ or a virtual path.")
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")
    event_window_start: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")
    event_window_end: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")


class ConflictNTLGenerateAnalysisUnitsInput(BaseModel):
    screened_events_path: str = Field(..., description="Screened event CSV/JSON from conflict_ntl_screen_events_tool.")
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")
    buffer_radii_m: str = Field(default="2000,5000", description="Comma-separated point buffer radii in meters.")
    overlap_threshold: float = Field(default=0.6, description="Intersection/min-area threshold for same-day merge.")


class ConflictNTLSourceFreshnessInput(BaseModel):
    source: str = Field(default="isw_storymap", description="Currently supported: isw_storymap.")
    item_url: str = Field(default="", description="Optional ArcGIS item metadata URL override.")


class ConflictNTLFetchISWEventsInput(BaseModel):
    layer_urls_json: str = Field(
        default="",
        description="Optional JSON list of ISW ArcGIS FeatureServer layer definitions. Defaults to known ISW StoryMap layers.",
    )
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")
    event_window_start: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")
    event_window_end: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")
    page_size: int = Field(default=1000, description="ArcGIS query page size.")
    include_raw_layers: bool = Field(default=True, description="Write per-layer raw feature JSON snapshots.")


class ConflictNTLBuildCaseReportInput(BaseModel):
    case_name: str = Field(..., description="Human-readable case label.")
    screening_summary_path: str = Field(..., description="Path to screening_summary.json.")
    analysis_units_csv_path: str = Field(..., description="Path to analysis_units.csv.")
    top_candidates_csv_path: str = Field(default="", description="Optional path to top_candidates.csv.")
    freshness_json_path: str = Field(default="", description="Optional path to source freshness JSON.")
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")


class ConflictNTLCompareCaseBuffersInput(BaseModel):
    top_candidates_csv_path: str = Field(..., description="Path to top_candidates.csv or screened event CSV.")
    cases_json: str = Field(..., description="JSON list of cases: [{'case_id': str, 'event_ids': [..]}].")
    buffer_radii_m: str = Field(default="2000,5000,10000,20000", description="Comma-separated buffer radii.")
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")


class ConflictNTLAgentSystemInput(BaseModel):
    events_path: str = Field(..., description="CSV or JSON conflict event table under inputs/ or outputs/.")
    case_name: str = Field(default="ConflictNTL agent-system run", description="Human-readable run label.")
    output_root: str = Field(default="conflict_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")
    event_window_start: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")
    event_window_end: str = Field(default="", description="Optional inclusive YYYY-MM-DD event-date filter.")
    buffer_radii_m: str = Field(default="2000,5000", description="Comma-separated AOI/buffer radii.")
    overlap_threshold: float = Field(default=0.6, description="Intersection/min-area threshold for same-day merge.")


AGENT_ROLES: dict[str, dict[str, Any]] = {
    "ConflictNTL-Commander": {
        "responsibility": "Coordinate conflict-tracking requests, stage execution, state handoff, and final reporting.",
        "inputs": ["user request", "event source paths", "run configuration"],
        "outputs": ["agent_system_manifest", "agent_system_runbook", "case_report"],
    },
    "Conflict-Searcher": {
        "responsibility": "Track, normalize, and screen open-source conflict events for traceability and NTL applicability.",
        "inputs": ["raw event table", "event screening criteria"],
        "outputs": ["screened_events", "top_candidates", "screening_summary"],
    },
    "Data-Searcher": {
        "responsibility": "Build buffer/admin AOIs and task queues for official NTL data retrieval and downstream statistics.",
        "inputs": ["screened_events", "buffer radii", "overlap threshold"],
        "outputs": ["candidate_aois", "analysis_units", "task_queue"],
    },
    "Conflict-Analyst": {
        "responsibility": "Run multi-buffer NTL diagnostics, label caveats, and prepare non-attribution interpretations.",
        "inputs": ["top_candidates", "case definitions", "analysis_units", "VNP46A2 handoff contract"],
        "outputs": ["buffer_comparison", "case_report", "interpretation_guardrails"],
    },
}


INTERPRETATION_GUARDRAILS: dict[str, Any] = {
    "principle": "NTL anomalies are candidate signals for expert review, not standalone damage or attribution findings.",
    "preferred_language": (
        "The observed radiance anomaly is spatially and temporally consistent with the reported disruption, "
        "subject to quality-control, background-activity, and independent-validation checks."
    ),
    "avoid_claims": [
        "strike caused outage",
        "NTL verified damage",
        "automatic attribution",
        "all radiance drops are conflict impacts",
        "brightening proves fire without thermal validation",
    ],
    "required_caveats": [
        "source_caveat",
        "cloud_or_quality_caveat",
        "urban_background_caveat",
        "gas_flaring_or_fire_caveat",
        "low_baseline_radiance_caveat",
        "buffer_dilution_caveat",
        "requires_independent_validation",
    ],
}


VNP46A2_HANDOFF_CONTRACT: dict[str, Any] = {
    "ntl_product": "NASA/VIIRS/002/VNP46A2",
    "band": "Gap_Filled_DNB_BRDF_Corrected_NTL",
    "default_time_windows": {
        "baseline": "event_date -14 to event_date -7",
        "event_day": "event_date to event_date +1",
        "next_night_candidate": "event_date +1 to event_date +2",
        "recovery": "event_date +7 to event_date +14",
    },
    "recommended_metrics": [
        "mean_radiance",
        "valid_pixel_count",
        "delta_abs",
        "delta_pct",
        "lit_pixel_count_N",
        "delta_N_pct",
        "target_reference_ratio",
        "valid_day_ratio",
    ],
}


def _resolve_thread_id(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = config if isinstance(config, dict) else None
    if runtime_config is None:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
        configurable = runtime_config.get("configurable")
        if isinstance(configurable, dict):
            tid = str(configurable.get("thread_id") or "").strip()
            if tid:
                return tid
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _is_at_or_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _resolve_read_path(path_text: str, thread_id: str) -> Path:
    raw = str(path_text or "").strip()
    raw_path = Path(raw)
    workspace = storage_manager.get_workspace(thread_id).resolve()
    inputs_root = (workspace / "inputs").resolve()
    outputs_root = (workspace / "outputs").resolve()
    if storage_manager._is_deepagents_virtual_path(raw):
        path = storage_manager.resolve_workspace_relative_path(
            raw,
            thread_id=thread_id,
            default_root="outputs",
            create_parent=False,
            allow_memory=False,
        )
        if not path.exists():
            raise FileNotFoundError(f"Input not found: {path}")
        return path
    if raw_path.is_absolute():
        resolved = raw_path.resolve()
        if not (_is_at_or_under(resolved, inputs_root) or _is_at_or_under(resolved, outputs_root)):
            raise PermissionError("Absolute read paths must stay under the thread inputs or outputs directory.")
        if not resolved.exists():
            raise FileNotFoundError(f"Input not found: {resolved}")
        return resolved
    cwd_candidate = raw_path.resolve()
    if cwd_candidate.exists():
        if _is_at_or_under(cwd_candidate, inputs_root) or _is_at_or_under(cwd_candidate, outputs_root):
            return cwd_candidate
        raise PermissionError("Repository-relative read paths must stay under the thread inputs or outputs directory.")
    shared_candidate = (storage_manager.shared_dir / raw).resolve()
    if shared_candidate.exists():
        return shared_candidate
    path = storage_manager.resolve_workspace_relative_path(
        raw,
        thread_id=thread_id,
        default_root="outputs",
        create_parent=False,
        allow_memory=False,
    )
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")
    return path


def _resolve_run_dir(output_root: str, run_label: str, thread_id: str) -> Path:
    label = (run_label or "").strip() or f"conflict_ntl_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    root = storage_manager.resolve_workspace_relative_path(
        output_root,
        thread_id=thread_id,
        default_root="outputs",
        create_parent=True,
        allow_memory=False,
    )
    run_dir = (root / label).resolve()
    outputs_root = (storage_manager.get_workspace(thread_id) / "outputs").resolve()
    if not _is_at_or_under(run_dir, outputs_root):
        raise PermissionError("ConflictNTL outputs must stay under the thread outputs directory.")
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _read_records(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [dict(row) for row in csv.DictReader(f)]
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(data, list):
            return [dict(row) for row in data if isinstance(row, dict)]
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return [dict(row) for row in data["records"] if isinstance(row, dict)]
        if isinstance(data, dict) and isinstance(data.get("features"), list):
            out: list[dict[str, Any]] = []
            for feature in data["features"]:
                if not isinstance(feature, dict):
                    continue
                props = dict(feature.get("properties") or {})
                geom = feature.get("geometry") or {}
                coords = geom.get("coordinates") if isinstance(geom, dict) else None
                if isinstance(coords, list) and len(coords) >= 2 and geom.get("type") == "Point":
                    props.setdefault("longitude", coords[0])
                    props.setdefault("latitude", coords[1])
                out.append(props)
            return out
    raise ValueError("events_path must be .csv or .json")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_csv_with_fields(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    seen = set(fieldnames)
    all_fields = list(fieldnames)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                all_fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _text(*values: Any) -> str:
    return " ".join(str(v or "").strip() for v in values if str(v or "").strip()).lower()


def _first_value(row: dict[str, Any], names: tuple[str, ...]) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _event_date(row: dict[str, Any]) -> str:
    raw = _first_value(row, ("event_date_utc", "event_date", "date", "post_date_utc", "publication_date_utc"))
    return raw[:10] if raw else ""


def _has_float(row: dict[str, Any], name: str) -> bool:
    try:
        value = float(str(row.get(name, "")).strip())
    except (TypeError, ValueError):
        return False
    return math.isfinite(value)


def _collect_sources(row: dict[str, Any]) -> list[str]:
    keys = [k for k in row if k.lower().startswith("source") or k.lower() in {"url", "urls", "links", "references"}]
    values: list[str] = []
    for key in keys:
        raw = str(row.get(key) or "").strip()
        if not raw:
            continue
        for part in raw.replace("|", ";").split(";"):
            part = part.strip()
            if part:
                values.append(part)
    return values


def _score_time(row: dict[str, Any]) -> tuple[str, int]:
    if str(row.get("event_date_utc") or row.get("event_date") or row.get("date") or "").strip():
        return "event_date_available", 20
    if str(row.get("post_date_utc") or row.get("publication_date_utc") or "").strip():
        return "fallback_date_available", 10
    return "missing_date", 0


def _score_coord(row: dict[str, Any]) -> tuple[str, int, str]:
    coord_type = _first_value(row, ("coord_type", "coordinate_precision", "precision")).lower()
    has_xy = _has_float(row, "latitude") and _has_float(row, "longitude")
    if not has_xy:
        return "missing_coordinates", 0, coord_type
    if coord_type == "exact":
        return "exact", 25, coord_type
    if coord_type == "general neighborhood":
        return "general neighborhood", 15, coord_type
    if coord_type == "pov":
        return "pov", 15, coord_type
    if coord_type == "general town":
        return "general_town", 10, coord_type
    return "coordinate_precision_unknown", 8, coord_type


def _score_source(row: dict[str, Any]) -> tuple[str, int, str]:
    sources = _collect_sources(row)
    joined = " ".join(sources).lower()
    if any(domain in joined for domain in STRONG_SOURCE_DOMAINS):
        return "strong", 15, "strong source present"
    if len(sources) >= 2 and any("map" in s.lower() or "arcgis" in s.lower() for s in sources):
        return "reference_plus_leads", 8, "reference source plus leads"
    if len(sources) >= 3 and sum(1 for marker in WEAK_SOURCE_MARKERS if marker in joined) >= 1:
        return "social_multi_lead", 5, "multiple social/news leads"
    if sources:
        return "weak_lead", 3, "lead source only; not confirmation"
    return "missing_sources", 0, "missing source links"


def _is_non_conflict(row: dict[str, Any]) -> bool:
    blob = _text(row.get("event_type"), row.get("category"), row.get("description"), row.get("notes"))
    return any(term in blob for term in NON_CONFLICT_TERMS) and not any(term in blob for term in CONFLICT_TERMS)


def _ntl_relevance(row: dict[str, Any]) -> tuple[str, str]:
    if _is_non_conflict(row):
        return "out_of_scope_non_conflict", "not a ConflictNTL event class"
    event_blob = _text(row.get("event_type"), row.get("description"), row.get("notes"))
    target_blob = _text(row.get("site_type"), row.get("site_subtype"), row.get("target"), row.get("subject"))
    if any(term in event_blob or term in target_blob for term in UNCERTAIN_TARGET_TERMS):
        return "ntl_uncertain", "unknown or non-ground-impact target"
    if any(term in target_blob for term in FIXED_TARGET_TERMS):
        return "ntl_applicable", "fixed or interpretable ground target"
    if any(term in event_blob for term in CONFLICT_TERMS) and any(term in target_blob for term in FIXED_TARGET_TERMS):
        return "ntl_applicable", "conflict event with fixed target"
    return "ntl_uncertain", "no fixed NTL-relevant target identified"


def _round1_status(time_score: int, coord_score: int, source_score: int, total: int) -> str:
    if time_score >= 10 and coord_score >= 8 and source_score >= 3 and total >= 25:
        return "event_candidate"
    if time_score >= 10 and coord_score < 8:
        return "needs_geocoding"
    if time_score >= 10 and coord_score >= 8 and source_score < 3:
        return "needs_source_hardening"
    return "archive_only"


def _within_window(event_date: str, start: str, end: str) -> bool:
    if start and event_date and event_date < start:
        return False
    if end and event_date and event_date > end:
        return False
    return True


def _arcgis_ms_to_iso(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number) or number < 946684800000:
        return ""
    return datetime.fromtimestamp(number / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _finite_float(value: Any) -> Optional[float]:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_lon_lat(lon: Any, lat: Any) -> bool:
    lon_number = _finite_float(lon)
    lat_number = _finite_float(lat)
    return lon_number is not None and lat_number is not None and -180 <= lon_number <= 180 and -90 <= lat_number <= 90


def _web_mercator_to_lon_lat(x: Any, y: Any) -> tuple[Any, Any]:
    x_number = _finite_float(x)
    y_number = _finite_float(y)
    if x_number is None or y_number is None:
        return x, y
    radius = 6378137.0
    lon = (x_number / radius) * (180.0 / math.pi)
    lat = (2.0 * math.atan(math.exp(y_number / radius)) - math.pi / 2.0) * (180.0 / math.pi)
    if _valid_lon_lat(lon, lat):
        return round(lon, 8), round(lat, 8)
    return x, y


def _feature_attr(feature: dict[str, Any], key: str, default: Any = "") -> Any:
    attrs = feature.get("attributes") if isinstance(feature.get("attributes"), dict) else {}
    return attrs.get(key, default)


def _parse_isw_layers(layer_urls_json: str) -> list[dict[str, str]]:
    raw = str(layer_urls_json or "").strip()
    data: Any = DEFAULT_ISW_STORYMAP_LAYERS
    if raw:
        data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("layer_urls_json must decode to a list of layer definitions.")
    layers: list[dict[str, str]] = []
    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError("Each ISW layer definition must be an object.")
        url = str(item.get("url") or "").strip()
        if not url:
            raise ValueError("Each ISW layer definition must include a url.")
        name = str(item.get("name") or f"isw_layer_{idx}").strip()
        label = str(item.get("label") or name).strip()
        family = str(item.get("event_family") or name).strip()
        layers.append({"name": name, "label": label, "url": url, "event_family": family})
    return layers


def _arcgis_rate_limit_delay_seconds(message: str) -> int:
    match = re.search(r"Retry after\s+(\d+)\s+sec", message, flags=re.IGNORECASE)
    if match:
        return max(1, int(match.group(1)) + 2)
    return 62


def _arcgis_query_json(
    layer_url: str,
    params: dict[str, Any],
    timeout_seconds: int,
    *,
    max_retries: int = 1,
) -> dict[str, Any]:
    query_url = f"{layer_url.rstrip('/')}/query?{urlencode(params)}"
    for attempt in range(max_retries + 1):
        req = Request(query_url, headers={"User-Agent": "NTL-GPT ConflictNTL/1.0"})
        try:
            with urlopen(req, timeout=timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429 and attempt < max_retries:
                time.sleep(62)
                continue
            raise
        if not isinstance(payload, dict):
            raise RuntimeError("ArcGIS FeatureServer query returned a non-object payload.")
        error = payload.get("error")
        if error:
            message = str(error)
            if ("429" in message or "Too many requests" in message) and attempt < max_retries:
                time.sleep(_arcgis_rate_limit_delay_seconds(message))
                continue
            raise RuntimeError(f"ArcGIS FeatureServer query failed: {error}")
        return payload
    raise RuntimeError("ArcGIS FeatureServer query failed after retry.")


def _fetch_arcgis_object_ids(
    layer_url: str,
    *,
    timeout_seconds: int,
    time_extent_ms: str,
) -> list[Any]:
    params: dict[str, Any] = {
        "f": "json",
        "where": "1=1",
        "returnIdsOnly": "true",
    }
    if time_extent_ms:
        params["time"] = time_extent_ms
    payload = _arcgis_query_json(layer_url, params, timeout_seconds)
    object_ids = payload.get("objectIds")
    if not isinstance(object_ids, list):
        return []
    return [object_id for object_id in object_ids if object_id not in ("", None)]


def _fetch_arcgis_features_by_ids(
    layer_url: str,
    object_ids: list[Any],
    *,
    timeout_seconds: int,
    chunk_size: int,
) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    safe_chunk_size = max(1, int(chunk_size or 1000))
    for start in range(0, len(object_ids), safe_chunk_size):
        batch_ids = object_ids[start : start + safe_chunk_size]
        params = {
            "f": "json",
            "objectIds": ",".join(str(object_id) for object_id in batch_ids),
            "outFields": "*",
            "returnGeometry": "true",
        }
        payload = _arcgis_query_json(layer_url, params, timeout_seconds)
        batch = payload.get("features")
        if isinstance(batch, list):
            features.extend(dict(item) for item in batch if isinstance(item, dict))
    return features


def _fetch_arcgis_feature_layer(
    layer_url: str,
    *,
    timeout_seconds: int = 30,
    page_size: int = 1000,
    time_extent_ms: str = "",
) -> list[dict[str, Any]]:
    if time_extent_ms:
        object_ids = _fetch_arcgis_object_ids(
            layer_url,
            timeout_seconds=timeout_seconds,
            time_extent_ms=time_extent_ms,
        )
        if not object_ids:
            return []
        return _fetch_arcgis_features_by_ids(
            layer_url,
            object_ids,
            timeout_seconds=timeout_seconds,
            chunk_size=page_size,
        )

    features: list[dict[str, Any]] = []
    offset = 0
    safe_page_size = max(1, int(page_size or 1000))
    while True:
        params = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": safe_page_size,
            "orderByFields": "OBJECTID",
        }
        if time_extent_ms:
            params["time"] = time_extent_ms
        payload = _arcgis_query_json(layer_url, params, timeout_seconds)
        batch = payload.get("features")
        if not isinstance(batch, list) or not batch:
            break
        features.extend(dict(item) for item in batch if isinstance(item, dict))
        exceeded = bool(payload.get("exceededTransferLimit")) if isinstance(payload, dict) else False
        if len(batch) < safe_page_size and not exceeded:
            break
        offset += len(batch)
    return features


def _date_window_to_arcgis_time(start: str, end: str) -> str:
    if not start and not end:
        return ""
    start_text = (start or end).strip()
    end_text = (end or start).strip()
    try:
        start_dt = datetime.fromisoformat(start_text[:10]).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(end_text[:10]).replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999000,
            tzinfo=timezone.utc,
        )
    except ValueError:
        return ""
    return f"{int(start_dt.timestamp() * 1000)},{int(end_dt.timestamp() * 1000)}"


def _normalize_isw_feature(feature: dict[str, Any], layer: dict[str, str]) -> dict[str, Any]:
    geom = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
    attr_lon = _feature_attr(feature, "longitude", "")
    attr_lat = _feature_attr(feature, "latitude", "")
    geom_lon = geom.get("x", "")
    geom_lat = geom.get("y", "")
    if _valid_lon_lat(attr_lon, attr_lat):
        lon, lat = attr_lon, attr_lat
    elif _valid_lon_lat(geom_lon, geom_lat):
        lon, lat = geom_lon, geom_lat
    else:
        lon, lat = _web_mercator_to_lon_lat(geom_lon, geom_lat)
    strike_date = _arcgis_ms_to_iso(_feature_attr(feature, "strikedate"))
    post_date = _arcgis_ms_to_iso(_feature_attr(feature, "post_date"))
    pub_date = _arcgis_ms_to_iso(_feature_attr(feature, "pub_date"))
    event_date = strike_date or post_date or pub_date
    return {
        "source_storymap_url": ISW_STORYMAP_URL,
        "source_layer": layer["label"],
        "source_layer_url": layer["url"],
        "event_family": layer["event_family"],
        "objectid": _feature_attr(feature, "OBJECTID"),
        "event_id": _feature_attr(feature, "event_id", _feature_attr(feature, "OBJECTID")),
        "event_date_utc": event_date,
        "post_date_utc": post_date,
        "publication_date_utc": pub_date,
        "time_utc": _arcgis_ms_to_iso(_feature_attr(feature, "time")),
        "time_raw": _feature_attr(feature, "time"),
        "event_type": _feature_attr(feature, "event_type"),
        "confirmed": _feature_attr(feature, "confirmed"),
        "struck": _feature_attr(feature, "struck"),
        "actor": _feature_attr(feature, "actor"),
        "side": _feature_attr(feature, "side"),
        "subject": _feature_attr(feature, "subject"),
        "site_type": _feature_attr(feature, "site_type"),
        "site_subtype": _feature_attr(feature, "siteStype"),
        "city": _feature_attr(feature, "city"),
        "province": _feature_attr(feature, "province"),
        "country": _feature_attr(feature, "country"),
        "latitude": lat,
        "longitude": lon,
        "coord_type": _feature_attr(feature, "coord_type"),
        "source_1": _feature_attr(feature, "source_1"),
        "source_2": _feature_attr(feature, "source_2"),
        "sources": _feature_attr(feature, "sources"),
    }


def run_conflict_ntl_fetch_isw_events(
    layer_urls_json: str = "",
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    event_window_start: str = "",
    event_window_end: str = "",
    page_size: int = 1000,
    include_raw_layers: bool = True,
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    layers = _parse_isw_layers(layer_urls_json)

    rows: list[dict[str, Any]] = []
    features: list[dict[str, Any]] = []
    layer_meta: list[dict[str, Any]] = []
    raw_files: dict[str, str] = {}
    errors: list[dict[str, str]] = []
    time_extent_ms = _date_window_to_arcgis_time(event_window_start, event_window_end)

    for layer in layers:
        try:
            raw_features = _fetch_arcgis_feature_layer(
                layer["url"],
                page_size=page_size,
                time_extent_ms=time_extent_ms,
            )
            layer_status = "complete"
            layer_error = ""
        except Exception as exc:
            raw_features = []
            layer_status = "error"
            layer_error = str(exc)
            errors.append({"layer": layer["name"], "url": layer["url"], "error": layer_error})
        if include_raw_layers:
            raw_path = run_dir / f"{layer['name']}_raw.json"
            raw_path.write_text(json.dumps(raw_features, ensure_ascii=False, indent=2), encoding="utf-8")
            raw_files[f"{layer['name']}_raw"] = str(raw_path)
        layer_rows = [_normalize_isw_feature(feature, layer) for feature in raw_features]
        in_window = [
            row
            for row in layer_rows
            if _within_window(str(row.get("event_date_utc") or "")[:10], event_window_start, event_window_end)
        ]
        rows.extend(in_window)
        for row in in_window:
            if _has_float(row, "latitude") and _has_float(row, "longitude"):
                features.append(
                    {
                        "type": "Feature",
                        "properties": row,
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(row["longitude"]), float(row["latitude"])],
                        },
                    }
                )
        layer_meta.append(
            {
                "name": layer["name"],
                "label": layer["label"],
                "url": layer["url"],
                "event_family": layer["event_family"],
                "status": layer_status,
                "feature_count_raw": len(raw_features),
                "feature_count_in_window": len(in_window),
                "error": layer_error,
            }
        )

    csv_path = run_dir / "isw_storymap_events.csv"
    geojson_path = run_dir / "isw_storymap_events.geojson"
    metadata_path = run_dir / "isw_storymap_events_metadata.json"
    _write_csv_with_fields(csv_path, rows, ISW_EVENT_FIELDS)
    geojson_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    metadata = {
        "schema": "conflict_ntl.isw_event_fetch.metadata.v1",
        "status": "complete" if not errors else "partial",
        "storymap_url": ISW_STORYMAP_URL,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "date_window_utc": {"start": event_window_start, "end": event_window_end},
        "arcgis_time_filter_ms": time_extent_ms,
        "layers": layer_meta,
        "total_features_raw": sum(layer["feature_count_raw"] for layer in layer_meta),
        "total_records": len(rows),
        "errors": errors,
        "output_files": {
            "events_csv": str(csv_path),
            "events_geojson": str(geojson_path),
            **raw_files,
        },
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "schema": "conflict_ntl.isw_event_fetch.v1",
        "status": metadata["status"],
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary": {
            "layer_count": len(layers),
            "total_features_raw": metadata["total_features_raw"],
            "total_records": len(rows),
            "error_count": len(errors),
        },
        "errors": errors,
        "output_files": {
            "events_csv": str(csv_path),
            "events_geojson": str(geojson_path),
            "metadata_json": str(metadata_path),
            **raw_files,
        },
    }


def run_conflict_ntl_screen_events(
    events_path: str,
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    event_window_start: str = "",
    event_window_end: str = "",
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    input_path = _resolve_read_path(events_path, thread_id)
    run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    raw_records = _read_records(input_path)
    records = [r for r in raw_records if _within_window(_event_date(r), event_window_start, event_window_end)]

    screened: list[dict[str, Any]] = []
    for idx, row in enumerate(records, start=1):
        out = dict(row)
        event_id = _first_value(out, ("event_id", "id", "OBJECTID", "objectid")) or f"event_{idx}"
        date_text = _event_date(out)
        time_quality, time_score = _score_time(out)
        coord_quality, coord_score, coord_type = _score_coord(out)
        source_quality, source_score, source_note = _score_source(out)
        round1_score = time_score + coord_score + source_score
        round1 = _round1_status(time_score, coord_score, source_score, round1_score)
        ntl_level, ntl_reason = _ntl_relevance(out)
        conflict_candidate = round1 == "event_candidate" and ntl_level == "ntl_applicable"
        verification_notes = (
            f"{source_note}; screening supports queue triage, not confirmation; {ntl_reason}"
        )
        out.update(
            {
                "event_id": event_id,
                "event_date_utc": date_text,
                "time_quality": time_quality,
                "time_score": time_score,
                "coord_quality": coord_quality,
                "coord_score": coord_score,
                "coord_type": coord_type or str(out.get("coord_type") or ""),
                "source_quality": source_quality,
                "source_score": source_score,
                "round1_score": round1_score,
                "round1_event_candidate_status": round1,
                "ntl_relevance_level": ntl_level,
                "conflict_ntl_candidate": str(conflict_candidate).lower(),
                "event_confirmation_status": "not_confirmed_by_screening",
                "verification_notes": verification_notes,
            }
        )
        screened.append(out)

    top_candidates = [r for r in screened if r["conflict_ntl_candidate"] == "true"]
    summary = {
        "schema": "conflict_ntl.event_screening.summary.v1",
        "status": "complete",
        "input_path": str(input_path),
        "total_input_records": len(raw_records),
        "total_screened_records": len(screened),
        "top_candidate_count": len(top_candidates),
        "round1_status_counts": dict(Counter(r["round1_event_candidate_status"] for r in screened)),
        "ntl_relevance_counts": dict(Counter(r["ntl_relevance_level"] for r in screened)),
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
    }

    screened_path = run_dir / "screened_events.csv"
    top_path = run_dir / "top_candidates.csv"
    summary_path = run_dir / "screening_summary.json"
    _write_csv(screened_path, screened)
    _write_csv(top_path, top_candidates)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "schema": "conflict_ntl.event_screening.v1",
        "status": "complete",
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary": summary,
        "output_files": {
            "screened_events": str(screened_path),
            "top_candidates": str(top_path),
            "screening_summary": str(summary_path),
        },
    }


def _parse_radii(raw: str) -> list[int]:
    out: list[int] = []
    for part in str(raw or "").split(","):
        text = part.strip()
        if not text:
            continue
        value = int(float(text))
        if value <= 0:
            raise ValueError("buffer radii must be positive")
        if value not in out:
            out.append(value)
    return out or [2000, 5000]


def _float(row: dict[str, Any], key: str) -> float:
    value = float(str(row.get(key, "")).strip())
    if not math.isfinite(value):
        raise ValueError(f"{key} must be finite")
    return value


def _is_admin_coord_type(coord_type: str) -> bool:
    return coord_type.lower().strip() in {"pov", "general town", "general_town", "coordinate_precision_unknown", "unknown"}


def _circle_polygon(lon: float, lat: float, radius_m: int, vertices: int = 48) -> list[list[float]]:
    lat_factor = 111_320.0
    lon_factor = max(1.0, 111_320.0 * math.cos(math.radians(lat)))
    coords: list[list[float]] = []
    for i in range(vertices):
        angle = 2 * math.pi * i / vertices
        coords.append([lon + math.cos(angle) * radius_m / lon_factor, lat + math.sin(angle) * radius_m / lat_factor])
    coords.append(coords[0])
    return coords


def _distance_m(a: dict[str, Any], b: dict[str, Any]) -> float:
    lat1 = float(a["latitude"])
    lon1 = float(a["longitude"])
    lat2 = float(b["latitude"])
    lon2 = float(b["longitude"])
    mean_lat = math.radians((lat1 + lat2) / 2)
    dx = (lon2 - lon1) * 111_320.0 * math.cos(mean_lat)
    dy = (lat2 - lat1) * 111_320.0
    return math.hypot(dx, dy)


def _distance_m_points(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    mean_lat = math.radians((lat1 + lat2) / 2)
    dx = (lon2 - lon1) * 111_320.0 * math.cos(mean_lat)
    dy = (lat2 - lat1) * 111_320.0
    return math.hypot(dx, dy)


def _same_radius_overlap_ratio(distance_m: float, radius_m: int) -> float:
    r = float(radius_m)
    d = float(distance_m)
    if d <= 0:
        return 1.0
    if d >= 2 * r:
        return 0.0
    intersection = 2 * r * r * math.acos(d / (2 * r)) - 0.5 * d * math.sqrt(max(0.0, 4 * r * r - d * d))
    return intersection / (math.pi * r * r)


def _cluster_buffer_aois(aois: list[dict[str, Any]], threshold: float) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    for aoi in aois:
        placed = False
        for group in groups:
            if any(
                _same_radius_overlap_ratio(_distance_m(aoi, existing), int(aoi["radius_m"])) >= threshold
                for existing in group
            ):
                group.append(aoi)
                placed = True
                break
        if not placed:
            groups.append([aoi])
    return groups


def _eligible_for_analysis(row: dict[str, Any]) -> bool:
    return (
        str(row.get("round1_event_candidate_status") or "") == "event_candidate"
        and str(row.get("ntl_relevance_level") or "") == "ntl_applicable"
    )


def run_conflict_ntl_generate_analysis_units(
    screened_events_path: str,
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    buffer_radii_m: str = "2000,5000",
    overlap_threshold: float = 0.6,
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    input_path = _resolve_read_path(screened_events_path, thread_id)
    run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    rows = [r for r in _read_records(input_path) if _eligible_for_analysis(r)]
    radii = _parse_radii(buffer_radii_m)

    aois: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    for row in rows:
        event_id = str(row.get("event_id") or row.get("id") or "").strip()
        event_date = _event_date(row)
        coord_type = str(row.get("coord_type") or row.get("coord_quality") or "").strip().lower()
        if not event_id:
            continue
        if _has_float(row, "latitude") and _has_float(row, "longitude") and not _is_admin_coord_type(coord_type):
            lat = _float(row, "latitude")
            lon = _float(row, "longitude")
            for radius in radii:
                aoi_id = f"{event_id}_buffer_{radius}m"
                aoi = {
                    "aoi_id": aoi_id,
                    "event_id": event_id,
                    "event_date_utc": event_date,
                    "aoi_type": "buffer",
                    "radius_m": radius,
                    "latitude": lat,
                    "longitude": lon,
                }
                aois.append(aoi)
                tasks.append(aoi.copy())
        else:
            admin_iso3 = str(row.get("admin_iso3") or row.get("country_iso3") or "").strip()
            admin_level = str(row.get("admin_level") or "").strip()
            admin_id = str(row.get("admin_id") or row.get("city") or row.get("admin_name") or event_id).strip()
            task = {
                "aoi_id": f"{event_id}_admin",
                "event_id": event_id,
                "event_date_utc": event_date,
                "aoi_type": "admin",
                "admin_iso3": admin_iso3,
                "admin_level": admin_level,
                "admin_id": admin_id,
            }
            tasks.append(task)

    units: list[dict[str, Any]] = []
    by_buffer_key: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for aoi in aois:
        by_buffer_key[(str(aoi["event_date_utc"]), int(aoi["radius_m"]))].append(aoi)
    for (event_date, radius), group in by_buffer_key.items():
        for idx, cluster in enumerate(_cluster_buffer_aois(group, float(overlap_threshold)), start=1):
            event_ids = sorted(str(item["event_id"]) for item in cluster)
            units.append(
                {
                    "analysis_unit_id": f"buffer_{event_date}_{radius}m_{idx}",
                    "unit_type": "buffer_overlap_day",
                    "event_date_utc": event_date,
                    "radius_m": radius,
                    "source_event_ids": ";".join(event_ids),
                    "source_event_count": len(event_ids),
                    "aoi_count": len(cluster),
                }
            )

    admin_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        if task["aoi_type"] == "admin":
            key = (
                str(task["event_date_utc"]),
                str(task.get("admin_iso3") or ""),
                str(task.get("admin_level") or ""),
                str(task.get("admin_id") or ""),
            )
            admin_groups[key].append(task)
    for idx, ((event_date, admin_iso3, admin_level, admin_id), group) in enumerate(admin_groups.items(), start=1):
        event_ids = sorted(str(item["event_id"]) for item in group)
        units.append(
            {
                "analysis_unit_id": f"admin_{event_date}_{admin_iso3}_{admin_level}_{idx}",
                "unit_type": "admin_day",
                "event_date_utc": event_date,
                "admin_iso3": admin_iso3,
                "admin_level": admin_level,
                "admin_id": admin_id,
                "source_event_ids": ";".join(event_ids),
                "source_event_count": len(event_ids),
                "aoi_count": len(group),
            }
        )

    features = [
        {
            "type": "Feature",
            "properties": {k: v for k, v in aoi.items() if k not in {"latitude", "longitude"}},
            "geometry": {
                "type": "Polygon",
                "coordinates": [_circle_polygon(float(aoi["longitude"]), float(aoi["latitude"]), int(aoi["radius_m"]))],
            },
        }
        for aoi in aois
    ]
    candidate_aois = {"type": "FeatureCollection", "features": features}
    task_queue = {
        "schema": "conflict_ntl.task_queue.v1",
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "tasks": tasks,
    }

    candidate_aois_path = run_dir / "candidate_aois.geojson"
    analysis_units_geojson_path = run_dir / "analysis_units.geojson"
    analysis_units_csv_path = run_dir / "analysis_units.csv"
    task_queue_path = run_dir / "task_queue.json"
    candidate_aois_path.write_text(json.dumps(candidate_aois, ensure_ascii=False, indent=2), encoding="utf-8")
    analysis_units_geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "properties": unit, "geometry": None}
                    for unit in units
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_csv(analysis_units_csv_path, units)
    task_queue_path.write_text(json.dumps(task_queue, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "schema": "conflict_ntl.analysis_units.v1",
        "status": "complete",
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary": {
            "eligible_event_count": len(rows),
            "buffer_aoi_count": len(aois),
            "task_count": len(tasks),
            "analysis_unit_count": len(units),
        },
        "output_files": {
            "candidate_aois": str(candidate_aois_path),
            "analysis_units_geojson": str(analysis_units_geojson_path),
            "analysis_units_csv": str(analysis_units_csv_path),
            "task_queue": str(task_queue_path),
        },
    }


def run_conflict_ntl_source_freshness(
    source: str = "isw_storymap",
    item_url: str = "",
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    _ = config
    source_key = str(source or "").strip().lower()
    if source_key != "isw_storymap":
        return {
            "schema": "conflict_ntl.source_freshness.v1",
            "status": "failed",
            "source": source_key,
            "error": "unsupported source",
            "supported_sources": ["isw_storymap"],
        }

    url = item_url.strip() or ISW_STORYMAP_ITEM_URL
    retrieved_at = datetime.now(timezone.utc)
    try:
        req = Request(url, headers={"User-Agent": "地缘环境智能计算平台-ConflictNTL/1.0"})
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {
            "schema": "conflict_ntl.source_freshness.v1",
            "status": "failed",
            "source": source_key,
            "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
            "error": str(exc),
        }

    modified_ms = payload.get("modified")
    modified_utc = ""
    age_hours = None
    if isinstance(modified_ms, (int, float)):
        modified_dt = datetime.fromtimestamp(float(modified_ms) / 1000.0, tz=timezone.utc)
        modified_utc = modified_dt.isoformat().replace("+00:00", "Z")
        age_hours = max(0.0, (retrieved_at.timestamp() - modified_dt.timestamp()) / 3600.0)
    status = "complete" if modified_utc else "partial"
    return {
        "schema": "conflict_ntl.source_freshness.v1",
        "status": status,
        "source": source_key,
        "item_url": url,
        "title": payload.get("title", ""),
        "source_modified_utc": modified_utc,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "age_hours": age_hours,
        "freshness_status": "freshness_observed" if modified_utc else "modified_time_missing",
        "notes": "Check StoryMap coverage label separately before assuming same-day coverage.",
    }


def run_conflict_ntl_build_case_report(
    case_name: str,
    screening_summary_path: str,
    analysis_units_csv_path: str,
    top_candidates_csv_path: str = "",
    freshness_json_path: str = "",
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    screening_path = _resolve_read_path(screening_summary_path, thread_id)
    units_path = _resolve_read_path(analysis_units_csv_path, thread_id)
    screening = json.loads(screening_path.read_text(encoding="utf-8-sig"))
    units = _read_csv_rows(units_path)
    unit_type_counts = dict(Counter(row.get("unit_type") or row.get("analysis_unit_type") or "" for row in units))

    top_sample: list[dict[str, str]] = []
    top_count = 0
    if str(top_candidates_csv_path or "").strip():
        top_path = _resolve_read_path(top_candidates_csv_path, thread_id)
        top_rows = _read_csv_rows(top_path)
        top_count = len(top_rows)
        keep = [
            "event_id",
            "event_date_utc",
            "country",
            "city",
            "site_type",
            "site_subtype",
            "source_quality",
            "round1_event_candidate_status",
            "ntl_relevance_level",
        ]
        top_sample = [{k: row.get(k, "") for k in keep if k in row} for row in top_rows[:10]]

    freshness: dict[str, Any] = {}
    if str(freshness_json_path or "").strip():
        freshness_path = _resolve_read_path(freshness_json_path, thread_id)
        freshness = json.loads(freshness_path.read_text(encoding="utf-8-sig"))

    report = {
        "schema": "conflict_ntl.case_report.v1",
        "status": "complete",
        "case_name": case_name,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "screening": {
            "total_input_records": screening.get("total_input_records", 0),
            "total_screened_records": screening.get("total_screened_records", 0),
            "top_candidate_count": screening.get("top_candidate_count", top_count),
            "round1_status_counts": screening.get("round1_status_counts", {}),
            "ntl_relevance_counts": screening.get("ntl_relevance_counts", {}),
        },
        "analysis_units": {
            "total_units": len(units),
            "unit_type_counts": unit_type_counts,
        },
        "freshness": freshness,
        "top_candidate_sample": top_sample,
        "interpretation_limits": [
            "Screening does not confirm event truth.",
            "Nighttime-light change does not prove conflict attribution without source and control checks.",
            "Task queue size should be reduced by source hardening and operational priorities before expensive imagery execution.",
        ],
    }

    report_json_path = run_dir / "case_report.json"
    report_md_path = run_dir / "case_report.md"
    report_json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [
        f"# {case_name}",
        "",
        f"- generated_at_utc: {report['generated_at_utc']}",
        f"- total input records: {report['screening']['total_input_records']}",
        f"- top candidates: {report['screening']['top_candidate_count']}",
        f"- analysis units: {report['analysis_units']['total_units']}",
        f"- unit types: {json.dumps(unit_type_counts, ensure_ascii=False)}",
    ]
    if freshness:
        md_lines.extend(
            [
                f"- source freshness: {freshness.get('source', '')} {freshness.get('status', '')}",
                f"- source_modified_utc: {freshness.get('source_modified_utc', '')}",
            ]
        )
    md_lines.extend(["", "## Interpretation Limits"])
    md_lines.extend(f"- {item}" for item in report["interpretation_limits"])
    report_md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return {
        "schema": "conflict_ntl.case_report.v1",
        "status": "complete",
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary": {
            "top_candidate_count": report["screening"]["top_candidate_count"],
            "analysis_unit_count": report["analysis_units"]["total_units"],
        },
        "output_files": {
            "case_report_json": str(report_json_path),
            "case_report_md": str(report_md_path),
        },
    }


def _parse_cases_json(raw: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("cases_json must be a JSON list.") from exc
    if not isinstance(data, list):
        raise ValueError("cases_json must be a JSON list.")
    cases: list[dict[str, Any]] = []
    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError("Each case must be an object.")
        event_ids = [str(v).strip() for v in item.get("event_ids", []) if str(v).strip()]
        if not event_ids:
            raise ValueError("Each case must include non-empty event_ids.")
        cases.append(
            {
                "case_id": str(item.get("case_id") or f"case_{idx}").strip(),
                "case_name": str(item.get("case_name") or item.get("case_id") or f"case_{idx}").strip(),
                "event_ids": event_ids,
            }
        )
    return cases


def _signal_hypothesis(rows: list[dict[str, str]]) -> str:
    blob = " ".join(
        f"{row.get('site_type', '')} {row.get('site_subtype', '')} {row.get('event_type', '')}"
        for row in rows
    ).lower()
    if any(term in blob for term in ("oil", "refinery", "lng", "gas", "energy", "industrial")):
        return "industrial_energy_signal"
    if any(term in blob for term in ("airbase", "airport", "naval base", "military")):
        return "military_facility_signal"
    if any(term in blob for term in ("police", "irgc", "basij", "political", "administrative")):
        return "urban_security_signal"
    return "mixed_or_uncertain_signal"


def _risk_for_radius(radius_m: int, rows: list[dict[str, str]]) -> str:
    signal = _signal_hypothesis(rows)
    if radius_m >= 20000:
        return "high"
    if radius_m >= 10000:
        return "medium_high"
    if signal == "urban_security_signal" and radius_m >= 5000:
        return "high"
    if signal == "urban_security_signal":
        return "medium"
    if radius_m >= 5000:
        return "medium"
    return "low"


def _json_counts(counter: Counter[str]) -> str:
    return json.dumps(dict(counter), ensure_ascii=False, sort_keys=True)


def run_conflict_ntl_compare_case_buffers(
    top_candidates_csv_path: str,
    cases_json: str,
    buffer_radii_m: str = "2000,5000,10000,20000",
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    input_path = _resolve_read_path(top_candidates_csv_path, thread_id)
    run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    rows = _read_csv_rows(input_path)
    by_id = {str(row.get("event_id") or "").strip(): row for row in rows}
    cases = _parse_cases_json(cases_json)
    radii = _parse_radii(buffer_radii_m)

    comparison_rows: list[dict[str, Any]] = []
    case_summaries: list[dict[str, Any]] = []
    for case in cases:
        seed_rows = [by_id[event_id] for event_id in case["event_ids"] if event_id in by_id]
        if not seed_rows:
            raise ValueError(f"No matching events found for case {case['case_id']}.")
        points = [
            {
                "event_id": str(row.get("event_id") or ""),
                "lat": _float(row, "latitude"),
                "lon": _float(row, "longitude"),
            }
            for row in seed_rows
            if _has_float(row, "latitude") and _has_float(row, "longitude")
        ]
        if not points:
            raise ValueError(f"Case {case['case_id']} has no usable point coordinates.")
        centroid_lat = sum(p["lat"] for p in points) / len(points)
        centroid_lon = sum(p["lon"] for p in points) / len(points)
        event_dates = sorted({str(row.get("event_date_utc") or "")[:10] for row in seed_rows if str(row.get("event_date_utc") or "")})
        signal = _signal_hypothesis(seed_rows)
        case_summaries.append(
            {
                "case_id": case["case_id"],
                "case_name": case["case_name"],
                "seed_event_ids": ";".join(case["event_ids"]),
                "seed_event_count": len(seed_rows),
                "centroid_latitude": centroid_lat,
                "centroid_longitude": centroid_lon,
                "event_dates": ";".join(event_dates),
                "dominant_signal_hypothesis": signal,
            }
        )

        candidate_pool: list[dict[str, str]] = []
        for row in rows:
            if not (_has_float(row, "latitude") and _has_float(row, "longitude")):
                continue
            row_date = str(row.get("event_date_utc") or "")[:10]
            if event_dates and row_date not in event_dates:
                continue
            candidate_pool.append(row)

        for radius in radii:
            included: list[dict[str, str]] = []
            for row in candidate_pool:
                dist = _distance_m_points(centroid_lat, centroid_lon, _float(row, "latitude"), _float(row, "longitude"))
                if dist <= radius:
                    included.append(row)
            area_km2 = math.pi * (radius / 1000.0) ** 2
            comparison_rows.append(
                {
                    "case_id": case["case_id"],
                    "case_name": case["case_name"],
                    "radius_m": radius,
                    "area_km2": round(area_km2, 6),
                    "centroid_latitude": round(centroid_lat, 8),
                    "centroid_longitude": round(centroid_lon, 8),
                    "seed_event_ids": ";".join(case["event_ids"]),
                    "included_event_ids": ";".join(str(row.get("event_id") or "") for row in included),
                    "included_event_count": len(included),
                    "event_density_per_km2": round(len(included) / area_km2, 6) if area_km2 else 0.0,
                    "source_quality_counts": _json_counts(Counter(str(row.get("source_quality") or "") for row in included)),
                    "site_type_counts": _json_counts(Counter(str(row.get("site_type") or "") for row in included)),
                    "site_subtype_counts": _json_counts(Counter(str(row.get("site_subtype") or "") for row in included)),
                    "dominant_signal_hypothesis": signal,
                    "background_dilution_risk": _risk_for_radius(radius, seed_rows),
                }
            )

    comparison_csv = run_dir / "buffer_comparison.csv"
    summary_json = run_dir / "buffer_comparison_summary.json"
    comparison_md = run_dir / "buffer_comparison.md"
    _write_csv(comparison_csv, comparison_rows)
    summary = {
        "schema": "conflict_ntl.buffer_comparison.v1",
        "status": "complete",
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "case_count": len(cases),
        "radii_m": radii,
        "cases": case_summaries,
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# ConflictNTL Buffer Comparison", ""]
    for case in case_summaries:
        lines.extend(
            [
                f"## {case['case_id']}",
                "",
                f"- signal: {case['dominant_signal_hypothesis']}",
                f"- seed events: {case['seed_event_ids']}",
                f"- centroid: {case['centroid_latitude']:.6f}, {case['centroid_longitude']:.6f}",
                "",
                "| radius_m | included_events | density_per_km2 | dilution_risk |",
                "|---:|---:|---:|---|",
            ]
        )
        for row in comparison_rows:
            if row["case_id"] != case["case_id"]:
                continue
            lines.append(
                f"| {row['radius_m']} | {row['included_event_count']} | "
                f"{row['event_density_per_km2']} | {row['background_dilution_risk']} |"
            )
        lines.append("")
    comparison_md.write_text("\n".join(lines), encoding="utf-8")

    return {
        "schema": "conflict_ntl.buffer_comparison.v1",
        "status": "complete",
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary": {
            "case_count": len(cases),
            "radii_m": radii,
        },
        "output_files": {
            "buffer_comparison_csv": str(comparison_csv),
            "buffer_comparison_summary": str(summary_json),
            "buffer_comparison_md": str(comparison_md),
        },
    }


def _child_output_root(output_root: str, parent_label: str) -> str:
    root = str(output_root or "conflict_ntl_runs").replace("\\", "/").strip("/")
    if root.startswith("outputs/"):
        root = root[len("outputs/") :]
    return f"{root}/{parent_label}".strip("/")


def _load_json_file(path_text: str, thread_id: Optional[str] = None) -> dict[str, Any]:
    tid = str(thread_id or current_thread_id.get() or "debug").strip() or "debug"
    path = storage_manager.resolve_workspace_relative_path(
        path_text, thread_id=tid,
        default_root="outputs", create_parent=False, allow_memory=False,
    )
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    return data if isinstance(data, dict) else {}


def _agent_system_runbook(
    *,
    case_name: str,
    summary: dict[str, Any],
    stages: dict[str, Any],
    handoff_contract: dict[str, Any],
) -> str:
    lines = [
        f"# ConflictNTL Agent System Run: {case_name}",
        "",
        "## Agent Roles",
        "",
    ]
    for name, spec in AGENT_ROLES.items():
        lines.extend(
            [
                f"### {name}",
                "",
                str(spec["responsibility"]),
                "",
                f"- inputs: {', '.join(spec['inputs'])}",
                f"- outputs: {', '.join(spec['outputs'])}",
                "",
            ]
        )
    lines.extend(
        [
            "## Run Summary",
            "",
            f"- input records: {summary.get('input_records', 0)}",
            f"- top NTL candidates: {summary.get('top_candidates', 0)}",
            f"- analysis units: {summary.get('analysis_units', 0)}",
            f"- buffer radii: {', '.join(str(v) for v in summary.get('buffer_radii_m', []))}",
            "",
            "## Completed Stages",
            "",
        ]
    )
    for stage_name, stage in stages.items():
        lines.extend(
            [
                f"### {stage_name}",
                "",
                f"- schema: {stage.get('schema', '')}",
                f"- status: {stage.get('status', '')}",
                f"- run_dir: {stage.get('run_dir', '')}",
                "",
            ]
        )
    lines.extend(
        [
            "## VNP46A2 Handoff Contract",
            "",
            f"- product: {handoff_contract['ntl_product']}",
            f"- band: {handoff_contract['band']}",
            f"- time windows: {json.dumps(handoff_contract['default_time_windows'], ensure_ascii=False)}",
            f"- recommended metrics: {', '.join(handoff_contract['recommended_metrics'])}",
            "",
            "## Interpretation Guardrails",
            "",
            INTERPRETATION_GUARDRAILS["principle"],
            "",
            f"Preferred language: {INTERPRETATION_GUARDRAILS['preferred_language']}",
            "",
            "Required caveats:",
        ]
    )
    for caveat in INTERPRETATION_GUARDRAILS["required_caveats"]:
        lines.append(f"- {caveat}")
    lines.extend(
        [
            "",
            "## Recommended Next Steps",
            "",
            "1. Run VNP46A2 multi-window statistics for priority analysis units.",
            "2. Add reference AOIs or control cities before making conflict-related interpretations.",
            "3. Add valid-day quality summaries and lit-pixel-count metrics.",
            "4. Use FIRMS, VIIRS Nightfire, SAR, optical imagery, or source hardening for independent validation.",
            "",
        ]
    )
    return "\n".join(lines)


def run_conflict_ntl_agent_system(
    events_path: str,
    case_name: str = "ConflictNTL agent-system run",
    output_root: str = "conflict_ntl_runs",
    run_label: str = "",
    event_window_start: str = "",
    event_window_end: str = "",
    buffer_radii_m: str = "2000,5000",
    overlap_threshold: float = 0.6,
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    """Run the reusable ConflictNTL agent-system staging chain.

    This tool intentionally stops at auditable tasking and diagnostics. It prepares
    VNP46A2 handoff metadata but does not claim damage, attribution, or ground truth.
    """

    thread_id = _resolve_thread_id(config)
    main_run_dir = _resolve_run_dir(output_root, run_label, thread_id)
    parent_label = main_run_dir.name
    child_root = _child_output_root(output_root, parent_label)
    radii = _parse_radii(buffer_radii_m)

    child_config: dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    configurable = child_config.setdefault("configurable", {})
    if isinstance(configurable, dict):
        configurable["thread_id"] = thread_id

    stages: dict[str, Any] = {}

    screen = run_conflict_ntl_screen_events(
        events_path=events_path,
        output_root=child_root,
        run_label="01_screen_events",
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        config=child_config,
    )
    stages["screen_events"] = screen

    units = run_conflict_ntl_generate_analysis_units(
        screened_events_path=screen["output_files"]["screened_events"],
        output_root=child_root,
        run_label="02_analysis_units",
        buffer_radii_m=buffer_radii_m,
        overlap_threshold=overlap_threshold,
        config=child_config,
    )
    stages["generate_analysis_units"] = units

    report = run_conflict_ntl_build_case_report(
        case_name=case_name,
        screening_summary_path=screen["output_files"]["screening_summary"],
        analysis_units_csv_path=units["output_files"]["analysis_units_csv"],
        top_candidates_csv_path=screen["output_files"]["top_candidates"],
        output_root=child_root,
        run_label="03_case_report",
        config=child_config,
    )
    stages["build_case_report"] = report

    screening_summary = screen.get("summary", {})
    units_summary = units.get("summary", {})
    summary = {
        "input_records": int(screening_summary.get("total_input_records", 0)),
        "screened_records": int(screening_summary.get("total_screened_records", 0)),
        "top_candidates": int(screening_summary.get("top_candidate_count", 0)),
        "analysis_units": int(units_summary.get("analysis_unit_count", 0)),
        "task_count": int(units_summary.get("task_count", 0)),
        "buffer_radii_m": radii,
    }

    manifest = {
        "schema": "conflict_ntl.agent_system_run.v1",
        "status": "complete",
        "case_name": case_name,
        "thread_id": thread_id,
        "run_dir": str(main_run_dir),
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "agent_roles": AGENT_ROLES,
        "summary": summary,
        "stages": stages,
        "handoff_contract": VNP46A2_HANDOFF_CONTRACT,
        "interpretation_guardrails": INTERPRETATION_GUARDRAILS,
        "recommended_next_steps": [
            "Run VNP46A2 multi-window statistics for priority analysis units.",
            "Add reference AOIs or control cities before interpreting conflict-related signals.",
            "Add strict quality summaries, lit-pixel count N, and independent validation checks.",
        ],
    }

    manifest_path = main_run_dir / "agent_system_manifest.json"
    runbook_path = main_run_dir / "agent_system_runbook.md"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    runbook_path.write_text(
        _agent_system_runbook(
            case_name=case_name,
            summary=summary,
            stages=stages,
            handoff_contract=VNP46A2_HANDOFF_CONTRACT,
        ),
        encoding="utf-8",
    )

    return {
        "schema": "conflict_ntl.agent_system_run.v1",
        "status": "complete",
        "thread_id": thread_id,
        "run_dir": str(main_run_dir),
        "agent_roles": AGENT_ROLES,
        "summary": summary,
        "stages": stages,
        "handoff_contract": VNP46A2_HANDOFF_CONTRACT,
        "interpretation_guardrails": INTERPRETATION_GUARDRAILS,
        "output_files": {
            "agent_system_manifest": str(manifest_path),
            "agent_system_runbook": str(runbook_path),
            "case_report_json": report["output_files"]["case_report_json"],
            "case_report_md": report["output_files"]["case_report_md"],
        },
    }


conflict_ntl_screen_events_tool = StructuredTool.from_function(
    func=run_conflict_ntl_screen_events,
    name="conflict_ntl_screen_events_tool",
    description="Screen conflict event records for traceability and NTL verification applicability.",
    args_schema=ConflictNTLScreenEventsInput,
)

conflict_ntl_generate_analysis_units_tool = StructuredTool.from_function(
    func=run_conflict_ntl_generate_analysis_units,
    name="conflict_ntl_generate_analysis_units_tool",
    description="Generate ConflictNTL buffer/admin AOI tasks and same-day analysis units from screened events.",
    args_schema=ConflictNTLGenerateAnalysisUnitsInput,
)

conflict_ntl_fetch_isw_events_tool = StructuredTool.from_function(
    func=run_conflict_ntl_fetch_isw_events,
    name="conflict_ntl_fetch_isw_events_tool",
    description="Fetch ISW/CTP StoryMap ArcGIS FeatureServer event points and export standardized CSV, GeoJSON, and metadata for ConflictNTL.",
    args_schema=ConflictNTLFetchISWEventsInput,
)

conflict_ntl_build_case_report_tool = StructuredTool.from_function(
    func=run_conflict_ntl_build_case_report,
    name="conflict_ntl_build_case_report_tool",
    description="Build a compact JSON and Markdown report from ConflictNTL screening and analysis-unit outputs.",
    args_schema=ConflictNTLBuildCaseReportInput,
)

conflict_ntl_agent_system_tool = StructuredTool.from_function(
    func=run_conflict_ntl_agent_system,
    name="conflict_ntl_agent_system_tool",
    description=(
        "Run the reusable ConflictNTL agent-system staging chain: event tracking/screening, "
        "AOI tasking, report generation, and VNP46A2 handoff contracts."
    ),
    args_schema=ConflictNTLAgentSystemInput,
)


__all__ = [
    "conflict_ntl_agent_system_tool",
    "conflict_ntl_screen_events_tool",
    "conflict_ntl_generate_analysis_units_tool",
    "conflict_ntl_fetch_isw_events_tool",
    "conflict_ntl_build_case_report_tool",
    "run_conflict_ntl_agent_system",
    "run_conflict_ntl_screen_events",
    "run_conflict_ntl_generate_analysis_units",
    "run_conflict_ntl_fetch_isw_events",
    "run_conflict_ntl_build_case_report",
]

