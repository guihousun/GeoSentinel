"""Deterministic conflict-event retrieval and city-level source-record ranking.

This module deliberately separates event discovery from nighttime-light analysis.
It can rank an authorized local snapshot, or (only after an explicit source-terms
acknowledgement) reuse the existing ISW/CTP ArcGIS fetcher.  Counts are counts of
distinct source records, not verified counts of physical attacks.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager
from .conflict_ntl import run_conflict_ntl_fetch_isw_events


DEFAULT_TARGET_COUNTRIES = ("Iran", "Israel")
DEFAULT_ATTACK_EVENT_TYPES = (
    "Confirmed Airstrike",
    "Reported Airstrike",
    "Anti-Tank Fire",
    "Direct Engagement",
    "Drone & Missile Attack",
    "Drone & Rocket Attack",
    "Drone Attack",
    "Missile Attack",
    "Mortar Attack",
    "Rocket Attack",
)

COUNTRY_ALIASES = {
    "iran": "Iran",
    "islamic republic of iran": "Iran",
    "irn": "Iran",
    "israel": "Israel",
    "state of israel": "Israel",
    "isr": "Israel",
}

AUDIT_FIELDS = [
    "record_key",
    "source_event_key",
    "source_event_id",
    "source_identity",
    "country_normalized",
    "city_normalized",
    "city_key",
    "event_date_normalized",
    "event_type_normalized",
    "eligible_for_ranking",
    "exclusion_reason",
]


class ConflictCityEventRankingInput(BaseModel):
    events_path: str = Field(
        default="",
        description=(
            "Optional authorized CSV, JSON, or GeoJSON event snapshot in the current thread workspace. "
            "When omitted, the tool uses the configured live ArcGIS source only after explicit terms acknowledgement."
        ),
    )
    output_root: str = Field(default="conflict_city_ntl_runs", description="Output folder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional deterministic run label.")
    event_window_start: str = Field(default="", description="Optional inclusive event-date start, YYYY-MM-DD.")
    event_window_end: str = Field(default="", description="Inclusive cutoff/test date, YYYY-MM-DD.")
    countries_csv: str = Field(default="Iran,Israel", description="Comma-separated target countries.")
    eligible_event_types_json: str = Field(
        default="",
        description="Optional JSON list overriding the fixed attack-event taxonomy.",
    )
    city_aliases_json: str = Field(
        default="{}",
        description="Optional JSON object mapping source city labels to canonical city labels.",
    )
    layer_urls_json: str = Field(
        default="",
        description="Optional authorized ArcGIS layer definitions passed to the existing conflict-event fetcher.",
    )
    page_size: int = Field(default=1000, description="ArcGIS query page size for live retrieval.")
    include_raw_layers: bool = Field(default=True, description="Preserve raw live layer responses as source snapshots.")
    source_terms_acknowledged: bool = Field(
        default=False,
        description=(
            "Set true only when the user has confirmed authorization to retrieve and use the configured source. "
            "Public query access alone is not authorization."
        ),
    )


def _resolve_thread_id(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = config if isinstance(config, dict) else None
    if runtime_config is None:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if isinstance(runtime_config, dict):
        try:
            thread_id = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if thread_id:
                return thread_id
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _resolve_input_path(path_text: str, thread_id: str) -> Path:
    raw = str(path_text or "").strip()
    if not raw:
        raise ValueError("events_path is required for local-snapshot mode.")
    path = storage_manager.resolve_workspace_relative_path(
        raw,
        thread_id=thread_id,
        default_root="inputs",
        allow_memory=False,
        allowed_roots=("inputs", "outputs"),
    )
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Event snapshot does not exist: {path}")
    return path


def _resolve_output_dir(output_root: str, run_label: str, thread_id: str) -> Path:
    label = str(run_label or "").strip() or f"conflict_city_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    root = storage_manager.resolve_workspace_relative_path(
        output_root,
        thread_id=thread_id,
        default_root="outputs",
        create_parent=True,
        allow_memory=False,
        allowed_roots=("outputs",),
    )
    output_dir = (root / label).resolve()
    workspace_outputs = (storage_manager.get_workspace(thread_id) / "outputs").resolve()
    if output_dir != workspace_outputs and workspace_outputs not in output_dir.parents:
        raise PermissionError("Conflict city outputs must remain under the thread outputs directory.")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _clean_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def _key_text(value: Any) -> str:
    return _clean_text(value).casefold()


def _canonical_identifier(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return text
    if math.isfinite(number) and number.is_integer():
        return str(int(number))
    return text


def _parse_iso_date(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    candidate = text[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return ""


def _parse_json_list(raw: str, default: Iterable[str]) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return list(default)
    payload = json.loads(text)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise ValueError("Expected a JSON list of strings.")
    return [_clean_text(item) for item in payload if _clean_text(item)]


def _parse_aliases(raw: str) -> dict[str, str]:
    payload = json.loads(str(raw or "{}").strip() or "{}")
    if not isinstance(payload, dict):
        raise ValueError("city_aliases_json must be a JSON object.")
    aliases: dict[str, str] = {}
    for source, canonical in payload.items():
        source_key = _key_text(source)
        canonical_text = _clean_text(canonical)
        if source_key and canonical_text:
            aliases[source_key] = canonical_text
    return aliases


def _read_records(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        raise ValueError("Event JSON must be an object or list.")
    if payload.get("type") == "FeatureCollection" and isinstance(payload.get("features"), list):
        records: list[dict[str, Any]] = []
        for feature in payload["features"]:
            if not isinstance(feature, dict):
                continue
            row = dict(feature.get("properties") or {})
            geometry = feature.get("geometry") or {}
            coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
            if isinstance(coordinates, list) and len(coordinates) >= 2:
                row.setdefault("longitude", coordinates[0])
                row.setdefault("latitude", coordinates[1])
            records.append(row)
        return records
    for key in ("records", "events", "data", "results"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
    raise ValueError("Event JSON does not contain a supported record collection.")


def _request_json(url: str, *, timeout_seconds: int = 30) -> dict[str, Any]:
    separator = "&" if "?" in url else "?"
    request = Request(
        f"{url}{separator}{urlencode({'f': 'json'})}",
        headers={"User-Agent": "NTL-GPT conflict-city-ntl-analysis/1.0"},
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"ArcGIS metadata endpoint returned a non-object payload: {url}")
    if payload.get("error"):
        raise RuntimeError(f"ArcGIS metadata endpoint failed for {url}: {payload['error']}")
    return payload


def _epoch_ms_to_iso(value: Any) -> Optional[str]:
    try:
        milliseconds = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(milliseconds):
        return None
    return datetime.fromtimestamp(milliseconds / 1000.0, tz=timezone.utc).isoformat()


def _collect_live_layer_provenance(fetch_result: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    metadata_path = Path(str((fetch_result.get("output_files") or {}).get("metadata_json") or ""))
    if not metadata_path.is_file():
        return [], ["fetch metadata JSON is missing"]
    fetch_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    layers = fetch_metadata.get("layers") if isinstance(fetch_metadata, dict) else None
    if not isinstance(layers, list):
        return [], ["fetch metadata does not contain layer records"]

    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        layer_url = _clean_text(layer.get("url"))
        if not layer_url:
            errors.append("source layer URL is missing")
            continue
        try:
            layer_payload = _request_json(layer_url)
            marker = "/FeatureServer/"
            service_url = layer_url.split(marker, 1)[0] + "/FeatureServer" if marker in layer_url else layer_url
            service_payload = _request_json(service_url)
            service_item_id = _clean_text(
                service_payload.get("serviceItemId") or layer_payload.get("serviceItemId")
            )
            item_url = (
                f"https://www.arcgis.com/sharing/rest/content/items/{service_item_id}"
                if service_item_id
                else ""
            )
            item_payload = _request_json(item_url) if item_url else {}
            license_info = _clean_text(item_payload.get("licenseInfo") or item_payload.get("termsOfUse"))
            editing_info = layer_payload.get("editingInfo") if isinstance(layer_payload.get("editingInfo"), dict) else {}
            time_info = layer_payload.get("timeInfo") if isinstance(layer_payload.get("timeInfo"), dict) else {}
            records.append(
                {
                    "source_layer": layer.get("name"),
                    "source_layer_url": layer_url,
                    "service_item_id": service_item_id or None,
                    "layer_last_edit_utc": _epoch_ms_to_iso(editing_info.get("lastEditDate")),
                    "item_modified_utc": _epoch_ms_to_iso(item_payload.get("modified")),
                    "time_start_field": time_info.get("startTimeField"),
                    "license_metadata_url": f"{item_url}?f=json" if item_url else None,
                    "license_info_present": bool(license_info),
                    "license_info_sha256": (
                        hashlib.sha256(license_info.encode("utf-8")).hexdigest() if license_info else None
                    ),
                    "authorization_acknowledged": True,
                }
            )
        except Exception as exc:
            errors.append(f"{layer_url}: {exc}")
    return records, errors


def _source_identity(row: dict[str, Any]) -> str:
    return _clean_text(
        row.get("source_layer_url")
        or row.get("source_id")
        or row.get("source_layer")
        or row.get("event_family")
    )


def _record_identity(row: dict[str, Any]) -> tuple[str, str, str, str]:
    source_identity = _source_identity(row)
    namespace_seed = source_identity or "unknown_source"
    namespace = hashlib.sha256(namespace_seed.encode("utf-8")).hexdigest()[:16]
    source_event_id = _canonical_identifier(row.get("event_id") or row.get("source_event_id"))
    object_id = _canonical_identifier(
        row.get("objectid") or row.get("OBJECTID") or row.get("globalid") or row.get("GlobalID")
    )
    if object_id:
        record_key = f"{namespace}:record:{object_id}"
    elif source_event_id:
        record_key = f"{namespace}:event:{source_event_id}"
    else:
        fingerprint_fields = [
            row.get("event_date_utc") or row.get("event_date"),
            row.get("country"),
            row.get("city"),
            row.get("event_type"),
            row.get("latitude"),
            row.get("longitude"),
            row.get("source_1") or row.get("sources"),
        ]
        fingerprint = "|".join(_clean_text(value) for value in fingerprint_fields)
        record_key = f"{namespace}:fingerprint:{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()[:24]}"
    source_event_key = f"{namespace}:event:{source_event_id}" if source_event_id else record_key
    return record_key, source_event_key, source_event_id, source_identity


def _float_or_none(value: Any) -> Optional[float]:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def rank_conflict_city_records(
    records: Iterable[dict[str, Any]],
    *,
    event_window_start: str = "",
    event_window_end: str = "",
    target_countries: Iterable[str] = DEFAULT_TARGET_COUNTRIES,
    eligible_event_types: Iterable[str] = DEFAULT_ATTACK_EVENT_TYPES,
    city_aliases: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Normalize records and rank target-country cities by distinct source records."""

    start_date = _parse_iso_date(event_window_start) if event_window_start else ""
    end_date = _parse_iso_date(event_window_end) if event_window_end else ""
    if event_window_start and not start_date:
        raise ValueError("event_window_start must be YYYY-MM-DD.")
    if event_window_end and not end_date:
        raise ValueError("event_window_end must be YYYY-MM-DD.")
    if start_date and end_date and start_date > end_date:
        raise ValueError("event_window_start must not be after event_window_end.")

    aliases = {_key_text(key): _clean_text(value) for key, value in (city_aliases or {}).items()}
    target_names = [_clean_text(country) for country in target_countries if _clean_text(country)]
    target_by_key = {_key_text(country): country for country in target_names}
    event_type_keys = {_key_text(value) for value in eligible_event_types if _clean_text(value)}

    audited: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    record_keys_seen: set[str] = set()
    eligible_by_group: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    source_event_records: dict[str, set[str]] = defaultdict(set)

    for raw_row in records:
        row = dict(raw_row)
        record_key, source_event_key, source_event_id, source_identity = _record_identity(row)
        source_event_records[source_event_key].add(record_key)
        raw_country = _clean_text(row.get("country_normalized") or row.get("country"))
        country = COUNTRY_ALIASES.get(_key_text(raw_country), raw_country)
        if _key_text(country) in target_by_key:
            country = target_by_key[_key_text(country)]
        raw_city = _clean_text(row.get("city_normalized") or row.get("city"))
        city = aliases.get(_key_text(raw_city), raw_city)
        city_key = _key_text(city)
        event_date = _parse_iso_date(row.get("event_date_utc") or row.get("event_date"))
        event_type = _clean_text(row.get("event_type"))

        reason = ""
        if not source_identity:
            reason = "missing_source_identity"
        elif record_key in record_keys_seen:
            reason = "duplicate_record_key"
        elif _key_text(country) not in target_by_key:
            reason = "non_target_or_missing_country"
        elif not city_key:
            reason = "missing_city"
        elif not event_date:
            reason = "missing_or_invalid_event_date"
        elif start_date and event_date < start_date:
            reason = "before_event_window"
        elif end_date and event_date > end_date:
            reason = "after_cutoff_date"
        elif event_type_keys and _key_text(event_type) not in event_type_keys:
            reason = "excluded_event_type"

        eligible = not reason
        if reason:
            exclusions[reason] += 1
        else:
            group = (country, city_key)
            eligible_by_group[group][record_key] = {
                "record_key": record_key,
                "latitude": _float_or_none(row.get("latitude")),
                "longitude": _float_or_none(row.get("longitude")),
                "city": city,
            }
        record_keys_seen.add(record_key)

        row.update(
            {
                "record_key": record_key,
                "source_event_key": source_event_key,
                "source_event_id": source_event_id,
                "source_identity": source_identity,
                "country_normalized": country,
                "city_normalized": city,
                "city_key": city_key,
                "event_date_normalized": event_date,
                "event_type_normalized": event_type,
                "eligible_for_ranking": str(eligible).lower(),
                "exclusion_reason": reason,
            }
        )
        audited.append(row)

    counts: list[dict[str, Any]] = []
    for (country, city_key), unique_records in eligible_by_group.items():
        values = list(unique_records.values())
        lats = [item["latitude"] for item in values if item["latitude"] is not None]
        lons = [item["longitude"] for item in values if item["longitude"] is not None]
        counts.append(
            {
                "country": country,
                "city": values[0]["city"],
                "city_key": city_key,
                "attack_record_count": len(unique_records),
                "representative_latitude": round(sum(lats) / len(lats), 7) if lats else None,
                "representative_longitude": round(sum(lons) / len(lons), 7) if lons else None,
            }
        )
    counts.sort(key=lambda row: (target_names.index(row["country"]), -row["attack_record_count"], row["city_key"]))

    maxima: dict[str, int] = {}
    selected: list[dict[str, Any]] = []
    for country in target_names:
        country_counts = [row for row in counts if row["country"] == country]
        maximum = max((int(row["attack_record_count"]) for row in country_counts), default=0)
        maxima[country] = maximum
        tied = [row for row in country_counts if int(row["attack_record_count"]) == maximum and maximum > 0]
        for row in tied:
            selected.append({**row, "is_tied_maximum": len(tied) > 1})

    collision_groups = {
        key: sorted(values)
        for key, values in source_event_records.items()
        if len(values) > 1 and ":event:" in key
    }
    return {
        "schema": "ntl_gpt.conflict_city_event_ranking.v1",
        "count_semantics": "distinct source records after fixed filters; not independently verified physical attacks",
        "event_window": {"start": start_date or None, "end_inclusive": end_date or None},
        "target_countries": target_names,
        "eligible_event_types": sorted(_clean_text(value) for value in eligible_event_types if _clean_text(value)),
        "audited_records": audited,
        "city_counts": counts,
        "selected_cities": selected,
        "audit": {
            "raw_row_count": len(audited),
            "distinct_record_key_count": len(record_keys_seen),
            "eligible_record_count": sum(int(row["attack_record_count"]) for row in counts),
            "excluded_counts": dict(sorted(exclusions.items())),
            "missing_source_identity_record_count": int(exclusions.get("missing_source_identity", 0)),
            "source_event_id_collision_group_count": len(collision_groups),
            "source_event_id_collision_groups": collision_groups,
            "country_maxima": maxima,
            "countries_without_rankable_records": [country for country, maximum in maxima.items() if maximum == 0],
        },
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], preferred_fields: Optional[list[str]] = None) -> None:
    fields = list(preferred_fields or [])
    seen = set(fields)
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["status"], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_conflict_city_ranking(result: dict[str, Any], output_dir: Path, *, source: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    events_path = output_dir / "event_records_snapshot.csv"
    counts_path = output_dir / "city_attack_record_counts.csv"
    selected_path = output_dir / "selected_cities.csv"
    selected_geojson_path = output_dir / "selected_city_event_centroids.geojson"
    metadata_path = output_dir / "ranking_metadata.json"

    _write_csv(events_path, result["audited_records"], AUDIT_FIELDS)
    _write_csv(counts_path, result["city_counts"])
    _write_csv(selected_path, result["selected_cities"])
    features = []
    for row in result["selected_cities"]:
        lon = row.get("representative_longitude")
        lat = row.get("representative_latitude")
        geometry = None if lon is None or lat is None else {"type": "Point", "coordinates": [lon, lat]}
        features.append({"type": "Feature", "properties": row, "geometry": geometry})
    selected_geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "name": "selected_city_event_centroids",
                "geometry_note": "Event-coordinate centroid for discovery only; not a city boundary.",
                "features": features,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata = {
        "schema": "ntl_gpt.conflict_city_event_ranking.metadata.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "count_semantics": result["count_semantics"],
        "event_window": result["event_window"],
        "target_countries": result["target_countries"],
        "eligible_event_types": result["eligible_event_types"],
        "selected_cities": result["selected_cities"],
        "audit": result["audit"],
        "limitations": [
            "City labels come from the authorized source snapshot unless an explicit alias map is supplied.",
            "Counts describe distinct source records and are not independently verified counts of physical attacks.",
            "The point GeoJSON contains event-coordinate centroids, not administrative city boundaries.",
            "Nighttime-light changes must not be interpreted as proof of damage or causality.",
        ],
    }
    files = {
        "event_records_snapshot": str(events_path),
        "city_attack_record_counts": str(counts_path),
        "selected_cities": str(selected_path),
        "selected_city_event_centroids": str(selected_geojson_path),
        "ranking_metadata": str(metadata_path),
    }
    hashes = {
        name: _sha256(Path(path))
        for name, path in files.items()
        if name != "ranking_metadata"
    }
    metadata["output_sha256"] = hashes
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return files


def run_conflict_city_event_retrieval(
    events_path: str = "",
    output_root: str = "conflict_city_ntl_runs",
    run_label: str = "",
    event_window_start: str = "",
    event_window_end: str = "",
    countries_csv: str = "Iran,Israel",
    eligible_event_types_json: str = "",
    city_aliases_json: str = "{}",
    layer_urls_json: str = "",
    page_size: int = 1000,
    include_raw_layers: bool = True,
    source_terms_acknowledged: bool = False,
    config: Optional[RunnableConfig] = None,
    **_: Any,
) -> dict[str, Any]:
    """Retrieve or load a conflict-event snapshot and rank target-country cities."""

    thread_id = _resolve_thread_id(config)
    target_countries = [_clean_text(item) for item in countries_csv.split(",") if _clean_text(item)]
    if not target_countries:
        raise ValueError("countries_csv must contain at least one country.")
    event_types = _parse_json_list(eligible_event_types_json, DEFAULT_ATTACK_EVENT_TYPES)
    aliases = _parse_aliases(city_aliases_json)
    label = str(run_label or "").strip() or f"conflict_city_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    fetch_result: Optional[dict[str, Any]] = None
    if events_path:
        resolved_events_path = _resolve_input_path(events_path, thread_id)
        source = {
            "mode": "authorized_local_snapshot",
            "snapshot_path": str(resolved_events_path),
            "authorization_note": "Caller supplied the snapshot; source terms remain the caller's responsibility.",
        }
    else:
        if not source_terms_acknowledged:
            return {
                "schema": "ntl_gpt.conflict_city_event_ranking.v1",
                "status": "needs_source_authorization",
                "thread_id": thread_id,
                "message": (
                    "Live event retrieval was not attempted. Confirm the configured source terms and set "
                    "source_terms_acknowledged=true only when authorized."
                ),
                "count_semantics": "No ranking generated.",
                "output_files": {},
            }
        fetch_result = run_conflict_ntl_fetch_isw_events(
            layer_urls_json=layer_urls_json,
            output_root=output_root,
            run_label=f"{label}_source",
            event_window_start="",
            event_window_end="",
            page_size=page_size,
            include_raw_layers=include_raw_layers,
            config=config,
        )
        resolved_events_path = Path(fetch_result["output_files"]["events_csv"])
        layer_freshness_records, source_metadata_errors = _collect_live_layer_provenance(fetch_result)
        source = {
            "mode": "authorized_live_arcgis_retrieval",
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "fetch_status": fetch_result.get("status"),
            "fetch_outputs": fetch_result.get("output_files", {}),
            "layer_freshness_records": layer_freshness_records,
            "source_metadata_errors": source_metadata_errors,
            "authorization_note": "Live retrieval was enabled by an explicit caller acknowledgement.",
            "completeness_note": "Snapshot time is recorded; same-day source completeness is not independently guaranteed.",
        }

    records = _read_records(resolved_events_path)
    if source.get("mode") == "authorized_live_arcgis_retrieval":
        for layer_record in source.get("layer_freshness_records", []):
            layer_url = _clean_text(layer_record.get("source_layer_url"))
            event_dates = [
                _parse_iso_date(row.get("event_date_utc") or row.get("event_date"))
                for row in records
                if _clean_text(row.get("source_layer_url")) == layer_url
            ]
            layer_record["maximum_event_date_utc"] = max((value for value in event_dates if value), default=None)
    result = rank_conflict_city_records(
        records,
        event_window_start=event_window_start,
        event_window_end=event_window_end,
        target_countries=target_countries,
        eligible_event_types=event_types,
        city_aliases=aliases,
    )
    output_dir = _resolve_output_dir(output_root, label, thread_id)
    output_files = write_conflict_city_ranking(result, output_dir, source=source)
    missing = result["audit"]["countries_without_rankable_records"]
    fetch_partial = bool(
        fetch_result
        and (
            fetch_result.get("status") != "complete"
            or source.get("source_metadata_errors")
            or not source.get("layer_freshness_records")
        )
    )
    source_identity_partial = bool(result["audit"]["excluded_counts"].get("missing_source_identity"))
    status = "complete" if not missing and not fetch_partial and not source_identity_partial else "partial"
    return {
        "schema": result["schema"],
        "status": status,
        "thread_id": thread_id,
        "run_dir": str(output_dir),
        "count_semantics": result["count_semantics"],
        "summary": {
            "raw_row_count": result["audit"]["raw_row_count"],
            "eligible_record_count": result["audit"]["eligible_record_count"],
            "selected_city_count": len(result["selected_cities"]),
            "countries_without_rankable_records": missing,
            "country_maxima": result["audit"]["country_maxima"],
        },
        "selected_cities": result["selected_cities"],
        "audit": result["audit"],
        "source_fetch": fetch_result,
        "output_files": output_files,
    }


conflict_city_event_ranking_tool = StructuredTool.from_function(
    func=run_conflict_city_event_retrieval,
    name="conflict_city_event_ranking_tool",
    description=(
        "Load an authorized conflict-event snapshot or retrieve an authorized live ArcGIS snapshot, apply a fixed "
        "attack-record taxonomy and cutoff date, count distinct source records by country and city, preserve all "
        "country-level ties, and write auditable CSV/GeoJSON/metadata outputs. Counts are source-record counts, not "
        "verified physical-attack counts."
    ),
    args_schema=ConflictCityEventRankingInput,
)


__all__ = [
    "ConflictCityEventRankingInput",
    "DEFAULT_ATTACK_EVENT_TYPES",
    "conflict_city_event_ranking_tool",
    "rank_conflict_city_records",
    "run_conflict_city_event_retrieval",
    "write_conflict_city_ranking",
]

