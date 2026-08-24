from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from experiments.official_daily_ntl_fastpath.cmr_client import (  # noqa: E402
    latest_granule_day,
    search_granules,
)
from experiments.official_daily_ntl_fastpath.gee_baseline import (  # noqa: E402
    DEFAULT_GEE_PROJECT,
    get_gee_monitor_products,
    query_gee_products_latest,
)
from experiments.official_daily_ntl_fastpath.source_registry import (  # noqa: E402
    get_nrt_priority_sources,
    get_source_spec,
    parse_sources_arg,
)

CMR_COLLECTIONS_API = "https://cmr.earthdata.nasa.gov/search/collections.json"


def _latest_day_for_source(source: str, granules: list[Any]) -> str | None:
    spec = get_source_spec(source)
    return latest_granule_day(granules, night_only=spec.night_only)


def _run_curl_json(url: str, timeout: int = 120) -> dict[str, Any]:
    cmd = ["curl.exe", "--silent", "--show-error", "--location", url]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"curl failed ({proc.returncode}): {proc.stderr.strip()}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON payload: {exc}") from exc


def _query_collection_time_range(short_name: str) -> tuple[str | None, str | None]:
    url = f"{CMR_COLLECTIONS_API}?{urlencode([('short_name', short_name), ('page_size', '1')])}"
    payload = _run_curl_json(url)
    entries = payload.get("feed", {}).get("entry", [])
    if not entries:
        return None, None
    entry = entries[0] if isinstance(entries[0], dict) else {}
    start = str(entry.get("time_start") or "")[:10] or None
    end = str(entry.get("time_end") or "")[:10] or None
    return start, end


def _query_gee_time_ranges(project_id: str) -> dict[str, dict[str, str | None]]:
    rows = get_gee_monitor_products()
    out: dict[str, dict[str, str | None]] = {}
    try:
        import ee
    except Exception as exc:  # noqa: BLE001
        err = f"ee import failed: {exc}"
        for item in rows:
            out[item["dataset_id"]] = {"range_start": None, "range_end": None, "range_error": err}
        return out

    try:
        ee.Initialize(project=project_id)
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
        for item in rows:
            out[item["dataset_id"]] = {"range_start": None, "range_end": None, "range_error": err}
        return out

    for item in rows:
        dataset_id = item["dataset_id"]
        try:
            col = ee.ImageCollection(dataset_id)
            min_ms = col.aggregate_min("system:time_start").getInfo()
            max_ms = col.aggregate_max("system:time_start").getInfo()
            if min_ms is None or max_ms is None:
                out[dataset_id] = {"range_start": None, "range_end": None, "range_error": "no_data"}
                continue
            start = datetime.fromtimestamp(float(min_ms) / 1000, tz=UTC).strftime("%Y-%m-%d")
            end = datetime.fromtimestamp(float(max_ms) / 1000, tz=UTC).strftime("%Y-%m-%d")
            out[dataset_id] = {"range_start": start, "range_end": end, "range_error": None}
        except Exception as exc:  # noqa: BLE001
            out[dataset_id] = {"range_start": None, "range_end": None, "range_error": str(exc)}
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan NTL availability for official sources and project-used GEE products.")
    parser.add_argument(
        "--sources",
        default="nrt_priority",
        help=(
            "comma list (e.g., VNP46A1,VNP46A2,VNP46A3,VNP46A4) "
            "or nrt_priority"
        ),
    )
    parser.add_argument(
        "--granule-start-date",
        default="2012-01-01",
        help="start date for latest granule scan (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--granule-end-date",
        default=datetime.now(UTC).date().strftime("%Y-%m-%d"),
        help="end date for latest granule scan (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output-dir",
        default="experiments/official_daily_ntl_fastpath/workspace_monitor/outputs",
        help="output directory for json/csv",
    )
    parser.add_argument(
        "--include-gee",
        action="store_true",
        help="also scan project-used GEE NTL products",
    )
    parser.add_argument(
        "--gee-project",
        default=DEFAULT_GEE_PROJECT,
        help=f"GEE project id (default: {DEFAULT_GEE_PROJECT})",
    )
    args = parser.parse_args()

    sources = parse_sources_arg(args.sources)
    if not sources:
        sources = get_nrt_priority_sources()

    rows: list[dict[str, Any]] = []
    now_day = datetime.now(UTC).date()
    for source in sources:
        row: dict[str, Any] = {"source_type": "official", "source": source, "dataset_id": None, "temporal_resolution": "daily"}
        try:
            start, end = _query_collection_time_range(source)
            row["collection_time_start"] = start
            row["collection_time_end"] = end
            row["range_error"] = None
        except Exception as exc:  # noqa: BLE001
            row["collection_time_start"] = None
            row["collection_time_end"] = None
            row["collection_error"] = str(exc)
            row["range_error"] = str(exc)

        try:
            granules = search_granules(
                short_name=source,
                start_date=args.granule_start_date,
                end_date=args.granule_end_date,
                bbox=None,
                page_size=1,
            )
            latest = _latest_day_for_source(source, granules)
            row["latest_global_granule_date"] = latest
            row["latest_global_date"] = latest
            if latest:
                lag = (now_day - datetime.strptime(latest, "%Y-%m-%d").date()).days
            else:
                lag = None
            row["latest_global_lag_days"] = lag
            row["latest_error"] = None
        except Exception as exc:  # noqa: BLE001
            row["latest_global_granule_date"] = None
            row["latest_global_date"] = None
            row["latest_global_lag_days"] = None
            row["granule_error"] = str(exc)
            row["latest_error"] = str(exc)

        rows.append(row)

    gee_rows_count = 0
    if args.include_gee:
        gee_latest_rows, gee_query_error = query_gee_products_latest(bbox=None, project_id=args.gee_project)
        gee_range_map = _query_gee_time_ranges(project_id=args.gee_project)
        for item in gee_latest_rows:
            dataset_id = str(item.get("dataset_id") or "")
            r = gee_range_map.get(dataset_id, {})
            latest = item.get("latest_global_date")
            lag = None
            if latest:
                try:
                    lag = (now_day - datetime.strptime(str(latest), "%Y-%m-%d").date()).days
                except ValueError:
                    lag = None
            item_err = item.get("error")
            latest_error = None if item_err in (None, "bbox_missing") else item_err
            if gee_query_error and not latest_error:
                latest_error = gee_query_error
            row = {
                "source_type": "gee",
                "source": item.get("source"),
                "dataset_id": dataset_id,
                "temporal_resolution": item.get("temporal_resolution"),
                "collection_time_start": r.get("range_start"),
                "collection_time_end": r.get("range_end"),
                "latest_global_granule_date": latest,
                "latest_global_date": latest,
                "latest_global_lag_days": lag,
                "collection_error": r.get("range_error"),
                "granule_error": latest_error,
                "range_error": r.get("range_error"),
                "latest_error": latest_error,
            }
            rows.append(row)
            gee_rows_count += 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"official_ntl_availability_{ts}.json"
    csv_path = out_dir / f"official_ntl_availability_{ts}.csv"

    payload = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": sources,
        "granule_start_date": args.granule_start_date,
        "granule_end_date": args.granule_end_date,
        "include_gee": bool(args.include_gee),
        "gee_project": args.gee_project if args.include_gee else None,
        "official_rows": len([x for x in rows if x.get("source_type") == "official"]),
        "gee_rows": gee_rows_count,
        "rows": rows,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "source_type",
        "source",
        "dataset_id",
        "temporal_resolution",
        "collection_time_start",
        "collection_time_end",
        "latest_global_granule_date",
        "latest_global_date",
        "latest_global_lag_days",
        "collection_error",
        "granule_error",
        "range_error",
        "latest_error",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})

    print(f"json={json_path}")
    print(f"csv={csv_path}")
    print(f"sources={','.join(sources)}")
    print(f"official_rows={len([x for x in rows if x.get('source_type') == 'official'])}")
    print(f"gee_rows={gee_rows_count}")
    print(f"rows={len(rows)}")


if __name__ == "__main__":
    main()
