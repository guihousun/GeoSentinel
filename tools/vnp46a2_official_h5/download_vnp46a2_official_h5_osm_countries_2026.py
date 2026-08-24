from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path

import geopandas as gpd
import h5py
from shapely.geometry import box

from cmr_client import (  # noqa: E402
    download_file_with_curl,
    extract_download_link,
    group_granules_by_day,
    resolve_token,
    search_granules,
)
from env_utils import load_dotenv_file
from vnp46a2_country_common import (  # noqa: E402
    DATASET_ID,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_START,
    SIMPLIFY_TOLERANCE_DEG,
    clamp_end_day,
    gee_latest_day,
    init_ee,
    iter_dates,
    run_dir,
    selected_countries,
)

SHORT_NAME = "VNP46A2"
BAND = "DNB_BRDF_Corrected_NTL"


def redact_secrets(text: object) -> str:
    value = str(text or "")
    value = re.sub(r"(Authorization:\s*Bearer\s+)[A-Za-z0-9._-]+", r"\1<REDACTED>", value)
    value = re.sub(r"Bearer\s+[A-Za-z0-9._-]+", "Bearer <REDACTED>", value)
    value = re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "<REDACTED_JWT>", value)
    return value


def validate_h5_band(path: Path) -> tuple[bool, str]:
    try:
        with h5py.File(path, "r") as h5:
            hits: list[str] = []

            def walk(name, obj):
                normalized = name.replace("-", "_")
                if isinstance(obj, h5py.Dataset) and normalized.endswith(BAND) and "Gap_Filled" not in normalized:
                    hits.append(name)

            h5.visititems(walk)
            if not hits:
                return False, f"{BAND} dataset missing"
            ds = h5[hits[0]]
            if len(ds.shape) != 2 or min(ds.shape) <= 0:
                return False, f"invalid dataset shape: {ds.shape}"
            # Force a tiny read so truncated files fail before they are accepted.
            _ = ds[0, 0]
    except Exception as exc:  # noqa: BLE001
        return False, repr(exc)
    return True, ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official NASA Earthdata VNP46A2 H5 granules for OSM 0.001 country boundaries."
    )
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", required=True, help="Inclusive end date. Clamped to latest GEE product date.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--countries", nargs="*", default=None)
    parser.add_argument("--targets", nargs="+", default=None, help="Optional ISO3:YYYY-MM-DD values to retry specific country-days.")
    parser.add_argument("--targets-file", default="", help="Optional UTF-8 text file with one ISO3:YYYY-MM-DD target per line.")
    parser.add_argument("--limit-days", type=int, default=0)
    parser.add_argument("--workers", type=int, default=8, help="Parallel curl workers per country-day.")
    parser.add_argument("--download-timeout", type=int, default=240, help="Per-HDF curl timeout in seconds.")
    parser.add_argument("--run-label", default="", help="Optional suffix for manifest/summary files.")
    parser.add_argument("--token-env", default="EARTHDATA_TOKEN")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-gee-latest", action="store_true", help="Do not initialize GEE or clamp --end to the latest GEE VNP46A2 day.")
    return parser.parse_args()


def parse_targets(values: list[str]) -> dict[str, list[str]]:
    targets: dict[str, list[str]] = {}
    for value in values:
        iso3, day = value.split(":", 1)
        targets.setdefault(iso3.strip().upper(), []).append(day.strip())
    return {iso3: sorted(set(days)) for iso3, days in targets.items()}


def read_targets_file(path: str | Path | None) -> list[str]:
    if not path:
        return []
    target_path = Path(path)
    if not target_path.exists():
        raise FileNotFoundError(f"targets file not found: {target_path}")
    return [
        line.strip()
        for line in target_path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def acquisition_token(day: str) -> str:
    parsed = datetime.strptime(day, "%Y-%m-%d")
    return f"A{parsed.year}{parsed.timetuple().tm_yday:03d}"


def cmr_query_end(day: str) -> str:
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def select_acquisition_day(granules, day: str) -> list:
    token = acquisition_token(day)
    return [entry for entry in granules if f".{token}." in entry.producer_granule_id]


def resolve_token_for_run(env_name: str, search_dirs: list[Path]) -> str | None:
    token = resolve_token(env_name)
    if token:
        return token
    keys = [env_name]
    if env_name == "EARTHDATA_TOKEN":
        keys.extend(["EARTHDATA_BEARER_TOKEN", "EDL_TOKEN"])
    seen: set[Path] = set()
    for directory in search_dirs:
        path = (directory / ".env").resolve()
        if path in seen:
            continue
        seen.add(path)
        pairs = load_dotenv_file(path)
        for key in keys:
            if pairs.get(key):
                return pairs[key]
    return None


def safe_filename_from_url(url: str, fallback: str) -> str:
    name = Path(url.split("?", 1)[0]).name
    return name if name else fallback


def boundary_query_bboxes(path: Path) -> list[tuple[float, float, float, float]]:
    gdf = gpd.read_file(path).to_crs("EPSG:4326")
    total = tuple(round(float(x), 6) for x in gdf.total_bounds)
    if total[2] - total[0] <= 180:
        return [total]

    geometry = gdf.geometry.union_all()
    parts = list(geometry.geoms) if geometry.geom_type == "MultiPolygon" else [geometry]
    groups = {
        "west": [part.bounds for part in parts if part.centroid.x < 0],
        "east": [part.bounds for part in parts if part.centroid.x >= 0],
    }
    bboxes: list[tuple[float, float, float, float]] = []
    for bounds in groups.values():
        if not bounds:
            continue
        bboxes.append(
            (
                round(min(item[0] for item in bounds), 6),
                round(min(item[1] for item in bounds), 6),
                round(max(item[2] for item in bounds), 6),
                round(max(item[3] for item in bounds), 6),
            )
        )
    return bboxes


def tile_box_from_granule_id(granule_id: str):
    import re

    match = re.search(r"\.h(\d{2})v(\d{2})\.", str(granule_id or ""))
    if not match:
        return None
    h = int(match.group(1))
    v = int(match.group(2))
    xmin = -180.0 + h * 10.0
    xmax = xmin + 10.0
    ymax = 90.0 - v * 10.0
    ymin = ymax - 10.0
    return box(xmin, ymin, xmax, ymax)


def country_geometries(path: Path) -> list:
    gdf = gpd.read_file(path).to_crs("EPSG:4326")
    return [geom for geom in gdf.geometry if geom is not None and not geom.is_empty]


def filter_entries_to_country_tiles(entries, geometries: list) -> list:
    filtered = []
    for entry in entries:
        tile_geom = tile_box_from_granule_id(entry.producer_granule_id)
        if tile_geom is None:
            filtered.append(entry)
            continue
        if any(geom.intersects(tile_geom) for geom in geometries):
            filtered.append(entry)
    return filtered


def valid_h5_files(day_dir: Path, geometries: list | None = None) -> list[Path]:
    files = sorted(path for path in day_dir.glob("*.h5") if path.exists() and path.stat().st_size > 512)
    if geometries is None:
        return files
    out: list[Path] = []
    for path in files:
        tile_geom = tile_box_from_granule_id(path.name)
        if tile_geom is None or any(geom.intersects(tile_geom) for geom in geometries):
            out.append(path)
    return out


def download_one(
    entry,
    idx: int,
    day_dir: Path,
    iso3: str,
    day: str,
    token: str,
    timeout: int,
) -> tuple[bool, Path | None, str]:
    link = extract_download_link(entry.links)
    if not link:
        return False, None, f"{idx}:missing_link:{entry.producer_granule_id}"
    filename = safe_filename_from_url(link, f"VNP46A2_{iso3}_{day}_{idx}.h5")
    dst = day_dir / filename
    if dst.exists() and dst.stat().st_size > 512:
        valid, reason = validate_h5_band(dst)
        if valid:
            return True, dst, "existing"
        dst.unlink(missing_ok=True)
        # Continue into a fresh download below; the failed validation will be
        # reported only if the replacement download also fails.
    try:
        ok, err = download_file_with_curl(link, dst, earthdata_token=token, timeout=max(60, int(timeout)))
    except Exception as exc:  # noqa: BLE001
        dst.unlink(missing_ok=True)
        return False, None, redact_secrets(f"{idx}:{entry.producer_granule_id}:exception:{exc!r}")
    if ok:
        valid, reason = validate_h5_band(dst)
        if valid:
            return True, dst, ""
        dst.unlink(missing_ok=True)
        return False, None, redact_secrets(f"{idx}:{entry.producer_granule_id}:invalid_download:{reason}")
    dst.unlink(missing_ok=True)
    return False, None, redact_secrets(f"{idx}:{entry.producer_granule_id}:{err}")


def write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "country",
        "iso3",
        "date",
        "source",
        "gee_dataset",
        "band",
        "boundary_source",
        "simplify_tolerance_deg",
        "bbox",
        "status",
        "granules_found",
        "downloaded_count",
        "failed_count",
        "output_dir",
        "file_size_mb",
        "files",
        "note",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    if args.no_gee_latest:
        effective_end = args.end
    else:
        init_ee()
        effective_end = clamp_end_day(args.end, gee_latest_day())
    dates = iter_dates(args.start, effective_end)
    if args.limit_days > 0:
        dates = dates[: args.limit_days]
    target_values = list(args.targets or []) + read_targets_file(args.targets_file)
    target_dates_by_iso = parse_targets(target_values) if target_values else None

    base_dir = run_dir(Path(args.output_root), args.start, effective_end)
    token = resolve_token_for_run(
        args.token_env,
        [
            Path.cwd(),
            Path(args.output_root),
            base_dir,
            Path(__file__).resolve().parent,
            Path(__file__).resolve().parents[1],
        ],
    )
    simplified_dir = base_dir / "osm_boundaries_simplified_0p001"
    out_root = base_dir / "official_raw_h5"
    label = f"_{args.run_label.strip()}" if str(args.run_label or "").strip() else ""
    manifest = base_dir / f"vnp46a2_official_h5_osm_0p001{label}_manifest.csv"
    rows: list[dict[str, object]] = []

    country_tokens = sorted(target_dates_by_iso) if target_dates_by_iso else args.countries
    for country in selected_countries(country_tokens):
        matches = sorted(simplified_dir.glob(f"osm_admin0_{country.iso3}_*_simplified_0p001.geojson"))
        if not matches:
            rows.append(
                {
                    "country": country.name,
                    "iso3": country.iso3,
                    "date": "",
                    "source": "NASA CMR/Earthdata official H5",
                    "gee_dataset": DATASET_ID,
                    "band": BAND,
                    "boundary_source": str(simplified_dir),
                    "simplify_tolerance_deg": SIMPLIFY_TOLERANCE_DEG,
                    "bbox": "",
                    "status": "missing_simplified_boundary",
                    "granules_found": 0,
                    "downloaded_count": 0,
                    "failed_count": 0,
                    "output_dir": "",
                    "file_size_mb": "",
                    "files": "",
                    "note": "Run the country script dry-run first to build simplified OSM boundaries.",
                }
            )
            write_manifest(manifest, rows)
            continue
        boundary_path = matches[0]
        query_bboxes = boundary_query_bboxes(boundary_path)
        geometries = country_geometries(boundary_path)
        bbox_text = ";".join(",".join(str(x) for x in bbox) for bbox in query_bboxes)
        country_dates = target_dates_by_iso.get(country.iso3, []) if target_dates_by_iso else dates
        for day in country_dates:
            day_dir = out_root / country.iso3 / day
            record: dict[str, object] = {
                "country": country.name,
                "iso3": country.iso3,
                "date": day,
                "source": "NASA CMR/Earthdata official H5",
                "gee_dataset": DATASET_ID,
                "band": BAND,
                "boundary_source": str(boundary_path),
                "simplify_tolerance_deg": SIMPLIFY_TOLERANCE_DEG,
                "bbox": bbox_text,
                "status": "started",
                "granules_found": 0,
                "downloaded_count": 0,
                "failed_count": 0,
                "output_dir": str(day_dir),
                "file_size_mb": "",
                "files": "",
                "note": "",
            }
            try:
                granules_by_id = {}
                for query_bbox in query_bboxes:
                    for granule in search_granules(
                        SHORT_NAME,
                        day,
                        cmr_query_end(day),
                        bbox=query_bbox,
                        page_size=200,
                    ):
                        granules_by_id[granule.producer_granule_id] = granule
                granules = list(granules_by_id.values())
                # CMR temporal windows overlap adjacent VNP46A2 acquisition
                # dates. The AYYYYDDD token in the granule ID is authoritative.
                raw_entries = select_acquisition_day(granules, day)
                # CMR already applies the country bbox. Do not re-filter by
                # h/v tile IDs here: VNP46A2 uses MODLAND-style tiles, and a
                # naive lon/lat 10-degree h/v conversion can discard valid
                # country granules.
                entries = raw_entries
                record["granules_found"] = len(entries)
                if not entries:
                    record["status"] = "no_granules"
                    record["note"] = f"CMR returned {len(raw_entries)} bbox granules."
                    rows.append(record)
                    write_manifest(manifest, rows)
                    print(f"[{country.iso3} {day}] no_granules")
                    continue
                if not token:
                    record["status"] = "missing_earthdata_token"
                    record["failed_count"] = len(entries)
                    record["note"] = f"{args.token_env} missing; CMR granules found but H5 download requires Earthdata bearer token."
                    rows.append(record)
                    write_manifest(manifest, rows)
                    print(f"[{country.iso3} {day}] missing_earthdata_token granules={len(entries)}")
                    continue

                downloaded: list[Path] = []
                failures: list[str] = []
                with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
                    futures = [
                        executor.submit(download_one, entry, idx, day_dir, country.iso3, day, token, args.download_timeout)
                        for idx, entry in enumerate(entries, start=1)
                    ]
                    for future in as_completed(futures):
                        ok, path, note = future.result()
                        if ok and path is not None:
                            downloaded.append(path)
                        elif note:
                            failures.append(note)
                record["downloaded_count"] = len(downloaded)
                record["failed_count"] = len(failures)
                record["file_size_mb"] = round(sum(p.stat().st_size for p in downloaded) / 1024 / 1024, 3)
                record["files"] = ";".join(str(p) for p in sorted(downloaded))
                record["status"] = (
                    "official_h5_downloaded"
                    if downloaded and not failures
                    else "official_h5_partial"
                    if downloaded
                    else "official_h5_failed"
                )
                record["note"] = redact_secrets(" | ".join(failures[:5]))
                print(
                    f"[{country.iso3} {day}] {record['status']} "
                    f"granules={len(entries)} downloaded={len(downloaded)} failed={len(failures)}"
                )
            except Exception as exc:  # noqa: BLE001
                record["status"] = "official_h5_exception"
                record["note"] = redact_secrets(repr(exc))
                print(f"[{country.iso3} {day}] official_h5_exception {redact_secrets(repr(exc))}")
            rows.append(record)
            write_manifest(manifest, rows)
            time.sleep(0.1)

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "countries": args.countries or "ALL",
        "start": args.start,
        "end": effective_end,
        "rows": len(rows),
        "downloaded_rows": sum(1 for r in rows if r.get("status") == "official_h5_downloaded"),
        "failed_rows": sum(1 for r in rows if r.get("status") not in {"official_h5_downloaded"}),
        "manifest": str(manifest),
    }
    summary_path = base_dir / f"vnp46a2_official_h5_osm_0p001{label}_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed_rows"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
