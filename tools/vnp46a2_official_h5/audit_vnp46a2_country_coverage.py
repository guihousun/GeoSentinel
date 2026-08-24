from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import h5py
import numpy as np
import rasterio

from vnp46a2_country_common import (
    BAND,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_START,
    iter_dates,
    run_dir,
    selected_countries,
)


SUCCESS_DOWNLOAD = {"official_h5_downloaded"}
RETRY_DOWNLOAD = {
    "missing_earthdata_token",
    "official_h5_exception",
    "official_h5_failed",
    "official_h5_partial",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit VNP46A2 country-day H5 downloads and clipped mosaics."
    )
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--countries", nargs="*", default=None)
    parser.add_argument(
        "--skip-pixel-scan",
        action="store_true",
        help="Open GeoTIFF metadata only; do not verify that it contains valid pixels.",
    )
    return parser.parse_args()


def read_download_history(base_dir: Path) -> dict[tuple[str, str], list[dict[str, str]]]:
    history: dict[tuple[str, str], list[dict[str, str]]] = {}
    manifests = sorted(
        path
        for path in base_dir.glob("vnp46a2_official_h5_osm_0p001*_manifest.csv")
        if "_mosaics" not in path.name
        and "validation" not in path.name
        and "boundaries" not in path.name
    )
    for manifest in manifests:
        with manifest.open("r", newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                iso3 = (row.get("iso3") or "").strip().upper()
                day = (row.get("date") or "").strip()
                if not iso3 or not day:
                    continue
                item = dict(row)
                item["_manifest"] = manifest.name
                item["_manifest_mtime_ns"] = str(manifest.stat().st_mtime_ns)
                item["_row_number"] = str(row_number)
                history.setdefault((iso3, day), []).append(item)
    for rows in history.values():
        rows.sort(
            key=lambda row: (
                int(row["_manifest_mtime_ns"]),
                int(row["_row_number"]),
            )
        )
    return history


def has_unfilled_dataset(path: Path) -> bool:
    try:
        with h5py.File(path, "r") as h5:
            hits: list[str] = []

            def walk(name, obj):
                normalized = name.replace("-", "_")
                if (
                    isinstance(obj, h5py.Dataset)
                    and normalized.endswith(BAND)
                    and "Gap_Filled" not in normalized
                ):
                    hits.append(name)

            h5.visititems(walk)
            if not hits:
                return False
            _ = h5[hits[0]].shape
    except Exception:
        return False
    return True


def inspect_mosaic(path: Path, *, scan_pixels: bool) -> tuple[bool, int, str]:
    if not path.exists() or path.stat().st_size <= 0:
        return False, 0, "missing"
    try:
        with rasterio.open(path) as src:
            if src.count < 1 or src.width < 1 or src.height < 1 or src.crs is None:
                return False, 0, "invalid_metadata"
            if not scan_pixels:
                return True, -1, ""
            valid_pixels = 0
            for _, window in src.block_windows(1):
                data = src.read(1, window=window, masked=True)
                valid_pixels += int(np.count_nonzero(~np.ma.getmaskarray(data)))
                if valid_pixels:
                    break
            if valid_pixels == 0:
                return False, 0, "all_nodata"
    except Exception as exc:
        return False, 0, f"open_error:{type(exc).__name__}"
    return True, valid_pixels, ""


def find_mosaic(base_dir: Path, iso3: str, day: str) -> Path:
    mosaic_dir = base_dir / "official_h5_mosaics" / iso3
    canonical = (
        mosaic_dir
        / f"VNP46A2_{BAND}_{iso3}_{day}_official_h5_osm0p001_mosaic.tif"
    )
    if canonical.exists():
        return canonical
    # Earlier batches used names such as
    # VNP46A2_unfilled_<band>_<iso3>_<slug>_tol001_<date>.tif.
    matches = sorted(mosaic_dir.glob(f"*{iso3}*{day}*.tif"))
    return matches[0] if matches else canonical


def integer_values(rows: list[dict[str, str]], field: str) -> list[int]:
    values: list[int] = []
    for row in rows:
        try:
            values.append(int(row.get(field, "") or 0))
        except ValueError:
            continue
    return values


def classify_target(
    base_dir: Path,
    iso3: str,
    day: str,
    history: list[dict[str, str]],
    *,
    scan_pixels: bool,
) -> dict[str, object]:
    h5_dir = base_dir / "official_raw_h5" / iso3 / day
    h5_files = sorted(h5_dir.glob("*.h5"))
    valid_h5_count = sum(1 for path in h5_files if has_unfilled_dataset(path))
    invalid_h5_count = len(h5_files) - valid_h5_count
    mosaic = find_mosaic(base_dir, iso3, day)
    mosaic_valid, valid_pixels, mosaic_note = inspect_mosaic(
        mosaic, scan_pixels=scan_pixels
    )

    statuses = [row.get("status", "") for row in history]
    latest_status = statuses[-1] if statuses else ""
    ever_downloaded = any(status in SUCCESS_DOWNLOAD for status in statuses)
    ever_no_granules = any(status == "no_granules" for status in statuses)
    ever_retryable = any(status in RETRY_DOWNLOAD for status in statuses)
    expected_h5_count = max(integer_values(history, "granules_found"), default=0)
    h5_set_complete = expected_h5_count > 0 and valid_h5_count >= expected_h5_count

    mosaic_all_nodata = (
        mosaic.exists()
        and mosaic_note == "all_nodata"
        and valid_h5_count > 0
        and (ever_downloaded or h5_set_complete)
    )

    if mosaic_valid:
        audit_status = "mosaic_valid"
    elif mosaic_all_nodata:
        # This is a valid terminal product state: NASA returned H5 granules
        # and the country mosaic exists, but the clipped AOI has no valid
        # unfilled NTL pixels for that date.
        audit_status = "mosaic_all_nodata"
    elif latest_status == "no_granules" and valid_h5_count == 0:
        audit_status = "no_granules"
    elif ever_retryable and not ever_downloaded and not h5_set_complete:
        audit_status = "retry_download"
    elif valid_h5_count > 0 and (ever_downloaded or h5_set_complete):
        audit_status = "downloaded_without_mosaic"
    elif ever_no_granules and not ever_downloaded:
        audit_status = "no_granules"
    elif ever_retryable:
        audit_status = "retry_download"
    elif history:
        audit_status = "other_manifest_status"
    else:
        audit_status = "not_processed"

    return {
        "iso3": iso3,
        "date": day,
        "audit_status": audit_status,
        "latest_download_status": latest_status,
        "manifest_attempts": len(history),
        "h5_count": len(h5_files),
        "valid_h5_count": valid_h5_count,
        "invalid_h5_count": invalid_h5_count,
        "expected_h5_count": expected_h5_count,
        "mosaic_exists": mosaic.exists(),
        "mosaic_valid": mosaic_valid,
        "valid_pixel_probe": valid_pixels,
        "mosaic_file": str(mosaic),
        "note": mosaic_note,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_targets(
    path: Path,
    rows: list[dict[str, object]],
    statuses: str | set[str],
) -> int:
    wanted = {statuses} if isinstance(statuses, str) else statuses
    targets = [
        f"{row['iso3']}:{row['date']}"
        for row in rows
        if row["audit_status"] in wanted
    ]
    path.write_text(
        "\n".join(targets) + ("\n" if targets else ""),
        encoding="utf-8",
    )
    return len(targets)


def main() -> int:
    args = parse_args()
    base_dir = run_dir(Path(args.output_root), args.start, args.end)
    countries = selected_countries(args.countries)
    dates = iter_dates(args.start, args.end)
    history = read_download_history(base_dir)
    rows = [
        classify_target(
            base_dir,
            country.iso3,
            day,
            history.get((country.iso3, day), []),
            scan_pixels=not args.skip_pixel_scan,
        )
        for country in countries
        for day in dates
    ]

    csv_path = base_dir / "vnp46a2_country_day_coverage_audit.csv"
    write_csv(csv_path, rows)
    retry_count = write_targets(
        base_dir / "vnp46a2_retry_download_targets.txt",
        rows,
        {"retry_download", "not_processed"},
    )
    mosaic_count = write_targets(
        base_dir / "vnp46a2_pending_mosaic_targets.txt",
        rows,
        "downloaded_without_mosaic",
    )
    status_counts = Counter(str(row["audit_status"]) for row in rows)
    country_counts = {
        country.iso3: dict(
            Counter(
                str(row["audit_status"])
                for row in rows
                if row["iso3"] == country.iso3
            )
        )
        for country in countries
    }
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "start": args.start,
        "end": args.end,
        "countries": [country.iso3 for country in countries],
        "expected_country_days": len(rows),
        "status_counts": dict(status_counts),
        "country_status_counts": country_counts,
        "retry_download_targets": retry_count,
        "pending_mosaic_targets": mosaic_count,
        "coverage_csv": str(csv_path),
    }
    summary_path = base_dir / "vnp46a2_country_day_coverage_audit_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    incomplete = sum(
        status_counts.get(status, 0)
        for status in (
            "downloaded_without_mosaic",
            "retry_download",
            "not_processed",
            "other_manifest_status",
        )
    )
    return 0 if incomplete == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
