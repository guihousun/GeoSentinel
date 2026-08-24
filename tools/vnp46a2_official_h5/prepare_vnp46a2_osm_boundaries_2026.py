from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path

from vnp46a2_country_common import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_START,
    SIMPLIFY_TOLERANCE_DEG,
    area_km2,
    clamp_end_day,
    fetch_osm_boundary,
    gee_latest_day,
    init_ee,
    run_dir,
    selected_countries,
    simplified_boundary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare OSM admin0 boundaries simplified to 0.001 degrees for VNP46A2 country downloads.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", required=True, help="Inclusive requested end date.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--countries", nargs="*", default=None)
    parser.add_argument("--no-gee-latest", action="store_true", help="Do not clamp --end to the latest GEE VNP46A2 day.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.no_gee_latest:
        effective_end = args.end
        latest_day = ""
    else:
        init_ee()
        latest_day = gee_latest_day()
        effective_end = clamp_end_day(args.end, latest_day)

    base_dir = run_dir(Path(args.output_root), args.start, effective_end)
    boundary_dir = base_dir / "osm_boundaries_raw"
    simplified_dir = base_dir / "osm_boundaries_simplified_0p001"
    manifest = base_dir / "vnp46a2_osm_admin0_boundaries_0p001_manifest.csv"
    rows = []
    for country in selected_countries(args.countries):
        raw = fetch_osm_boundary(country, boundary_dir)
        simplified = simplified_boundary(raw, country, simplified_dir)
        path = simplified_dir / f"osm_admin0_{country.iso3}_{country.slug}_simplified_0p001.geojson"
        rows.append(
            {
                "iso3": country.iso3,
                "country": country.name,
                "simplify_tolerance_deg": SIMPLIFY_TOLERANCE_DEG,
                "raw_area_km2": round(area_km2(raw), 3),
                "simplified_area_km2": round(area_km2(simplified), 3),
                "simplified_path": str(path),
            }
        )
        print(f"[{country.iso3}] boundary prepared: {path}")

    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["iso3"])
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "start": args.start,
        "requested_end": args.end,
        "effective_end": effective_end,
        "latest_gee_day": latest_day,
        "countries": len(rows),
        "manifest": str(manifest),
    }
    summary_path = base_dir / "vnp46a2_osm_admin0_boundaries_0p001_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
