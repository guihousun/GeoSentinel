from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.GEE_specialist_toolkit import dataset_latest_availability


def _csv_list(raw: str) -> list[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Check latest available date for GEE datasets and LAADS/CMR products.")
    parser.add_argument("--gee-dataset-id", action="append", default=[], help="Repeat for each Earth Engine dataset id.")
    parser.add_argument(
        "--laads-short-name",
        action="append",
        default=[],
        help="Repeat for each LAADS/CMR short_name, e.g. VNP46A2 or VJ102DNB.",
    )
    parser.add_argument("--bbox", default="", help="Optional bbox minx,miny,maxx,maxy for LAADS/CMR queries.")
    parser.add_argument("--lookback-days", type=int, default=30, help="How many days back to search in CMR.")
    parser.add_argument("--requested-end-date", default="", help="Optional requested final observation date YYYY-MM-DD.")
    args = parser.parse_args()

    gee_ids = []
    for item in args.gee_dataset_id:
        gee_ids.extend(_csv_list(item))

    laads_names = []
    for item in args.laads_short_name:
        laads_names.extend(_csv_list(item))

    print(
        dataset_latest_availability(
            gee_dataset_ids=gee_ids or None,
            laads_short_names=laads_names or None,
            requested_end_date=args.requested_end_date or None,
            bbox=args.bbox or None,
            lookback_days=args.lookback_days,
        )
    )


if __name__ == "__main__":
    main()
