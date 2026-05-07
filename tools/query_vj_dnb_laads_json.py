from __future__ import annotations

import argparse
import json
import sys
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.official_daily_ntl_fastpath.cmr_client import extract_download_link, search_granules


SUPPORTED_SOURCES = {
    # Official VIIRS nighttime-light products currently monitored.
    "VNP46A1",
    "VNP46A2",
    "VNP46A3",
    "VNP46A4",
}


def _parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in (raw or "").split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = (float(x) for x in parts)
    if maxx <= minx or maxy <= miny:
        raise ValueError("Invalid bbox: maxx>minx and maxy>miny required")
    return minx, miny, maxx, maxy


def _parse_sources(raw: str) -> list[str]:
    out: list[str] = []
    for p in (raw or "").split(","):
        s = p.strip().upper()
        if not s:
            continue
        if s not in SUPPORTED_SOURCES:
            supported = ",".join(sorted(SUPPORTED_SOURCES))
            raise ValueError(f"Unsupported source '{s}'. Supported sources: {supported}")
        if s not in out:
            out.append(s)
    if not out:
        out = ["VNP46A1", "VNP46A2", "VNP46A3", "VNP46A4"]
    return out


def _size_bytes_from_entry(entry_raw: dict[str, Any]) -> int:
    # CMR granule_size is typically in MB.
    val = entry_raw.get("granule_size")
    if val is None:
        return 0
    try:
        return int(float(val) * 1024 * 1024)
    except Exception:  # noqa: BLE001
        return 0


def _make_item_key(url: str, used: set[str]) -> str:
    base = str(zlib.crc32(url.encode("utf-8")) & 0xFFFFFFFF)
    if base not in used:
        used.add(base)
        return base
    i = 1
    while True:
        k = f"{base}{i}"
        if k not in used:
            used.add(k)
            return k
        i += 1


def build_query_string(
    sources: list[str],
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float],
    count: int,
) -> str:
    minx, miny, maxx, maxy = bbox
    src_part = " ".join(f"{s}--5201" for s in sources)
    # Keep same style as LAADS exported query.
    return f"{src_part} {start_date}..{end_date} x{minx}y{maxy} x{maxx}y{miny}[{count}]"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query VIIRS products by bbox+date and export LAADS-like JSON download list."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--bbox", required=True, help="minx,miny,maxx,maxy")
    parser.add_argument(
        "--sources",
        default="VNP46A1,VNP46A2,VNP46A3,VNP46A4",
        help=(
            "Comma list of short_names. "
            "Examples: VNP46A1,VNP46A2 or VNP46A1,VNP46A2,VNP46A3,VNP46A4"
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output JSON path. Default: e:/Download/LAADS_query.<utc timestamp>.json",
    )
    parser.add_argument("--page-size", type=int, default=200, help="CMR page_size, default 200")
    args = parser.parse_args()

    # Validate dates.
    try:
        d0 = datetime.strptime(args.start_date, "%Y-%m-%d")
        d1 = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("start/end date must be YYYY-MM-DD") from exc
    if d1 < d0:
        raise ValueError("end-date must be >= start-date")

    bbox = _parse_bbox(args.bbox)
    sources = _parse_sources(args.sources)

    # Collect all matching granules.
    all_items: list[dict[str, Any]] = []
    for src in sources:
        granules = search_granules(
            short_name=src,
            start_date=args.start_date,
            end_date=args.end_date,
            bbox=bbox,
            page_size=max(1, int(args.page_size)),
        )
        for g in granules:
            url = extract_download_link(g.links)
            if not url:
                continue
            # Keep only likely data file links.
            lower_url = url.lower()
            if not (
                lower_url.endswith(".nc")
                or ".nc?" in lower_url
                or lower_url.endswith(".h5")
                or ".h5?" in lower_url
                or lower_url.endswith(".hdf")
                or ".hdf?" in lower_url
            ):
                continue
            all_items.append(
                {
                    "source": src,
                    "producer_granule_id": g.producer_granule_id,
                    "time_start": g.time_start,
                    "url": url,
                    "size": _size_bytes_from_entry(g.raw),
                }
            )

    # Sort by source then time.
    all_items.sort(key=lambda x: (x["source"], x["time_start"], x["producer_granule_id"]))

    query_text = build_query_string(
        sources=sources,
        start_date=args.start_date,
        end_date=args.end_date,
        bbox=bbox,
        count=len(all_items),
    )

    out: dict[str, Any] = {"query": query_text}
    used_keys: set[str] = set()
    for item in all_items:
        k = _make_item_key(item["url"], used_keys)
        out[k] = {"url": item["url"], "size": item["size"]}

    if args.output:
        out_path = Path(args.output).resolve()
    else:
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H_%M")
        out_path = Path("e:/Download") / f"LAADS_query.{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    print(f"saved: {out_path}")
    print(f"sources: {sources}")
    print(f"items: {len(all_items)}")


if __name__ == "__main__":
    main()
