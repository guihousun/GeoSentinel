from __future__ import annotations

import argparse
import json
import shutil
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from experiments.official_daily_ntl_fastpath.cmr_client import (  # noqa: E402
    download_file_with_curl,
    extract_download_link,
    group_granules_by_day,
    resolve_token,
    search_granules,
)
from experiments.official_daily_ntl_fastpath.gridded_pipeline import process_gridded_day  # noqa: E402
from experiments.official_daily_ntl_fastpath.source_registry import (  # noqa: E402
    get_source_spec,
    parse_sources_arg,
)


def _parse_bbox(raw_values: list[str]) -> tuple[float, float, float, float]:
    if not raw_values:
        raise ValueError("bbox is required")

    if len(raw_values) == 1:
        parts = [x.strip() for x in raw_values[0].split(",") if x.strip()]
    elif len(raw_values) == 4:
        parts = [x.strip() for x in raw_values]
    else:
        raise ValueError("bbox must be 'minx,miny,maxx,maxy' or four values: minx miny maxx maxy")

    if len(parts) != 4:
        raise ValueError("bbox must be minx,miny,maxx,maxy")

    minx, miny, maxx, maxy = (float(x) for x in parts)
    if maxx < minx or maxy < miny:
        raise ValueError("bbox max must be >= min")
    return minx, miny, maxx, maxy


def _zip_files(files: list[Path], out_zip: Path) -> Path:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            zf.write(p, arcname=p.name)
    return out_zip


def _build_roi_gdf_from_bbox(bbox: tuple[float, float, float, float]):
    import geopandas as gpd
    from shapely.geometry import box

    minx, miny, maxx, maxy = bbox
    return gpd.GeoDataFrame(geometry=[box(minx, miny, maxx, maxy)], crs="EPSG:4326")


def _download_raw_for_source(
    source: str,
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float],
    workspace: Path,
    token: str,
) -> dict[str, Any]:
    spec = get_source_spec(source)
    granules = search_granules(
        short_name=spec.short_name,
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        page_size=200,
    )
    groups = group_granules_by_day(granules, night_only=spec.night_only)
    days = sorted(groups.keys())
    if not days:
        return {"source": source, "status": "no_granules", "files": [], "notes": "no granules found"}

    downloaded: list[Path] = []
    failed: list[str] = []
    for day in days:
        entries = groups.get(day, [])
        raw_dir = workspace / "downloads" / "raw" / source / day
        for idx, entry in enumerate(entries, start=1):
            link = extract_download_link(entry.links)
            if not link:
                failed.append(f"{day}#{idx}:missing_download_link")
                continue
            filename = Path(link.split("?")[0]).name or f"{source}_{day}_{idx}.bin"
            dst = raw_dir / filename
            ok, err = download_file_with_curl(link, dst, earthdata_token=token, timeout=300)
            if not ok:
                failed.append(f"{day}#{idx}:{err}")
                continue
            downloaded.append(dst)

    if not downloaded:
        return {
            "source": source,
            "status": "download_failed",
            "files": [],
            "notes": " | ".join(failed[:5]) if failed else "all downloads failed",
        }

    outputs: list[Path] = []
    if len(downloaded) == 1:
        outputs = [downloaded[0]]
    else:
        zip_path = _zip_files(
            downloaded,
            workspace / "downloads" / "raw" / source / f"{source}_{days[0]}_to_{days[-1]}_raw.zip",
        )
        outputs = [zip_path]

    return {
        "source": source,
        "status": "ok",
        "files": [str(p) for p in outputs],
        "downloaded_count": len(downloaded),
        "failed_count": len(failed),
        "notes": " | ".join(failed[:5]) if failed else "",
    }


def _download_clipped_for_source(
    source: str,
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float],
    workspace: Path,
    token: str,
    qa_mode: str = "",
) -> dict[str, Any]:
    spec = get_source_spec(source)
    if spec.processing_mode != "gridded_tile_clip":
        return {
            "source": source,
            "status": "unsupported_format",
            "files": [],
            "notes": "source is feasibility_only in current pipeline (no clipped_tif)",
        }

    granules = search_granules(
        short_name=spec.short_name,
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        page_size=200,
    )
    groups = group_granules_by_day(granules, night_only=spec.night_only)
    days = sorted(groups.keys())
    if not days:
        return {"source": source, "status": "no_granules", "files": [], "notes": "no granules found"}

    roi_gdf = _build_roi_gdf_from_bbox(bbox)
    tif_paths: list[Path] = []
    failed: list[str] = []
    for day in days:
        entries = groups.get(day, [])
        result = process_gridded_day(
            source=source,
            day=day,
            entries=entries,
            variable_candidates=spec.variable_candidates,
            qa_variable_candidates=spec.qa_variable_candidates,
            roi_gdf=roi_gdf,
            workspace=workspace,
            earthdata_token=token,
            qa_mode=(str(qa_mode).strip() or spec.default_qa_mode),
        )
        if result.get("status") == "ok" and result.get("output_path"):
            tif_paths.append(Path(str(result["output_path"])))
        else:
            failed.append(f"{day}:{result.get('status')}:{result.get('notes', '')}")

    if not tif_paths:
        return {
            "source": source,
            "status": "clipped_failed",
            "files": [],
            "notes": " | ".join(failed[:5]) if failed else "all clipped outputs failed",
        }

    outputs: list[Path] = []
    if len(tif_paths) == 1:
        outputs = [tif_paths[0]]
    else:
        zip_path = _zip_files(
            tif_paths,
            workspace / "downloads" / "clipped" / source / f"{source}_{days[0]}_to_{days[-1]}_clipped.zip",
        )
        outputs = [zip_path]

    return {
        "source": source,
        "status": "ok",
        "files": [str(p) for p in outputs],
        "tif_count": len(tif_paths),
        "failed_count": len(failed),
        "notes": " | ".join(failed[:5]) if failed else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Download official NTL sources by bbox and date range.")
    parser.add_argument(
        "--sources",
        required=True,
        help=(
            "comma list (e.g., VNP46A1,VNP46A2,VNP46A3,VNP46A4) "
            "or nrt_priority"
        ),
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--bbox",
        required=True,
        nargs="+",
        help="bbox: either 'minx,miny,maxx,maxy' or four values 'minx miny maxx maxy'",
    )
    parser.add_argument(
        "--format",
        default="raw_h5",
        choices=["raw_h5", "clipped_tif"],
        help="download raw granules or clipped tif",
    )
    parser.add_argument(
        "--workspace",
        default="experiments/official_daily_ntl_fastpath/workspace_monitor",
        help="workspace directory",
    )
    parser.add_argument(
        "--qa-mode",
        default="",
        choices=["", "balanced", "strict", "clear_only"],
        help="Optional QA mode override for gridded clipped outputs. Default: source-specific default.",
    )
    parser.add_argument(
        "--earthdata-token-env",
        default="EARTHDATA_TOKEN",
        help="env key for Earthdata bearer token",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="clean previous downloads under workspace/downloads before run",
    )
    args = parser.parse_args()

    bbox = _parse_bbox(args.bbox)
    sources = parse_sources_arg(args.sources)
    token = resolve_token(args.earthdata_token_env)
    if not token:
        raise RuntimeError(f"{args.earthdata_token_env} missing")

    workspace = Path(args.workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    if args.clean:
        shutil.rmtree(workspace / "downloads", ignore_errors=True)

    rows: list[dict[str, Any]] = []
    for source in sources:
        if args.format == "raw_h5":
            row = _download_raw_for_source(source, args.start_date, args.end_date, bbox, workspace, token)
        else:
            row = _download_clipped_for_source(
                source,
                args.start_date,
                args.end_date,
                bbox,
                workspace,
                token,
                qa_mode=str(args.qa_mode or ""),
            )
        rows.append(row)
        print(f"{source}: {row.get('status')} | files={len(row.get('files', []))}")

    payload = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": sources,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "bbox": bbox,
        "format": args.format,
        "qa_mode": str(args.qa_mode or ""),
        "rows": rows,
    }
    out_json = workspace / "outputs" / "official_download_manifest.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"manifest={out_json}")


if __name__ == "__main__":
    main()
