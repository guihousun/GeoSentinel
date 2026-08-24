from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import h5py
import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.transform import from_origin
from shapely.geometry import box
from shapely.ops import transform as shapely_transform

from vnp46a2_country_common import (  # noqa: E402
    BAND,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_START,
    iter_dates,
    run_dir,
    selected_countries,
)

NODATA = -9999.0


@dataclass(frozen=True)
class TileInfo:
    h: int
    v: int
    width: int
    height: int
    xmin: float
    ymax: float
    pixel_size: float
    crs: str = "EPSG:4326"

    @property
    def transform(self):
        return from_origin(self.xmin, self.ymax, self.pixel_size, self.pixel_size)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mosaic official VNP46A2 H5 granules into OSM-clipped country GeoTIFFs.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", required=True, help="Effective end date used in the parent run dir.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--countries", nargs="*", default=None)
    parser.add_argument("--targets", nargs="+", default=None, help="ISO3:YYYY-MM-DD values.")
    parser.add_argument("--targets-file", default="", help="Optional UTF-8 text file with one ISO3:YYYY-MM-DD target per line.")
    parser.add_argument("--limit-days", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--run-label", default="", help="Optional suffix for manifest/summary files.")
    return parser.parse_args()


def parse_targets(values: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for value in values:
        iso3, day = value.split(":", 1)
        out.append((iso3.strip().upper(), day.strip()))
    return out


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


def generated_targets(country_tokens: list[str] | None, start: str, end: str, limit_days: int) -> list[tuple[str, str]]:
    dates = iter_dates(start, end)
    if limit_days > 0:
        dates = dates[:limit_days]
    return [(country.iso3, day) for country in selected_countries(country_tokens) for day in dates]


def as_scalar(value, default=None):
    if value is None:
        return default
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return default
        value = value.ravel()[0]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def find_unfilled_dataset(h5: h5py.File):
    hits: list[str] = []

    def walk(name, obj):
        normalized = name.replace("-", "_")
        if isinstance(obj, h5py.Dataset) and normalized.endswith(BAND) and "Gap_Filled" not in normalized:
            hits.append(name)

    h5.visititems(walk)
    if not hits:
        raise KeyError(f"{BAND} not found")
    return hits[0], h5[hits[0]]


def parse_tile(path: Path, width: int, height: int, attrs: dict[str, object], *, shift_dateline: bool = False) -> TileInfo:
    match = re.search(r"\.h(\d{2})v(\d{2})\.", path.name)
    if not match:
        raise ValueError(f"Cannot parse h/v tile id from {path.name}")
    h = int(match.group(1))
    v = int(match.group(2))
    west = as_scalar(attrs.get("WestBoundingCoord"))
    east = as_scalar(attrs.get("EastBoundingCoord"))
    north = as_scalar(attrs.get("NorthBoundingCoord"))
    if west is not None and east is not None and north is not None:
        xmin = float(west)
        ymax = float(north)
        pixel_size = (float(east) - float(west)) / float(width)
    else:
        pixel_size = 10.0 / 2400.0
        xmin = -180.0 + h * 10.0
        ymax = 90.0 - v * 10.0
    if shift_dateline and xmin < 0:
        xmin += 360.0
    return TileInfo(h=h, v=v, width=width, height=height, xmin=xmin, ymax=ymax, pixel_size=pixel_size)


def tile_box_from_path(path: Path, *, shift_dateline: bool = False):
    match = re.search(r"\.h(\d{2})v(\d{2})\.", path.name)
    if not match:
        return None
    h = int(match.group(1))
    v = int(match.group(2))
    xmin = -180.0 + h * 10.0
    if shift_dateline and xmin < 0:
        xmin += 360.0
    xmax = xmin + 10.0
    ymax = 90.0 - v * 10.0
    ymin = ymax - 10.0
    return box(xmin, ymin, xmax, ymax)


def filter_h5_files_to_country_tiles(h5_files: list[Path], geometries: list, *, shift_dateline: bool = False) -> list[Path]:
    filtered: list[Path] = []
    for path in h5_files:
        tile_geom = tile_box_from_path(path, shift_dateline=shift_dateline)
        if tile_geom is None or any(geom.intersects(tile_geom) for geom in geometries):
            filtered.append(path)
    return filtered


def shift_negative_longitudes(geom):
    def shift_x(x):
        if hasattr(x, "__iter__"):
            return [value + 360.0 if value < 0 else value for value in x]
        return x + 360.0 if x < 0 else x

    def shift_coords(x, y, z=None):
        shifted_x = shift_x(x)
        if z is None:
            return shifted_x, y
        return shifted_x, y, z

    return shapely_transform(shift_coords, geom)


def should_shift_dateline(iso3: str, h5_files: list[Path], geometries: list) -> bool:
    if iso3 != "NZL":
        return False
    tile_ids = {re.search(r"\.h(\d{2})v(\d{2})\.", path.name).group(1) for path in h5_files if re.search(r"\.h(\d{2})v(\d{2})\.", path.name)}
    bounds = [geom.bounds for geom in geometries if geom is not None and not geom.is_empty]
    spans_dateline = any(minx < -170 and maxx > 150 for minx, _, maxx, _ in bounds)
    return ("00" in tile_ids and any(int(h) >= 34 for h in tile_ids)) or spans_dateline


def h5_to_tif(h5_path: Path, tif_path: Path, *, force: bool, shift_dateline: bool = False) -> Path:
    if tif_path.exists() and tif_path.stat().st_size > 0 and not force:
        return tif_path
    with h5py.File(h5_path, "r") as h5:
        dataset_path, ds = find_unfilled_dataset(h5)
        arr = ds[()].astype("float32")
        attrs = {k: as_scalar(v) for k, v in ds.attrs.items()}
        file_attrs = {k: as_scalar(v) for k, v in h5.attrs.items()}
        fill = float(attrs.get("_FillValue", attrs.get("FillValue", NODATA)))
        scale = float(attrs.get("scale_factor", attrs.get("ScaleFactor", 1.0)))
        offset = float(attrs.get("offset", attrs.get("add_offset", attrs.get("Offset", 0.0))))
        arr[arr == fill] = np.nan
        arr = arr * scale + offset
        arr[~np.isfinite(arr)] = NODATA
        tile = parse_tile(h5_path, arr.shape[1], arr.shape[0], file_attrs, shift_dateline=shift_dateline)

    tif_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype="float32",
        crs=tile.crs,
        transform=tile.transform,
        nodata=NODATA,
        compress="deflate",
        predictor=2,
    ) as dst:
        dst.write(arr, 1)
        dst.update_tags(1, source_h5=str(h5_path), source_dataset=dataset_path)
    return tif_path


def write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    fields = ["iso3", "date", "status", "h5_count", "tile_tif_count", "output_file", "file_size_mb", "note"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def mosaic_target(base_dir: Path, iso3: str, day: str, *, force: bool) -> dict[str, object]:
    h5_dir = base_dir / "official_raw_h5" / iso3 / day
    raw_h5_files = sorted(h5_dir.glob("*.h5"))
    if not raw_h5_files:
        return {"status": "missing_h5", "h5_count": 0, "tile_tif_count": 0, "note": f"No H5 files under {h5_dir}"}

    simplified_dir = base_dir / "osm_boundaries_simplified_0p001"
    matches = sorted(simplified_dir.glob(f"osm_admin0_{iso3}_*_simplified_0p001.geojson"))
    if not matches:
        return {"status": "missing_boundary", "h5_count": len(raw_h5_files), "tile_tif_count": 0, "note": str(simplified_dir)}

    gdf = gpd.read_file(matches[0]).to_crs("EPSG:4326")
    shapes = [geom for geom in gdf.geometry if geom is not None and not geom.is_empty]
    shift_dateline = should_shift_dateline(iso3, raw_h5_files, shapes)
    filter_shapes = [shift_negative_longitudes(geom) for geom in shapes] if shift_dateline else shapes
    # CMR bbox selection already constrained the raw H5 set. Avoid a second
    # h/v-to-lon/lat approximation here because VNP46A2 tile IDs are not a
    # simple geographic 10-degree grid and can discard valid country granules.
    h5_files = raw_h5_files
    if not h5_files:
        return {
            "status": "no_intersecting_h5",
            "h5_count": 0,
            "tile_tif_count": 0,
            "note": f"{len(raw_h5_files)} H5 files found but none intersected {matches[0]}",
        }

    out_file = (
        base_dir
        / "official_h5_mosaics"
        / iso3
        / f"VNP46A2_{BAND}_{iso3}_{day}_official_h5_osm0p001_mosaic.tif"
    )
    if out_file.exists() and out_file.stat().st_size > 0 and not force:
        return {
            "status": "mosaic_exists",
            "h5_count": len(h5_files),
            "tile_tif_count": len(h5_files),
            "output_file": str(out_file),
            "file_size_mb": round(out_file.stat().st_size / 1024 / 1024, 3),
            "note": "Existing official H5 mosaic reused.",
        }

    mask_shapes = filter_shapes
    tile_tifs = []
    tile_root = base_dir / "official_h5_tile_tifs" / iso3 / day
    invalid_h5: list[str] = []
    for h5_path in h5_files:
        try:
            suffix = f"{BAND}_lon360" if shift_dateline else BAND
            tile_tifs.append(h5_to_tif(h5_path, tile_root / f"{h5_path.stem}_{suffix}.tif", force=force, shift_dateline=shift_dateline))
        except Exception as exc:  # noqa: BLE001
            invalid_h5.append(f"{h5_path.name}:{exc!r}")
    if invalid_h5:
        return {
            "status": "invalid_h5",
            "h5_count": len(h5_files),
            "tile_tif_count": len(tile_tifs),
            "note": " | ".join(invalid_h5[:5]),
        }

    srcs = [rasterio.open(path) for path in tile_tifs]
    tmp_file = tile_root / f"_{iso3}_{day}_merged_unclipped.tif"
    try:
        data, transform = merge(srcs)
        profile = srcs[0].profile.copy()
        profile.update(
            {
                "height": data.shape[1],
                "width": data.shape[2],
                "transform": transform,
                "count": data.shape[0],
                "compress": "deflate",
                "predictor": 2,
                "tiled": True,
                "blockxsize": 256,
                "blockysize": 256,
                "bigtiff": "if_safer",
            }
        )
        with rasterio.open(tmp_file, "w", **profile) as dst:
            dst.write(data)
    finally:
        for src in srcs:
            src.close()

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(tmp_file) as src:
        clipped, clipped_transform = mask(src, mask_shapes, crop=True, nodata=NODATA, filled=True)
        profile = src.profile.copy()
        profile.update(
            {
                "height": clipped.shape[1],
                "width": clipped.shape[2],
                "transform": clipped_transform,
                "compress": "deflate",
                "predictor": 2,
                "tiled": True,
                "blockxsize": 256,
                "blockysize": 256,
                "bigtiff": "if_safer",
            }
        )
        with rasterio.open(out_file, "w", **profile) as dst:
            dst.write(clipped)
            dst.update_tags(source="NASA CMR/Earthdata VNP46A2 official H5", band=BAND, boundary=str(matches[0]))
    tmp_file.unlink(missing_ok=True)
    return {
        "status": "mosaicked",
        "h5_count": len(h5_files),
        "tile_tif_count": len(tile_tifs),
        "output_file": str(out_file),
        "file_size_mb": round(out_file.stat().st_size / 1024 / 1024, 3),
        "note": "",
    }


def main() -> int:
    args = parse_args()
    base_dir = run_dir(Path(args.output_root), args.start, args.end)
    target_values = list(args.targets or []) + read_targets_file(args.targets_file)
    targets = parse_targets(target_values) if target_values else generated_targets(args.countries, args.start, args.end, args.limit_days)
    suffix = f"_{args.run_label}" if args.run_label else ""
    manifest = base_dir / f"vnp46a2_official_h5_osm_0p001_mosaics{suffix}_manifest.csv"
    rows: list[dict[str, object]] = []
    for iso3, day in targets:
        result = mosaic_target(base_dir, iso3, day, force=args.force)
        record = {
            "iso3": iso3,
            "date": day,
            "status": result.get("status"),
            "h5_count": result.get("h5_count", 0),
            "tile_tif_count": result.get("tile_tif_count", 0),
            "output_file": result.get("output_file", ""),
            "file_size_mb": result.get("file_size_mb", ""),
            "note": result.get("note", ""),
        }
        rows.append(record)
        write_manifest(manifest, rows)
        print(f"[{iso3} {day}] {record['status']} h5={record['h5_count']}")

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_count": len(targets),
        "rows": len(rows),
        "mosaicked": sum(1 for r in rows if r.get("status") in {"mosaicked", "mosaic_exists"}),
        "failed": sum(1 for r in rows if r.get("status") not in {"mosaicked", "mosaic_exists"}),
        "manifest": str(manifest),
    }
    summary_path = base_dir / f"vnp46a2_official_h5_osm_0p001_mosaics{suffix}_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
