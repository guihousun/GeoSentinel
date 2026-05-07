from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import rasterio
from pyresample import geometry, kd_tree
from rasterio.transform import from_bounds


WGS84_CRS = "EPSG:4326"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Precise preprocessing for VIIRS DNB using paired VJ102DNB (radiance) + VJ103DNB (lat/lon)."
    )
    parser.add_argument("--input-dir", required=True, help="Folder containing both VJ102DNB*.nc and VJ103DNB*.nc")
    parser.add_argument("--output-dir", required=True, help="Output folder for per-granule and per-day GeoTIFF")
    parser.add_argument("--date", default="", help="Optional exact date filter: YYYY-MM-DD")
    parser.add_argument("--start-date", default="", help="Optional start date: YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="Optional end date: YYYY-MM-DD")
    parser.add_argument("--bbox", default="", help="Optional clip bbox (minx,miny,maxx,maxy) in lon/lat")
    parser.add_argument(
        "--resolution-deg",
        type=float,
        default=0.0,
        help="Output resolution in degrees; <=0 means auto from --resolution-m",
    )
    parser.add_argument(
        "--resolution-m",
        type=float,
        default=500.0,
        help="Target output resolution in meters (used when --resolution-deg<=0). Default 500m.",
    )
    parser.add_argument("--radius-m", type=float, default=2000.0, help="Nearest-neighbor radius of influence in meters")
    parser.add_argument(
        "--radiance-scale",
        type=float,
        default=1e9,
        help="Multiply DNB radiance by this factor. Default 1e9 converts W/cm^2/sr to nW/cm^2/sr.",
    )
    parser.add_argument("--nodata", type=float, default=-9999.0, help="NoData value for outputs")
    parser.add_argument(
        "--use-cldmsk-cloud-mask",
        action="store_true",
        help="Apply CLDMSK_L2_VIIRS_NOAA20 Integer_Cloud_Mask during preprocess.",
    )
    parser.add_argument(
        "--cldmsk-clear-threshold",
        type=int,
        default=1,
        choices=(1, 2, 3),
        help="Keep CLDMSK Integer_Cloud_Mask values >= threshold. 1 keeps probably_cloudy+probably_clear+confident_clear; 2 keeps probably_clear+confident_clear; 3 keeps only confident_clear.",
    )
    parser.add_argument(
        "--cldmsk-radius-m",
        type=float,
        default=2500.0,
        help="Nearest-neighbor radius of influence for CLDMSK resampling in meters.",
    )
    parser.add_argument(
        "--composite",
        choices=("mean", "max", "min", "best_view"),
        default="mean",
        help="Daily compositing method across matched granules",
    )
    parser.add_argument("--disable-qf-mask", action="store_true", help="Disable observation quality-flag masking")
    parser.add_argument("--disable-geo-mask", action="store_true", help="Disable geolocation quality-flag masking")
    parser.add_argument(
        "--edge-cols",
        type=int,
        default=230,
        help="Mask edge-of-swath columns on both left/right sides (NOAA20-like). Default 230.",
    )
    parser.add_argument("--disable-edge-mask", action="store_true", help="Disable edge-of-swath mask")
    parser.add_argument(
        "--solar-zenith-min-deg",
        type=float,
        default=118.5,
        help="Mask pixels where solar zenith < threshold. Default 118.5.",
    )
    parser.add_argument("--disable-solar-mask", action="store_true", help="Disable solar zenith mask")
    parser.add_argument(
        "--lunar-zenith-max-deg",
        type=float,
        default=90.0,
        help="Mask pixels where lunar zenith <= threshold. Default 90.",
    )
    parser.add_argument("--disable-lunar-mask", action="store_true", help="Disable lunar zenith mask")
    return parser.parse_args()


def _resolve_resolution_deg(resolution_deg: float, resolution_m: float) -> tuple[float, float]:
    if resolution_deg and resolution_deg > 0:
        return float(resolution_deg), float(resolution_deg) * 111320.0
    if not resolution_m or resolution_m <= 0:
        raise ValueError("resolution-m must be > 0 when resolution-deg <= 0")
    # Approximate conversion at equator for EPSG:4326 output.
    deg = float(resolution_m) / 111320.0
    return deg, float(resolution_m)


def _parse_iso_date(raw: str, name: str) -> datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got: {raw}") from exc


def _doy_to_date_str(year: int, doy: int) -> str:
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return dt.strftime("%Y-%m-%d")


def _extract_date_from_name(name: str) -> str | None:
    m = re.search(r"A(\d{4})(\d{3})", name, flags=re.IGNORECASE)
    if not m:
        return None
    year = int(m.group(1))
    doy = int(m.group(2))
    if doy < 1 or doy > 366:
        return None
    return _doy_to_date_str(year, doy)


def _extract_granule_key(name: str) -> str | None:
    # Example: VJ102DNB.A2026059.2142.021....
    m = re.search(r"\.A\d{7}\.\d{4}\.", name, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(0).strip(".")


def _date_filter(date_exact: str, start_date: str, end_date: str) -> tuple[callable[[str], bool], dict[str, Any]]:
    d_exact = _parse_iso_date(date_exact, "--date")
    d_start = _parse_iso_date(start_date, "--start-date")
    d_end = _parse_iso_date(end_date, "--end-date")

    if d_exact and (d_start or d_end):
        raise ValueError("Use either --date OR (--start-date/--end-date), not both.")
    if (d_start and not d_end) or (d_end and not d_start):
        raise ValueError("--start-date and --end-date must be provided together.")
    if d_start and d_end and d_end < d_start:
        raise ValueError("--end-date must be >= --start-date.")

    if d_exact:
        target = d_exact.strftime("%Y-%m-%d")
        return (lambda ds: ds == target), {"mode": "date", "date": target}
    if d_start and d_end:
        left, right = d_start.strftime("%Y-%m-%d"), d_end.strftime("%Y-%m-%d")
        return (lambda ds: left <= ds <= right), {"mode": "range", "start_date": left, "end_date": right}
    return (lambda _: True), {"mode": "all"}


def _parse_bbox(raw: str) -> tuple[float, float, float, float] | None:
    text = (raw or "").strip()
    if not text:
        return None
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = (float(x) for x in parts)
    if maxx <= minx or maxy <= miny:
        raise ValueError("Invalid bbox: maxx>minx and maxy>miny required")
    return minx, miny, maxx, maxy


def _load_vj102(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    with h5py.File(path, "r") as h:
        obs_ds = h["observation_data/DNB_observations"]
        qf_ds = h["observation_data/DNB_quality_flags"]
        data = np.array(obs_ds, dtype=np.float32)
        qf = np.array(qf_ds)

        fill = None
        valid_min = None
        valid_max = None
        units = None
        scale_factor = None
        add_offset = None
        if "units" in obs_ds.attrs:
            raw_units = obs_ds.attrs["units"]
            if isinstance(raw_units, (bytes, np.bytes_)):
                units = raw_units.decode("utf-8", errors="ignore")
            else:
                units = str(np.array(raw_units).reshape(-1)[0])
        if "scale_factor" in obs_ds.attrs:
            scale_factor = float(np.array(obs_ds.attrs["scale_factor"]).reshape(-1)[0])
        if "add_offset" in obs_ds.attrs:
            add_offset = float(np.array(obs_ds.attrs["add_offset"]).reshape(-1)[0])
        if "_FillValue" in obs_ds.attrs:
            fill = float(np.array(obs_ds.attrs["_FillValue"]).reshape(-1)[0])
        if "valid_min" in obs_ds.attrs:
            valid_min = float(np.array(obs_ds.attrs["valid_min"]).reshape(-1)[0])
        if "valid_max" in obs_ds.attrs:
            valid_max = float(np.array(obs_ds.attrs["valid_max"]).reshape(-1)[0])

        if scale_factor is not None:
            data = data * np.float32(scale_factor)
        if add_offset is not None:
            data = data + np.float32(add_offset)

        invalid_base = ~np.isfinite(data)
        if fill is not None:
            invalid_base |= np.isclose(data.astype(np.float64), fill, equal_nan=False)
        if valid_min is not None:
            invalid_base |= data < valid_min
        if valid_max is not None:
            invalid_base |= data > valid_max

        meta = {
            "valid_min": valid_min,
            "valid_max": valid_max,
            "fill_value": fill,
            "units_raw": units,
            "scale_factor_attr": scale_factor,
            "add_offset_attr": add_offset,
            "qf_nonzero_ratio": float(np.mean(qf != 0)),
            "invalid_base_ratio": float(np.mean(invalid_base)),
        }
    return data, qf, invalid_base, meta


def _load_vj103(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    def _scaled_dataset(h: h5py.File, ds_name: str, dtype: Any) -> np.ndarray:
        ds = h[ds_name]
        arr = np.array(ds, dtype=dtype)
        fill = None
        if "_FillValue" in ds.attrs:
            fill = float(np.array(ds.attrs["_FillValue"]).reshape(-1)[0])
        if fill is not None:
            arr = np.where(np.isclose(arr.astype(np.float64), fill, equal_nan=False), np.nan, arr)
        sf = float(np.array(ds.attrs.get("scale_factor", [1.0])).reshape(-1)[0])
        ao = float(np.array(ds.attrs.get("add_offset", [0.0])).reshape(-1)[0])
        if sf != 1.0:
            arr = arr * np.float32(sf)
        if ao != 0.0:
            arr = arr + np.float32(ao)
        return arr.astype(np.float32)

    with h5py.File(path, "r") as h:
        lat = np.array(h["geolocation_data/latitude"], dtype=np.float32)
        lon = np.array(h["geolocation_data/longitude"], dtype=np.float32)
        gq = np.array(h["geolocation_data/quality_flag"], dtype=np.uint8)
        sensor_zenith = _scaled_dataset(h, "geolocation_data/sensor_zenith", np.float32)
        solar_zenith = _scaled_dataset(h, "geolocation_data/solar_zenith", np.float32)
        lunar_zenith = _scaled_dataset(h, "geolocation_data/lunar_zenith", np.float32)
        day_night = h.attrs.get("DayNightFlag", b"")
        if isinstance(day_night, bytes):
            day_night = day_night.decode("utf-8", errors="ignore")
        else:
            day_night = str(np.array(day_night).reshape(-1)[0]) if day_night is not None else ""

    invalid_geo_base = ~np.isfinite(lat) | ~np.isfinite(lon)
    invalid_geo_base |= (lat < -90) | (lat > 90) | (lon < -180) | (lon > 180)
    lat_vis = np.where(invalid_geo_base, np.nan, lat)
    lon_vis = np.where(invalid_geo_base, np.nan, lon)
    meta = {
        "geo_qf_nonzero_ratio": float(np.mean(gq != 0)),
        "day_night_flag": day_night,
        "lat_min": float(np.nanmin(lat_vis)) if np.any(np.isfinite(lat_vis)) else None,
        "lat_max": float(np.nanmax(lat_vis)) if np.any(np.isfinite(lat_vis)) else None,
        "lon_min": float(np.nanmin(lon_vis)) if np.any(np.isfinite(lon_vis)) else None,
        "lon_max": float(np.nanmax(lon_vis)) if np.any(np.isfinite(lon_vis)) else None,
        "invalid_geo_base_ratio": float(np.mean(invalid_geo_base)),
        "sensor_zenith_deg_min": float(np.nanmin(sensor_zenith)) if np.any(np.isfinite(sensor_zenith)) else None,
        "sensor_zenith_deg_max": float(np.nanmax(sensor_zenith)) if np.any(np.isfinite(sensor_zenith)) else None,
        "solar_zenith_deg_min": float(np.nanmin(solar_zenith)) if np.any(np.isfinite(solar_zenith)) else None,
        "solar_zenith_deg_max": float(np.nanmax(solar_zenith)) if np.any(np.isfinite(solar_zenith)) else None,
        "lunar_zenith_deg_min": float(np.nanmin(lunar_zenith)) if np.any(np.isfinite(lunar_zenith)) else None,
        "lunar_zenith_deg_max": float(np.nanmax(lunar_zenith)) if np.any(np.isfinite(lunar_zenith)) else None,
    }
    return lat, lon, gq, sensor_zenith, solar_zenith, lunar_zenith, meta


def _load_cldmsk(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    with h5py.File(path, "r") as h:
        lat = np.array(h["geolocation_data/latitude"], dtype=np.float32)
        lon = np.array(h["geolocation_data/longitude"], dtype=np.float32)
        integer_cloud_mask = np.array(h["geophysical_data/Integer_Cloud_Mask"], dtype=np.int16)

    invalid = ~np.isfinite(lat) | ~np.isfinite(lon) | (lat < -90) | (lat > 90) | (lon < -180) | (lon > 180)
    integer_cloud_mask = np.where(invalid, -1, integer_cloud_mask).astype(np.int16)
    values, counts = np.unique(integer_cloud_mask, return_counts=True)
    hist = {str(int(v)): int(c) for v, c in zip(values, counts, strict=False)}
    meta = {
        "cloudmask_shape": list(integer_cloud_mask.shape),
        "cloudmask_histogram": hist,
        "cloudmask_no_result_ratio": float(np.mean(integer_cloud_mask < 0)),
        "cloudmask_cloudy_ratio": float(np.mean(integer_cloud_mask == 0)),
        "cloudmask_probably_cloudy_ratio": float(np.mean(integer_cloud_mask == 1)),
        "cloudmask_probably_clear_ratio": float(np.mean(integer_cloud_mask == 2)),
        "cloudmask_confident_clear_ratio": float(np.mean(integer_cloud_mask == 3)),
    }
    return integer_cloud_mask.astype(np.float32), lat, lon, meta


def _build_cloud_valid_mask(integer_cloud_mask: np.ndarray, clear_threshold: int) -> np.ndarray:
    return np.isfinite(integer_cloud_mask) & (integer_cloud_mask >= float(clear_threshold))


def _build_noaa20_style_mask(
    shape: tuple[int, int],
    invalid_base: np.ndarray,
    qf_obs: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    qf_geo: np.ndarray,
    solar_zenith: np.ndarray,
    lunar_zenith: np.ndarray,
    *,
    use_qf_mask: bool,
    use_geo_mask: bool,
    use_edge_mask: bool,
    edge_cols: int,
    use_solar_mask: bool,
    solar_zenith_min_deg: float,
    use_lunar_mask: bool,
    lunar_zenith_max_deg: float,
) -> tuple[np.ndarray, dict[str, float]]:
    height, width = shape

    mask_geo_base = ~np.isfinite(lat) | ~np.isfinite(lon) | (lat < -90) | (lat > 90) | (lon < -180) | (lon > 180)
    mask_qf_obs = (qf_obs != 0) if use_qf_mask else np.zeros(shape, dtype=bool)
    mask_qf_geo = (qf_geo != 0) if use_geo_mask else np.zeros(shape, dtype=bool)

    mask_edge = np.zeros(shape, dtype=bool)
    if use_edge_mask and edge_cols > 0 and edge_cols * 2 < width:
        mask_edge[:, :edge_cols] = True
        mask_edge[:, width - edge_cols :] = True

    mask_solar = np.zeros(shape, dtype=bool)
    if use_solar_mask:
        mask_solar = np.isfinite(solar_zenith) & (solar_zenith < float(solar_zenith_min_deg))

    mask_lunar = np.zeros(shape, dtype=bool)
    if use_lunar_mask:
        mask_lunar = np.isfinite(lunar_zenith) & (lunar_zenith <= float(lunar_zenith_max_deg))

    final_mask = (
        invalid_base
        | mask_geo_base
        | mask_qf_obs
        | mask_qf_geo
        | mask_edge
        | mask_solar
        | mask_lunar
    )
    stats = {
        "mask_invalid_base_ratio": float(np.mean(invalid_base)),
        "mask_geo_base_ratio": float(np.mean(mask_geo_base)),
        "mask_qf_obs_ratio": float(np.mean(mask_qf_obs)),
        "mask_qf_geo_ratio": float(np.mean(mask_qf_geo)),
        "mask_edge_ratio": float(np.mean(mask_edge)),
        "mask_solar_ratio": float(np.mean(mask_solar)),
        "mask_lunar_ratio": float(np.mean(mask_lunar)),
        "mask_final_ratio": float(np.mean(final_mask)),
    }
    return final_mask, stats


def _build_area_def(
    bbox: tuple[float, float, float, float],
    resolution_deg: float,
) -> tuple[geometry.AreaDefinition, rasterio.Affine, int, int]:
    minx, miny, maxx, maxy = bbox
    width = max(1, int(math.ceil((maxx - minx) / resolution_deg)))
    height = max(1, int(math.ceil((maxy - miny) / resolution_deg)))
    area_extent = (minx, miny, maxx, maxy)
    area_def = geometry.AreaDefinition(
        area_id="roi4326",
        description="roi4326",
        proj_id="latlon",
        projection={"proj": "longlat", "datum": "WGS84", "no_defs": None},
        width=width,
        height=height,
        area_extent=area_extent,
    )
    transform = from_bounds(minx, miny, maxx, maxy, width=width, height=height)
    return area_def, transform, width, height


def _resample_pair(
    data: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    area_def: geometry.AreaDefinition,
    radius_m: float,
    nodata: float,
) -> np.ndarray:
    valid = np.isfinite(lat) & np.isfinite(lon) & np.isfinite(data) & (data != nodata)
    if not np.any(valid):
        return np.full((area_def.height, area_def.width), nodata, dtype=np.float32)

    swath = geometry.SwathDefinition(lons=lon[valid], lats=lat[valid])
    values = data[valid].astype(np.float32)
    out = kd_tree.resample_nearest(
        source_geo_def=swath,
        data=values,
        target_geo_def=area_def,
        radius_of_influence=radius_m,
        fill_value=nodata,
        epsilon=0.5,
        nprocs=1,
    )
    return np.asarray(out, dtype=np.float32)


def _write_tif(path: Path, arr: np.ndarray, transform: rasterio.Affine, nodata: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    height, width = arr.shape
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs=WGS84_CRS,
        transform=transform,
        nodata=nodata,
        compress="deflate",
    ) as dst:
        dst.write(arr, 1)


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    granule_dir = output_dir / "granules_4326"
    daily_dir = output_dir / "daily_4326"

    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    files_102 = sorted(input_dir.glob("VJ102DNB*.nc"))
    files_103 = sorted(input_dir.glob("VJ103DNB*.nc"))
    files_cld = sorted(
        p
        for p in input_dir.glob("CLDMSK_L2_VIIRS_NOAA20*")
        if p.suffix.lower() in {".nc", ".h5", ".hdf"}
    )
    if not files_102:
        raise FileNotFoundError("No VJ102DNB*.nc found")
    if not files_103:
        raise FileNotFoundError("No VJ103DNB*.nc found")
    if args.use_cldmsk_cloud_mask and not files_cld:
        raise FileNotFoundError("CLDMSK cloud mask enabled but no CLDMSK_L2_VIIRS_NOAA20 files found")

    by_key_102: dict[str, Path] = {}
    by_key_103: dict[str, Path] = {}
    by_key_cld: dict[str, Path] = {}
    for p in files_102:
        k = _extract_granule_key(p.name)
        if k:
            by_key_102[k] = p
    for p in files_103:
        k = _extract_granule_key(p.name)
        if k:
            by_key_103[k] = p
    for p in files_cld:
        k = _extract_granule_key(p.name)
        if k:
            by_key_cld[k] = p

    common_keys = sorted(set(by_key_102).intersection(by_key_103))
    missing_103 = sorted(set(by_key_102) - set(by_key_103))
    missing_102 = sorted(set(by_key_103) - set(by_key_102))
    missing_cld = sorted(set(common_keys) - set(by_key_cld))
    if not common_keys:
        raise RuntimeError("No matched VJ102/VJ103 granule keys.")

    date_pred, date_filter_info = _date_filter(args.date, args.start_date, args.end_date)
    selected_keys = []
    for k in common_keys:
        ds = _extract_date_from_name(k)
        if ds and date_pred(ds):
            selected_keys.append(k)
    if args.use_cldmsk_cloud_mask:
        selected_keys = [k for k in selected_keys if k in by_key_cld]
    if not selected_keys:
        dates = sorted({_extract_date_from_name(k) for k in common_keys if _extract_date_from_name(k)})
        if args.use_cldmsk_cloud_mask:
            raise RuntimeError(
                f"No matched VJ102/VJ103/CLDMSK triplets after date filter {date_filter_info}. "
                f"Available dates from VJ102/VJ103 pairs: {dates}"
            )
        raise RuntimeError(f"No matched pairs after date filter {date_filter_info}. Available dates: {dates}")

    # Build target area
    bbox = _parse_bbox(args.bbox)
    if bbox is None:
        # derive from selected VJ103 attributes quickly
        minx, miny, maxx, maxy = 180.0, 90.0, -180.0, -90.0
        for k in selected_keys:
            with h5py.File(by_key_103[k], "r") as h:
                w = float(np.array(h.attrs["WestBoundingCoordinate"]).reshape(-1)[0])
                e = float(np.array(h.attrs["EastBoundingCoordinate"]).reshape(-1)[0])
                s = float(np.array(h.attrs["SouthBoundingCoordinate"]).reshape(-1)[0])
                n = float(np.array(h.attrs["NorthBoundingCoordinate"]).reshape(-1)[0])
                minx, miny, maxx, maxy = min(minx, w), min(miny, s), max(maxx, e), max(maxy, n)
        bbox = (minx, miny, maxx, maxy)

    resolution_deg, resolution_m_approx = _resolve_resolution_deg(args.resolution_deg, args.resolution_m)
    area_def, transform, width, height = _build_area_def(bbox, resolution_deg)

    per_granule: list[dict[str, Any]] = []
    per_day_arrays: dict[str, list[np.ndarray]] = defaultdict(list)
    per_day_sensor_zenith: dict[str, list[np.ndarray]] = defaultdict(list)
    errors: list[str] = []
    nodata = float(args.nodata)

    for i, key in enumerate(selected_keys, start=1):
        f102 = by_key_102[key]
        f103 = by_key_103[key]
        day = _extract_date_from_name(key) or "unknown_day"
        try:
            data_raw, qf_obs, invalid_base, m102 = _load_vj102(f102)
            lat, lon, qf_geo, sensor_zenith, solar_zenith, lunar_zenith, m103 = _load_vj103(f103)
            if data_raw.shape != lat.shape:
                raise ValueError(f"Shape mismatch VJ102{data_raw.shape} vs VJ103{lat.shape}")
            final_mask, mask_stats = _build_noaa20_style_mask(
                shape=data_raw.shape,
                invalid_base=invalid_base,
                qf_obs=qf_obs,
                lat=lat,
                lon=lon,
                qf_geo=qf_geo,
                solar_zenith=solar_zenith,
                lunar_zenith=lunar_zenith,
                use_qf_mask=(not args.disable_qf_mask),
                use_geo_mask=(not args.disable_geo_mask),
                use_edge_mask=(not args.disable_edge_mask),
                edge_cols=int(args.edge_cols),
                use_solar_mask=(not args.disable_solar_mask),
                solar_zenith_min_deg=float(args.solar_zenith_min_deg),
                use_lunar_mask=(not args.disable_lunar_mask),
                lunar_zenith_max_deg=float(args.lunar_zenith_max_deg),
            )
            data_scaled = data_raw * np.float32(args.radiance_scale)
            data = np.where(final_mask, nodata, data_scaled).astype(np.float32)
            lat_masked = np.where(final_mask, np.nan, lat).astype(np.float32)
            lon_masked = np.where(final_mask, np.nan, lon).astype(np.float32)
            sensor_zenith_masked = np.where(final_mask, np.nan, sensor_zenith).astype(np.float32)
            arr = _resample_pair(
                data=data,
                lat=lat_masked,
                lon=lon_masked,
                area_def=area_def,
                radius_m=float(args.radius_m),
                nodata=nodata,
            )
            sz_arr = _resample_pair(
                data=sensor_zenith_masked,
                lat=lat_masked,
                lon=lon_masked,
                area_def=area_def,
                radius_m=float(args.radius_m),
                nodata=nodata,
            )
            cld_meta: dict[str, Any] = {}
            cld_stats: dict[str, Any] = {}
            if args.use_cldmsk_cloud_mask:
                cld_path = by_key_cld.get(key)
                if cld_path is None:
                    raise RuntimeError(f"Missing CLDMSK match for granule key: {key}")
                cloud_class, cld_lat, cld_lon, cld_meta = _load_cldmsk(cld_path)
                cloud_class_resampled = _resample_pair(
                    data=cloud_class,
                    lat=cld_lat,
                    lon=cld_lon,
                    area_def=area_def,
                    radius_m=float(args.cldmsk_radius_m),
                    nodata=nodata,
                )
                cloud_valid_grid = _build_cloud_valid_mask(
                    cloud_class_resampled,
                    int(args.cldmsk_clear_threshold),
                )
                observed_cloud_grid = np.isfinite(cloud_class_resampled) & (~np.isclose(cloud_class_resampled, nodata))
                cloud_mask_applied = observed_cloud_grid & (~cloud_valid_grid)
                arr = np.where(cloud_valid_grid, arr, nodata).astype(np.float32)
                sz_arr = np.where(cloud_valid_grid, sz_arr, nodata).astype(np.float32)
                cld_stats = {
                    "cldmsk_path": str(cld_path),
                    "cldmsk_clear_threshold": int(args.cldmsk_clear_threshold),
                    "cldmsk_radius_m": float(args.cldmsk_radius_m),
                    "cloudmask_observed_ratio_output": float(np.mean(observed_cloud_grid)),
                    "cloudmask_valid_ratio_output": float(np.mean(cloud_valid_grid)),
                    "cloudmask_applied_ratio_output": float(np.mean(cloud_mask_applied)),
                }
            granule_out = granule_dir / day / f"{f102.stem}_geo.tif"
            _write_tif(granule_out, arr, transform, nodata)
            per_day_arrays[day].append(arr)
            per_day_sensor_zenith[day].append(sz_arr)
            valid_ratio = float(np.mean(arr != nodata))
            rec = {
                "key": key,
                "date": day,
                "vj102": f102.name,
                "vj103": f103.name,
                "output_tif": str(granule_out),
                "valid_ratio": valid_ratio,
                **m102,
                "radiance_scale_applied": float(args.radiance_scale),
                "units_output": "nW/cm^2/sr" if abs(float(args.radiance_scale) - 1e9) < 1 else "scaled_from_raw",
                **m103,
                **cld_meta,
                **cld_stats,
                **mask_stats,
            }
            per_granule.append(rec)
            print(f"[{i}/{len(selected_keys)}] ok {key} valid_ratio={valid_ratio:.4f}")
        except Exception as exc:  # noqa: BLE001
            msg = f"{key}: {exc}"
            errors.append(msg)
            print(f"[{i}/{len(selected_keys)}] fail {msg}")

    if not per_day_arrays:
        raise RuntimeError("No granule was successfully processed.")

    daily_outputs: list[dict[str, Any]] = []
    for day, arrs in sorted(per_day_arrays.items()):
        stack = np.stack(arrs, axis=0)
        if args.composite == "mean":
            valid = stack != nodata
            sum_arr = np.where(valid, stack, 0.0).sum(axis=0, dtype=np.float64)
            cnt_arr = valid.sum(axis=0)
            daily = np.where(cnt_arr > 0, sum_arr / np.maximum(cnt_arr, 1), nodata).astype(np.float32)
        elif args.composite == "max":
            masked = np.where(stack == nodata, -np.inf, stack)
            mx = np.max(masked, axis=0)
            daily = np.where(np.isfinite(mx), mx, nodata).astype(np.float32)
        elif args.composite == "min":
            masked = np.where(stack == nodata, np.inf, stack)
            mn = np.min(masked, axis=0)
            daily = np.where(np.isfinite(mn), mn, nodata).astype(np.float32)
        else:  # best_view
            sz_stack = np.stack(per_day_sensor_zenith[day], axis=0)
            sz_valid = (sz_stack != nodata) & np.isfinite(sz_stack) & (stack != nodata)
            sz_masked = np.where(sz_valid, sz_stack, np.inf)
            best_idx = np.argmin(sz_masked, axis=0)
            has_valid = np.any(sz_valid, axis=0)
            chosen = np.take_along_axis(stack, best_idx[None, :, :], axis=0)[0]
            daily = np.where(has_valid, chosen, nodata).astype(np.float32)

        out = daily_dir / f"VJ102DNB_VJ103DNB_{day}_{args.composite}.tif"
        _write_tif(out, daily, transform, nodata)
        daily_outputs.append(
            {
                "date": day,
                "output_tif": str(out),
                "granule_count": len(arrs),
                "valid_ratio": float(np.mean(daily != nodata)),
            }
        )
        print(f"[daily] {day} -> {out.name} granules={len(arrs)}")

    summary = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "bbox": list(bbox),
        "resolution_deg": float(resolution_deg),
        "resolution_m_approx": float(resolution_m_approx),
        "radius_m": float(args.radius_m),
        "radiance_scale": float(args.radiance_scale),
        "nodata": nodata,
        "composite": args.composite,
        "qc_config": {
            "use_qf_mask": not args.disable_qf_mask,
            "use_geo_mask": not args.disable_geo_mask,
            "use_edge_mask": not args.disable_edge_mask,
            "edge_cols": int(args.edge_cols),
            "use_solar_mask": not args.disable_solar_mask,
            "solar_zenith_min_deg": float(args.solar_zenith_min_deg),
            "use_lunar_mask": not args.disable_lunar_mask,
            "lunar_zenith_max_deg": float(args.lunar_zenith_max_deg),
            "use_cldmsk_cloud_mask": bool(args.use_cldmsk_cloud_mask),
            "cldmsk_clear_threshold": int(args.cldmsk_clear_threshold),
            "cldmsk_radius_m": float(args.cldmsk_radius_m),
        },
        "date_filter": date_filter_info,
        "matched_pair_count_total": len(common_keys),
        "matched_pair_count_selected": len(selected_keys),
        "missing_103_keys": missing_103,
        "missing_102_keys": missing_102,
        "missing_cldmsk_keys": missing_cld if args.use_cldmsk_cloud_mask else [],
        "per_granule": per_granule,
        "daily_outputs": daily_outputs,
        "errors": errors,
    }
    out_json = output_dir / "precise_preprocess_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[summary] {out_json}")


if __name__ == "__main__":
    main()
