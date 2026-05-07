from __future__ import annotations

import json
import os
from collections import defaultdict
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import numpy as np
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None

try:
    import rasterio  # type: ignore
    from rasterio.windows import Window  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    Window = None

import storage_manager as sm_module

sm = sm_module.storage_manager
DEFAULT_GEE_PROJECT = "empyrean-caster-430308-m2"
DedupeMode = Literal["none", "exact_path", "stem_no_digits"]


def simple_key(path: str) -> str:
    """Name normalization key used for simple deduplication."""
    stem = os.path.splitext(os.path.basename(path))[0].lower()
    no_digits = "".join(ch for ch in stem if not ch.isdigit())
    no_digits = no_digits.replace("-", "_").replace(".", "_")
    while "__" in no_digits:
        no_digits = no_digits.replace("__", "_")
    return no_digits.strip("_")


def dedupe_by_name_simple(paths: List[str], keep: Literal["first", "last"] = "first") -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Group files by normalized name (digits removed) and keep only one.
    """
    groups: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for i, p in enumerate(paths):
        groups[simple_key(p)].append((i, p))

    kept: List[str] = []
    dropped: List[Dict[str, str]] = []
    for key, items in groups.items():
        items_sorted = sorted(items, key=lambda x: x[0])
        keep_item = items_sorted[0] if keep == "first" else items_sorted[-1]
        _, keep_path = keep_item
        kept.append(keep_path)
        for _, path in items_sorted:
            if path != keep_path:
                dropped.append({"group": key, "path": path})

    kept = [p for _, p in sorted([(paths.index(p), p) for p in kept], key=lambda x: x[0])]
    return kept, dropped


def dedupe_by_exact_path(paths: List[str], keep: Literal["first", "last"] = "first") -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Remove exact duplicate absolute paths while preserving order.
    """
    positions: Dict[str, List[int]] = defaultdict(list)
    for idx, p in enumerate(paths):
        positions[p].append(idx)

    kept: List[str] = []
    dropped: List[Dict[str, str]] = []
    for path, idx_list in positions.items():
        keep_idx = idx_list[0] if keep == "first" else idx_list[-1]
        kept.append(paths[keep_idx])
        for idx in idx_list:
            if idx != keep_idx:
                dropped.append({"group": "exact_path", "path": paths[idx]})

    kept = [p for _, p in sorted([(paths.index(p), p) for p in kept], key=lambda x: x[0])]
    return kept, dropped


class GeoDataInspectorInput(BaseModel):
    raster_paths: Optional[List[str]] = Field(
        default=None,
        description="Raster filenames in current workspace inputs (e.g., 'ntl_2020.tif').",
    )
    vector_paths: Optional[List[str]] = Field(
        default=None,
        description="Vector filenames in current workspace inputs (e.g., 'boundary.shp').",
    )
    gee_assets: Optional[List[str]] = Field(
        default=None,
        description="Optional GEE asset IDs for cloud validation. Supports 'ee://ASSET_ID' or raw dataset IDs.",
    )
    mode: Literal["basic", "full"] = Field(
        default="full",
        description="basic: existence/availability + key metadata only; full: includes raster stats and sample records.",
    )
    sample_pixels: int = Field(
        default=0,
        description="Only used in full mode for raster stats. If >0, compute stats on a subsample for speed.",
    )
    dedupe_mode: DedupeMode = Field(
        default="none",
        description=(
            "Raster dedupe policy before inspection. "
            "'none' keeps all files; 'exact_path' removes exact duplicates only; "
            "'stem_no_digits' groups files by filename after removing digits."
        ),
    )
    workspace_lookup: Literal["auto", "inputs", "outputs"] = Field(
        default="auto",
        description=(
            "Where logical filenames are looked up. "
            "'auto' checks inputs then outputs; "
            "'inputs' prefers inputs then outputs fallback; "
            "'outputs' prefers outputs then inputs fallback."
        ),
    )


class GeoDataQuickCheckInput(BaseModel):
    raster_paths: Optional[List[str]] = Field(
        default=None,
        description="Raster filenames in current workspace inputs (e.g., 'ntl_2020.tif').",
    )
    vector_paths: Optional[List[str]] = Field(
        default=None,
        description="Vector filenames in current workspace inputs (e.g., 'boundary.shp').",
    )
    gee_assets: Optional[List[str]] = Field(
        default=None,
        description="Optional GEE asset IDs for cloud validation. Supports 'ee://ASSET_ID' or raw dataset IDs.",
    )
    dedupe_mode: DedupeMode = Field(
        default="none",
        description=(
            "Raster dedupe policy for quick checks. Default is 'none' to preserve "
            "per-year/per-month coverage counting."
        ),
    )
    workspace_lookup: Literal["auto", "inputs", "outputs"] = Field(
        default="auto",
        description=(
            "Where logical filenames are looked up. "
            "'auto' checks inputs then outputs; "
            "'inputs' prefers inputs then outputs fallback; "
            "'outputs' prefers outputs then inputs fallback."
        ),
    )


def _normalize_workspace_relative_path(raw_path: str) -> Tuple[Optional[str], str]:
    normalized = str(raw_path or "").strip().replace("\\", "/")
    if not normalized:
        return None, ""
    lowered = normalized.lower()
    if lowered.startswith("inputs/"):
        return "inputs", normalized.split("/", 1)[1].strip()
    if lowered.startswith("outputs/"):
        return "outputs", normalized.split("/", 1)[1].strip()
    return None, normalized


def _resolve_workspace_file(
    raw_path: str,
    *,
    workspace_lookup: Literal["auto", "inputs", "outputs"] = "auto",
) -> Tuple[Optional[str], Optional[str], List[str]]:
    explicit_location, relative_path = _normalize_workspace_relative_path(raw_path)
    if not relative_path:
        return None, None, []

    if explicit_location == "inputs":
        search_order: List[str] = ["inputs", "outputs"]
    elif explicit_location == "outputs":
        search_order = ["outputs", "inputs"]
    elif workspace_lookup == "outputs":
        search_order = ["outputs", "inputs"]
    else:
        # Keep backward compatibility: default lookup still prefers inputs.
        search_order = ["inputs", "outputs"]

    attempted: List[str] = []
    for location in search_order:
        if location == "inputs":
            resolved = sm.resolve_input_path(relative_path)
        else:
            resolved = sm.resolve_output_path(relative_path)
        attempted.append(resolved)
        if os.path.exists(resolved):
            return location, resolved, attempted

    return None, None, attempted


def _raster_basic_stats(arr: np.ndarray) -> Dict[str, Any]:
    data = arr.compressed()
    if data.size == 0:
        return {"count_valid": 0, "min": None, "max": None, "mean": None, "std": None}
    return {
        "count_valid": int(data.size),
        "min": float(np.nanmin(data)),
        "max": float(np.nanmax(data)),
        "mean": float(np.nanmean(data)),
        "std": float(np.nanstd(data)),
    }


def _raster_report(path: str, sample_pixels: int = 0, mode: Literal["basic", "full"] = "full") -> Dict[str, Any]:
    if rasterio is None:
        raise RuntimeError("rasterio is not installed; raster inspection is unavailable.")

    report: Dict[str, Any] = {"path": path, "exists": True, "readable": True}
    if not os.path.isabs(path):
        report["warning"] = "Path is not absolute."

    with rasterio.open(path) as ds:
        report.update(
            {
                "driver": ds.driver,
                "crs": str(ds.crs) if ds.crs else None,
                "width": ds.width,
                "height": ds.height,
                "count_bands": ds.count,
                "dtype": ds.dtypes[0] if ds.count > 0 else None,
                "resolution": (abs(ds.transform.a), abs(ds.transform.e)),
                "nodata": ds.nodata,
                "bounds": {
                    "left": ds.bounds.left,
                    "bottom": ds.bounds.bottom,
                    "right": ds.bounds.right,
                    "top": ds.bounds.top,
                },
            }
        )

        if mode == "basic" or ds.count < 1:
            return report

        if sample_pixels and sample_pixels > 0:
            step_x = max(1, int(np.sqrt((ds.width * ds.height) / sample_pixels)))
            window = Window(col_off=0, row_off=0, width=ds.width, height=ds.height)
            band = ds.read(1, window=window)[::step_x, ::step_x]
        else:
            band = ds.read(1)

        nd = ds.nodata
        if nd is None:
            mask = np.isfinite(band)
        else:
            mask = (band != nd) & np.isfinite(band)

        marr = np.ma.array(band, mask=~mask)
        report["band1_stats"] = _raster_basic_stats(marr)

        hints: List[str] = []
        if "mean" in report["band1_stats"] and report["band1_stats"]["mean"] is not None:
            mn = report["band1_stats"]["min"]
            mx = report["band1_stats"]["max"]
            if mn is not None and mn < -1e-6:
                hints.append("Contains negative values; verify nodata and sensor units.")
            if mx is not None and mx > 1e6:
                hints.append("Very large max; check radiance units or scale.")
        report["hints"] = hints
    return report


def _vector_report(path: str, mode: Literal["basic", "full"] = "full") -> Dict[str, Any]:
    if gpd is None and mode == "full":
        raise RuntimeError("geopandas is not installed; full vector inspection is unavailable.")

    report: Dict[str, Any] = {"path": path, "exists": True, "readable": True}
    if not os.path.isabs(path):
        report["warning"] = "Path is not absolute."

    # Basic mode: prefer lightweight file-level metadata.
    if mode == "basic":
        try:
            import fiona

            with fiona.open(path) as src:
                schema = src.schema or {}
                fields = schema.get("properties", {}) if isinstance(schema, dict) else {}
                geometry_name = schema.get("geometry") if isinstance(schema, dict) else None
                b = src.bounds
                report.update(
                    {
                        "crs": src.crs_wkt or (str(src.crs) if src.crs else None),
                        "feature_count": int(len(src)),
                        "geometry_types": [geometry_name] if geometry_name else [],
                        "bounds": {"minx": float(b[0]), "miny": float(b[1]), "maxx": float(b[2]), "maxy": float(b[3])},
                        "fields": {str(k): str(v) for k, v in fields.items()},
                    }
                )
                return report
        except Exception:
            # Fallback to GeoPandas if Fiona metadata path fails.
            pass

    if gpd is None:
        raise RuntimeError("geopandas is not installed and Fiona fallback failed.")

    gdf = gpd.read_file(path)
    geom_types = sorted(list(gdf.geom_type.unique()))
    report.update(
        {
            "crs": str(gdf.crs) if gdf.crs else None,
            "feature_count": int(len(gdf)),
            "geometry_types": geom_types,
            "bounds": {
                "minx": float(gdf.total_bounds[0]),
                "miny": float(gdf.total_bounds[1]),
                "maxx": float(gdf.total_bounds[2]),
                "maxy": float(gdf.total_bounds[3]),
            },
            "fields": {c: str(gdf[c].dtype) for c in gdf.columns if c != gdf.geometry.name},
        }
    )
    if mode == "full":
        report["sample_records"] = gdf.drop(columns=gdf.geometry.name).head(1).to_dict(orient="records")
    return report


def _bbox_intersect(a: Dict[str, float], b: Dict[str, float]) -> bool:
    return not (
        a["right"] <= b["minx"]
        or a["left"] >= b["maxx"]
        or a["top"] <= b["miny"]
        or a["bottom"] >= b["maxy"]
    )


def _normalize_gee_asset_id(asset_id: str) -> str:
    raw = (asset_id or "").strip()
    if raw.lower().startswith("ee://"):
        return raw[5:].strip("/")
    return raw


def _millis_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def _init_ee():
    try:
        import ee  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return None, f"earthengine-api import failed: {exc}"

    try:
        ee.Initialize(project=DEFAULT_GEE_PROJECT)
        return ee, None
    except Exception:
        try:
            ee.Initialize()
            return ee, None
        except Exception as exc:  # noqa: BLE001
            return None, f"ee.Initialize failed: {exc}"


def _gee_asset_report(asset_id: str, mode: Literal["basic", "full"] = "basic") -> Dict[str, Any]:
    normalized_id = _normalize_gee_asset_id(asset_id)
    report: Dict[str, Any] = {
        "asset_id": normalized_id,
        "exists": False,
        "readable": False,
        "source": "earthengine-api",
    }
    if not normalized_id:
        report["error"] = "Empty GEE asset id."
        return report

    ee, init_error = _init_ee()
    if init_error:
        report["error"] = init_error
        return report

    try:
        info = ee.data.getAsset(normalized_id)  # type: ignore[attr-defined]
        if isinstance(info, dict) and info:
            report["exists"] = True
            report["readable"] = True
            report["asset_type"] = info.get("type")
            report["update_time"] = info.get("updateTime")
            if mode == "basic":
                return report
    except Exception:
        pass

    # Public catalog IDs are not always accessible via ee.data.getAsset;
    # fallback to constructor-based validation.
    errors: Dict[str, str] = {}

    try:
        col = ee.ImageCollection(normalized_id)
        size = col.size().getInfo()
        report.update({"exists": True, "readable": True, "asset_type": "ImageCollection", "collection_size": int(size)})
        if mode == "full":
            first = ee.Image(col.first())
            report["band_names"] = first.bandNames().getInfo()
            start_ms = col.aggregate_min("system:time_start").getInfo()
            end_ms = col.aggregate_max("system:time_start").getInfo()
            report["temporal_coverage"] = {"start": _millis_to_iso(start_ms), "end": _millis_to_iso(end_ms)}
        return report
    except Exception as exc:  # noqa: BLE001
        errors["ImageCollection"] = str(exc)

    try:
        img = ee.Image(normalized_id)
        report.update({"exists": True, "readable": True, "asset_type": "Image"})
        if mode == "full":
            report["band_names"] = img.bandNames().getInfo()
        return report
    except Exception as exc:  # noqa: BLE001
        errors["Image"] = str(exc)

    try:
        fc = ee.FeatureCollection(normalized_id)
        size = fc.size().getInfo()
        report.update({"exists": True, "readable": True, "asset_type": "FeatureCollection", "collection_size": int(size)})
        if mode == "full":
            first = ee.Feature(fc.first()).toDictionary().getInfo()
            report["sample_properties"] = list(first.keys()) if isinstance(first, dict) else []
        return report
    except Exception as exc:  # noqa: BLE001
        errors["FeatureCollection"] = str(exc)

    report["error"] = "Unable to open GEE asset as ImageCollection/Image/FeatureCollection."
    report["attempts"] = errors
    return report


def inspect_geospatial_assets(
    raster_paths: Optional[List[str]] = None,
    vector_paths: Optional[List[str]] = None,
    gee_assets: Optional[List[str]] = None,
    mode: Literal["basic", "full"] = "full",
    sample_pixels: int = 0,
    dedupe_mode: DedupeMode = "none",
    workspace_lookup: Literal["auto", "inputs", "outputs"] = "auto",
    include_cross_checks: bool = True,
) -> str:
    report: Dict[str, Any] = {
        "mode": mode,
        "raster_reports": [],
        "vector_reports": [],
        "gee_reports": [],
        "cross_checks": [],
    }

    resolved_raster_paths: List[Tuple[str, str, str]] = []
    report["requested_raster_count"] = len(raster_paths or [])
    if raster_paths:
        for p in raster_paths:
            resolved_location, abs_path, attempted_paths = _resolve_workspace_file(
                p,
                workspace_lookup=workspace_lookup,
            )
            if not abs_path or not resolved_location:
                report["raster_reports"].append(
                    {
                        "path": p,
                        "resolved_path": None,
                        "resolved_location": None,
                        "attempted_paths": attempted_paths,
                        "exists": False,
                        "readable": False,
                        "error": "File not found in workspace (inputs/outputs).",
                    }
                )
                continue
            resolved_raster_paths.append((p, abs_path, resolved_location))

        dedupe_dropped: List[Dict[str, str]] = []
        if dedupe_mode == "stem_no_digits":
            original_paths = [item[1] for item in resolved_raster_paths]
            deduped_paths, dedupe_dropped = dedupe_by_name_simple(original_paths, keep="first")
            allowed_counts = Counter(deduped_paths)
            deduped_entries: List[Tuple[str, str, str]] = []
            for entry in resolved_raster_paths:
                abs_path = entry[1]
                if allowed_counts.get(abs_path, 0) > 0:
                    deduped_entries.append(entry)
                    allowed_counts[abs_path] -= 1
            resolved_raster_paths = deduped_entries
            report["dedupe_raster"] = {"policy": "by_name_no_digits_keep_first", "dropped": dedupe_dropped}
        elif dedupe_mode == "exact_path":
            original_paths = [item[1] for item in resolved_raster_paths]
            deduped_paths, dedupe_dropped = dedupe_by_exact_path(original_paths, keep="first")
            allowed_counts = Counter(deduped_paths)
            deduped_entries = []
            for entry in resolved_raster_paths:
                abs_path = entry[1]
                if allowed_counts.get(abs_path, 0) > 0:
                    deduped_entries.append(entry)
                    allowed_counts[abs_path] -= 1
            resolved_raster_paths = deduped_entries
            report["dedupe_raster"] = {"policy": "exact_path_keep_first", "dropped": dedupe_dropped}
        else:
            report["dedupe_raster"] = {"policy": "none", "dropped": []}
        report["dedupe_applied"] = dedupe_mode != "none"
    else:
        report["dedupe_raster"] = {"policy": "none", "dropped": []}
        report["dedupe_applied"] = False
    report["resolved_raster_count"] = len(resolved_raster_paths)

    resolved_vector_paths: List[Tuple[str, str, str]] = []
    if vector_paths:
        for p in vector_paths:
            resolved_location, abs_path, attempted_paths = _resolve_workspace_file(
                p,
                workspace_lookup=workspace_lookup,
            )
            if not abs_path or not resolved_location:
                report["vector_reports"].append(
                    {
                        "path": p,
                        "resolved_path": None,
                        "resolved_location": None,
                        "attempted_paths": attempted_paths,
                        "exists": False,
                        "readable": False,
                        "error": "File not found in workspace (inputs/outputs).",
                    }
                )
                continue
            resolved_vector_paths.append((p, abs_path, resolved_location))

    for requested_path, rp, resolved_location in resolved_raster_paths:
        try:
            raster_item = _raster_report(rp, sample_pixels=sample_pixels, mode=mode)
            raster_item["requested_path"] = requested_path
            raster_item["resolved_location"] = resolved_location
            report["raster_reports"].append(raster_item)
        except Exception as exc:  # noqa: BLE001
            report["raster_reports"].append(
                {
                    "path": rp,
                    "requested_path": requested_path,
                    "resolved_location": resolved_location,
                    "exists": True,
                    "readable": False,
                    "error": str(exc),
                }
            )

    for requested_path, vp, resolved_location in resolved_vector_paths:
        try:
            vector_item = _vector_report(vp, mode=mode)
            vector_item["requested_path"] = requested_path
            vector_item["resolved_location"] = resolved_location
            report["vector_reports"].append(vector_item)
        except Exception as exc:  # noqa: BLE001
            report["vector_reports"].append(
                {
                    "path": vp,
                    "requested_path": requested_path,
                    "resolved_location": resolved_location,
                    "exists": True,
                    "readable": False,
                    "error": str(exc),
                }
            )

    if gee_assets:
        for asset in gee_assets:
            report["gee_reports"].append(_gee_asset_report(asset, mode=mode))

    if include_cross_checks and report["raster_reports"] and report["vector_reports"]:
        r0 = next((r for r in report["raster_reports"] if "error" not in r), None)
        if r0:
            r_bounds = r0.get("bounds")
            r_crs = r0.get("crs")
            for vrep in report["vector_reports"]:
                if "error" in vrep:
                    continue
                v_bounds = vrep.get("bounds")
                v_crs = vrep.get("crs")
                cc = {
                    "raster_path": r0.get("path"),
                    "vector_path": vrep.get("path"),
                    "crs_match": (r_crs == v_crs) if (r_crs and v_crs) else False,
                    "bbox_intersection": _bbox_intersect(r_bounds, v_bounds) if (r_bounds and v_bounds) else None,
                    "advice": [],
                }
                if not cc["crs_match"]:
                    cc["advice"].append("CRS mismatch: reproject vector to raster CRS before analysis.")
                if cc["bbox_intersection"] is False:
                    cc["advice"].append("No spatial overlap: verify ROI or clip/align inputs.")
                report["cross_checks"].append(cc)

    report["summary"] = {
        "raster_ok": sum(1 for r in report["raster_reports"] if r.get("exists") and r.get("readable")),
        "raster_fail": sum(1 for r in report["raster_reports"] if not (r.get("exists") and r.get("readable"))),
        "vector_ok": sum(1 for v in report["vector_reports"] if v.get("exists") and v.get("readable")),
        "vector_fail": sum(1 for v in report["vector_reports"] if not (v.get("exists") and v.get("readable"))),
        "gee_ok": sum(1 for g in report["gee_reports"] if g.get("exists") and g.get("readable")),
        "gee_fail": sum(1 for g in report["gee_reports"] if not (g.get("exists") and g.get("readable"))),
    }

    return json.dumps(report, indent=2, ensure_ascii=False)


def inspect_geospatial_assets_quick(
    raster_paths: Optional[List[str]] = None,
    vector_paths: Optional[List[str]] = None,
    gee_assets: Optional[List[str]] = None,
    dedupe_mode: DedupeMode = "none",
    workspace_lookup: Literal["auto", "inputs", "outputs"] = "auto",
) -> str:
    """
    Fast availability check for Data_Searcher:
    returns existence/readability + basic metadata only.
    """
    return inspect_geospatial_assets(
        raster_paths=raster_paths,
        vector_paths=vector_paths,
        gee_assets=gee_assets,
        mode="basic",
        sample_pixels=0,
        dedupe_mode=dedupe_mode,
        workspace_lookup=workspace_lookup,
        include_cross_checks=False,
    )


geodata_inspector_tool = StructuredTool.from_function(
    func=inspect_geospatial_assets,
    name="geodata_inspector_tool",
    description=(
        "Inspect local raster/vector assets and optional GEE cloud assets. "
        "Use mode='basic' for fast availability checks (exists/readable + key metadata), "
        "or mode='full' for extended raster stats and sample records. "
        "When both raster and vector are provided, checks CRS match and bbox overlap."
    ),
    args_schema=GeoDataInspectorInput,
)


geodata_quick_check_tool = StructuredTool.from_function(
    func=inspect_geospatial_assets_quick,
    name="geodata_quick_check_tool",
    description=(
        "Quick-check local raster/vector files and optional GEE assets for existence/readability "
        "with basic metadata (CRS, bounds, dimensions/feature_count). "
        "Default dedupe_mode is 'none' so per-year/per-month files are all counted. "
        "Intended for Data_Searcher boundary and cloud-asset verification only."
    ),
    args_schema=GeoDataQuickCheckInput,
)
