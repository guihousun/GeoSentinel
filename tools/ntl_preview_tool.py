from __future__ import annotations

import json
import os
import shutil
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from PIL import Image

from storage_manager import current_thread_id, storage_manager


DEFAULT_GEE_PROJECT = str(os.getenv("GEE_DEFAULT_PROJECT_ID") or "").strip() or "empyrean-caster-430308-m2"
DEFAULT_OUTPUT_ROOT = "ntl_preview_runs"
PALETTE_PRESETS: dict[str, list[str]] = {
    "report_dark": ["000000", "1f3b73", "ffcc33", "ff5e3a", "ffffff"],
    "night_blue": ["04111f", "143d6b", "4c8eda", "b9d6ff", "ffffff"],
    "mono_gray": ["000000", "3a3a3a", "7a7a7a", "cfcfcf", "ffffff"],
    "impact_hot": ["120a1f", "4b1d6b", "a11d77", "ff6f61", "ffd166", "ffffff"],
    "white_viridis": ["ffffff", "dcecc7", "86d49d", "2f9e8f", "0b6e6e"],
}
DATASET_SPECS: dict[str, dict[str, Any]] = {
    "NPP-VIIRS-Like": {
        "collection_id": "projects/sat-io/open-datasets/npp-viirs-ntl",
        "band": "b1",
        "year_min": 2000,
        "year_max": 2024,
    },
    "NPP-VIIRS": {
        "collection_id_v21": "NOAA/VIIRS/DNB/ANNUAL_V21",
        "collection_id_v22": "NOAA/VIIRS/DNB/ANNUAL_V22",
        "band": "average",
        "year_min": 2012,
        "year_max": 2024,
    },
    "DMSP-OLS": {
        "collection_id": "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS",
        "band": "avg_vis",
        "year_min": 1992,
        "year_max": 2013,
    },
}


class AnnualNTLPreviewInput(BaseModel):
    years: list[int] = Field(..., description="Years to preview, e.g. [2000, 2010, 2020].")
    dataset_name: str = Field(
        default="NPP-VIIRS-Like",
        description="Annual dataset: NPP-VIIRS-Like | NPP-VIIRS | DMSP-OLS.",
    )
    region_name: str = Field(default="China", description="Region name used with the boundary collection.")
    boundary_collection_id: str = Field(
        default="FAO/GAUL/2015/level0",
        description="GEE boundary collection id used to resolve the preview region.",
    )
    region_field: str = Field(default="ADM0_NAME", description="Boundary property name used to filter the region.")
    output_root: str = Field(default=DEFAULT_OUTPUT_ROOT, description="Output folder under current thread workspace outputs/.")
    run_label: str = Field(default="", description="Optional run label. Auto-generated when empty.")
    generate_gif: bool = Field(default=True, description="Generate a GIF preview in addition to PNG thumbnails.")
    thumb_dimensions: str = Field(default="1600x1200", description="Thumbnail size passed to getThumbURL.")
    gif_dimensions: str = Field(default="1400x1050", description="GIF size passed to getVideoThumbURL.")
    style_palette: str = Field(
        default="report_dark",
        description="Palette preset: report_dark | night_blue | mono_gray | impact_hot | white_viridis.",
    )
    min_value: float = Field(default=0.0, description="Visualization lower bound.")
    max_value: float = Field(default=60.0, description="Visualization upper bound.")
    gif_fps: float = Field(default=1.0, description="GIF frame rate.")
    ask_user_for_params: bool = Field(
        default=False,
        description="If true, return a question checklist instead of executing.",
    )


@dataclass(frozen=True)
class DatasetSpec:
    collection_id: str
    band: str
    year_min: int
    year_max: int
    collection_id_v22: Optional[str] = None


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = None
    if isinstance(config, dict):
        runtime_config = config
    else:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _resolve_output_root(output_root: str, thread_id: str) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = (output_root or "").strip() or DEFAULT_OUTPUT_ROOT
    if raw.startswith("/data/processed/"):
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))
    if raw.startswith(("/data/raw/", "/memories/", "/shared/")):
        raise PermissionError("output_root must be writable under outputs/, not raw/memory/shared.")
    p = Path(raw)
    if p.is_absolute():
        raise ValueError("output_root must be workspace-relative.")
    if ".." in p.parts:
        raise ValueError("output_root must not contain '..'.")
    if p.parts and p.parts[0] == "outputs":
        target = (workspace / p).resolve()
    else:
        target = (outputs_root / p).resolve()
    if not str(target).startswith(str(outputs_root)):
        raise PermissionError("output_root resolved outside workspace outputs root.")
    return target


def _normalize_years(years: list[int]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for item in years:
        year = int(item)
        if year in seen:
            continue
        seen.add(year)
        out.append(year)
    return sorted(out)


def _dataset_spec(dataset_name: str) -> DatasetSpec:
    name = str(dataset_name or "").strip()
    if name not in DATASET_SPECS:
        raise ValueError("dataset_name must be one of: NPP-VIIRS-Like, NPP-VIIRS, DMSP-OLS")
    spec = DATASET_SPECS[name]
    return DatasetSpec(
        collection_id=str(spec["collection_id"]),
        band=str(spec["band"]),
        year_min=int(spec["year_min"]),
        year_max=int(spec["year_max"]),
        collection_id_v22=str(spec.get("collection_id_v22") or "") or None,
    )


def _region_name_candidates(region_name: str) -> list[str]:
    name = str(region_name or "").strip()
    if not name:
        raise ValueError("region_name must not be empty.")
    candidates = [name]
    if name.casefold() == "china":
        for alias in ("Taiwan", "Hong Kong", "Macao"):
            if alias not in candidates:
                candidates.append(alias)
    return candidates


def _palette_values(style_palette: str) -> list[str]:
    raw = str(style_palette or "").strip()
    if not raw:
        return list(PALETTE_PRESETS["report_dark"])
    if "," in raw:
        values = [part.strip().lstrip("#") for part in raw.split(",") if part.strip()]
        if values:
            return values
    return list(PALETTE_PRESETS.get(raw, PALETTE_PRESETS["report_dark"]))


def _initialize_earth_engine():
    try:
        import ee  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"earthengine-api import failed: {exc}") from exc

    try:
        ee.Initialize(project=DEFAULT_GEE_PROJECT)
    except Exception:
        try:
            ee.Initialize()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"ee.Initialize failed: {exc}") from exc
    return ee


def _resolve_country_geometry(ee: Any, boundary_collection_id: str, region_field: str, region_name: str):
    candidates = _region_name_candidates(region_name)
    fc = ee.FeatureCollection(boundary_collection_id)
    if len(candidates) == 1:
        filtered = fc.filter(ee.Filter.eq(region_field, candidates[0]))
    else:
        filtered = fc.filter(ee.Filter.inList(region_field, candidates))
    try:
        size = int(filtered.size().getInfo())
    except Exception:
        size = -1
    if size == 0:
        raise ValueError(
            f"Region '{region_name}' was not found in boundary collection '{boundary_collection_id}' using field '{region_field}'."
        )
    return filtered.geometry(), candidates


def _build_annual_image(ee: Any, year: int, dataset_name: str, region_geom: Any):
    spec = _dataset_spec(dataset_name)
    if year < spec.year_min or year > spec.year_max:
        raise ValueError(f"{dataset_name} valid year range: {spec.year_min}-{spec.year_max}")

    start = f"{year}-01-01"
    end = f"{year + 1}-01-01"

    if dataset_name == "NPP-VIIRS-Like":
        col = ee.ImageCollection(spec.collection_id).filterDate(start, end).select(spec.band).filterBounds(region_geom)
    elif dataset_name == "NPP-VIIRS":
        src_id = spec.collection_id if year <= 2021 else (spec.collection_id_v22 or spec.collection_id)
        col = ee.ImageCollection(src_id).filterDate(start, end).select(spec.band).filterBounds(region_geom)
    else:
        col = ee.ImageCollection(spec.collection_id).filterDate(start, end).select(spec.band).filterBounds(region_geom)

    return col.mean().clip(region_geom).set(
        {
            "system:time_start": ee.Date(start).millis(),
            "preview_year": int(year),
            "dataset_name": dataset_name,
        }
    )


def _download_remote_file(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as response, output_path.open("wb") as f:  # nosec: B310
        shutil.copyfileobj(response, f)


def _compose_gif_from_pngs(
    png_paths: list[Path],
    output_path: Path,
    *,
    fps: float,
    background_color: tuple[int, int, int] = (255, 255, 255),
) -> None:
    if not png_paths:
        raise ValueError("png_paths must contain at least one frame.")

    duration_ms = max(1, int(round(1000.0 / max(float(fps), 0.001))))
    frames: list[Image.Image] = []
    for png_path in png_paths:
        with Image.open(png_path) as src:
            rgba = src.convert("RGBA")
            background = Image.new("RGBA", rgba.size, (*background_color, 255))
            flattened = Image.alpha_composite(background, rgba).convert("P", palette=Image.Palette.ADAPTIVE, colors=256)
            frames.append(flattened)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    first, rest = frames[0], frames[1:]
    first.save(
        output_path,
        format="GIF",
        save_all=True,
        append_images=rest,
        duration=duration_ms,
        loop=0,
        optimize=False,
        disposal=2,
    )


def _render_thumbnail(
    image: Any,
    output_path: Path,
    *,
    dimensions: str,
    min_value: float,
    max_value: float,
    palette: list[str],
    region_geom: Any,
) -> str:
    vis = image.visualize(min=float(min_value), max=float(max_value), palette=palette)
    url = vis.getThumbURL(
        {
            "region": region_geom,
            "dimensions": dimensions,
            "format": "png",
        }
    )
    _download_remote_file(url, output_path)
    return str(url)


def _render_gif(
    png_paths: list[Path],
    output_path: Path,
    *,
    fps: float,
) -> str:
    _compose_gif_from_pngs(list(png_paths), output_path, fps=float(fps))
    return str(output_path)


def _auto_run_label(dataset_name: str, country_name: str, years: list[int], run_label: str) -> str:
    if run_label.strip():
        return run_label.strip()
    year_part = "_".join(str(y) for y in years)
    safe_country = str(country_name or "region").strip().replace(" ", "_")
    safe_dataset = str(dataset_name or "dataset").strip().replace(" ", "_")
    return f"ntl_preview_{safe_country}_{safe_dataset}_{year_part}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def run_annual_ntl_preview(
    years: list[int],
    dataset_name: str = "NPP-VIIRS-Like",
    country_name: str = "China",
    boundary_collection_id: str = "FAO/GAUL/2015/level0",
    region_field: str = "ADM0_NAME",
    output_root: str = DEFAULT_OUTPUT_ROOT,
    run_label: str = "",
    generate_gif: bool = True,
    thumb_dimensions: str = "1600x1200",
    gif_dimensions: str = "1400x1050",
    style_palette: str = "report_dark",
    min_value: float = 0.0,
    max_value: float = 60.0,
    gif_fps: float = 1.0,
    ask_user_for_params: bool = False,
    config: Optional[RunnableConfig] = None,
    **kwargs,
) -> Dict[str, Any]:
    if ask_user_for_params:
        return {
            "status": "need_user_input",
            "tool": "NTL_preview_tool",
            "questions": [
                "请提供要展示的年份列表，例如 [2000, 2010, 2020]。",
                "请选择数据集：NPP-VIIRS-Like / NPP-VIIRS / DMSP-OLS。",
                "是否生成 GIF？默认会生成；如不需要可设置 generate_gif=False。",
                "如果不是中国，请提供 country_name 和 boundary_collection_id/region_field。",
            ],
        }

    _ = kwargs
    thread_id = _resolve_thread_id_from_config(config)
    years_norm = _normalize_years(list(years or []))
    if not years_norm:
        raise ValueError("years must contain at least one year.")

    spec = _dataset_spec(dataset_name)
    for year in years_norm:
        if year < spec.year_min or year > spec.year_max:
            raise ValueError(f"{dataset_name} valid year range: {spec.year_min}-{spec.year_max}")

    ee = _initialize_earth_engine()
    region_geom, region_members = _resolve_country_geometry(ee, boundary_collection_id, region_field, country_name)
    palette = _palette_values(style_palette)
    out_root = _resolve_output_root(output_root, thread_id)
    label = _auto_run_label(dataset_name, country_name, years_norm, run_label)
    run_dir = out_root / label
    run_dir.mkdir(parents=True, exist_ok=True)

    preview_items: list[dict[str, Any]] = []
    png_paths: list[Path] = []
    for year in years_norm:
        image = _build_annual_image(ee, year, dataset_name, region_geom)
        png_path = run_dir / f"{year}.png"
        thumb_url = _render_thumbnail(
            image,
            png_path,
            dimensions=thumb_dimensions,
            min_value=min_value,
            max_value=max_value,
            palette=palette,
            region_geom=region_geom,
        )
        preview_items.append(
            {
                "year": int(year),
                "png_path": str(png_path),
                "thumb_url": thumb_url,
            }
        )
        png_paths.append(png_path)

    gif_path: Optional[Path] = None
    gif_url: Optional[str] = None
    if generate_gif:
        gif_path = run_dir / "timeline.gif"
        gif_url = _render_gif(png_paths, gif_path, fps=gif_fps)

    summary = {
        "status": "success",
        "tool": "NTL_preview_tool",
        "thread_id": thread_id,
        "dataset_name": dataset_name,
        "country_name": country_name,
        "region_members": region_members,
        "boundary_collection_id": boundary_collection_id,
        "region_field": region_field,
        "years": years_norm,
        "output_root": str(out_root),
        "run_dir": str(run_dir),
        "generate_gif": bool(generate_gif),
        "style_palette": style_palette,
        "min_value": float(min_value),
        "max_value": float(max_value),
        "thumb_dimensions": thumb_dimensions,
        "gif_dimensions": gif_dimensions,
        "gif_fps": float(gif_fps),
        "preview_items": preview_items,
        "gif_path": str(gif_path) if gif_path else None,
        "gif_url": gif_url,
    }
    summary_path = run_dir / "preview_summary.json"
    summary["summary_path"] = str(summary_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


NTL_preview_tool = StructuredTool.from_function(
    func=run_annual_ntl_preview,
    name="NTL_preview_tool",
    description=(
        "Generate multi-year nighttime-light preview images from Earth Engine as PNG thumbnails and an optional GIF. "
        "Supports selectable year lists, annual datasets, configurable region boundaries, and service-side rendering."
    ),
    args_schema=AnnualNTLPreviewInput,
)
