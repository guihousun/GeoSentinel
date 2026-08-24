from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from ntl_toolkit.core.gee_download import GeeDownloadRequest, download_gee_raster
from ntl_toolkit.core.gee_planning import estimate_bbox_area_sq_km
from storage_manager import current_thread_id, storage_manager


class GEERasterDownloadInput(BaseModel):
    dataset_id: str = Field(..., description="Exact, already validated Earth Engine Image or ImageCollection id.")
    bands: List[str] = Field(..., min_length=1, description="One or more validated source bands.")
    bbox: List[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="WGS84 bounds [minx,miny,maxx,maxy].",
    )
    out_name: str = Field(..., description="GeoTIFF filename written into the current thread inputs directory.")
    start_date: Optional[str] = Field(default=None, description="ImageCollection start date YYYY-MM-DD.")
    end_date: Optional[str] = Field(default=None, description="ImageCollection end date YYYY-MM-DD, inclusive.")
    asset_type: Literal["auto", "Image", "ImageCollection"] = "auto"
    reducer: Literal["first", "mean", "median", "mosaic"] = "first"
    scale: int = Field(default=500, ge=1, le=10000)
    crs: str = "EPSG:4326"
    processing_preset: Literal[
        "raw",
        "sentinel2_cloud_score_plus",
        "sentinel2_scl_mask",
        "landsat_c2_l2",
        "modis_vi_summary_qa",
        "normalized_difference",
        "sentinel2_cloud_score_plus_normalized_difference",
        "sentinel2_scl_mask_normalized_difference",
        "landsat_c2_l2_normalized_difference",
    ] = "raw"
    quality_threshold: float = Field(default=0.60, ge=0, le=1)
    index_bands: Optional[List[str]] = Field(default=None, min_length=2, max_length=2)
    output_band_name: str = "index"
    project: Optional[str] = None


def _resolve_thread_id(config: Optional[RunnableConfig]) -> str:
    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    if isinstance(runtime, dict):
        thread_id = str(storage_manager.get_thread_id_from_config(runtime) or "").strip()
        if thread_id:
            return thread_id
    return str(current_thread_id.get() or "debug").strip() or "debug"


def gee_raster_download(
    dataset_id: str,
    bands: List[str],
    bbox: List[float],
    out_name: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asset_type: str = "auto",
    reducer: str = "first",
    scale: int = 500,
    crs: str = "EPSG:4326",
    processing_preset: str = "raw",
    quality_threshold: float = 0.60,
    index_bands: Optional[List[str]] = None,
    output_band_name: str = "index",
    project: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> dict:
    try:
        thread_id = _resolve_thread_id(config)
        output_path = storage_manager.resolve_workspace_relative_path(
            out_name,
            thread_id,
            default_root="inputs",
            create_parent=True,
            allow_memory=False,
        )
        if Path(output_path).suffix.lower() not in {".tif", ".tiff"}:
            output_path = Path(output_path).with_suffix(".tif")

        bounds = tuple(float(value) for value in bbox)
        area_sq_km = estimate_bbox_area_sq_km(bounds)
        estimated_bytes = max(1, int(area_sq_km * 1_000_000 / (float(scale) ** 2))) * max(1, len(bands)) * 4
        quota = storage_manager.thread_quota_snapshot(thread_id, additional_bytes=estimated_bytes)
        if not bool(quota.get("allowed", True)):
            return {
                "status": "failed",
                "tool": "GEE_raster_download_tool",
                "error": {
                    "code": "THREAD_WORKSPACE_QUOTA_EXCEEDED",
                    "message": "The estimated GeoTIFF would exceed the current thread workspace quota.",
                    "suggestion": "Use a coarser scale, smaller AOI, fewer bands, or batch export destination.",
                },
                "metrics": {"estimated_bytes": estimated_bytes, "quota": quota},
            }

        request = GeeDownloadRequest(
            dataset_id=dataset_id,
            bands=bands,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
            bbox=bounds,
            output=str(output_path),
            asset_type=asset_type,
            reducer=reducer,
            scale=scale,
            crs=crs,
            project=project,
            processing_preset=processing_preset,
            quality_threshold=quality_threshold,
            index_bands=tuple(index_bands) if index_bands else None,
            output_band_name=output_band_name,
        )
        result = download_gee_raster(request)
        payload = result.model_dump(mode="json", by_alias=True)
        payload.setdefault("metrics", {})
        payload["metrics"].update(
            {
                "aoi_area_sq_km": round(area_sq_km, 3),
                "estimated_output_bytes": estimated_bytes,
                "thread_id": thread_id,
            }
        )
        return payload
    except Exception as exc:  # noqa: BLE001 - return an agent-safe structured failure
        return {
            "status": "failed",
            "tool": "GEE_raster_download_tool",
            "error": {
                "code": "INVALID_OR_FAILED_GEE_DOWNLOAD",
                "message": str(exc),
                "suggestion": "Re-run GEE_request_plan_tool and verify dataset metadata, AOI, dates, bands, and execution mode.",
            },
        }


GEE_raster_download_tool = StructuredTool.from_function(
    func=gee_raster_download,
    name="GEE_raster_download_tool",
    description=(
        "Execute only a validated direct_local plan for a general Earth Engine Image or ImageCollection. "
        "Writes a single- or multi-band GeoTIFF into the current thread inputs directory, enforces workspace "
        "quota, and supports controlled Sentinel-2, Landsat, MODIS, and normalized-difference presets."
    ),
    args_schema=GEERasterDownloadInput,
)
