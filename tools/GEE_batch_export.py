from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import re
from typing import List, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from ntl_toolkit.core.gee_batch import (
    GeeBatchExportRequest,
    cancel_gee_batch_export,
    inspect_gee_batch_export,
    submit_gee_batch_export,
)
from storage_manager import current_thread_id, storage_manager


_PROCESSING_PRESETS = Literal[
    "raw",
    "sentinel2_cloud_score_plus",
    "sentinel2_scl_mask",
    "landsat_c2_l2",
    "modis_vi_summary_qa",
    "normalized_difference",
    "sentinel2_cloud_score_plus_normalized_difference",
    "sentinel2_scl_mask_normalized_difference",
    "landsat_c2_l2_normalized_difference",
]


class GEEBatchExportInput(BaseModel):
    dataset_id: str = Field(..., description="Exact dataset id validated by GEE_request_plan_tool.")
    bands: List[str] = Field(..., min_length=1, description="Validated source bands.")
    bbox: List[float] = Field(..., min_length=4, max_length=4, description="WGS84 [minx,miny,maxx,maxy].")
    description: str = Field(..., description="Short export label; unsafe characters are normalized.")
    destination: Literal["drive", "cloud_storage", "asset"] = "drive"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    asset_type: Literal["auto", "Image", "ImageCollection"] = "auto"
    reducer: Literal["first", "mean", "median", "mosaic"] = "median"
    scale: int = Field(default=500, ge=1, le=10000)
    crs: str = "EPSG:4326"
    processing_preset: _PROCESSING_PRESETS = "raw"
    quality_threshold: float = Field(default=0.60, ge=0, le=1)
    index_bands: Optional[List[str]] = Field(default=None, min_length=2, max_length=2)
    output_band_name: str = "index"
    file_name_prefix: Optional[str] = None
    drive_folder: Optional[str] = None
    bucket: Optional[str] = None
    bucket_prefix: Optional[str] = None
    asset_id: Optional[str] = None
    manifest_name: Optional[str] = Field(
        default=None,
        description="Optional relative JSON path under current thread memory/gee_exports.",
    )
    project: Optional[str] = None


class GEEExportStatusInput(BaseModel):
    manifest_name: str = Field(..., description="Manifest returned by GEE_batch_export_tool, e.g. /memories/gee_exports/x.json.")
    project: Optional[str] = None


def _resolve_thread_id(config: Optional[RunnableConfig]) -> str:
    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    if isinstance(runtime, dict):
        thread_id = str(storage_manager.get_thread_id_from_config(runtime) or "").strip()
        if thread_id:
            return thread_id
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _manifest_path(thread_id: str, manifest_name: str) -> Path:
    logical = str(manifest_name or "").strip().replace("\\", "/")
    if logical.startswith("/memories/"):
        logical = logical.removeprefix("/memories/")
    if not logical:
        raise ValueError("manifest_name is required")
    if not logical.lower().endswith(".json"):
        logical += ".json"
    return storage_manager.resolve_workspace_relative_path(
        logical,
        thread_id,
        default_root="memory",
        create_parent=True,
        allow_memory=True,
    )


def _default_manifest_name(description: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", description.strip()).strip("_") or "gee_export"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"gee_exports/{stem[:60]}-{timestamp}.json"


def _agent_payload(result, thread_id: str, manifest: Path) -> dict:
    payload = result.model_dump(mode="json", by_alias=True)
    virtual_path = f"/memories/gee_exports/{manifest.name}"
    for output in payload.get("outputs", []):
        if output.get("role") == "manifest":
            output["path"] = virtual_path
    payload.setdefault("metrics", {})
    payload["metrics"].update({"thread_id": thread_id, "manifest": virtual_path})
    return payload


def gee_batch_export(
    dataset_id: str,
    bands: List[str],
    bbox: List[float],
    description: str,
    destination: str = "drive",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asset_type: str = "auto",
    reducer: str = "median",
    scale: int = 500,
    crs: str = "EPSG:4326",
    processing_preset: str = "raw",
    quality_threshold: float = 0.60,
    index_bands: Optional[List[str]] = None,
    output_band_name: str = "index",
    file_name_prefix: Optional[str] = None,
    drive_folder: Optional[str] = None,
    bucket: Optional[str] = None,
    bucket_prefix: Optional[str] = None,
    asset_id: Optional[str] = None,
    manifest_name: Optional[str] = None,
    project: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> dict:
    try:
        thread_id = _resolve_thread_id(config)
        manifest = _manifest_path(thread_id, manifest_name or _default_manifest_name(description))
        request = GeeBatchExportRequest(
            dataset_id=dataset_id,
            bands=bands,
            bbox=tuple(float(value) for value in bbox),
            manifest_path=str(manifest),
            description=description,
            destination=destination,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
            asset_type=asset_type,
            reducer=reducer,
            scale=scale,
            crs=crs,
            project=project,
            processing_preset=processing_preset,
            quality_threshold=quality_threshold,
            index_bands=tuple(index_bands) if index_bands else None,
            output_band_name=output_band_name,
            file_name_prefix=file_name_prefix,
            drive_folder=drive_folder,
            bucket=bucket,
            bucket_prefix=bucket_prefix,
            asset_id=asset_id,
        )
        return _agent_payload(submit_gee_batch_export(request), thread_id, manifest)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "tool": "GEE_batch_export_tool",
            "summary": str(exc),
            "error": {
                "code": "INVALID_OR_FAILED_GEE_BATCH_EXPORT",
                "message": str(exc),
                "suggestion": "Re-run GEE_request_plan_tool and verify destination permissions and export parameters.",
            },
        }


def gee_export_status(
    manifest_name: str,
    project: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> dict:
    try:
        thread_id = _resolve_thread_id(config)
        manifest = _manifest_path(thread_id, manifest_name)
        return _agent_payload(inspect_gee_batch_export(str(manifest), project=project), thread_id, manifest)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "tool": "GEE_export_status_tool",
            "summary": str(exc),
            "error": {"code": "INVALID_GEE_EXPORT_MANIFEST", "message": str(exc)},
        }


def gee_export_cancel(
    manifest_name: str,
    project: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> dict:
    try:
        thread_id = _resolve_thread_id(config)
        manifest = _manifest_path(thread_id, manifest_name)
        return _agent_payload(cancel_gee_batch_export(str(manifest), project=project), thread_id, manifest)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "tool": "GEE_export_cancel_tool",
            "summary": str(exc),
            "error": {"code": "INVALID_GEE_EXPORT_MANIFEST", "message": str(exc)},
        }


GEE_batch_export_tool = StructuredTool.from_function(
    func=gee_batch_export,
    name="GEE_batch_export_tool",
    description=(
        "Execute only a validated batch_export plan. Submits a large raster export to Google Drive, "
        "Cloud Storage, or an Earth Engine asset and returns a recoverable /memories manifest plus task id."
    ),
    args_schema=GEEBatchExportInput,
)

GEE_export_status_tool = StructuredTool.from_function(
    func=gee_export_status,
    name="GEE_export_status_tool",
    description="Refresh the live state, progress, destination, and failure reason for a GEE batch export manifest.",
    args_schema=GEEExportStatusInput,
)

GEE_export_cancel_tool = StructuredTool.from_function(
    func=gee_export_cancel,
    name="GEE_export_cancel_tool",
    description="Cancel a non-terminal GEE batch export using its current-thread manifest.",
    args_schema=GEEExportStatusInput,
)
