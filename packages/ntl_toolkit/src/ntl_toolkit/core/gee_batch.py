from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from ntl_toolkit.core.gee_download import GeeDownloadRequest, _initialize_ee, _materialize_image
from ntl_toolkit.runtime.downloads import read_download_manifest, write_download_manifest
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult


class GeeBatchExportRequest(BaseModel):
    dataset_id: str
    bands: list[str] = Field(min_length=1)
    bbox: tuple[float, float, float, float]
    manifest_path: str
    description: str
    destination: Literal["drive", "cloud_storage", "asset"] = "drive"
    start_date: date | None = None
    end_date: date | None = None
    asset_type: Literal["auto", "Image", "ImageCollection"] = "auto"
    reducer: Literal["first", "mean", "median", "mosaic"] = "median"
    scale: int = Field(default=500, ge=1, le=10000)
    crs: str = "EPSG:4326"
    project: str | None = None
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
    index_bands: tuple[str, str] | None = None
    output_band_name: str = "index"
    file_name_prefix: str | None = None
    drive_folder: str | None = None
    bucket: str | None = None
    bucket_prefix: str | None = None
    asset_id: str | None = None
    max_pixels: int = Field(default=10_000_000_000_000, gt=0)

    @model_validator(mode="after")
    def validate_destination(self) -> "GeeBatchExportRequest":
        if self.destination == "cloud_storage" and not self.bucket:
            raise ValueError("cloud_storage destination requires bucket")
        if self.destination == "asset" and not self.asset_id:
            raise ValueError("asset destination requires asset_id")
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self


_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_export_name(value: str, *, fallback: str = "ntl_gee_export") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value or "").strip()).strip("_")
    return (cleaned or fallback)[:100]


def _status_progress(state: str) -> int:
    return {
        "UNSUBMITTED": 0,
        "READY": 5,
        "RUNNING": 50,
        "COMPLETED": 100,
        "FAILED": 100,
        "CANCEL_REQUESTED": 95,
        "CANCELLED": 100,
    }.get(state.upper(), 0)


def _load_export_manifest(manifest_path: str) -> tuple[Path, dict]:
    manifest = Path(manifest_path).expanduser().resolve()
    payload = read_download_manifest(manifest)
    if payload.get("schema") != "ntl.gee.export.v1" or not payload.get("task_id"):
        raise ValueError("Not a valid ntl.gee.export.v1 manifest")
    return manifest, payload


def _append_status_history(payload: dict, status: dict) -> None:
    state = str(status.get("state") or payload.get("state") or "UNKNOWN").upper()
    history = list(payload.get("history") or [])
    current = {
        "checked_at": _utc_now(),
        "state": state,
        "error_message": status.get("error_message"),
    }
    if not history or any(history[-1].get(key) != current.get(key) for key in ("state", "error_message")):
        history.append(current)
    payload["history"] = history[-50:]
    payload["state"] = state
    payload["status"] = status
    payload["progress_percent"] = _status_progress(state)
    payload["updated_at"] = current["checked_at"]


def submit_gee_batch_export(request: GeeBatchExportRequest) -> ToolResult:
    try:
        manifest = Path(request.manifest_path).expanduser().resolve()
        if manifest.exists():
            raise FileExistsError(f"Export manifest already exists: {manifest}")
        ee = _initialize_ee(request.project)
        materialization = GeeDownloadRequest(
            dataset_id=request.dataset_id,
            bands=request.bands,
            start_date=request.start_date,
            end_date=request.end_date,
            bbox=request.bbox,
            output="unused.tif",
            asset_type=request.asset_type,
            reducer=request.reducer,
            scale=request.scale,
            crs=request.crs,
            project=request.project,
            processing_preset=request.processing_preset,
            quality_threshold=request.quality_threshold,
            index_bands=request.index_bands,
            output_band_name=request.output_band_name,
        )
        image = _materialize_image(ee, materialization)
        region = ee.Geometry.Rectangle(list(request.bbox))
        description = _safe_export_name(request.description)
        prefix = _safe_export_name(request.file_name_prefix or description)
        common = {
            "image": image,
            "description": description,
            "region": region,
            "scale": request.scale,
            "crs": request.crs,
            "maxPixels": request.max_pixels,
        }
        if request.destination == "drive":
            kwargs = {**common, "fileNamePrefix": prefix, "fileFormat": "GeoTIFF"}
            if request.drive_folder:
                kwargs["folder"] = request.drive_folder
            task = ee.batch.Export.image.toDrive(**kwargs)
            destination_details = {"folder": request.drive_folder, "file_name_prefix": prefix}
        elif request.destination == "cloud_storage":
            object_prefix = request.bucket_prefix or prefix
            task = ee.batch.Export.image.toCloudStorage(
                **common,
                bucket=request.bucket,
                fileNamePrefix=object_prefix,
                fileFormat="GeoTIFF",
            )
            destination_details = {"bucket": request.bucket, "object_prefix": object_prefix}
        else:
            task = ee.batch.Export.image.toAsset(**common, assetId=request.asset_id)
            destination_details = {"asset_id": request.asset_id}

        task.start()
        status = task.status() or {}
        task_id = str(status.get("id") or getattr(task, "id", "") or "").strip()
        if not task_id:
            raise RuntimeError("Earth Engine did not return a task id after export submission")

        payload = {
            "schema": "ntl.gee.export.v1",
            "task_id": task_id,
            "state": str(status.get("state") or "READY").upper(),
            "description": description,
            "dataset_id": request.dataset_id,
            "bands": request.bands,
            "bbox": list(request.bbox),
            "start_date": request.start_date.isoformat() if request.start_date else None,
            "end_date": request.end_date.isoformat() if request.end_date else None,
            "asset_type": request.asset_type,
            "reducer": request.reducer,
            "scale": request.scale,
            "crs": request.crs,
            "processing_preset": request.processing_preset,
            "destination": request.destination,
            "destination_details": destination_details,
            "status": status,
            "created_at": _utc_now(),
            "history": [],
        }
        _append_status_history(payload, status)
        write_download_manifest(manifest, payload)
        return ToolResult.succeeded(
            tool="submit_gee_batch_export",
            summary=f"Submitted Earth Engine export task {task_id}.",
            outputs=[OutputArtifact(path=str(manifest), media_type="application/json", role="manifest")],
            metrics={
                "task_id": task_id,
                "state": payload["state"],
                "progress_percent": payload["progress_percent"],
                "destination": request.destination,
                "dataset_id": request.dataset_id,
            },
        ).model_copy(update={"job_id": task_id})
    except Exception as exc:  # noqa: BLE001
        return ToolResult.failed(
            tool="submit_gee_batch_export",
            error=ToolError(
                code="GEE_BATCH_SUBMIT_FAILED",
                message=str(exc) or type(exc).__name__,
                suggestion="Verify the validated plan, destination permissions, quota, AOI, bands, and export parameters.",
            ),
        )


def inspect_gee_batch_export(manifest_path: str, *, project: str | None = None) -> ToolResult:
    try:
        manifest, payload = _load_export_manifest(manifest_path)
        ee = _initialize_ee(project)
        statuses = ee.data.getTaskStatus(str(payload["task_id"]))
        status = statuses[0] if isinstance(statuses, list) and statuses else {}
        _append_status_history(payload, status)
        state = payload["state"]
        write_download_manifest(manifest, payload)
        warnings = []
        if state in {"FAILED", "CANCELLED"}:
            warnings.append(str(status.get("error_message") or f"Earth Engine task is {state}."))
        result = ToolResult.succeeded(
            tool="inspect_gee_batch_export",
            summary=f"Earth Engine export task {payload['task_id']} is {state}.",
            outputs=[OutputArtifact(path=str(manifest), media_type="application/json", role="manifest")],
            metrics={
                "task_id": payload["task_id"],
                "state": state,
                "terminal": state in _TERMINAL_STATES,
                "progress_percent": payload["progress_percent"],
                "destination": payload.get("destination"),
                "destination_details": payload.get("destination_details"),
                "error_message": status.get("error_message"),
            },
            warnings=warnings,
        )
        return result.model_copy(update={"job_id": str(payload["task_id"])})
    except Exception as exc:  # noqa: BLE001
        return ToolResult.failed(
            tool="inspect_gee_batch_export",
            error=ToolError(
                code="GEE_BATCH_STATUS_FAILED",
                message=str(exc) or type(exc).__name__,
                suggestion="Provide an existing ntl.gee.export.v1 manifest and verify Earth Engine initialization.",
            ),
        )


def cancel_gee_batch_export(manifest_path: str, *, project: str | None = None) -> ToolResult:
    try:
        manifest, payload = _load_export_manifest(manifest_path)
        ee = _initialize_ee(project)
        task_id = str(payload["task_id"])
        current = ee.data.getTaskStatus(task_id)
        status = current[0] if isinstance(current, list) and current else {}
        state = str(status.get("state") or payload.get("state") or "UNKNOWN").upper()
        warnings: list[str] = []
        if state in _TERMINAL_STATES:
            warnings.append(f"Task was already terminal ({state}); no cancellation was sent.")
        else:
            ee.data.cancelTask(task_id)
            status = {**status, "state": "CANCEL_REQUESTED"}
            state = "CANCEL_REQUESTED"
        _append_status_history(payload, status)
        write_download_manifest(manifest, payload)
        result = ToolResult.succeeded(
            tool="cancel_gee_batch_export",
            summary=f"Earth Engine export task {task_id} is {state}.",
            outputs=[OutputArtifact(path=str(manifest), media_type="application/json", role="manifest")],
            metrics={
                "task_id": task_id,
                "state": state,
                "terminal": state in _TERMINAL_STATES,
                "progress_percent": payload["progress_percent"],
            },
            warnings=warnings,
        )
        return result.model_copy(update={"job_id": task_id})
    except Exception as exc:  # noqa: BLE001
        return ToolResult.failed(
            tool="cancel_gee_batch_export",
            error=ToolError(
                code="GEE_BATCH_CANCEL_FAILED",
                message=str(exc) or type(exc).__name__,
                suggestion="Provide an existing export manifest and verify that the Earth Engine task can be cancelled.",
            ),
        )
