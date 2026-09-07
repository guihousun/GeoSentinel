"""Google Earth Engine implementation of VNP46A2 16-day angle correction.

All raster filtering, quality masking, group means, correction, stacking, and
regional statistics are evaluated in Earth Engine. The client receives only
small provenance/statistics records and, when explicitly requested, one remote
output asset; daily source rasters are never downloaded to the local workspace.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, field_validator

from storage_manager import storage_manager
from gee_runtime import initialize_ee, resolve_gee_boundary_asset_project_id


DATASET_ID = "NASA/VIIRS/002/VNP46A2"
RADIANCE_BAND = "DNB_BRDF_Corrected_NTL"
QA_BANDS = ["Mandatory_Quality_Flag", "QF_Cloud_Mask", "Snow_Flag"]
DEFAULT_BOUNDARY_ASSET = ""
METHOD_NAME = "fixed-anchor 16-day group-mean angle-effect correction"
STATISTICS_BATCH_SIZE = 8


class VNP46A2AngularCorrectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    study_area: str = Field("上海市", description="Exact boundary-name value, normally 上海市.")
    start_date: str = Field("2024-01-01", description="Inclusive analysis start date (YYYY-MM-DD).")
    end_date: str = Field("2025-01-01", description="Exclusive analysis end date (YYYY-MM-DD).")
    anchor_date: str = Field(
        "2024-01-01",
        description="Fixed phase anchor for group = days_since_anchor mod 16.",
    )
    group_period_days: Literal[16] = Field(16, description="Fixed 16-day grouping period.")
    boundary_asset_id: str = Field(
        DEFAULT_BOUNDARY_ASSET,
        description=(
            "Earth Engine FeatureCollection containing the study-area boundary. "
            "When empty, use the system-configured boundary asset project."
        ),
    )
    boundary_name_field: str = Field("name", description="Boundary attribute matched to study_area.")
    output_asset_id: str = Field(
        "",
        description=(
            "Optional full Earth Engine image asset id for a persistent corrected daily "
            "multiband stack. Leave empty when compact corrected daily statistics and "
            "metadata are the requested outputs."
        ),
    )
    output_statistics_csv: str = Field(
        "vnp46a2_angular_correction_daily_statistics.csv",
        description="Small daily statistics table written under workspace outputs/.",
    )
    output_metadata_json: str = Field(
        "vnp46a2_angular_correction.metadata.json",
        description="Run, source, QA, export, and provenance metadata written under outputs/.",
    )
    scale_m: Literal[500] = Field(500, description="Fixed VNP46A2 export scale in metres.")
    crs: Literal["EPSG:4326"] = Field("EPSG:4326", description="Fixed output CRS.")
    wait_for_completion: bool = Field(
        True,
        description="Wait for the single remote asset export to reach a terminal state.",
    )
    max_wait_seconds: int = Field(3600, ge=60, le=14400)
    poll_seconds: int = Field(10, ge=2, le=60)

    @field_validator("start_date", "end_date", "anchor_date")
    @classmethod
    def _iso_date(cls, value: str) -> str:
        date.fromisoformat(value)
        return value

    @field_validator("output_asset_id")
    @classmethod
    def _asset_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            return ""
        if not re.fullmatch(r"projects/[^/]+/assets/[A-Za-z0-9_./-]+", normalized):
            raise ValueError("output_asset_id must be a full projects/<project>/assets/<name> id")
        return normalized


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _qa_rule() -> dict[str, Any]:
    return {
        "radiance_band": RADIANCE_BAND,
        "use_gap_filled_band": False,
        "Mandatory_Quality_Flag": [0, 1],
        "QF_Cloud_Mask_bit_0_day_night": 0,
        "QF_Cloud_Mask_bits_4_5_minimum_quality": 2,
        "QF_Cloud_Mask_bits_6_7_allowed_cloud_classes": [0, 1],
        "QF_Cloud_Mask_bit_8_shadow": 0,
        "QF_Cloud_Mask_bit_9_cirrus": 0,
        "Snow_Flag": 0,
        "zero_radiance_is_valid_before_division_checks": True,
    }


def _validate_dates(start_date: str, end_date: str, anchor_date: str) -> None:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    anchor = date.fromisoformat(anchor_date)
    if end <= start:
        raise ValueError("end_date must be later than start_date")
    if anchor > start:
        raise ValueError("anchor_date must be on or before start_date")


def _load_ee(project: Optional[str]):
    import ee  # imported lazily so registry discovery does not initialize Earth Engine

    initialize_ee(explicit_project_id=project, ee_module=ee)
    return ee


def _build_gee_workflow(
    ee,
    *,
    study_area: str,
    start_date: str,
    end_date: str,
    anchor_date: str,
    boundary_asset_id: str,
    boundary_name_field: str,
):
    boundary_rows = ee.FeatureCollection(boundary_asset_id).filter(
        ee.Filter.eq(boundary_name_field, study_area)
    )
    boundary_count = int(boundary_rows.size().getInfo())
    if boundary_count != 1:
        raise ValueError(
            f"expected exactly one boundary feature for {study_area!r}, found {boundary_count}"
        )
    region = boundary_rows.geometry()
    anchor = ee.Date(anchor_date)

    source = (
        ee.ImageCollection(DATASET_ID)
        .filterDate(start_date, end_date)
        .filterBounds(region)
        .sort("system:time_start")
    )
    source_count = int(source.size().getInfo())
    if source_count == 0:
        raise ValueError("VNP46A2 source collection is empty for the requested AOI/date range")

    def prepare(image):
        image = ee.Image(image)
        mandatory = image.select("Mandatory_Quality_Flag")
        cloud = image.select("QF_Cloud_Mask").toInt()
        snow = image.select("Snow_Flag")
        qa_mask = (
            mandatory.lte(1)
            .And(cloud.bitwiseAnd(1).eq(0))
            .And(cloud.rightShift(4).bitwiseAnd(3).gte(2))
            .And(cloud.rightShift(6).bitwiseAnd(3).lte(1))
            .And(cloud.rightShift(8).bitwiseAnd(1).eq(0))
            .And(cloud.rightShift(9).bitwiseAnd(1).eq(0))
            .And(snow.eq(0))
        )
        image_date = ee.Date(image.get("system:time_start"))
        group_number = image_date.difference(anchor, "day").mod(16).int()
        raw = image.select(RADIANCE_BAND).updateMask(qa_mask).rename("raw_ntl")
        return raw.copyProperties(image, image.propertyNames()).set(
            {
                "group_number": group_number,
                "observation_date": image_date.format("YYYY-MM-dd"),
                "source_image_id": image.get("system:index"),
            }
        )

    prepared = source.map(prepare)
    annual_mean = prepared.select("raw_ntl").mean().rename("annual_mean_ntl")
    group_numbers = ee.List.sequence(0, 15)

    def group_coefficient(group_number):
        group_number = ee.Number(group_number).int()
        group_mean = (
            prepared.filter(ee.Filter.eq("group_number", group_number))
            .select("raw_ntl")
            .mean()
            .rename("group_mean_ntl")
        )
        valid_denominator = annual_mean.gt(0).And(group_mean.gt(0))
        coefficient = group_mean.divide(annual_mean).updateMask(valid_denominator).rename(
            "angle_coefficient"
        )
        return coefficient.set("group_number", group_number)

    coefficients = ee.ImageCollection.fromImages(group_numbers.map(group_coefficient))

    def correct(image):
        image = ee.Image(image)
        coefficient = ee.Image(
            coefficients.filter(ee.Filter.eq("group_number", image.get("group_number"))).first()
        )
        corrected = image.select("raw_ntl").divide(coefficient).rename("corrected_ntl")
        return image.addBands(corrected).copyProperties(image, image.propertyNames())

    corrected = prepared.map(correct).sort("system:time_start")

    dates = corrected.aggregate_array("observation_date").getInfo()
    source_ids = corrected.aggregate_array("source_image_id").getInfo()
    if len(dates) != source_count or len(source_ids) != source_count:
        raise ValueError("source provenance arrays do not match the source collection size")
    if len(dates) != len(set(dates)):
        raise ValueError("more than one VNP46A2 image was found for at least one observation date")
    band_names = [f"ntl_{value.replace('-', '')}" for value in dates]
    stack = corrected.select("corrected_ntl").toBands().rename(band_names).clip(region)
    stack = stack.set(
        {
            "dataset_id": DATASET_ID,
            "radiance_band": RADIANCE_BAND,
            "analysis_start_date": start_date,
            "analysis_end_date_exclusive": end_date,
            "group_anchor_date": anchor_date,
            "group_period_days": 16,
            "qa_rule_sha256": _canonical_sha256(_qa_rule()),
            "source_image_count": source_count,
            "method": METHOD_NAME,
        }
    )
    return {
        "region": region,
        "stack": stack,
        "corrected": corrected,
        "dates": dates,
        "source_image_ids": source_ids,
        "source_image_count": source_count,
        "boundary_count": boundary_count,
    }


def _statistics_batch_ranges(total: int, batch_size: int = STATISTICS_BATCH_SIZE) -> list[tuple[int, int]]:
    if total < 0:
        raise ValueError("total must be non-negative")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [(offset, min(batch_size, total - offset)) for offset in range(0, total, batch_size)]


def _statistics_rows_batched(
    ee,
    *,
    corrected,
    region,
    source_count: int,
    batch_size: int = STATISTICS_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """Fetch daily reducers in small requests to avoid concurrent-aggregation quotas."""

    def daily_stats(image):
        image = ee.Image(image)
        values = image.select(["raw_ntl", "corrected_ntl"]).reduceRegion(
            reducer=ee.Reducer.count().combine(ee.Reducer.mean(), sharedInputs=True),
            geometry=region,
            scale=500,
            maxPixels=10**10,
            bestEffort=False,
            tileScale=4,
        )
        return ee.Feature(
            None,
            {
                "date": image.get("observation_date"),
                "source_image_id": image.get("source_image_id"),
                "group_number": image.get("group_number"),
                "valid_pixels": values.get("corrected_ntl_count"),
                "mean_before": values.get("raw_ntl_mean"),
                "mean_after": values.get("corrected_ntl_mean"),
            },
        )

    rows: list[dict[str, Any]] = []
    corrected_list = corrected.toList(source_count)
    for offset, count in _statistics_batch_ranges(source_count, batch_size):
        batch = corrected_list.slice(offset, offset + count)
        features = ee.FeatureCollection(batch.map(lambda image: daily_stats(ee.Image(image))))
        payload = features.getInfo()
        batch_rows = [
            dict(feature.get("properties") or {}) for feature in payload.get("features", [])
        ]
        if len(batch_rows) != count:
            raise ValueError(
                f"daily statistics batch {offset}:{offset + count} returned {len(batch_rows)} rows"
            )
        rows.extend(batch_rows)
    rows = sorted(rows, key=lambda row: str(row.get("date") or ""))
    if len(rows) != source_count:
        raise ValueError(f"daily statistics returned {len(rows)} rows for {source_count} sources")
    return rows


def _write_statistics(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["date", "source_image_id", "group_number", "valid_pixels", "mean_before", "mean_after"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def _wait_for_export(task, *, max_wait_seconds: int, poll_seconds: int) -> dict[str, Any]:
    started = time.monotonic()
    while True:
        status = dict(task.status() or {})
        state = str(status.get("state") or "UNKNOWN").upper()
        if state in {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}:
            return status
        if time.monotonic() - started >= max_wait_seconds:
            return {**status, "state": "TIMED_OUT", "waited_seconds": max_wait_seconds}
        time.sleep(poll_seconds)


def run_vnp46a2_angular_correction(
    study_area: str = "上海市",
    start_date: str = "2024-01-01",
    end_date: str = "2025-01-01",
    anchor_date: str = "2024-01-01",
    group_period_days: int = 16,
    boundary_asset_id: str = DEFAULT_BOUNDARY_ASSET,
    boundary_name_field: str = "name",
    output_asset_id: str = "",
    output_statistics_csv: str = "vnp46a2_angular_correction_daily_statistics.csv",
    output_metadata_json: str = "vnp46a2_angular_correction.metadata.json",
    scale_m: int = 500,
    crs: str = "EPSG:4326",
    project: Optional[str] = None,
    wait_for_completion: bool = True,
    max_wait_seconds: int = 3600,
    poll_seconds: int = 10,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    """Run the fixed 2024 GEE-side correction and export one remote stack."""

    thread_id = storage_manager.get_thread_id_from_config(config) if config else None
    statistics_path = Path(storage_manager.resolve_output_path(output_statistics_csv, thread_id))
    metadata_path = Path(storage_manager.resolve_output_path(output_metadata_json, thread_id))
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        _validate_dates(start_date, end_date, anchor_date)
        if group_period_days != 16 or scale_m != 500 or crs != "EPSG:4326":
            raise ValueError("group_period_days, scale_m, and crs are fixed at 16, 500, and EPSG:4326")
        if output_asset_id and not re.fullmatch(
            r"projects/[^/]+/assets/[A-Za-z0-9_./-]+", output_asset_id
        ):
            raise ValueError("output_asset_id must be a full projects/<project>/assets/<name> id")

        ee = _load_ee(project)
        if not boundary_asset_id:
            boundary_asset_id = (
                f"projects/{resolve_gee_boundary_asset_project_id()}/assets/province"
            )
        workflow = _build_gee_workflow(
            ee,
            study_area=study_area,
            start_date=start_date,
            end_date=end_date,
            anchor_date=anchor_date,
            boundary_asset_id=boundary_asset_id,
            boundary_name_field=boundary_name_field,
        )
        statistics = _statistics_rows_batched(
            ee,
            corrected=workflow["corrected"],
            region=workflow["region"],
            source_count=workflow["source_image_count"],
        )
        _write_statistics(statistics_path, statistics)

        export_task = None
        if output_asset_id:
            export_task = ee.batch.Export.image.toAsset(
                image=workflow["stack"],
                description=f"VNP46A2_angle_corrected_{start_date}_{end_date}",
                assetId=output_asset_id,
                region=workflow["region"],
                scale=scale_m,
                crs=crs,
                maxPixels=10**13,
            )
            export_task.start()
            export_status = (
                _wait_for_export(
                    export_task,
                    max_wait_seconds=max_wait_seconds,
                    poll_seconds=poll_seconds,
                )
                if wait_for_completion
                else dict(export_task.status() or {})
            )
            state = str(export_status.get("state") or "SUBMITTED").upper()
        else:
            export_status = {
                "state": "NOT_REQUESTED",
                "reason": "compact daily statistics and metadata were requested without a remote asset",
            }
            state = "NOT_REQUESTED"
        remote_asset_metadata = None
        if state == "COMPLETED" and output_asset_id:
            remote_asset_metadata = ee.data.getAsset(output_asset_id)

        config_record = {
            "dataset_id": DATASET_ID,
            "radiance_band": RADIANCE_BAND,
            "qa_rule": _qa_rule(),
            "study_area": study_area,
            "boundary_asset_id": boundary_asset_id,
            "boundary_name_field": boundary_name_field,
            "start_date": start_date,
            "end_date_exclusive": end_date,
            "anchor_date": anchor_date,
            "group_period_days": 16,
            "formula": "corrected = raw / (group_mean / annual_mean)",
            "scale_m": 500,
            "crs": "EPSG:4326",
            "output_asset_id": output_asset_id,
            "statistics_batch_size": STATISTICS_BATCH_SIZE,
        }
        metadata = {
            "schema": "ntl_gpt.vnp46a2_angular_correction.gee.v1",
            "status": "success" if state in {"COMPLETED", "NOT_REQUESTED"} else "submitted" if state not in {"FAILED", "CANCELLED", "TIMED_OUT"} else "error",
            "started_at_utc": started_at,
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "method": METHOD_NAME,
            "configuration": config_record,
            "configuration_sha256": _canonical_sha256(config_record),
            "implementation_file_sha256": _sha256(Path(__file__)),
            "source": {
                "image_count": workflow["source_image_count"],
                "image_ids": workflow["source_image_ids"],
                "observation_dates": workflow["dates"],
            },
            "statistics": {
                "row_count": len(statistics),
                "path": str(statistics_path),
                "sha256": _sha256(statistics_path),
            },
            "export": {
                "task_id": getattr(export_task, "id", None),
                "state": state,
                "status": export_status,
                "asset_id": output_asset_id or None,
                "asset_metadata": remote_asset_metadata,
                "asset_metadata_sha256": _canonical_sha256(remote_asset_metadata) if remote_asset_metadata else None,
                "pixel_checksum_status": "requires post-export frozen reference validation",
            },
            "local_raster_download_performed": False,
            "intermediate_raster_export_performed": False,
        }
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        metadata["metadata_path"] = str(metadata_path)
        metadata["metadata_sha256"] = _sha256(metadata_path)
        return metadata
    except Exception as exc:  # noqa: BLE001
        failure = {
            "schema": "ntl_gpt.vnp46a2_angular_correction.gee.v1",
            "status": "error",
            "started_at_utc": started_at,
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "local_raster_download_performed": False,
        }
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return failure


VNP46A2_angular_correction_tool = StructuredTool.from_function(
    func=lambda **kwargs: run_vnp46a2_angular_correction(project=None, **kwargs),
    name="VNP46A2_angular_correction_tool",
    description=(
        "Apply the fixed-anchor 16-day group-mean angle-effect correction to a VNP46A2 daily series entirely "
        "in Google Earth Engine. The tool uses the direct BRDF-corrected radiance band with explicit Collection 2 "
        "QA and writes compact corrected daily statistics and metadata locally; it exports a remote multiband asset only "
        "when an explicit output_asset_id is requested."
    ),
    args_schema=VNP46A2AngularCorrectionInput,
)


__all__ = [
    "VNP46A2AngularCorrectionInput",
    "VNP46A2_angular_correction_tool",
    "run_vnp46a2_angular_correction",
]
