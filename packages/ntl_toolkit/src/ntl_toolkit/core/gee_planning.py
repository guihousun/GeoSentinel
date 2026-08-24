from __future__ import annotations

import math
import re
from datetime import date
from typing import Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator


AssetType = Literal["Image", "ImageCollection", "FeatureCollection", "unknown"]
ExecutionMode = Literal[
    "direct_local",
    "server_reduce",
    "batch_export",
    "official_earthdata",
    "needs_input",
]


NTL_DATASET_IDS = frozenset(
    {
        "projects/sat-io/open-datasets/npp-viirs-ntl",
        "NOAA/VIIRS/DNB/ANNUAL_V21",
        "NOAA/VIIRS/DNB/ANNUAL_V22",
        "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
        "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG",
        "NASA/VIIRS/002/VNP46A2",
        "NOAA/VIIRS/001/VNP46A1",
        "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS",
        "NOAA/DMSP-OLS/CALIBRATED_LIGHTS_V4",
    }
)

_NTL_MARKERS = (
    "nighttime light",
    "night-time light",
    "night light",
    "nightlight",
    "viirs dnb",
    "vnp46a1",
    "vnp46a2",
    "dmsp-ols",
    "dmsp ols",
    "ntl",
    "夜间灯光",
    "夜光",
)


class GeeRequest(BaseModel):
    """Normalized user intent used by both LangChain and MCP adapters."""

    model_config = ConfigDict(str_strip_whitespace=True)

    query: str
    dataset_id: str | None = None
    dataset_name: str | None = None
    bands: list[str] = Field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    bbox: tuple[float, float, float, float] | None = None
    aoi_area_sq_km: float | None = Field(default=None, gt=0)
    temporal_resolution: Literal["static", "annual", "monthly", "daily", "subdaily", "unknown"] = "unknown"
    output_kind: Literal["raster", "table", "vector", "map"] = "raster"
    analysis_kind: Literal["download", "composite", "statistics", "time_series", "classification"] = "download"
    reducer: str | None = None
    scale_m: float | None = Field(default=None, gt=0)
    destination: Literal["local", "drive", "cloud_storage", "asset", "unspecified"] = "local"
    prefer_official: bool = True
    require_official_hdf5: bool = False
    processing_preset: str | None = None

    @model_validator(mode="after")
    def validate_ranges(self) -> "GeeRequest":
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if self.bbox is not None:
            minx, miny, maxx, maxy = self.bbox
            if not -180 <= minx < maxx <= 180:
                raise ValueError("bbox longitudes must satisfy -180 <= minx < maxx <= 180")
            if not -90 <= miny < maxy <= 90:
                raise ValueError("bbox latitudes must satisfy -90 <= miny < maxy <= 90")
        return self


class DatasetCandidate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    dataset_id: str
    title: str = ""
    asset_type: AssetType = "unknown"
    bands: list[str] = Field(default_factory=list)
    default_bands: list[str] = Field(default_factory=list)
    scale_m: float | None = Field(default=None, gt=0)
    temporal_resolution: Literal["static", "annual", "monthly", "daily", "subdaily", "unknown"] = "unknown"
    temporal_start: date | None = None
    temporal_end: date | None = None
    source: Literal["explicit", "ntl_registry", "curated", "official_catalog", "community_catalog", "live_metadata"] = "curated"
    official: bool = False
    live_checked: bool = False
    collection_size: int | None = Field(default=None, ge=0)
    qa_preset: str | None = None
    processing_presets: list[str] = Field(default_factory=list)
    score: float = 0.0
    warnings: list[str] = Field(default_factory=list)


class DatasetValidation(BaseModel):
    status: Literal["verified", "partial", "unverified", "unavailable", "invalid"]
    live_checked: bool = False
    coverage_status: Literal["supported", "outside_known_range", "unknown"] = "unknown"
    missing_fields: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DatasetPlan(BaseModel):
    domain: Literal["ntl", "general_gee"]
    selected: DatasetCandidate | None = None
    alternatives: list[DatasetCandidate] = Field(default_factory=list)
    validation: DatasetValidation
    selection_reasons: list[str] = Field(default_factory=list)


class ExecutionEstimate(BaseModel):
    aoi_area_sq_km: float | None = None
    estimated_images: int | None = None
    estimated_output_pixels: int | None = None
    estimated_source_pixel_reads: int | None = None
    estimated_output_bytes: int | None = None


class ExecutionPlan(BaseModel):
    mode: ExecutionMode
    reason_codes: list[str] = Field(default_factory=list)
    estimate: ExecutionEstimate = Field(default_factory=ExecutionEstimate)
    fallback_modes: list[ExecutionMode] = Field(default_factory=list)
    required_inputs: list[str] = Field(default_factory=list)
    safeguards: list[str] = Field(default_factory=list)


class GeePlan(BaseModel):
    schema_: Literal["ntl.gee.plan.v1"] = Field(default="ntl.gee.plan.v1", alias="schema")
    request: GeeRequest
    dataset: DatasetPlan
    execution: ExecutionPlan

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


class PlannerPolicy(BaseModel):
    """Configurable safety limits; callers may override after environment probing."""

    direct_max_output_pixels: int = Field(default=25_000_000, gt=0)
    direct_max_estimated_bytes: int = Field(default=96_000_000, gt=0)
    direct_max_source_images: int = Field(default=32, gt=0)
    bytes_per_pixel: int = Field(default=4, gt=0)
    require_live_metadata_for_general: bool = True


def classify_request_domain(request: GeeRequest) -> Literal["ntl", "general_gee"]:
    if request.dataset_id:
        return "ntl" if request.dataset_id in NTL_DATASET_IDS else "general_gee"
    haystack = f"{request.query} {request.dataset_name or ''}".lower()
    return "ntl" if any(marker in haystack for marker in _NTL_MARKERS) else "general_gee"


def estimate_bbox_area_sq_km(bbox: tuple[float, float, float, float]) -> float:
    """Approximate WGS84 rectangle area using a spherical Earth."""

    minx, miny, maxx, maxy = bbox
    earth_radius_km = 6371.0088
    lon_span = math.radians(maxx - minx)
    lat_term = abs(math.sin(math.radians(maxy)) - math.sin(math.radians(miny)))
    return earth_radius_km**2 * lon_span * lat_term


def estimate_image_count(request: GeeRequest, candidate: DatasetCandidate | None) -> int | None:
    if candidate and candidate.asset_type == "Image":
        return 1
    if request.temporal_resolution == "static" or (candidate and candidate.temporal_resolution == "static"):
        return 1
    if not request.start_date or not request.end_date:
        return candidate.collection_size if candidate and candidate.collection_size == 1 else None

    cadence = request.temporal_resolution
    if cadence == "unknown" and candidate:
        cadence = candidate.temporal_resolution
    if cadence == "daily":
        return (request.end_date - request.start_date).days + 1
    if cadence == "monthly":
        return (
            (request.end_date.year - request.start_date.year) * 12
            + request.end_date.month
            - request.start_date.month
            + 1
        )
    if cadence == "annual":
        return request.end_date.year - request.start_date.year + 1
    return None


def validate_candidate(request: GeeRequest, candidate: DatasetCandidate | None) -> DatasetValidation:
    if candidate is None:
        return DatasetValidation(
            status="invalid",
            missing_fields=["dataset"],
            warnings=["No dataset candidate could be selected."],
        )

    missing: list[str] = []
    warnings = list(candidate.warnings)
    if candidate.asset_type == "unknown":
        missing.append("asset_type")
    if request.output_kind == "raster" and not (request.bands or candidate.default_bands or candidate.bands):
        missing.append("bands")

    coverage = "unknown"
    if request.start_date and request.end_date and candidate.temporal_start and candidate.temporal_end:
        if request.start_date >= candidate.temporal_start and request.end_date <= candidate.temporal_end:
            coverage = "supported"
        else:
            coverage = "outside_known_range"
            warnings.append("Requested dates fall outside the candidate's known temporal range.")

    if candidate.collection_size == 0:
        return DatasetValidation(
            status="unavailable",
            live_checked=candidate.live_checked,
            coverage_status=coverage,
            missing_fields=missing,
            warnings=warnings + ["The live collection probe returned zero images."],
        )
    if missing:
        status = "partial" if candidate.dataset_id else "invalid"
    elif candidate.live_checked:
        status = "verified"
    else:
        status = "unverified"
    return DatasetValidation(
        status=status,
        live_checked=candidate.live_checked,
        coverage_status=coverage,
        missing_fields=missing,
        warnings=warnings,
    )


def rank_candidates(request: GeeRequest, candidates: Sequence[DatasetCandidate]) -> list[DatasetCandidate]:
    """Rank deterministically; an explicit dataset id is never silently replaced."""

    if request.dataset_id:
        exact = [candidate for candidate in candidates if candidate.dataset_id == request.dataset_id]
        if exact:
            return exact + [candidate for candidate in candidates if candidate.dataset_id != request.dataset_id]
        return [
            DatasetCandidate(
                dataset_id=request.dataset_id,
                title=request.dataset_name or request.dataset_id,
                bands=list(request.bands),
                default_bands=list(request.bands),
                scale_m=request.scale_m,
                temporal_resolution=request.temporal_resolution,
                source="explicit",
                official=request.prefer_official,
                warnings=["Explicit dataset id has not yet been validated against Earth Engine metadata."],
            ),
            *candidates,
        ]

    query_terms = set(re.findall(r"[a-z0-9_+-]+|[\u4e00-\u9fff]+", request.query.lower()))

    def key(candidate: DatasetCandidate) -> tuple[float, int, int, int]:
        title_terms = set(re.findall(r"[a-z0-9_+-]+|[\u4e00-\u9fff]+", candidate.title.lower()))
        overlap = len(query_terms & title_terms)
        official_bonus = 1 if candidate.official else 0
        live_bonus = 1 if candidate.live_checked else 0
        usable_band_bonus = 1 if candidate.default_bands or candidate.bands else 0
        return candidate.score + overlap * 3.0, official_bonus, live_bonus, usable_band_bonus

    return sorted(candidates, key=key, reverse=True)


def choose_execution_plan(
    request: GeeRequest,
    dataset_plan: DatasetPlan,
    policy: PlannerPolicy | None = None,
) -> ExecutionPlan:
    policy = policy or PlannerPolicy()
    candidate = dataset_plan.selected
    area = request.aoi_area_sq_km
    if area is None and request.bbox is not None:
        area = estimate_bbox_area_sq_km(request.bbox)

    image_count = estimate_image_count(request, candidate)
    scale = request.scale_m or (candidate.scale_m if candidate else None)
    bands = request.bands or (candidate.default_bands if candidate else []) or (candidate.bands if candidate else [])
    band_count = max(1, len(bands))
    output_pixels: int | None = None
    source_reads: int | None = None
    output_bytes: int | None = None
    if area and scale:
        pixels_per_band = max(1, math.ceil(area * 1_000_000 / (scale**2)))
        output_pixels = pixels_per_band * band_count
        source_reads = output_pixels * max(1, image_count or 1)
        output_bytes = output_pixels * policy.bytes_per_pixel

    estimate = ExecutionEstimate(
        aoi_area_sq_km=round(area, 3) if area else None,
        estimated_images=image_count,
        estimated_output_pixels=output_pixels,
        estimated_source_pixel_reads=source_reads,
        estimated_output_bytes=output_bytes,
    )

    required: list[str] = []
    if candidate is None:
        required.append("dataset_id_or_searchable_dataset_intent")
    if request.output_kind in {"raster", "table", "vector"} and request.bbox is None and area is None:
        required.append("aoi")
    if candidate and candidate.asset_type == "ImageCollection" and not request.start_date:
        required.append("start_date")
    if candidate and candidate.asset_type == "ImageCollection" and not request.end_date:
        required.append("end_date")
    if dataset_plan.validation.status in {"invalid", "unavailable"}:
        required.extend(dataset_plan.validation.missing_fields)
    if required:
        return ExecutionPlan(
            mode="needs_input",
            reason_codes=["MISSING_OR_INVALID_EXECUTION_INPUT"],
            estimate=estimate,
            required_inputs=list(dict.fromkeys(required)),
            safeguards=["Do not execute until dataset and AOI validation succeeds."],
        )

    if request.require_official_hdf5 and dataset_plan.domain == "ntl":
        return ExecutionPlan(
            mode="official_earthdata",
            reason_codes=["OFFICIAL_NTL_PROVENANCE_REQUIRED"],
            estimate=estimate,
            fallback_modes=["batch_export"],
            safeguards=["Preserve official granule audit and exact retry targets."],
        )

    if request.output_kind in {"table", "vector"} or request.analysis_kind in {"statistics", "time_series"}:
        return ExecutionPlan(
            mode="server_reduce",
            reason_codes=["SERVER_SIDE_ANALYTICS_PREFERRED"],
            estimate=estimate,
            fallback_modes=["batch_export"],
            safeguards=["Return compact tables or vectors instead of downloading source rasters."],
        )

    if request.destination in {"drive", "cloud_storage", "asset"}:
        return ExecutionPlan(
            mode="batch_export",
            reason_codes=["EXPLICIT_BATCH_DESTINATION"],
            estimate=estimate,
            fallback_modes=["server_reduce"],
            safeguards=["Track the Earth Engine task id and destination manifest."],
        )

    general_requires_live = (
        dataset_plan.domain == "general_gee"
        and policy.require_live_metadata_for_general
        and not dataset_plan.validation.live_checked
    )
    if general_requires_live:
        return ExecutionPlan(
            mode="needs_input",
            reason_codes=["LIVE_METADATA_REQUIRED_FOR_GENERAL_DATASET"],
            estimate=estimate,
            required_inputs=["live_dataset_metadata"],
            safeguards=["Never execute an arbitrary catalog hit without live asset and band validation."],
        )

    direct_fits = (
        output_pixels is not None
        and output_bytes is not None
        and output_pixels <= policy.direct_max_output_pixels
        and output_bytes <= policy.direct_max_estimated_bytes
        and (image_count is None or image_count <= policy.direct_max_source_images)
    )
    if direct_fits:
        return ExecutionPlan(
            mode="direct_local",
            reason_codes=["LOCAL_EXPORT_WITHIN_POLICY"],
            estimate=estimate,
            fallback_modes=["batch_export", "server_reduce"],
            safeguards=["Validate the resulting raster dimensions, CRS, bands, and non-empty pixels."],
        )

    return ExecutionPlan(
        mode="batch_export",
        reason_codes=["LOCAL_EXPORT_SIZE_UNKNOWN_OR_EXCEEDS_POLICY"],
        estimate=estimate,
        fallback_modes=["server_reduce"],
        safeguards=["Track task status and preserve a resumable export manifest."],
    )


def build_gee_plan(
    request: GeeRequest,
    candidates: Sequence[DatasetCandidate],
    policy: PlannerPolicy | None = None,
) -> GeePlan:
    ranked = rank_candidates(request, candidates)
    selected = ranked[0] if ranked else None
    domain = classify_request_domain(request)
    if selected and selected.dataset_id in NTL_DATASET_IDS:
        domain = "ntl"
    validation = validate_candidate(request, selected)
    reasons: list[str] = []
    if request.dataset_id:
        reasons.append("explicit_dataset_id_preserved")
    elif selected:
        reasons.append("highest_ranked_valid_candidate")
    if selected and selected.official:
        reasons.append("official_source_preferred")
    if selected and selected.live_checked:
        reasons.append("live_metadata_checked")

    dataset_plan = DatasetPlan(
        domain=domain,
        selected=selected,
        alternatives=ranked[1:4],
        validation=validation,
        selection_reasons=reasons,
    )
    return GeePlan(
        request=request,
        dataset=dataset_plan,
        execution=choose_execution_plan(request, dataset_plan, policy),
    )
