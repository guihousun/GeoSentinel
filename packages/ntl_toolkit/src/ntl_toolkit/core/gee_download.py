from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from ntl_toolkit.runtime.downloads import DownloadProgress, resolve_download_output, sanitize_download_text
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult


class GeeDownloadRequest(BaseModel):
    dataset_id: str
    band: str | None = None
    bands: list[str] = Field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    bbox: tuple[float, float, float, float]
    output: str
    asset_type: Literal["auto", "Image", "ImageCollection"] = "auto"
    reducer: Literal["first", "mean", "median", "mosaic"] = "first"
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

    @model_validator(mode="after")
    def normalize_legacy_band(self) -> "GeeDownloadRequest":
        values = [value.strip() for value in self.bands if value and value.strip()]
        if self.band and self.band.strip() and self.band.strip() not in values:
            values.insert(0, self.band.strip())
        self.bands = values
        if _uses_normalized_difference(self.processing_preset) and self.index_bands is None:
            if len(values) != 2:
                raise ValueError("normalized_difference requires index_bands or exactly two bands")
            self.index_bands = (values[0], values[1])
        return self


def validate_gee_request(request: GeeDownloadRequest) -> None:
    if not request.dataset_id.strip():
        raise ValueError("dataset_id must not be empty")
    if not request.bands:
        raise ValueError("at least one band must be provided")
    if request.start_date and request.end_date and request.end_date < request.start_date:
        raise ValueError("end_date must be on or after start_date")
    if request.asset_type == "ImageCollection" and (not request.start_date or not request.end_date):
        raise ValueError("ImageCollection downloads require start_date and end_date")
    if request.asset_type == "Image" and request.processing_preset.endswith("_normalized_difference"):
        raise ValueError("Combined QA and normalized-difference presets require an ImageCollection")
    minx, miny, maxx, maxy = request.bbox
    if not -180 <= minx < maxx <= 180:
        raise ValueError("bbox longitudes must satisfy -180 <= minx < maxx <= 180")
    if not -90 <= miny < maxy <= 90:
        raise ValueError("bbox latitude values must satisfy -90 <= miny < maxy <= 90")
    if not request.output.strip():
        raise ValueError("output must not be empty")
    if not request.crs.strip():
        raise ValueError("crs must not be empty")


def download_gee_raster(
    request: GeeDownloadRequest,
    *,
    progress: DownloadProgress | None = None,
) -> ToolResult:
    """Export one explicit GEE image or collection reduction without OAuth."""
    try:
        validate_gee_request(request)
    except ValueError as exc:
        return _failed("INVALID_PARAMETER", str(exc), "Correct the request and retry.")

    _report(progress, 0, 4, "initializing Earth Engine")
    try:
        ee = _initialize_ee(request.project)
    except Exception as exc:  # noqa: BLE001 - package the local setup failure
        return _failed(
            "GEE_NOT_INITIALIZED",
            sanitize_download_text(str(exc) or "Earth Engine initialization failed."),
            "Use EasyGEE to inspect the local Earth Engine setup and authorize it, then retry.",
        )

    try:
        _report(progress, 1, 4, "selecting imagery")
        image = _materialize_image(ee, request)
        output = resolve_download_output(request.output, Path.cwd())
        _report(progress, 2, 4, "exporting GeoTIFF")
        _export_image(image, request, output)
        _report(progress, 3, 4, "validating output")
        _validate_geotiff(output)
        _report(progress, 4, 4, "completed")
        return ToolResult.succeeded(
            tool="download_gee_raster",
            summary="Downloaded GEE raster.",
            outputs=[OutputArtifact(path=str(output), media_type="image/tiff")],
            metrics={
                "dataset_id": request.dataset_id,
                "bands": list(request.bands),
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "asset_type": request.asset_type,
                "reducer": request.reducer,
                "processing_preset": request.processing_preset,
            },
        )
    except Exception as exc:  # noqa: BLE001 - preserve a structured MCP error
        return _failed(
            "GEE_DOWNLOAD_FAILED",
            sanitize_download_text(str(exc) or type(exc).__name__),
            "Inspect the AOI, date window, output path, and GEE request-size limits before retrying.",
        )


def _initialize_ee(project: str | None):
    import ee

    if project:
        ee.Initialize(project=project)
    else:
        ee.Initialize()
    return ee


def _materialize_image(ee, request: GeeDownloadRequest):
    geometry = ee.Geometry.Rectangle(list(request.bbox))
    asset_type = _resolve_asset_type(ee, request)
    if asset_type == "Image":
        if request.processing_preset.endswith("_normalized_difference"):
            raise ValueError("Combined QA and normalized-difference presets require an ImageCollection")
        image = ee.Image(request.dataset_id)
        return _apply_image_processing(ee, image, request)

    if request.start_date is None or request.end_date is None:
        raise ValueError("ImageCollection downloads require start_date and end_date")
    end_exclusive = request.end_date + timedelta(days=1)
    collection = (
        ee.ImageCollection(request.dataset_id)
        .filterDate(request.start_date.isoformat(), end_exclusive.isoformat())
        .filterBounds(geometry)
    )
    if int(collection.size().getInfo()) <= 0:
        raise ValueError("No images found for the requested dataset, dates, and AOI.")
    collection = _apply_collection_processing(ee, collection, request)
    if request.reducer == "first":
        image = collection.first()
    else:
        image = getattr(collection, request.reducer)()
    return _apply_post_reduction_processing(image, request)


def _resolve_asset_type(ee, request: GeeDownloadRequest) -> Literal["Image", "ImageCollection"]:
    if request.asset_type != "auto":
        return request.asset_type
    try:
        asset = ee.data.getAsset(request.dataset_id)
        asset_type = str((asset or {}).get("type") or "").upper()
        if asset_type == "IMAGE":
            return "Image"
        if asset_type == "IMAGE_COLLECTION":
            return "ImageCollection"
    except Exception:
        pass
    # Static requests without dates are expected to be ee.Image assets.
    return "ImageCollection" if request.start_date or request.end_date else "Image"


def _apply_collection_processing(ee, collection, request: GeeDownloadRequest):
    bands = list(request.bands)
    preset = _base_processing_preset(request.processing_preset)
    if preset == "sentinel2_cloud_score_plus":
        companion = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")
        linked = collection.linkCollection(companion, ["cs_cdf"])

        def mask_cloud_score(image):
            clear = image.select("cs_cdf").gte(request.quality_threshold)
            return image.updateMask(clear).select(bands).multiply(0.0001)

        return linked.map(mask_cloud_score)
    if preset == "sentinel2_scl_mask":

        def mask_scl(image):
            scl = image.select("SCL")
            clear = (
                scl.neq(3)
                .And(scl.neq(8))
                .And(scl.neq(9))
                .And(scl.neq(10))
                .And(scl.neq(11))
            )
            return image.updateMask(clear).select(bands).multiply(0.0001)

        return collection.map(mask_scl)
    if preset == "landsat_c2_l2":

        def prepare_landsat(image):
            qa_mask = image.select("QA_PIXEL").bitwiseAnd(0b111111).eq(0)
            outputs = []
            optical = [band for band in bands if band.startswith("SR_B")]
            thermal = [band for band in bands if band.startswith("ST_B")]
            passthrough = [band for band in bands if band not in {*optical, *thermal}]
            if optical:
                outputs.append(image.select(optical).multiply(0.0000275).add(-0.2))
            if thermal:
                outputs.append(image.select(thermal).multiply(0.00341802).add(149.0))
            if passthrough:
                outputs.append(image.select(passthrough))
            return ee.Image.cat(outputs).updateMask(qa_mask)

        return collection.map(prepare_landsat)
    if preset == "modis_vi_summary_qa":

        def prepare_modis(image):
            good = image.select("SummaryQA").lte(1)
            return image.select(bands).multiply(0.0001).updateMask(good)

        return collection.map(prepare_modis)
    return collection.select(bands)


def _apply_image_processing(ee, image, request: GeeDownloadRequest):
    selected = image.select(list(request.bands))
    if _uses_normalized_difference(request.processing_preset):
        return image.normalizedDifference(list(request.index_bands or ())).rename(request.output_band_name)
    return selected


def _apply_post_reduction_processing(image, request: GeeDownloadRequest):
    if _uses_normalized_difference(request.processing_preset):
        return image.normalizedDifference(list(request.index_bands or ())).rename(request.output_band_name)
    return image


def _uses_normalized_difference(preset: str) -> bool:
    return preset == "normalized_difference" or preset.endswith("_normalized_difference")


def _base_processing_preset(preset: str) -> str:
    suffix = "_normalized_difference"
    return preset[: -len(suffix)] if preset.endswith(suffix) else preset


def _export_image(image, request: GeeDownloadRequest, output: Path) -> Path:
    import ee
    import geemap

    geemap.ee_export_image(
        ee_object=image,
        filename=str(output),
        scale=request.scale,
        region=ee.Geometry.Rectangle(list(request.bbox)),
        crs=request.crs,
        file_per_band=False,
    )
    return output


def _validate_geotiff(path: Path) -> None:
    import rasterio

    if not path.exists() or path.stat().st_size == 0:
        raise ValueError(f"GEE export did not create a non-empty GeoTIFF: {path}")
    with rasterio.open(path) as dataset:
        if dataset.count < 1 or dataset.width < 1 or dataset.height < 1 or dataset.crs is None:
            raise ValueError(f"GEE export is not a valid projected GeoTIFF: {path}")


def _report(progress: DownloadProgress | None, current: float, total: float, message: str) -> None:
    if progress is not None:
        progress(float(current), float(total), message)


def _failed(code: str, message: str, suggestion: str) -> ToolResult:
    return ToolResult.failed(
        tool="download_gee_raster",
        error=ToolError(code=code, message=message, suggestion=suggestion),
    )
