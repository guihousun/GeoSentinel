from __future__ import annotations

from contextlib import ExitStack
from numbers import Integral
from pathlib import Path
from typing import Any, Iterable

import geopandas as gpd
import numpy as np
import rasterio
from affine import Affine
from rasterio.crs import CRS
from rasterio.errors import MergeError, RasterioError
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import (
    Resampling,
    calculate_default_transform,
    reproject,
    transform_bounds,
)

from ntl_toolkit.runtime import (
    require_input_path,
    reserve_output_path,
    resolve_local_path,
    runtime_workdir,
)
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult

_RASTER_TOOL_NAME = {
    "inspect": "inspect_raster",
    "validate": "validate_geodata",
    "clip": "clip_raster",
    "reproject": "reproject_raster",
    "mosaic": "mosaic_rasters",
}
_VALID_MODES = {"basic", "full"}
_GRID_TOLERANCE = 1e-15
_GRID_INDEX_TOLERANCE = 1e-9
_RESAMPLING_METHODS = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "average": Resampling.average,
}
_MOSAIC_METHODS = {"first", "mean"}


class _KnownRasterFailure(Exception):
    def __init__(self, error: ToolError) -> None:
        super().__init__(error.message)
        self.error = error


def _fail(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    suggestion: str | None = None,
) -> None:
    raise _KnownRasterFailure(
        ToolError(
            code=code,
            message=message,
            details=details or {},
            suggestion=suggestion,
        )
    )


def _tool_failure(tool: str, error: ToolError) -> ToolResult:
    return ToolResult.failed(tool=tool, error=error)


def _resolve_input_path(path: str | Path) -> Path:
    return require_input_path(path, runtime_workdir())


def _resolve_output_path(path: str | Path) -> Path:
    requested = resolve_local_path(path, runtime_workdir())
    return reserve_output_path(requested)


def _resolve_tool_input_path(path: str | Path, *, parameter: str) -> Path:
    try:
        return _resolve_input_path(path)
    except ValueError as exc:
        _fail(
            "INVALID_PARAMETER",
            f"Invalid path for '{parameter}'.",
            details={"parameter": parameter, "path": str(path), "reason": str(exc)},
            suggestion="Use an ordinary relative path or a fully qualified absolute Windows path.",
        )


def _resolve_tool_output_path(path: str | Path, *, parameter: str) -> Path:
    try:
        return _resolve_output_path(path)
    except ValueError as exc:
        _fail(
            "INVALID_PARAMETER",
            f"Invalid path for '{parameter}'.",
            details={"parameter": parameter, "path": str(path), "reason": str(exc)},
            suggestion="Use an ordinary relative path or a fully qualified absolute Windows path.",
        )


def _artifact_for(path: Path) -> OutputArtifact:
    return OutputArtifact(path=str(path), media_type="image/tiff")


def _raster_processing_error(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    suggestion: str | None = None,
) -> ToolError:
    return ToolError(
        code=code,
        message=message,
        details=details or {},
        suggestion=suggestion,
    )


def _normalize_mode(mode: str) -> str:
    if mode not in _VALID_MODES:
        _fail(
            "INVALID_PARAMETER",
            "mode must be either 'basic' or 'full'.",
            details={"parameter": "mode", "value": mode},
        )
    return mode


def _normalize_sample_pixels(sample_pixels: Any) -> int:
    if isinstance(sample_pixels, bool) or not isinstance(sample_pixels, Integral):
        _fail(
            "INVALID_PARAMETER",
            "sample_pixels must be an integer greater than or equal to zero.",
            details={"parameter": "sample_pixels", "value": sample_pixels},
        )

    value = int(sample_pixels)
    if value < 0:
        _fail(
            "INVALID_PARAMETER",
            "sample_pixels must be an integer greater than or equal to zero.",
            details={"parameter": "sample_pixels", "value": sample_pixels},
        )
    return value


def _transform_to_list(transform: Any) -> list[float]:
    return [float(value) for value in tuple(transform)]


def _bounds_to_list(bounds: Any) -> list[float]:
    return [float(bounds.left), float(bounds.bottom), float(bounds.right), float(bounds.top)]


def _resolution(transform: Any) -> list[float]:
    return [abs(float(transform.a)), abs(float(transform.e))]


def _grid_signature(
    *,
    crs: str | None,
    width: int,
    height: int,
    resolution: list[float],
    transform: list[float],
) -> str:
    parts = [crs or "None", str(width), str(height), str(resolution[0]), str(resolution[1])]
    parts.extend(str(value) for value in transform)
    return "|".join(parts)


def _empty_stats() -> dict[str, Any]:
    return {
        "valid_count": 0,
        "min": None,
        "max": None,
        "mean": None,
        "std": None,
    }


def _finite_bounds(bounds: Iterable[Any] | None) -> list[float] | None:
    if bounds is None:
        return None

    values = [float(value) for value in bounds]
    if len(values) != 4 or not np.all(np.isfinite(values)):
        return None
    return values


def _sample_band(band: np.ndarray, sample_pixels: int) -> np.ndarray:
    if sample_pixels <= 0 or band.size <= sample_pixels:
        return band

    step = max(1, int(np.sqrt(band.size / sample_pixels)))
    return band[::step, ::step]


def _band_statistics(band: np.ndarray, nodata: float | None) -> dict[str, Any]:
    if nodata is None:
        valid_mask = np.isfinite(band)
    else:
        valid_mask = np.isfinite(band) & (band != nodata)

    valid = band[valid_mask]
    if valid.size == 0:
        return _empty_stats()

    return {
        "valid_count": int(valid.size),
        "min": float(np.nanmin(valid)),
        "max": float(np.nanmax(valid)),
        "mean": float(np.nanmean(valid)),
        "std": float(np.nanstd(valid)),
    }


def _stat_hints(stats: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    minimum = stats.get("min")
    maximum = stats.get("max")
    if minimum is not None and minimum < -1e-6:
        hints.append("Contains negative values; verify nodata and sensor units.")
    if maximum is not None and maximum > 1e6:
        hints.append("Very large max; check radiance units or scale.")
    return hints


def _inspect_raster_metrics(path: Path, *, mode: str, sample_pixels: int) -> dict[str, Any]:
    with rasterio.open(path) as dataset:
        transform = _transform_to_list(dataset.transform)
        resolution = _resolution(dataset.transform)
        metrics: dict[str, Any] = {
            "path": str(path.resolve(strict=False)),
            "driver": dataset.driver,
            "crs": str(dataset.crs) if dataset.crs else None,
            "width": int(dataset.width),
            "height": int(dataset.height),
            "band_count": int(dataset.count),
            "dtype": dataset.dtypes[0] if dataset.count > 0 and dataset.dtypes else None,
            "resolution": resolution,
            "nodata": None if dataset.nodata is None else float(dataset.nodata),
            "bounds": _bounds_to_list(dataset.bounds),
            "transform": transform,
            "grid_signature": _grid_signature(
                crs=str(dataset.crs) if dataset.crs else None,
                width=int(dataset.width),
                height=int(dataset.height),
                resolution=resolution,
                transform=transform,
            ),
            "readable": True,
        }

        if mode == "basic":
            return metrics

        if dataset.count < 1:
            metrics.update(_empty_stats())
            metrics["sample_pixels"] = int(sample_pixels)
            metrics["hints"] = []
            return metrics

        sampled = _sample_band(dataset.read(1), sample_pixels)
        stats = _band_statistics(sampled, dataset.nodata)
        metrics.update(stats)
        metrics["sample_pixels"] = int(sample_pixels)
        metrics["hints"] = _stat_hints(stats)
        return metrics


def _normalize_paths(paths: Iterable[str | Path] | str | Path | None) -> list[str | Path]:
    if paths is None:
        return []
    if isinstance(paths, (str, Path)):
        return [paths]
    return list(paths)


def _raster_validation_report(path: str | Path) -> dict[str, Any]:
    requested_path = str(path)
    try:
        resolved = _resolve_input_path(path)
    except FileNotFoundError as exc:
        return {
            "requested_path": requested_path,
            "path": str(exc),
            "exists": False,
            "readable": False,
            "warning_codes": ["UNREADABLE"],
            "error_code": "INPUT_NOT_FOUND",
        }

    try:
        metrics = _inspect_raster_metrics(resolved, mode="basic", sample_pixels=0)
    except (RasterioError, ValueError, OSError) as exc:
        return {
            "requested_path": requested_path,
            "path": str(resolved.resolve(strict=False)),
            "exists": True,
            "readable": False,
            "warning_codes": ["UNREADABLE"],
            "error_code": "RASTER_READ_FAILED",
            "error_message": str(exc),
        }

    warning_codes: list[str] = []
    empty_dataset = bool(
        metrics["band_count"] < 1 or metrics["width"] == 0 or metrics["height"] == 0
    )
    if empty_dataset:
        warning_codes.append("EMPTY_DATASET")

    metrics.update(
        {
            "requested_path": requested_path,
            "exists": True,
            "empty_dataset": empty_dataset,
            "warning_codes": warning_codes,
        }
    )
    return metrics


def _geometry_problem(geometry: Any) -> str | None:
    if geometry is None:
        return "missing"
    if geometry.is_empty:
        return "empty"
    if not geometry.is_valid:
        return "invalid"
    return None


def _vector_validation_report(path: str | Path) -> tuple[dict[str, Any], gpd.GeoDataFrame | None]:
    requested_path = str(path)
    try:
        resolved = _resolve_input_path(path)
    except FileNotFoundError as exc:
        return (
            {
                "requested_path": requested_path,
                "path": str(exc),
                "exists": False,
                "readable": False,
                "warning_codes": ["UNREADABLE"],
                "error_code": "INPUT_NOT_FOUND",
            },
            None,
        )

    try:
        gdf = gpd.read_file(resolved)
    except Exception as exc:  # noqa: BLE001
        return (
            {
                "requested_path": requested_path,
                "path": str(resolved.resolve(strict=False)),
                "exists": True,
                "readable": False,
                "warning_codes": ["UNREADABLE"],
                "error_code": "VECTOR_READ_FAILED",
                "error_message": str(exc),
            },
            None,
        )

    warning_codes: list[str] = []
    empty_dataset = gdf.empty
    invalid_geometry = False
    if empty_dataset:
        warning_codes.append("EMPTY_DATASET")
    else:
        invalid_geometry = any(
            _geometry_problem(geometry) is not None for geometry in gdf.geometry
        )
        if invalid_geometry:
            warning_codes.append("INVALID_GEOMETRY")

    raw_bounds = None if empty_dataset else _finite_bounds(gdf.total_bounds)
    if raw_bounds is None and not empty_dataset and "INVALID_GEOMETRY" not in warning_codes:
        warning_codes.append("INVALID_GEOMETRY")
        invalid_geometry = True

    report = {
        "requested_path": requested_path,
        "path": str(resolved.resolve(strict=False)),
        "exists": True,
        "readable": True,
        "crs": str(gdf.crs) if gdf.crs else None,
        "feature_count": int(len(gdf)),
        "geometry_types": sorted(str(value) for value in gdf.geometry.geom_type.dropna().unique()),
        "bounds": raw_bounds,
        "empty_dataset": empty_dataset,
        "invalid_geometry": invalid_geometry,
        "warning_codes": warning_codes,
    }
    return report, gdf


def _warning_union(*code_groups: list[str]) -> list[str]:
    ordered: list[str] = []
    for group in code_groups:
        for code in group:
            if code not in ordered:
                ordered.append(code)
    return ordered


def _validate_bool_parameter(value: Any, *, parameter: str) -> bool:
    if not isinstance(value, bool):
        _fail(
            "INVALID_PARAMETER",
            f"{parameter} must be a boolean value.",
            details={"parameter": parameter, "value": value},
            suggestion=f"Provide True or False for '{parameter}'.",
        )
    return value


def _validate_raster_crs(crs: Any, path: Path) -> CRS:
    crs_object = _crs_object(crs)
    if crs_object is None:
        _fail(
            "CRS_MISSING",
            f"Raster dataset '{path}' does not define a CRS.",
            details={"path": str(path)},
            suggestion="Assign a source CRS before running this raster operation.",
        )
    return crs_object


def _normalize_dst_crs(value: Any) -> CRS:
    try:
        crs = CRS.from_user_input(value)
    except Exception:  # noqa: BLE001
        crs = None
    if crs is None:
        _fail(
            "INVALID_PARAMETER",
            "dst_crs must be a valid CRS definition.",
            details={"parameter": "dst_crs", "value": value},
            suggestion="Provide a valid CRS such as 'EPSG:3857'.",
        )
    return crs


def _normalize_resampling(value: str) -> tuple[str, Resampling]:
    method = _RESAMPLING_METHODS.get(value)
    if method is None:
        _fail(
            "UNSUPPORTED_RESAMPLING",
            f"Resampling method '{value}' is not supported.",
            details={"resampling": value},
            suggestion="Use one of: nearest, bilinear, cubic, average.",
        )
    return value, method


def _validate_vector_geometries(gdf: gpd.GeoDataFrame, path: Path) -> None:
    if gdf.crs is None:
        _fail(
            "CRS_MISSING",
            f"Vector dataset '{path}' does not define a CRS.",
            details={"path": str(path)},
            suggestion="Assign a CRS to the vector dataset before clipping.",
        )
    if gdf.empty:
        _fail(
            "INVALID_GEOMETRY",
            f"Vector dataset '{path}' contains no features.",
            details={"path": str(path), "reason": "empty_dataset"},
            suggestion="Provide a vector dataset with at least one valid feature.",
        )
    for index, geometry in enumerate(gdf.geometry):
        problem = _geometry_problem(geometry)
        if problem is not None:
            _fail(
                "INVALID_GEOMETRY",
                f"Vector dataset '{path}' contains {problem} geometry.",
                details={"path": str(path), "feature_index": index, "reason": problem},
                suggestion="Repair or remove invalid geometries before clipping.",
            )
    bounds = _finite_bounds(gdf.total_bounds)
    if bounds is None:
        _fail(
            "INVALID_GEOMETRY",
            f"Vector dataset '{path}' has invalid bounds.",
            details={"path": str(path), "reason": "invalid_bounds"},
            suggestion="Repair the vector dataset and ensure its bounds are finite.",
        )


def _read_vector_dataset(path: Path) -> gpd.GeoDataFrame:
    try:
        return gpd.read_file(path)
    except Exception as exc:  # noqa: BLE001
        _fail(
            "VECTOR_READ_FAILED",
            f"Failed to read vector dataset '{path}'.",
            details={"path": str(path), "reason": str(exc)},
            suggestion="Confirm the vector file exists and can be opened by GeoPandas.",
        )


def _cleanup_partial_output(path: Path | None) -> None:
    if path is None:
        return
    if path.exists():
        path.unlink()
    mask_sidecar = path.with_name(f"{path.name}.msk")
    if mask_sidecar.exists():
        mask_sidecar.unlink()


def _dataset_resolution(dataset: rasterio.io.DatasetReader) -> tuple[float, float]:
    return abs(float(dataset.transform.a)), abs(float(dataset.transform.e))


def _dataset_orientation(dataset: rasterio.io.DatasetReader) -> tuple[float, float, int, int]:
    return (
        float(dataset.transform.b),
        float(dataset.transform.d),
        int(np.sign(dataset.transform.a)),
        int(np.sign(dataset.transform.e)),
    )


def _grid_basis_compatible(
    reference_transform: Affine,
    other_transform: Affine,
) -> bool:
    return bool(
        np.allclose(
            [reference_transform.a, reference_transform.b, reference_transform.d, reference_transform.e],
            [other_transform.a, other_transform.b, other_transform.d, other_transform.e],
            atol=_GRID_INDEX_TOLERANCE,
            rtol=_GRID_INDEX_TOLERANCE,
        )
    )


def _origin_is_integer_grid_offset(
    reference_transform: Affine,
    other_transform: Affine,
) -> bool:
    try:
        col_offset, row_offset = (~reference_transform) * (other_transform.c, other_transform.f)
    except Exception:  # noqa: BLE001
        return False
    nearest = np.rint([col_offset, row_offset])
    return bool(
        np.allclose(
            [col_offset, row_offset],
            nearest,
            atol=_GRID_INDEX_TOLERANCE,
            rtol=0.0,
        )
    )


def _normalize_mosaic_method(value: str) -> str:
    if value not in _MOSAIC_METHODS:
        _fail(
            "UNSUPPORTED_METHOD",
            f"Mosaic method '{value}' is not supported.",
            details={"method": value},
            suggestion="Use 'first' or 'mean'.",
        )
    return value


def _validate_raster_sources_for_mosaic(
    datasets: list[rasterio.io.DatasetReader],
    paths: list[Path],
) -> None:
    if not datasets:
        _fail(
            "INVALID_PARAMETER",
            "raster_paths must contain at least one raster path.",
            details={"parameter": "raster_paths", "value": []},
            suggestion="Provide at least one readable raster path to mosaic.",
        )

    reference = datasets[0]
    reference_path = paths[0]
    reference_crs = _validate_raster_crs(reference.crs, reference_path)
    reference_count = int(reference.count)
    reference_dtype = reference.dtypes[0] if reference.dtypes else None
    reference_resolution = _dataset_resolution(reference)
    reference_orientation = _dataset_orientation(reference)
    reference_transform = reference.transform

    for dataset, path in zip(datasets[1:], paths[1:]):
        current_crs = _validate_raster_crs(dataset.crs, path)
        if current_crs != reference_crs:
            _fail(
                "CRS_MISMATCH",
                f"Raster dataset '{path}' does not match CRS '{reference_crs}'.",
                details={
                    "reference_path": str(reference_path),
                    "path": str(path),
                    "reference_crs": str(reference_crs),
                    "crs": str(current_crs),
                },
                suggestion="Reproject the input rasters to a common CRS before mosaicking.",
            )
        if int(dataset.count) != reference_count:
            _fail(
                "BAND_COUNT_MISMATCH",
                f"Raster dataset '{path}' does not match the expected band count.",
                details={
                    "reference_path": str(reference_path),
                    "path": str(path),
                    "reference_band_count": reference_count,
                    "band_count": int(dataset.count),
                },
                suggestion="Use rasters with the same number of bands.",
            )
        current_dtype = dataset.dtypes[0] if dataset.dtypes else None
        if current_dtype != reference_dtype:
            _fail(
                "DTYPE_MISMATCH",
                f"Raster dataset '{path}' does not match dtype '{reference_dtype}'.",
                details={
                    "reference_path": str(reference_path),
                    "path": str(path),
                    "reference_dtype": reference_dtype,
                    "dtype": current_dtype,
                },
                suggestion="Convert the rasters to a shared dtype before mosaicking.",
            )
        if (
            not np.allclose(_dataset_resolution(dataset), reference_resolution, atol=_GRID_TOLERANCE, rtol=_GRID_TOLERANCE)
            or not np.allclose(_dataset_orientation(dataset), reference_orientation, atol=_GRID_TOLERANCE, rtol=_GRID_TOLERANCE)
            or not _grid_basis_compatible(reference_transform, dataset.transform)
            or not _origin_is_integer_grid_offset(reference_transform, dataset.transform)
        ):
            _fail(
                "GRID_MISMATCH",
                f"Raster dataset '{path}' does not align to the reference grid.",
                details={
                    "reference_path": str(reference_path),
                    "path": str(path),
                    "reference_resolution": list(reference_resolution),
                    "resolution": list(_dataset_resolution(dataset)),
                },
                suggestion="Resample the rasters to a shared resolution and orientation before mosaicking.",
            )


def _crs_object(raw_crs: Any) -> CRS | None:
    if raw_crs in {None, ""}:
        return None
    try:
        return CRS.from_user_input(raw_crs)
    except Exception:  # noqa: BLE001
        return None


def _crs_equal(left: Any, right: Any) -> bool:
    left_crs = _crs_object(left)
    right_crs = _crs_object(right)
    if left_crs is None or right_crs is None:
        return False
    return left_crs == right_crs


def _transform_object(raw_transform: Any) -> Affine | None:
    if raw_transform is None:
        return None
    try:
        values = tuple(float(value) for value in raw_transform)
    except (TypeError, ValueError):
        return None
    if len(values) != 9 or not np.all(np.isfinite(values)):
        return None
    return Affine(*values[:6])


def _float_lists_close(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    try:
        left_values = np.asarray(left, dtype=float)
        right_values = np.asarray(right, dtype=float)
    except (TypeError, ValueError):
        return False
    return bool(
        left_values.shape == right_values.shape
        and np.all(np.isfinite(left_values))
        and np.all(np.isfinite(right_values))
        and np.allclose(
            left_values,
            right_values,
            rtol=_GRID_TOLERANCE,
            atol=_GRID_TOLERANCE,
        )
    )


def _grid_compatible(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_transform = _transform_object(left.get("transform"))
    right_transform = _transform_object(right.get("transform"))
    return (
        _crs_equal(left.get("crs"), right.get("crs"))
        and left.get("width") == right.get("width")
        and left.get("height") == right.get("height")
        and _float_lists_close(left.get("resolution"), right.get("resolution"))
        and left_transform is not None
        and right_transform is not None
        and left_transform.almost_equals(right_transform, precision=_GRID_TOLERANCE)
    )


def _bbox_intersects(raster_bounds: list[float], vector_bounds: list[float]) -> bool:
    left, bottom, right, top = raster_bounds
    minx, miny, maxx, maxy = vector_bounds
    return not (
        right <= minx
        or left >= maxx
        or top <= miny
        or bottom >= maxy
    )


def _transform_bounds_safe(
    source_crs: Any,
    target_crs: Any,
    bounds: Iterable[Any] | None,
) -> list[float] | None:
    normalized_bounds = _finite_bounds(bounds)
    source = _crs_object(source_crs)
    target = _crs_object(target_crs)
    if normalized_bounds is None or source is None or target is None:
        return None
    try:
        transformed = transform_bounds(
            source,
            target,
            *normalized_bounds,
            densify_pts=21,
        )
    except Exception:  # noqa: BLE001
        return None
    return _finite_bounds(transformed)


def inspect_raster(
    path: str | Path,
    *,
    mode: str = "full",
    sample_pixels: int = 0,
) -> ToolResult:
    """Return metadata and optional band statistics for a raster dataset."""
    tool = _RASTER_TOOL_NAME["inspect"]
    try:
        normalized_mode = _normalize_mode(mode)
        normalized_sample_pixels = _normalize_sample_pixels(sample_pixels)
        input_path = _resolve_input_path(path)
        metrics = _inspect_raster_metrics(
            input_path,
            mode=normalized_mode,
            sample_pixels=normalized_sample_pixels,
        )
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownRasterFailure as exc:
        return _tool_failure(tool, exc.error)
    except (RasterioError, ValueError, OSError) as exc:
        resolved_path = Path(path).expanduser().resolve(strict=False)
        return _tool_failure(
            tool,
            ToolError(
                code="RASTER_READ_FAILED",
                message=f"Failed to read raster '{resolved_path}'.",
                details={"path": str(resolved_path), "reason": str(exc)},
            ),
        )

    summary = f"Inspected raster {metrics['width']}x{metrics['height']}."
    return ToolResult.succeeded(
        tool=tool,
        summary=summary,
        metrics=metrics,
    )


def clip_raster(
    raster_path: str | Path,
    vector_path: str | Path,
    output_path: str | Path,
    *,
    all_touched: bool = False,
) -> ToolResult:
    tool = _RASTER_TOOL_NAME["clip"]
    reserved_output: Path | None = None
    try:
        all_touched = _validate_bool_parameter(all_touched, parameter="all_touched")
        input_raster = _resolve_tool_input_path(raster_path, parameter="raster_path")
        input_vector = _resolve_tool_input_path(vector_path, parameter="vector_path")
        with rasterio.open(input_raster) as dataset:
            raster_crs = _validate_raster_crs(dataset.crs, input_raster)
            vectors = _read_vector_dataset(input_vector)
            _validate_vector_geometries(vectors, input_vector)
            projected = vectors.to_crs(raster_crs)
            bounds = _finite_bounds(projected.total_bounds)
            if bounds is None or not _bbox_intersects(_bounds_to_list(dataset.bounds), bounds):
                _fail(
                    "NO_SPATIAL_OVERLAP",
                    "Vector geometries do not overlap the raster extent.",
                    details={"raster_path": str(input_raster), "vector_path": str(input_vector)},
                    suggestion="Provide clip geometries that intersect the raster extent.",
                )
            clipped, clipped_transform = mask(
                dataset,
                projected.geometry,
                crop=True,
                all_touched=all_touched,
                filled=False,
            )
            clipped_mask = np.ma.getmaskarray(clipped)
            profile = dataset.profile.copy()
            profile.update(
                transform=clipped_transform,
                width=int(clipped.shape[2]),
                height=int(clipped.shape[1]),
                count=int(clipped.shape[0]),
                nodata=dataset.nodata,
            )
            fill_value = (
                dataset.nodata
                if dataset.nodata is not None
                else np.zeros((), dtype=clipped.dtype).item()
            )
            clipped_data = np.ma.filled(clipped, fill_value=fill_value)
            valid_mask = np.where(np.all(clipped_mask, axis=0), 0, 255).astype(np.uint8)
            reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
            with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
                with rasterio.open(reserved_output, "w", **profile) as output_dataset:
                    output_dataset.write(clipped_data)
                    output_dataset.write_mask(valid_mask)
    except FileNotFoundError as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
                suggestion="Confirm both raster and vector input paths exist inside the workspace.",
            ),
        )
    except _KnownRasterFailure as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(tool, exc.error)
    except ValueError as exc:
        _cleanup_partial_output(reserved_output)
        if "do not overlap" in str(exc).lower():
            return _tool_failure(
                tool,
                ToolError(
                    code="NO_SPATIAL_OVERLAP",
                    message="Vector geometries do not overlap the raster extent.",
                    details={"raster_path": str(raster_path), "vector_path": str(vector_path)},
                    suggestion="Provide clip geometries that intersect the raster extent.",
                ),
            )
        raise
    except (RasterioError, OSError) as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            _raster_processing_error(
                "RASTER_READ_FAILED",
                f"Failed to process raster '{raster_path}'.",
                details={
                    "path": str(raster_path),
                    "reason": str(exc),
                },
                suggestion="Confirm the raster can be opened and the output path is writable.",
            ),
        )

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Clipped raster to {clipped.shape[2]}x{clipped.shape[1]}.",
        outputs=[_artifact_for(reserved_output)],
        metrics={
            "width": int(clipped.shape[2]),
            "height": int(clipped.shape[1]),
            "band_count": int(clipped.shape[0]),
            "crs": str(raster_crs),
            "all_touched": all_touched,
        },
    )


def reproject_raster(
    raster_path: str | Path,
    output_path: str | Path,
    *,
    dst_crs: Any,
    resampling: str = "bilinear",
) -> ToolResult:
    tool = _RASTER_TOOL_NAME["reproject"]
    reserved_output: Path | None = None
    try:
        dst_crs_object = _normalize_dst_crs(dst_crs)
        resampling_name, resampling_method = _normalize_resampling(resampling)
        input_raster = _resolve_tool_input_path(raster_path, parameter="raster_path")
        with rasterio.open(input_raster) as dataset:
            source_crs = _validate_raster_crs(dataset.crs, input_raster)
            transform, width, height = calculate_default_transform(
                source_crs,
                dst_crs_object,
                dataset.width,
                dataset.height,
                *dataset.bounds,
            )
            profile = dataset.profile.copy()
            profile.update(
                crs=dst_crs_object,
                transform=transform,
                width=int(width),
                height=int(height),
                count=int(dataset.count),
                nodata=dataset.nodata,
            )
            reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
            with rasterio.open(reserved_output, "w", **profile) as destination:
                for band_index in range(1, dataset.count + 1):
                    reproject(
                        source=rasterio.band(dataset, band_index),
                        destination=rasterio.band(destination, band_index),
                        src_transform=dataset.transform,
                        src_crs=source_crs,
                        dst_transform=transform,
                        dst_crs=dst_crs_object,
                        src_nodata=dataset.nodata,
                        dst_nodata=dataset.nodata,
                        resampling=resampling_method,
                    )
    except FileNotFoundError as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
                suggestion="Confirm the input raster path exists inside the workspace.",
            ),
        )
    except _KnownRasterFailure as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(tool, exc.error)
    except (RasterioError, OSError) as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            _raster_processing_error(
                "RASTER_READ_FAILED",
                f"Failed to process raster '{raster_path}'.",
                details={"path": str(raster_path), "reason": str(exc)},
                suggestion="Confirm the raster can be opened and the output path is writable.",
            ),
        )

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Reprojected raster to {dst_crs_object}.",
        outputs=[_artifact_for(reserved_output)],
        metrics={
            "width": int(width),
            "height": int(height),
            "band_count": int(profile["count"]),
            "crs": str(dst_crs_object),
            "resampling": resampling_name,
        },
    )


def mosaic_rasters(
    raster_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    method: str = "first",
) -> ToolResult:
    tool = _RASTER_TOOL_NAME["mosaic"]
    reserved_output: Path | None = None
    raster_items = list(raster_paths)
    resolved_paths: list[Path] = []
    try:
        method = _normalize_mosaic_method(method)
        for raster_path in raster_items:
            resolved_paths.append(_resolve_tool_input_path(raster_path, parameter="raster_paths"))

        with ExitStack() as stack:
            datasets = [stack.enter_context(rasterio.open(path)) for path in resolved_paths]
            _validate_raster_sources_for_mosaic(datasets, resolved_paths)

            first_dataset = datasets[0]
            nodata = first_dataset.nodata
            profile = first_dataset.profile.copy()
            source_dtype = np.dtype(profile["dtype"])

            if method == "first":
                mosaic_array, transform = merge(datasets, method="first", nodata=nodata)
                output_dtype = profile["dtype"]
                output_nodata = nodata
            else:
                sum_array, transform = merge(datasets, method="sum", nodata=nodata, dtype="float64")
                count_array, _ = merge(datasets, method="count", nodata=nodata, dtype="float64")
                promoted_dtype = np.result_type(source_dtype, np.float32)
                output_dtype = np.dtype(promoted_dtype).name
                output_nodata = np.nan if nodata is None else promoted_dtype.type(nodata)
                mean_array = np.full(sum_array.shape, output_nodata, dtype=promoted_dtype)
                np.divide(sum_array, count_array, out=mean_array, where=count_array > 0)
                mean_array[count_array <= 0] = output_nodata
                mosaic_array = mean_array.astype(promoted_dtype, copy=False)

            profile.update(
                transform=transform,
                width=int(mosaic_array.shape[2]),
                height=int(mosaic_array.shape[1]),
                count=int(mosaic_array.shape[0]),
                nodata=output_nodata,
                dtype=output_dtype,
            )
            reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
            with rasterio.open(reserved_output, "w", **profile) as destination:
                destination.write(mosaic_array)
    except FileNotFoundError as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
                suggestion="Confirm every raster path exists inside the workspace.",
            ),
        )
    except _KnownRasterFailure as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(tool, exc.error)
    except (MergeError, RasterioError, OSError) as exc:
        _cleanup_partial_output(reserved_output)
        return _tool_failure(
            tool,
            ToolError(
                code="RASTER_READ_FAILED",
                message="Failed to process raster inputs for mosaicking.",
                details={"reason": str(exc)},
                suggestion="Confirm the rasters are readable and the output path is writable.",
            ),
        )

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Mosaicked {len(raster_items)} raster(s).",
        outputs=[_artifact_for(reserved_output)],
        metrics={
            "width": int(mosaic_array.shape[2]),
            "height": int(mosaic_array.shape[1]),
            "band_count": int(mosaic_array.shape[0]),
            "crs": str(first_dataset.crs),
            "method": method,
        },
    )


def validate_geodata(
    *,
    raster_paths: Iterable[str | Path] | str | Path | None = None,
    vector_paths: Iterable[str | Path] | str | Path | None = None,
) -> ToolResult:
    """Validate raster and vector inputs without mutating caller-provided sequences."""
    tool = _RASTER_TOOL_NAME["validate"]
    raster_items = _normalize_paths(raster_paths)
    vector_items = _normalize_paths(vector_paths)

    raster_reports = [_raster_validation_report(path) for path in raster_items]
    vector_results = [_vector_validation_report(path) for path in vector_items]
    vector_reports = [report for report, _ in vector_results]

    warnings = _warning_union(
        *[report["warning_codes"] for report in raster_reports],
        *[report["warning_codes"] for report in vector_reports],
    )

    readable_rasters = [report for report in raster_reports if report.get("readable")]
    raster_pair_reports: list[dict[str, Any]] = []
    for index, left in enumerate(readable_rasters):
        for right in readable_rasters[index + 1 :]:
            warning_codes: list[str] = []
            grid_compatible = _grid_compatible(left, right)
            if not grid_compatible:
                warning_codes.append("GRID_MISMATCH")
            raster_pair_reports.append(
                {
                    "left_path": left["path"],
                    "right_path": right["path"],
                    "grid_compatible": grid_compatible,
                    "warning_codes": warning_codes,
                }
            )
            warnings = _warning_union(warnings, warning_codes)

    raster_vector_reports: list[dict[str, Any]] = []
    readable_vector_results = [
        (report, gdf)
        for report, gdf in vector_results
        if report.get("readable") and gdf is not None
    ]
    for raster_report in readable_rasters:
        raster_crs = raster_report.get("crs")
        raster_bounds = raster_report.get("bounds")
        for vector_report, vector_gdf in readable_vector_results:
            warning_codes: list[str] = []
            warning_codes = _warning_union(
                warning_codes,
                [code for code in vector_report["warning_codes"] if code in {"INVALID_GEOMETRY", "EMPTY_DATASET"}],
            )
            crs_match = _crs_equal(raster_crs, vector_report.get("crs"))
            comparison_bounds = _finite_bounds(vector_report.get("bounds"))
            if not crs_match:
                warning_codes.append("CRS_MISMATCH")
                comparison_bounds = _transform_bounds_safe(
                    vector_report.get("crs"),
                    raster_crs,
                    comparison_bounds,
                )

            bbox_intersects = (
                _bbox_intersects(raster_bounds, comparison_bounds)
                if raster_bounds is not None and comparison_bounds is not None
                else None
            )
            if bbox_intersects is False:
                warning_codes.append("NO_BBOX_INTERSECTION")

            raster_vector_reports.append(
                {
                    "raster_path": raster_report["path"],
                    "vector_path": vector_report["path"],
                    "crs_match": bool(crs_match),
                    "bbox_intersects": bbox_intersects,
                    "warning_codes": warning_codes,
                }
            )
            warnings = _warning_union(warnings, warning_codes)

    metrics = {
        "raster_count": len(raster_items),
        "vector_count": len(vector_items),
        "readable_raster_count": sum(1 for report in raster_reports if report.get("readable")),
        "readable_vector_count": sum(1 for report in vector_reports if report.get("readable")),
        "raster_reports": raster_reports,
        "vector_reports": vector_reports,
        "raster_pair_reports": raster_pair_reports,
        "raster_vector_reports": raster_vector_reports,
    }
    return ToolResult.succeeded(
        tool=tool,
        summary=f"Validated {len(raster_items)} raster(s) and {len(vector_items)} vector(s).",
        metrics=metrics,
        warnings=warnings,
    )
