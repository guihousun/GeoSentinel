from __future__ import annotations

import re
import warnings
from contextlib import ExitStack
from collections.abc import Sequence
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from affine import TransformNotInvertibleError
from rasterio.errors import RasterioError
from rasterio.mask import mask, raster_geometry_mask
from scipy import ndimage
from scipy.stats import kendalltau, theilslopes
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ntl_toolkit.runtime import (
    require_input_path,
    reserve_output_path,
    resolve_local_path,
    runtime_workdir,
)
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult

_METRIC_ORDER = (
    "MaxNTL",
    "MinNTL",
    "SDNTL",
    "TNTL",
    "LArea",
    "3DPLand",
    "3DED",
    "3DLPI",
    "ANTL",
)
_RASTER_TOOL = "calculate_ntl_metrics_for_raster"
_ZONAL_TOOL = "calculate_zonal_statistics"
_COMPOSITE_TOOL = "composite_ntl_rasters"
_TREND_TOOL = "analyze_ntl_trend"
_ANOMALY_TOOL = "detect_ntl_anomaly"
_GRID_RELATIVE_TOLERANCE = 1e-9
_GRID_ABSOLUTE_FLOOR = 1e-12


class _KnownNTLFailure(Exception):
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
    raise _KnownNTLFailure(
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


def _normalize_selected(selected: Sequence[str] | None) -> list[str]:
    if selected is None:
        return list(_METRIC_ORDER)
    if isinstance(selected, (str, bytes)):
        raise ValueError("selected must be a sequence of metric names.")

    selected_list = [str(name) for name in selected]
    if not selected_list:
        return []

    unknown = [name for name in selected_list if name not in _METRIC_ORDER]
    if unknown:
        raise ValueError(f"Unknown metric name(s): {', '.join(unknown)}")

    selected_set = set(selected_list)
    return [name for name in _METRIC_ORDER if name in selected_set]


def _normalize_pixel_area(pixel_area: Any) -> float:
    try:
        value = float(pixel_area)
    except (TypeError, ValueError) as exc:
        raise ValueError("pixel_area must be a finite number greater than zero.") from exc

    if not np.isfinite(value) or value <= 0:
        raise ValueError("pixel_area must be a finite number greater than zero.")
    return value


def _normalize_band(band: Any) -> int:
    if isinstance(band, bool) or not isinstance(band, Integral):
        raise ValueError("band must be an integer between 1 and the raster band count.")

    value = int(band)
    if value < 1:
        raise ValueError("band must be an integer between 1 and the raster band count.")
    return value


def _normalize_only_global(only_global: Any) -> bool:
    if not isinstance(only_global, bool):
        raise ValueError("only_global must be a boolean value.")
    return only_global


def _sanitize_array(values: Any) -> np.ndarray:
    array = np.array(values, dtype=float, copy=True)
    array[~np.isfinite(array)] = np.nan
    return array


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    scalar = float(value)
    if not np.isfinite(scalar):
        return None
    return scalar


def _pixel_area_for(dataset: rasterio.io.DatasetReader) -> float:
    transform = dataset.transform
    return abs(float(transform.a * transform.e - transform.b * transform.d))


def _validate_raster_transform(
    dataset: rasterio.io.DatasetReader,
    *,
    path: Path,
) -> None:
    transform = dataset.transform
    determinant = float(transform.a * transform.e - transform.b * transform.d)
    details = {
        "path": str(path),
        "transform": repr(transform),
        "determinant": determinant,
    }
    suggestion = "Check the raster geotransform and regenerate the file with a finite, non-degenerate affine transform."

    if not np.isfinite(determinant) or determinant == 0.0:
        _fail(
            "INVALID_RASTER_TRANSFORM",
            f"Raster dataset '{path}' has a non-invertible affine transform.",
            details=details,
            suggestion=suggestion,
        )

    try:
        ~transform
    except TransformNotInvertibleError as exc:
        details["reason"] = str(exc)
        _fail(
            "INVALID_RASTER_TRANSFORM",
            f"Raster dataset '{path}' has a non-invertible affine transform.",
            details=details,
            suggestion=suggestion,
        )


def _read_raster_band(
    path: Path,
    *,
    band: int,
) -> tuple[np.ndarray, rasterio.io.DatasetReader, list[str]]:
    try:
        dataset = rasterio.open(path)
    except RasterioError as exc:
        _fail(
            "RASTER_READ_FAILED",
            f"Unable to read raster '{path}'.",
            details={"path": str(path), "reason": str(exc)},
        )

    warnings: list[str] = []
    try:
        _validate_raster_transform(dataset, path=path)
        if dataset.crs is None:
            _fail(
                "CRS_MISSING",
                f"Raster dataset '{path}' does not define a CRS.",
                details={"path": str(path)},
            )
        if band > dataset.count:
            _fail(
                "INVALID_PARAMETER",
                f"band must be between 1 and {dataset.count}.",
                details={"parameter": "band", "value": band, "band_count": dataset.count},
            )

        masked = dataset.read(band, masked=True).astype(float)
        array = np.asarray(masked.filled(np.nan), dtype=float)
        array[~np.isfinite(array)] = np.nan
        if dataset.crs.is_geographic:
            warnings.append("GEOGRAPHIC_PIXEL_AREA")
        return array, dataset, warnings
    except Exception:
        dataset.close()
        raise


def _validate_geometry(gdf: gpd.GeoDataFrame, path: Path) -> None:
    if gdf.crs is None:
        _fail(
            "CRS_MISSING",
            f"Vector dataset '{path}' does not define a CRS.",
            details={"path": str(path)},
        )
    if gdf.empty:
        _fail(
            "EMPTY_DATASET",
            f"Vector dataset '{path}' contains no features.",
            details={"path": str(path)},
        )

    for index, geometry in enumerate(gdf.geometry):
        problem = _geometry_problem(geometry)
        if problem is not None:
            _fail(
                "INVALID_GEOMETRY",
                f"Vector dataset '{path}' contains {problem} geometry.",
                details={"path": str(path), "feature_index": index, "reason": problem},
            )


def _geometry_problem(geometry: BaseGeometry | None) -> str | None:
    if geometry is None:
        return "missing"
    if geometry.is_empty:
        return "empty"
    if not geometry.is_valid:
        return "invalid"
    return None


def _read_vector(path: Path) -> gpd.GeoDataFrame:
    try:
        gdf = gpd.read_file(path)
    except Exception as exc:  # pragma: no cover - driver-specific failures
        _fail(
            "VECTOR_READ_FAILED",
            f"Unable to read vector dataset '{path}'.",
            details={"path": str(path), "reason": str(exc)},
        )

    _validate_geometry(gdf, path)
    return gdf


def _region_label_series(gdf: gpd.GeoDataFrame) -> pd.Series:
    columns_by_casefold = {
        str(column).casefold(): column
        for column in gdf.columns
        if column != gdf.geometry.name
    }
    for preferred in ("name", "shapename", "adm_name", "admin_name", "region"):
        matched = columns_by_casefold.get(preferred)
        if matched is not None:
            return gdf[matched].astype(str)

    non_geometry_columns = [column for column in gdf.columns if column != gdf.geometry.name]
    if non_geometry_columns:
        return gdf[non_geometry_columns[0]].astype(str)

    return pd.Series(
        [f"feature_{index}" for index in range(len(gdf))],
        index=gdf.index,
        dtype="object",
    )


def _extract_year_from_name(name: str) -> int | None:
    matches = re.findall(r"(19\d{2}|20\d{2})", name or "")
    return int(matches[-1]) if matches else None


def _masked_values_for_geometry(
    dataset: rasterio.io.DatasetReader,
    values: np.ndarray,
    geometry: BaseGeometry,
) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="shapes are outside bounds of raster.*",
            category=UserWarning,
        )
        geometry_mask, _, _ = raster_geometry_mask(
            dataset,
            [geometry],
            invert=False,
        )
    return np.where(~geometry_mask, values, np.nan)


def calculate_ntl_metrics(
    values: Any,
    *,
    pixel_area: float,
    selected: Sequence[str] | None = None,
) -> dict[str, float | None]:
    ordered_metrics = _normalize_selected(selected)
    normalized_pixel_area = _normalize_pixel_area(pixel_area)
    if not ordered_metrics:
        return {}

    array = _sanitize_array(values)
    valid_mask = ~np.isnan(array)
    valid_count = int(np.sum(valid_mask))
    lit_mask = array > 0
    total_intensity = float(np.nansum(array))

    result: dict[str, float | None] = {}
    for metric in ordered_metrics:
        value: float | None
        if metric == "MaxNTL":
            value = None if valid_count == 0 else _float_or_none(np.nanmax(array))
        elif metric == "MinNTL":
            value = None if valid_count == 0 else _float_or_none(np.nanmin(array))
        elif metric == "SDNTL":
            value = None if valid_count == 0 else _float_or_none(np.nanstd(array))
        elif metric == "TNTL":
            value = _float_or_none(total_intensity)
        elif metric == "LArea":
            value = _float_or_none(np.sum(lit_mask) * normalized_pixel_area)
        elif metric == "3DPLand":
            if valid_count == 0:
                value = None
            else:
                max_ntl = float(np.nanmax(array))
                value = None if max_ntl == 0 else _float_or_none(total_intensity / (max_ntl * valid_count))
        elif metric == "3DED":
            labeled, num_features = ndimage.label(lit_mask)
            if total_intensity == 0 or num_features == 0:
                value = None
            else:
                perimeter = 0
                for region_label in range(1, num_features + 1):
                    region = labeled == region_label
                    edges = ndimage.binary_dilation(region) ^ region
                    perimeter += int(np.sum(edges))
                value = _float_or_none(perimeter / total_intensity)
        elif metric == "3DLPI":
            labeled, num_features = ndimage.label(lit_mask)
            if total_intensity == 0 or num_features == 0:
                value = None
            else:
                region_intensities = [
                    float(np.nansum(array[labeled == region_label]))
                    for region_label in range(1, num_features + 1)
                ]
                value = None if not region_intensities else _float_or_none(max(region_intensities) / total_intensity)
        else:  # ANTL
            value = None if valid_count == 0 else _float_or_none(total_intensity / valid_count)

        result[metric] = value

    return result


def calculate_ntl_metrics_for_raster(
    raster_path: str | Path,
    *,
    band: int = 1,
    selected: Sequence[str] | None = None,
) -> ToolResult:
    try:
        normalized_band = _normalize_band(band)
    except ValueError as exc:
        return _tool_failure(
            _RASTER_TOOL,
            ToolError(
                code="INVALID_PARAMETER",
                message=str(exc),
                details={
                    "parameter": "band",
                    "value": band,
                    "received_type": type(band).__name__,
                },
            ),
        )

    try:
        ordered_metrics = _normalize_selected(selected)
        input_path = _resolve_tool_input_path(raster_path, parameter="raster_path")
    except FileNotFoundError as exc:
        return _tool_failure(
            _RASTER_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except ValueError as exc:
        return _tool_failure(
            _RASTER_TOOL,
            ToolError(
                code="INVALID_PARAMETER",
                message=str(exc),
                details={"parameter": "selected"},
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_RASTER_TOOL, exc.error)

    try:
        values, dataset, warnings = _read_raster_band(input_path, band=normalized_band)
        with dataset:
            pixel_area = _pixel_area_for(dataset)
            metrics = calculate_ntl_metrics(values, pixel_area=pixel_area, selected=ordered_metrics)
            metrics.update(
                {
                    "band": normalized_band,
                    "pixel_area": pixel_area,
                    "crs": str(dataset.crs),
                    "width": int(dataset.width),
                    "height": int(dataset.height),
                }
            )
    except ValueError as exc:
        return _tool_failure(
            _RASTER_TOOL,
            ToolError(
                code="INVALID_PARAMETER",
                message=str(exc),
                details={"parameter": "selected"},
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_RASTER_TOOL, exc.error)

    return ToolResult.succeeded(
        tool=_RASTER_TOOL,
        summary=f"Calculated NTL metrics for '{input_path.name}'.",
        metrics=metrics,
        warnings=warnings,
    )


def calculate_zonal_statistics(
    *,
    raster_paths: Sequence[str | Path],
    vector_path: str | Path,
    output_path: str | Path,
    selected_indices: Sequence[str] | None = None,
    only_global: bool = False,
) -> ToolResult:
    try:
        if isinstance(raster_paths, (str, bytes)) or not raster_paths:
            raise ValueError("raster_paths must be a non-empty sequence.")

        raster_path_list = list(raster_paths)
        ordered_metrics = _normalize_selected(selected_indices)
        only_global_value = _normalize_only_global(only_global)
        resolved_vector_path = _resolve_tool_input_path(vector_path, parameter="vector_path")
        reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
    except FileNotFoundError as exc:
        return _tool_failure(
            _ZONAL_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except ValueError as exc:
        return _tool_failure(
            _ZONAL_TOOL,
            ToolError(
                code="INVALID_PARAMETER",
                message=str(exc),
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_ZONAL_TOOL, exc.error)

    try:
        source_vector = _read_vector(resolved_vector_path)
        region_labels = _region_label_series(source_vector)
    except _KnownNTLFailure as exc:
        return _tool_failure(_ZONAL_TOOL, exc.error)

    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    raster_count = len(raster_path_list)
    polygon_count = int(len(source_vector))

    try:
        resolved_raster_paths = [
            _resolve_tool_input_path(path, parameter="raster_paths")
            for path in raster_path_list
        ]
    except FileNotFoundError as exc:
        return _tool_failure(
            _ZONAL_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_ZONAL_TOOL, exc.error)

    for resolved_raster_path in resolved_raster_paths:
        try:
            values, dataset, raster_warnings = _read_raster_band(resolved_raster_path, band=1)
            with dataset:
                pixel_area = _pixel_area_for(dataset)
                if raster_warnings and "GEOGRAPHIC_PIXEL_AREA" not in warnings:
                    warnings.extend(raster_warnings)

                try:
                    vector_for_raster = (
                        source_vector.copy()
                        if source_vector.crs == dataset.crs
                        else source_vector.to_crs(dataset.crs)
                    )
                except Exception as exc:
                    _fail(
                        "VECTOR_READ_FAILED",
                        f"Unable to reproject vector dataset '{resolved_vector_path}'.",
                        details={"path": str(resolved_vector_path), "reason": str(exc)},
                    )

                _validate_geometry(vector_for_raster, resolved_vector_path)
                global_geometry = unary_union(vector_for_raster.geometry)
                raster_label = resolved_raster_path.name
                year = _extract_year_from_name(raster_label)

                if not only_global_value:
                    for feature_index, geometry in enumerate(vector_for_raster.geometry):
                        metrics = calculate_ntl_metrics(
                            _masked_values_for_geometry(dataset, values, geometry),
                            pixel_area=pixel_area,
                            selected=ordered_metrics,
                        )
                        rows.append(
                            {
                                "Raster_file": raster_label,
                                "Year": year,
                                "Region": region_labels.iloc[feature_index],
                                **metrics,
                            }
                        )

                global_metrics = calculate_ntl_metrics(
                    _masked_values_for_geometry(dataset, values, global_geometry),
                    pixel_area=pixel_area,
                    selected=ordered_metrics,
                )
                rows.append(
                    {
                        "Raster_file": raster_label,
                        "Year": year,
                        "Region": "Global_Summary",
                        **global_metrics,
                    }
                )
        except ValueError as exc:
            return _tool_failure(
                _ZONAL_TOOL,
                ToolError(
                    code="INVALID_PARAMETER",
                    message=str(exc),
                    details={"parameter": "selected_indices"},
                ),
            )
        except _KnownNTLFailure as exc:
            return _tool_failure(_ZONAL_TOOL, exc.error)

    frame = pd.DataFrame(rows)
    try:
        frame.to_csv(reserved_output, index=False, encoding="utf-8")
    except Exception as exc:
        if reserved_output.exists():
            reserved_output.unlink()
        return _tool_failure(
            _ZONAL_TOOL,
            ToolError(
                code="OUTPUT_WRITE_FAILED",
                message=f"Unable to write zonal statistics CSV '{reserved_output}'.",
                details={"path": str(reserved_output), "reason": str(exc)},
            ),
        )

    return ToolResult.succeeded(
        tool=_ZONAL_TOOL,
        summary=f"Calculated zonal statistics for {raster_count} raster(s).",
        outputs=[OutputArtifact(path=str(reserved_output), media_type="text/csv")],
        metrics={
            "polygon_count": polygon_count,
            "raster_count": raster_count,
            "row_count": int(len(rows)),
            "only_global": only_global_value,
        },
        warnings=warnings,
    )


def _artifact_for_raster(path: Path, *, role: str) -> OutputArtifact:
    return OutputArtifact(path=str(path), media_type="image/tiff", role=role)


def _cleanup_output_path(path: Path | None) -> None:
    if path is None:
        return

    related_paths = (
        path,
        Path(f"{path}.aux.xml"),
        Path(f"{path}.msk"),
        Path(f"{path}.ovr"),
    )
    for candidate in related_paths:
        try:
            if candidate.exists():
                candidate.unlink()
        except OSError:
            continue


def _cleanup_output_paths(paths: Sequence[Path | None]) -> None:
    for path in paths:
        _cleanup_output_path(path)


def _resolve_tool_output_prefix(path: str | Path, *, parameter: str) -> Path:
    try:
        return resolve_local_path(path, runtime_workdir())
    except ValueError as exc:
        _fail(
            "INVALID_PARAMETER",
            f"Invalid path for '{parameter}'.",
            details={"parameter": parameter, "path": str(path), "reason": str(exc)},
            suggestion="Use an ordinary relative path or a fully qualified absolute Windows path.",
        )


def _normalize_raster_path_list(
    raster_paths: Sequence[str | Path],
    *,
    minimum_count: int,
) -> list[Path]:
    if isinstance(raster_paths, (str, bytes)):
        _fail(
            "INVALID_PARAMETER",
            "raster_paths must be a non-empty sequence.",
            details={"parameter": "raster_paths"},
        )

    path_list = list(raster_paths)
    if len(path_list) < minimum_count:
        _fail(
            "INVALID_PARAMETER",
            f"raster_paths must contain at least {minimum_count} raster path(s).",
            details={
                "parameter": "raster_paths",
                "minimum_count": minimum_count,
                "count": len(path_list),
            },
        )

    return [
        _resolve_tool_input_path(path, parameter="raster_paths")
        for path in path_list
    ]


def _normalize_composite_method(method: Any) -> str:
    if method != "mean":
        _fail(
            "UNSUPPORTED_METHOD",
            "method must be 'mean'.",
            details={"parameter": "method", "value": method},
        )
    return "mean"


def _normalize_fallback_nodata(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, Real):
        _fail(
            "INVALID_PARAMETER",
            "fallback_nodata must be a finite number when provided.",
            details={"parameter": "fallback_nodata", "value": value},
        )
    normalized = float(value)
    if not np.isfinite(normalized):
        _fail(
            "INVALID_PARAMETER",
            "fallback_nodata must be a finite number when provided.",
            details={"parameter": "fallback_nodata", "value": normalized},
        )
    return normalized


def _normalize_target_index(value: Any, *, raster_count: int) -> int:
    if value is None:
        return raster_count - 1
    if isinstance(value, bool) or not isinstance(value, Integral):
        _fail(
            "INVALID_PARAMETER",
            "target_index must be an integer between 0 and the final raster index.",
            details={"parameter": "target_index", "value": value},
        )

    normalized = int(value)
    if normalized < 0 or normalized >= raster_count:
        _fail(
            "INVALID_PARAMETER",
            "target_index must be an integer between 0 and the final raster index.",
            details={
                "parameter": "target_index",
                "value": normalized,
                "raster_count": raster_count,
            },
        )
    return normalized


def _normalize_k_sigma(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        _fail(
            "INVALID_PARAMETER",
            "k_sigma must be a finite number greater than zero.",
            details={"parameter": "k_sigma", "value": value},
        )

    normalized = float(value)
    if not np.isfinite(normalized) or normalized <= 0.0:
        _fail(
            "INVALID_PARAMETER",
            "k_sigma must be a finite number greater than zero.",
            details={"parameter": "k_sigma", "value": normalized},
        )
    return normalized


def _normalize_minimum_baseline_observations(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        _fail(
            "INVALID_PARAMETER",
            "minimum_baseline_observations must be a positive integer.",
            details={"parameter": "minimum_baseline_observations", "value": value},
        )
    normalized = int(value)
    if normalized < 1:
        _fail(
            "INVALID_PARAMETER",
            "minimum_baseline_observations must be a positive integer.",
            details={"parameter": "minimum_baseline_observations", "value": normalized},
        )
    return normalized


def _open_validated_raster(path: Path) -> rasterio.io.DatasetReader:
    try:
        dataset = rasterio.open(path)
    except RasterioError as exc:
        _fail(
            "RASTER_READ_FAILED",
            f"Unable to read raster '{path}'.",
            details={"path": str(path), "reason": str(exc)},
        )

    try:
        _validate_raster_transform(dataset, path=path)
        if dataset.crs is None:
            _fail(
                "CRS_MISSING",
                f"Raster dataset '{path}' does not define a CRS.",
                details={"path": str(path)},
            )
        if dataset.count < 1:
            _fail(
                "RASTER_READ_FAILED",
                f"Raster dataset '{path}' does not contain band 1.",
                details={"path": str(path), "band_count": int(dataset.count)},
            )
        return dataset
    except Exception:
        dataset.close()
        raise


def _crs_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    return bool(left == right)


def _pixel_scale(transform: Any) -> float:
    x_scale = float(np.hypot(transform.a, transform.d))
    y_scale = float(np.hypot(transform.b, transform.e))
    return max(x_scale, y_scale, 0.0)


def _transform_tolerance(left: Any, right: Any) -> float:
    scale = max(_pixel_scale(left), _pixel_scale(right))
    return max(scale * _GRID_RELATIVE_TOLERANCE, _GRID_ABSOLUTE_FLOOR)


def _transforms_equal(left: Any, right: Any) -> bool:
    tolerance = _transform_tolerance(left, right)
    return bool(
        np.allclose(
            tuple(left)[:6],
            tuple(right)[:6],
            rtol=0.0,
            atol=tolerance,
        )
    )


def _validate_aligned_raster_datasets(
    datasets: Sequence[rasterio.io.DatasetReader],
    paths: Sequence[Path],
) -> None:
    reference = datasets[0]
    reference_path = paths[0]
    for dataset, path in zip(datasets[1:], paths[1:]):
        if (
            not _crs_equal(reference.crs, dataset.crs)
            or reference.width != dataset.width
            or reference.height != dataset.height
            or not _transforms_equal(reference.transform, dataset.transform)
        ):
            _fail(
                "GRID_MISMATCH",
                "Raster inputs must share the same CRS, dimensions, and affine grid.",
                details={
                    "reference_path": str(reference_path),
                    "path": str(path),
                    "reference_crs": str(reference.crs),
                    "crs": str(dataset.crs),
                    "reference_width": int(reference.width),
                    "width": int(dataset.width),
                    "reference_height": int(reference.height),
                    "height": int(dataset.height),
                    "reference_transform": repr(reference.transform),
                    "transform": repr(dataset.transform),
                },
            )


def _load_aligned_raster_arrays(
    resolved_paths: Sequence[Path],
    *,
    fallback_nodata: float | None = None,
) -> tuple[list[np.ndarray], dict[str, Any], float | None]:
    arrays: list[np.ndarray] = []
    datasets: list[rasterio.io.DatasetReader] = []
    try:
        for path in resolved_paths:
            array, dataset, _ = _read_raster_band(path, band=1)
            if dataset.nodata is None and fallback_nodata is not None:
                array[array == fallback_nodata] = np.nan
            arrays.append(array)
            datasets.append(dataset)
        _validate_aligned_raster_datasets(datasets, resolved_paths)
        source_nodata = datasets[0].nodata
        if source_nodata is None and fallback_nodata is not None:
            source_nodata = fallback_nodata
        return arrays, datasets[0].profile.copy(), source_nodata
    finally:
        for dataset in datasets:
            dataset.close()


def _float32_nodata_value(source_nodata: float | None) -> float:
    if source_nodata is None:
        return float("nan")

    candidate = np.float32(source_nodata)
    if not np.isfinite(candidate):
        return float("nan")

    if not np.isclose(float(candidate), float(source_nodata), rtol=0.0, atol=0.0):
        return float("nan")
    return float(candidate)


def _write_single_band_raster(
    output_path: Path,
    data: np.ndarray,
    *,
    profile: dict[str, Any],
    valid_mask: np.ndarray,
) -> None:
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(output_path, "w", **profile) as dataset:
            dataset.write(data, 1)
            dataset.write_mask(np.where(valid_mask, 255, 0).astype(np.uint8))


def _bbox_intersects(raster_bounds: Any, vector_bounds: Sequence[float]) -> bool:
    left, bottom, right, top = (
        float(raster_bounds.left),
        float(raster_bounds.bottom),
        float(raster_bounds.right),
        float(raster_bounds.top),
    )
    minx, miny, maxx, maxy = [float(value) for value in vector_bounds]
    return not (
        right <= minx
        or left >= maxx
        or top <= miny
        or bottom >= maxy
    )


def composite_ntl_rasters(
    raster_paths: Sequence[str | Path],
    output_path: str | Path,
    *,
    method: str = "mean",
    fallback_nodata: float | None = None,
) -> ToolResult:
    reserved_output: Path | None = None
    try:
        normalized_method = _normalize_composite_method(method)
        normalized_fallback_nodata = _normalize_fallback_nodata(fallback_nodata)
        resolved_paths = _normalize_raster_path_list(raster_paths, minimum_count=1)
        arrays, profile, source_nodata = _load_aligned_raster_arrays(
            resolved_paths,
            fallback_nodata=normalized_fallback_nodata,
        )
        reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
    except FileNotFoundError as exc:
        return _tool_failure(
            _COMPOSITE_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_COMPOSITE_TOOL, exc.error)

    stack = np.stack(arrays, axis=0)
    valid_mask = np.isfinite(stack)
    valid_counts = np.sum(valid_mask, axis=0)
    output_valid_mask = valid_counts > 0
    output_nodata = _float32_nodata_value(source_nodata)
    output_data = np.full(valid_counts.shape, output_nodata, dtype=np.float32)
    with np.errstate(invalid="ignore", divide="ignore"):
        np.divide(
            np.nansum(stack, axis=0, dtype=np.float64),
            valid_counts,
            out=output_data,
            where=output_valid_mask,
        )

    profile.update(count=1, dtype="float32", nodata=output_nodata)

    try:
        _write_single_band_raster(
            reserved_output,
            output_data.astype(np.float32, copy=False),
            profile=profile,
            valid_mask=output_valid_mask,
        )
    except (RasterioError, OSError) as exc:
        _cleanup_output_path(reserved_output)
        return _tool_failure(
            _COMPOSITE_TOOL,
            ToolError(
                code="OUTPUT_WRITE_FAILED",
                message=f"Unable to write composite raster '{reserved_output}'.",
                details={"path": str(reserved_output), "reason": str(exc)},
            ),
        )

    total_pixels = int(output_valid_mask.size)
    valid_pixel_count = int(np.sum(output_valid_mask))
    return ToolResult.succeeded(
        tool=_COMPOSITE_TOOL,
        summary=f"Created a mean composite from {len(resolved_paths)} raster(s).",
        outputs=[_artifact_for_raster(reserved_output, role="composite")],
        metrics={
            "input_count": int(len(resolved_paths)),
            "valid_pixel_count": valid_pixel_count,
            "coverage": float(valid_pixel_count / total_pixels) if total_pixels else 0.0,
            "method": normalized_method,
        },
    )


def analyze_ntl_trend(
    raster_paths: Sequence[str | Path],
    vector_path: str | Path,
    output_prefix: str | Path,
) -> ToolResult:
    slope_output: Path | None = None
    pvalue_output: Path | None = None
    try:
        resolved_paths = _normalize_raster_path_list(raster_paths, minimum_count=2)
        resolved_vector_path = _resolve_tool_input_path(vector_path, parameter="vector_path")
        resolved_prefix = _resolve_tool_output_prefix(output_prefix, parameter="output_prefix")
        source_vector = _read_vector(resolved_vector_path)

        with ExitStack() as stack:
            datasets = [stack.enter_context(_open_validated_raster(path)) for path in resolved_paths]
            _validate_aligned_raster_datasets(datasets, resolved_paths)
            reference = datasets[0]
            try:
                vector_for_raster = (
                    source_vector.copy()
                    if _crs_equal(source_vector.crs, reference.crs)
                    else source_vector.to_crs(reference.crs)
                )
            except Exception as exc:
                _fail(
                    "VECTOR_READ_FAILED",
                    f"Unable to reproject vector dataset '{resolved_vector_path}'.",
                    details={"path": str(resolved_vector_path), "reason": str(exc)},
                )

            _validate_geometry(vector_for_raster, resolved_vector_path)
            vector_bounds = vector_for_raster.total_bounds
            if not np.all(np.isfinite(vector_bounds)) or not _bbox_intersects(reference.bounds, vector_bounds):
                _fail(
                    "NO_SPATIAL_OVERLAP",
                    "Vector geometries do not overlap the raster extent.",
                    details={"raster_path": str(resolved_paths[0]), "vector_path": str(resolved_vector_path)},
                )

            union_geometry = unary_union(vector_for_raster.geometry)
            try:
                first_crop, cropped_transform = mask(
                    reference,
                    [union_geometry],
                    crop=True,
                    nodata=np.nan,
                    filled=False,
                )
            except ValueError as exc:
                _fail(
                    "NO_SPATIAL_OVERLAP",
                    "Vector geometries do not overlap the raster extent.",
                    details={
                        "raster_path": str(resolved_paths[0]),
                        "vector_path": str(resolved_vector_path),
                        "reason": str(exc),
                    },
                )

            stack_arrays = np.empty((len(datasets), first_crop.shape[1], first_crop.shape[2]), dtype=np.float32)
            first_array = np.asarray(first_crop[0].filled(np.nan), dtype=np.float32)
            first_array[~np.isfinite(first_array)] = np.nan
            stack_arrays[0] = first_array

            for index, dataset in enumerate(datasets[1:], start=1):
                cropped, current_transform = mask(
                    dataset,
                    [union_geometry],
                    crop=True,
                    nodata=np.nan,
                    filled=False,
                )
                if (
                    cropped.shape[1:] != first_crop.shape[1:]
                    or not _transforms_equal(cropped_transform, current_transform)
                ):
                    _fail(
                        "GRID_MISMATCH",
                        "Raster inputs must share the same CRS, dimensions, and affine grid.",
                        details={"reference_path": str(resolved_paths[0]), "path": str(resolved_paths[index])},
                    )
                array = np.asarray(cropped[0].filled(np.nan), dtype=np.float32)
                array[~np.isfinite(array)] = np.nan
                stack_arrays[index] = array

            slope_map = np.full(first_crop.shape[1:], np.nan, dtype=np.float32)
            pvalue_map = np.full(first_crop.shape[1:], np.nan, dtype=np.float32)
            analyzed_pixel_count = 0
            time_index = np.arange(len(datasets), dtype=float)
            for row in range(stack_arrays.shape[1]):
                for col in range(stack_arrays.shape[2]):
                    series = stack_arrays[:, row, col]
                    valid = np.isfinite(series)
                    if int(np.sum(valid)) < 2:
                        continue
                    values = series[valid]
                    if np.ptp(values) <= 0.0:
                        continue

                    slope = theilslopes(values, time_index[valid]).slope
                    pvalue = kendalltau(time_index[valid], values).pvalue
                    if not np.isfinite(slope) or not np.isfinite(pvalue):
                        continue

                    slope_map[row, col] = np.float32(slope)
                    pvalue_map[row, col] = np.float32(pvalue)
                    analyzed_pixel_count += 1

            profile = reference.profile.copy()
            profile.update(
                count=1,
                dtype="float32",
                height=int(first_crop.shape[1]),
                width=int(first_crop.shape[2]),
                transform=cropped_transform,
                nodata=np.nan,
            )

        slope_output = reserve_output_path(
            resolved_prefix.with_name(f"{resolved_prefix.name}_slope_trend.tif")
        )
        pvalue_output = reserve_output_path(
            resolved_prefix.with_name(f"{resolved_prefix.name}_pvalue_map.tif")
        )
    except FileNotFoundError as exc:
        _cleanup_output_paths([slope_output, pvalue_output])
        return _tool_failure(
            _TREND_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownNTLFailure as exc:
        _cleanup_output_paths([slope_output, pvalue_output])
        return _tool_failure(_TREND_TOOL, exc.error)

    valid_output_mask = np.isfinite(slope_map) & np.isfinite(pvalue_map)
    try:
        _write_single_band_raster(
            slope_output,
            slope_map,
            profile=profile,
            valid_mask=valid_output_mask,
        )
        _write_single_band_raster(
            pvalue_output,
            pvalue_map,
            profile=profile,
            valid_mask=valid_output_mask,
        )
    except (RasterioError, OSError) as exc:
        _cleanup_output_paths([slope_output, pvalue_output])
        return _tool_failure(
            _TREND_TOOL,
            ToolError(
                code="OUTPUT_WRITE_FAILED",
                message="Unable to write trend analysis rasters.",
                details={"reason": str(exc)},
            ),
        )

    return ToolResult.succeeded(
        tool=_TREND_TOOL,
        summary=f"Computed trend outputs for {len(resolved_paths)} raster(s).",
        outputs=[
            _artifact_for_raster(slope_output, role="slope"),
            _artifact_for_raster(pvalue_output, role="pvalue"),
        ],
        metrics={
            "raster_count": int(len(resolved_paths)),
            "analyzed_pixel_count": int(analyzed_pixel_count),
            "minimum_observations": 2,
        },
    )


def detect_ntl_anomaly(
    raster_paths: Sequence[str | Path],
    output_path: str | Path,
    *,
    target_index: int | None = None,
    k_sigma: float = 3.0,
    minimum_baseline_observations: int = 3,
) -> ToolResult:
    reserved_output: Path | None = None
    try:
        normalized_minimum_baseline = _normalize_minimum_baseline_observations(
            minimum_baseline_observations
        )
        resolved_paths = _normalize_raster_path_list(
            raster_paths,
            minimum_count=normalized_minimum_baseline + 1,
        )
        normalized_target_index = _normalize_target_index(target_index, raster_count=len(resolved_paths))
        normalized_k_sigma = _normalize_k_sigma(k_sigma)
        arrays, profile, _ = _load_aligned_raster_arrays(resolved_paths)
        reserved_output = _resolve_tool_output_path(output_path, parameter="output_path")
    except FileNotFoundError as exc:
        return _tool_failure(
            _ANOMALY_TOOL,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input raster was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownNTLFailure as exc:
        return _tool_failure(_ANOMALY_TOOL, exc.error)

    stack = np.stack(arrays, axis=0)
    baseline = np.delete(stack, normalized_target_index, axis=0)
    baseline_valid = np.isfinite(baseline)
    baseline_observation_count = np.sum(baseline_valid, axis=0)
    baseline_sum = np.nansum(np.where(baseline_valid, baseline, 0.0), axis=0, dtype=np.float64)
    baseline_mean = np.full(stack.shape[1:], np.nan, dtype=np.float64)
    np.divide(
        baseline_sum,
        baseline_observation_count,
        out=baseline_mean,
        where=baseline_observation_count > 0,
    )

    squared_distance = np.where(baseline_valid, (baseline - baseline_mean) ** 2, 0.0)
    baseline_variance = np.full(stack.shape[1:], np.nan, dtype=np.float64)
    np.divide(
        np.sum(squared_distance, axis=0, dtype=np.float64),
        baseline_observation_count,
        out=baseline_variance,
        where=baseline_observation_count > 0,
    )
    baseline_std = np.sqrt(baseline_variance)

    target = stack[normalized_target_index]
    valid_output_mask = np.isfinite(target) & (baseline_observation_count >= normalized_minimum_baseline)
    z_score = (target - baseline_mean) / (baseline_std + 1e-6)
    anomaly_mask = np.where(valid_output_mask & (z_score > normalized_k_sigma), 1, 0).astype(np.uint8)

    profile.update(count=1, dtype="uint8", nodata=None)

    try:
        _write_single_band_raster(
            reserved_output,
            anomaly_mask,
            profile=profile,
            valid_mask=valid_output_mask,
        )
    except (RasterioError, OSError) as exc:
        _cleanup_output_path(reserved_output)
        return _tool_failure(
            _ANOMALY_TOOL,
            ToolError(
                code="OUTPUT_WRITE_FAILED",
                message=f"Unable to write anomaly raster '{reserved_output}'.",
                details={"path": str(reserved_output), "reason": str(exc)},
            ),
        )

    return ToolResult.succeeded(
        tool=_ANOMALY_TOOL,
        summary=f"Computed anomaly mask for raster index {normalized_target_index}.",
        outputs=[_artifact_for_raster(reserved_output, role="anomaly")],
        metrics={
            "target_index": int(normalized_target_index),
            "k_sigma": float(normalized_k_sigma),
            "raster_count": int(len(resolved_paths)),
            "baseline_count": int(len(resolved_paths) - 1),
            "anomaly_pixel_count": int(np.sum(anomaly_mask == 1)),
            "valid_pixel_count": int(np.sum(valid_output_mask)),
        },
    )
