"""Deterministic DMSP-OLS to VIIRS-like annual harmonization.

This first-version tool uses a fixed overlap-period polynomial calibration:

    VIIRS_like_overlap = f(DMSP_overlap)

The DMSP raster is first aligned to the VIIRS overlap grid.  The fitted
polynomial is then applied to historical DMSP rasters, while observed VIIRS
rasters are retained for the overlap year and later years.  The output is
VIIRS-like on the supplied VIIRS grid; it does not recover spatial detail that
is absent from DMSP-OLS.

The implementation is intentionally small and deterministic.  It is an
independent adapter inspired by the overlap-calibration pattern used in
public DMSP/VIIRS harmonization workflows, rather than a vendored copy of a
third-party repository.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Literal, Optional

import numpy as np
import rasterio
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field
from rasterio.enums import Resampling
from rasterio.warp import reproject

from storage_manager import storage_manager


METHOD_SOURCE_URL = "https://github.com/worldbank/NTL_Harmonizer"
METHOD_SCHEMA = "ntl.dmsp_viirs_harmonization.v1"
OUTPUT_NODATA = -9999.0
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
RESAMPLING_METHODS: dict[str, Resampling] = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "average": Resampling.average,
}


class DMSPVIIRSHarmonizationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dmsp_folder: str = Field(
        ...,
        description=(
            "Workspace-relative folder under inputs/ containing one annual "
            "DMSP-OLS GeoTIFF per year, including the overlap year."
        ),
    )
    viirs_folder: str = Field(
        ...,
        description=(
            "Workspace-relative folder under inputs/ containing one annual "
            "VIIRS GeoTIFF per year, including the overlap year."
        ),
    )
    output_folder: str = Field(
        ...,
        description="Folder name written under the current workspace outputs/.",
    )
    start_year: int = Field(..., ge=1992, le=2100)
    overlap_year: int = Field(
        2013,
        ge=1992,
        le=2100,
        description="Year in which DMSP and VIIRS overlap and the mapping is fitted.",
    )
    end_year: int = Field(..., ge=1992, le=2100)
    degree: int = Field(
        3,
        ge=1,
        le=5,
        description="Polynomial degree. The benchmark first version fixes this at 3.",
    )
    resampling: Literal["nearest", "bilinear", "average"] = Field(
        "bilinear",
        description="Resampling used to align DMSP and annual VIIRS rasters to the overlap VIIRS grid.",
    )
    clip_min: Optional[float] = Field(
        0.0,
        description="Optional lower bound for the VIIRS-like output. Use null to disable clipping.",
    )
    clip_max: Optional[float] = Field(
        None,
        description="Optional upper bound for the VIIRS-like output. Use null to disable clipping.",
    )
    max_training_pixels: int = Field(
        1_000_000,
        ge=4,
        description="Deterministic cap on overlap pixels used for fitting.",
    )
    output_prefix: str = Field(
        "viirs_like_",
        description="Filename prefix for annual output GeoTIFFs.",
    )
    summary_csv: str = Field(
        "annual_statistics.csv",
        description="Basename of the annual statistics CSV written inside output_folder.",
    )


def _validate_parameters(
    *,
    start_year: int,
    overlap_year: int,
    end_year: int,
    degree: int,
    clip_min: Optional[float],
    clip_max: Optional[float],
    output_prefix: str,
    summary_csv: str,
) -> None:
    if not start_year < overlap_year < end_year:
        raise ValueError("The years must satisfy start_year < overlap_year < end_year.")
    if degree < 1:
        raise ValueError("degree must be at least 1.")
    if clip_min is not None and clip_max is not None and clip_min > clip_max:
        raise ValueError("clip_min must not be greater than clip_max.")
    if not output_prefix or Path(output_prefix).name != output_prefix:
        raise ValueError("output_prefix must be a non-empty filename prefix without path separators.")
    if not summary_csv or Path(summary_csv).name != summary_csv:
        raise ValueError("summary_csv must be a basename without path separators.")


def _resolve_input_folder(folder: str) -> Path:
    path = Path(storage_manager.resolve_input_path(folder))
    if not path.is_dir():
        raise FileNotFoundError(f"Input folder not found: {folder}")
    return path


def _resolve_output_folder(folder: str) -> Path:
    path = Path(storage_manager.resolve_output_path(folder))
    path.mkdir(parents=True, exist_ok=True)
    return path


def _discover_annual_rasters(folder: Path) -> dict[int, Path]:
    rasters: dict[int, Path] = {}
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".tif", ".tiff"}:
            continue
        years = sorted({int(match) for match in YEAR_PATTERN.findall(path.stem)})
        if len(years) != 1:
            continue
        year = years[0]
        if year in rasters:
            raise ValueError(
                f"More than one annual raster was found for {year}: "
                f"{rasters[year].name} and {path.name}"
            )
        rasters[year] = path
    if not rasters:
        raise FileNotFoundError(f"No annual GeoTIFFs were found under {folder}.")
    return rasters


def _read_raster(path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    with rasterio.open(path) as src:
        if src.count < 1:
            raise ValueError(f"Raster has no bands: {path}")
        masked = src.read(1, masked=True).astype(np.float32)
        array = np.asarray(masked.filled(np.nan), dtype=np.float32)
        info = {
            "profile": src.profile.copy(),
            "shape": (src.height, src.width),
            "transform": src.transform,
            "crs": src.crs,
        }
    return array, info


def _align_to_reference(
    path: Path,
    reference: dict[str, Any],
    resampling: Resampling,
) -> np.ndarray:
    array, source = _read_raster(path)
    if (
        array.shape == reference["shape"]
        and source["transform"] == reference["transform"]
        and source["crs"] == reference["crs"]
    ):
        return array

    if source["crs"] is None or reference["crs"] is None:
        raise ValueError(
            f"Cannot align rasters without CRS metadata: source={path}, "
            "reference=VIIRS overlap raster."
        )

    destination = np.full(reference["shape"], np.nan, dtype=np.float32)
    reproject(
        source=array,
        destination=destination,
        src_transform=source["transform"],
        src_crs=source["crs"],
        src_nodata=np.nan,
        dst_transform=reference["transform"],
        dst_crs=reference["crs"],
        dst_nodata=np.nan,
        resampling=resampling,
    )
    return destination


def _design(values: np.ndarray, scale: float, degree: int) -> np.ndarray:
    scaled = np.asarray(values, dtype=np.float64) / scale
    return np.column_stack([scaled**power for power in range(degree + 1)])


def _fit_mapping(
    dmsp_overlap: np.ndarray,
    viirs_overlap: np.ndarray,
    *,
    degree: int,
    max_training_pixels: int,
) -> tuple[np.ndarray, float, dict[str, Any]]:
    valid = (
        np.isfinite(dmsp_overlap)
        & np.isfinite(viirs_overlap)
        & (dmsp_overlap >= 0)
        & (viirs_overlap >= 0)
    )
    valid_indices = np.flatnonzero(valid)
    required = max(degree + 1, 8)
    if valid_indices.size < required:
        raise ValueError(
            f"Only {valid_indices.size} valid overlap pixels are available; "
            f"at least {required} are required for degree-{degree} fitting."
        )

    if valid_indices.size > max_training_pixels:
        sample_positions = np.linspace(
            0,
            valid_indices.size - 1,
            num=max_training_pixels,
            dtype=np.int64,
        )
        fit_indices = valid_indices[sample_positions]
    else:
        fit_indices = valid_indices

    dmsp_values = dmsp_overlap.ravel()[fit_indices].astype(np.float64)
    viirs_values = viirs_overlap.ravel()[fit_indices].astype(np.float64)
    scale = float(np.max(dmsp_values))
    if not np.isfinite(scale) or scale <= 0:
        raise ValueError("The overlap DMSP raster has no positive valid signal.")

    design = _design(dmsp_values, scale, degree)
    coefficients, residuals, rank, singular_values = np.linalg.lstsq(
        design,
        viirs_values,
        rcond=None,
    )
    fitted = design @ coefficients
    residual = fitted - viirs_values
    ss_res = float(np.sum(residual**2))
    ss_tot = float(np.sum((viirs_values - np.mean(viirs_values)) ** 2))
    diagnostics = {
        "valid_overlap_pixels": int(valid_indices.size),
        "fit_overlap_pixels": int(fit_indices.size),
        "input_scale": scale,
        "coefficients_scaled_dmsp_basis": [float(value) for value in coefficients],
        "matrix_rank": int(rank),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "r2": None if ss_tot == 0 else float(1.0 - ss_res / ss_tot),
        "dmsp_overlap_min": float(np.min(dmsp_values)),
        "dmsp_overlap_max": float(np.max(dmsp_values)),
        "viirs_overlap_min": float(np.min(viirs_values)),
        "viirs_overlap_max": float(np.max(viirs_values)),
        "singular_values": [float(value) for value in singular_values],
    }
    return coefficients.astype(np.float64), scale, diagnostics


def _predict_mapping(
    dmsp_array: np.ndarray,
    coefficients: np.ndarray,
    scale: float,
    *,
    degree: int,
    clip_min: Optional[float],
    clip_max: Optional[float],
) -> np.ndarray:
    flat = dmsp_array.astype(np.float64, copy=False).ravel()
    valid = np.isfinite(flat) & (flat >= 0)
    output = np.full(flat.shape, np.nan, dtype=np.float32)
    if valid.any():
        predicted = _design(flat[valid], scale, degree) @ coefficients
        if clip_min is not None:
            predicted = np.maximum(predicted, clip_min)
        if clip_max is not None:
            predicted = np.minimum(predicted, clip_max)
        output[valid] = predicted.astype(np.float32)
    return output.reshape(dmsp_array.shape)


def _write_raster(path: Path, array: np.ndarray, reference: dict[str, Any]) -> None:
    profile = dict(reference["profile"])
    profile.update(
        driver="GTiff",
        height=int(array.shape[0]),
        width=int(array.shape[1]),
        count=1,
        dtype="float32",
        nodata=OUTPUT_NODATA,
        compress="deflate",
    )
    # Small benchmark fixtures cannot always use tiled GeoTIFF blocks.
    for key in ("tiled", "blockxsize", "blockysize", "interleave", "predictor"):
        profile.pop(key, None)
    payload = np.where(np.isfinite(array), array, OUTPUT_NODATA).astype(np.float32)
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(payload, 1)


def _summary_row(year: int, source_sensor: str, path: Path, array: np.ndarray) -> dict[str, Any]:
    valid = np.isfinite(array)
    values = array[valid]
    if values.size == 0:
        return {
            "year": year,
            "source_sensor": source_sensor,
            "output_file": path.name,
            "valid_pixels": 0,
            "mean": None,
            "sum": None,
            "min": None,
            "max": None,
        }
    return {
        "year": year,
        "source_sensor": source_sensor,
        "output_file": path.name,
        "valid_pixels": int(values.size),
        "mean": float(np.mean(values)),
        "sum": float(np.sum(values)),
        "min": float(np.min(values)),
        "max": float(np.max(values)),
    }


def run_dmsp_viirs_harmonization(
    dmsp_folder: str,
    viirs_folder: str,
    output_folder: str,
    start_year: int,
    overlap_year: int = 2013,
    end_year: int = 2020,
    degree: int = 3,
    resampling: Literal["nearest", "bilinear", "average"] = "bilinear",
    clip_min: Optional[float] = 0.0,
    clip_max: Optional[float] = None,
    max_training_pixels: int = 1_000_000,
    output_prefix: str = "viirs_like_",
    summary_csv: str = "annual_statistics.csv",
) -> dict[str, Any]:
    """Run the first-version DMSP-to-VIIRS-like annual calibration."""
    try:
        _validate_parameters(
            start_year=start_year,
            overlap_year=overlap_year,
            end_year=end_year,
            degree=degree,
            clip_min=clip_min,
            clip_max=clip_max,
            output_prefix=output_prefix,
            summary_csv=summary_csv,
        )
        if resampling not in RESAMPLING_METHODS:
            raise ValueError(f"Unsupported resampling method: {resampling}")

        dmsp_paths = _discover_annual_rasters(_resolve_input_folder(dmsp_folder))
        viirs_paths = _discover_annual_rasters(_resolve_input_folder(viirs_folder))
        required_dmsp = list(range(start_year, overlap_year + 1))
        required_viirs = list(range(overlap_year, end_year + 1))
        missing_dmsp = [year for year in required_dmsp if year not in dmsp_paths]
        missing_viirs = [year for year in required_viirs if year not in viirs_paths]
        if missing_dmsp:
            raise FileNotFoundError(f"Missing DMSP annual rasters: {missing_dmsp}")
        if missing_viirs:
            raise FileNotFoundError(f"Missing VIIRS annual rasters: {missing_viirs}")

        output_dir = _resolve_output_folder(output_folder)
        reference_array, reference = _read_raster(viirs_paths[overlap_year])
        dmsp_overlap = _align_to_reference(
            dmsp_paths[overlap_year],
            reference,
            RESAMPLING_METHODS[resampling],
        )
        coefficients, scale, fit_diagnostics = _fit_mapping(
            dmsp_overlap,
            reference_array,
            degree=degree,
            max_training_pixels=max_training_pixels,
        )

        annual_rows: list[dict[str, Any]] = []
        annual_outputs: list[dict[str, Any]] = []

        for year in range(start_year, end_year + 1):
            output_path = output_dir / f"{output_prefix}{year}.tif"
            if year <= overlap_year:
                source_sensor = "DMSP"
                source_array = _align_to_reference(
                    dmsp_paths[year],
                    reference,
                    RESAMPLING_METHODS[resampling],
                )
                if year == overlap_year:
                    # Keep the observed VIIRS overlap frame as the public series
                    # value; the fitted DMSP frame remains in diagnostics.
                    source_sensor = "VIIRS_overlap"
                    output_array = reference_array
                else:
                    output_array = _predict_mapping(
                        source_array,
                        coefficients,
                        scale,
                        degree=degree,
                        clip_min=clip_min,
                        clip_max=clip_max,
                    )
            else:
                source_sensor = "VIIRS"
                output_array = _align_to_reference(
                    viirs_paths[year],
                    reference,
                    RESAMPLING_METHODS[resampling],
                )

            _write_raster(output_path, output_array, reference)
            annual_outputs.append(
                {
                    "year": year,
                    "source_sensor": source_sensor,
                    "path": str(output_path),
                }
            )
            annual_rows.append(_summary_row(year, source_sensor, output_path, output_array))

        summary_path = output_dir / summary_csv
        with summary_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "year",
                    "source_sensor",
                    "output_file",
                    "valid_pixels",
                    "mean",
                    "sum",
                    "min",
                    "max",
                ],
            )
            writer.writeheader()
            writer.writerows(annual_rows)

        overlap_prediction = _predict_mapping(
            dmsp_overlap,
            coefficients,
            scale,
            degree=degree,
            clip_min=clip_min,
            clip_max=clip_max,
        )
        overlap_valid = np.isfinite(overlap_prediction) & np.isfinite(reference_array)
        overlap_residual = overlap_prediction[overlap_valid] - reference_array[overlap_valid]
        overlap_ss_tot = float(
            np.sum((reference_array[overlap_valid] - np.mean(reference_array[overlap_valid])) ** 2)
        )
        overlap_ss_res = float(np.sum(overlap_residual**2))
        overlap_diagnostics = {
            "valid_pixels": int(np.count_nonzero(overlap_valid)),
            "rmse": float(np.sqrt(np.mean(overlap_residual**2))),
            "mae": float(np.mean(np.abs(overlap_residual))),
            "r2": None if overlap_ss_tot == 0 else float(1.0 - overlap_ss_res / overlap_ss_tot),
        }

        metadata = {
            "schema": METHOD_SCHEMA,
            "status": "success",
            "method": "overlap_period_polynomial_calibration",
            "direction": "DMSP_to_VIIRS_like",
            "degree": degree,
            "overlap_year": overlap_year,
            "year_range": {"start": start_year, "end": end_year},
            "historical_years": list(range(start_year, overlap_year)),
            "observed_viirs_years": list(range(overlap_year, end_year + 1)),
            "resampling": resampling,
            "clip": {"min": clip_min, "max": clip_max},
            "output_nodata": OUTPUT_NODATA,
            "target_grid": {
                "crs": None if reference["crs"] is None else str(reference["crs"]),
                "width": int(reference["shape"][1]),
                "height": int(reference["shape"][0]),
                "transform": [float(value) for value in tuple(reference["transform"])],
            },
            "fit": fit_diagnostics,
            "overlap_diagnostics": overlap_diagnostics,
            "method_provenance": {
                "reference_workflow": METHOD_SOURCE_URL,
                "implementation_note": (
                    "Independent minimal polynomial adapter; it does not copy "
                    "third-party source code or claim to reconstruct missing DMSP detail."
                ),
            },
            "outputs": {
                "annual_geotiffs": annual_outputs,
                "annual_statistics_csv": str(summary_path),
            },
            "limitations": [
                "VIIRS-like values are calibrated to the supplied overlap raster and inherit its units.",
                "Upsampling DMSP to the VIIRS grid does not create real sub-DMSP spatial detail.",
                "The tool does not perform satellite-specific DMSP inter-satellite calibration.",
                "Input rasters must already represent comparable annual composites and masks.",
            ],
        }
        metadata_path = output_dir / "harmonization_manifest.json"
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {
            "status": "success",
            "method": metadata["method"],
            "direction": metadata["direction"],
            "output_folder": str(output_dir),
            "annual_output_count": len(annual_outputs),
            "annual_statistics_csv": str(summary_path),
            "metadata_json": str(metadata_path),
            "overlap_diagnostics": overlap_diagnostics,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
            "method": "overlap_period_polynomial_calibration",
            "direction": "DMSP_to_VIIRS_like",
        }


dmsp_viirs_harmonization_tool = StructuredTool.from_function(
    func=run_dmsp_viirs_harmonization,
    name="DMSP_VIIRS_Harmonization",
    description=(
        "Convert historical annual DMSP-OLS rasters into a comparable VIIRS-like "
        "series using a deterministic degree-3 polynomial fitted on a supplied "
        "DMSP/VIIRS overlap year. DMSP is aligned to the VIIRS overlap grid; "
        "historical DMSP years are transformed, while observed VIIRS years are "
        "retained. Outputs annual GeoTIFFs, a statistics CSV, and a provenance "
        "manifest under outputs/. This is intensity harmonization only and does "
        "not invent spatial detail or replace a learned DMSP-to-VIIRS model."
    ),
    args_schema=DMSPVIIRSHarmonizationInput,
)


__all__ = [
    "DMSPVIIRSHarmonizationInput",
    "run_dmsp_viirs_harmonization",
    "dmsp_viirs_harmonization_tool",
]

