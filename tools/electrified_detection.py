"""Deterministic nighttime-light proxy for electricity-access population.

The threshold follows Liu et al. (2024): the midpoint between the maximum
nighttime-light value in non-electrified calibration samples and the minimum
value in electrified calibration samples.  The result is explicitly a
nighttime-light proxy; it is not an official SDG 7.1.1 estimate.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field
from rasterio.warp import Resampling, reproject

from storage_manager import storage_manager


METHOD_DOI = "https://doi.org/10.1016/j.rse.2024.114079"
OUTPUT_NODATA = 255


class ElectrifiedDetectionInput(BaseModel):
    input_tif: str = Field(
        ...,
        description="Nighttime-light GeoTIFF in the current workspace inputs/.",
    )
    sample_labels_tif: str = Field(
        ...,
        description=(
            "Calibration-label GeoTIFF on the exact NTL grid: 0 marks "
            "non-electrified samples, 1 marks electrified samples, and NoData "
            "marks non-sample pixels."
        ),
    )
    population_tif: str = Field(
        ...,
        description=(
            "Population-count GeoTIFF in inputs/. The binary electricity mask "
            "is transferred to this grid with nearest-neighbour resampling."
        ),
    )
    output_tif: str = Field(
        "electrified_mask.tif",
        description="0/1 electricity-proxy mask written under outputs/; NoData is 255.",
    )
    population_access_tif: str = Field(
        "population_with_electricity_proxy.tif",
        description="Population-count raster retained only under the electricity-proxy mask.",
    )
    metadata_json: str = Field(
        "electrified_population.metadata.json",
        description="Threshold, provenance, grid, input hashes, and population totals under outputs/.",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _valid_mask(data: np.ndarray, nodata: Optional[float]) -> np.ndarray:
    valid = np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        valid &= data != nodata
    return valid


def _calibration_threshold(ntl: np.ndarray, valid: np.ndarray, labels: np.ndarray, label_nodata: Optional[float]) -> dict:
    label_valid = np.isfinite(labels)
    if label_nodata is not None and np.isfinite(label_nodata):
        label_valid &= labels != label_nodata
    non_electrified = ntl[valid & label_valid & (labels == 0)]
    electrified = ntl[valid & label_valid & (labels == 1)]
    if non_electrified.size == 0 or electrified.size == 0:
        raise ValueError("sample_labels_tif must contain at least one valid sample from each class 0 and 1")
    maximum_non_electrified = float(np.max(non_electrified))
    minimum_electrified = float(np.min(electrified))
    if maximum_non_electrified >= minimum_electrified:
        raise ValueError(
            "calibration samples overlap: maximum non-electrified NTL must be smaller than minimum electrified NTL"
        )
    return {
        "maximum_non_electrified_ntl": maximum_non_electrified,
        "minimum_electrified_ntl": minimum_electrified,
        "threshold": (maximum_non_electrified + minimum_electrified) / 2.0,
        "non_electrified_sample_count": int(non_electrified.size),
        "electrified_sample_count": int(electrified.size),
    }


def _same_grid(left: rasterio.io.DatasetReader, right: rasterio.io.DatasetReader) -> bool:
    return (
        left.crs == right.crs
        and left.transform == right.transform
        and left.width == right.width
        and left.height == right.height
    )


def detect_electrified_areas_by_thresholding(
    input_tif: str,
    sample_labels_tif: str,
    population_tif: str,
    output_tif: str = "electrified_mask.tif",
    population_access_tif: str = "population_with_electricity_proxy.tif",
    metadata_json: str = "electrified_population.metadata.json",
    config: Optional[RunnableConfig] = None,
) -> dict:
    """Classify the electricity-access proxy and aggregate population deterministically."""

    thread_id = storage_manager.get_thread_id_from_config(config) if config else None
    ntl_path = Path(storage_manager.resolve_input_path(input_tif, thread_id))
    labels_path = Path(storage_manager.resolve_input_path(sample_labels_tif, thread_id))
    population_path = Path(storage_manager.resolve_input_path(population_tif, thread_id))
    mask_path = Path(storage_manager.resolve_output_path(output_tif, thread_id))
    population_output_path = Path(storage_manager.resolve_output_path(population_access_tif, thread_id))
    metadata_path = Path(storage_manager.resolve_output_path(metadata_json, thread_id))

    try:
        for path, label in (
            (ntl_path, "input_tif"),
            (labels_path, "sample_labels_tif"),
            (population_path, "population_tif"),
        ):
            if not path.is_file():
                raise FileNotFoundError(f"{label} not found in workspace inputs/: {path.name}")

        with rasterio.open(ntl_path) as ntl_src, rasterio.open(labels_path) as label_src:
            if ntl_src.count != 1 or label_src.count != 1:
                raise ValueError("input_tif and sample_labels_tif must each contain exactly one band")
            if not _same_grid(ntl_src, label_src):
                raise ValueError("sample_labels_tif must have the exact CRS, transform, width, and height of input_tif")
            ntl = ntl_src.read(1).astype(np.float64)
            labels = label_src.read(1).astype(np.float64)
            ntl_valid = _valid_mask(ntl, ntl_src.nodata)
            calibration = _calibration_threshold(ntl, ntl_valid, labels, label_src.nodata)
            threshold = float(calibration["threshold"])

            electricity_mask = np.full(ntl.shape, OUTPUT_NODATA, dtype=np.uint8)
            electricity_mask[ntl_valid] = 0
            electricity_mask[ntl_valid & (ntl >= threshold)] = 1
            mask_profile = ntl_src.profile.copy()
            if not mask_profile.get("tiled"):
                mask_profile.pop("blockxsize", None)
                mask_profile.pop("blockysize", None)
            mask_profile.update(dtype="uint8", count=1, nodata=OUTPUT_NODATA, compress="lzw")
            mask_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(mask_path, "w", **mask_profile) as dst:
                dst.write(electricity_mask, 1)
                dst.set_band_description(1, "electricity_access_proxy")
                dst.update_tags(
                    threshold=str(threshold),
                    method_doi=METHOD_DOI,
                    class_0="non_electrified_proxy",
                    class_1="electrified_proxy",
                    nodata_class=str(OUTPUT_NODATA),
                )

            ntl_grid = {
                "crs": str(ntl_src.crs),
                "transform": list(ntl_src.transform)[:6],
                "width": ntl_src.width,
                "height": ntl_src.height,
            }

        with rasterio.open(mask_path) as mask_src, rasterio.open(population_path) as population_src:
            if population_src.count != 1:
                raise ValueError("population_tif must contain exactly one band")
            if mask_src.crs is None or population_src.crs is None:
                raise ValueError("input and population rasters must declare a CRS")
            population = population_src.read(1).astype(np.float64)
            population_valid = _valid_mask(population, population_src.nodata) & (population >= 0)
            mask_on_population = np.full(population.shape, OUTPUT_NODATA, dtype=np.uint8)
            reproject(
                source=mask_src.read(1),
                destination=mask_on_population,
                src_transform=mask_src.transform,
                src_crs=mask_src.crs,
                src_nodata=OUTPUT_NODATA,
                dst_transform=population_src.transform,
                dst_crs=population_src.crs,
                dst_nodata=OUTPUT_NODATA,
                resampling=Resampling.nearest,
            )
            accessible = population_valid & (mask_on_population == 1)
            population_with_access = np.full(population.shape, np.nan, dtype=np.float32)
            population_with_access[accessible] = population[accessible].astype(np.float32)
            total_population = float(np.sum(population[population_valid], dtype=np.float64))
            accessible_population = float(np.sum(population[accessible], dtype=np.float64))
            access_share = accessible_population / total_population if total_population > 0 else None

            population_profile = population_src.profile.copy()
            if not population_profile.get("tiled"):
                population_profile.pop("blockxsize", None)
                population_profile.pop("blockysize", None)
            population_profile.update(dtype="float32", count=1, nodata=np.nan, compress="lzw")
            population_output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(population_output_path, "w", **population_profile) as dst:
                dst.write(population_with_access, 1)
                dst.set_band_description(1, "population_with_electricity_access_proxy")

            population_grid = {
                "crs": str(population_src.crs),
                "transform": list(population_src.transform)[:6],
                "width": population_src.width,
                "height": population_src.height,
                "valid_population_pixel_count": int(np.count_nonzero(population_valid)),
                "accessible_population_pixel_count": int(np.count_nonzero(accessible)),
            }

        metadata = {
            "schema": "ntl_gpt.electrified_population_proxy.v1",
            "status": "success",
            "method": {
                "reference": METHOD_DOI,
                "threshold_formula": "(max(non_electrified_samples) + min(electrified_samples)) / 2",
                "comparison": "NTL >= threshold",
                "mask_resampling_to_population_grid": "nearest",
                "interpretation": "nighttime-light-detectable electricity-access population proxy; not an official SDG 7.1.1 estimate",
            },
            "calibration": calibration,
            "population": {
                "total_population": total_population,
                "population_with_electricity_access_proxy": accessible_population,
                "population_access_proxy_share": access_share,
                "negative_or_nodata_population_excluded": True,
            },
            "grids": {"ntl_and_samples": ntl_grid, "population": population_grid},
            "inputs": {
                "ntl": {"path": input_tif, "sha256": _sha256(ntl_path)},
                "sample_labels": {"path": sample_labels_tif, "sha256": _sha256(labels_path)},
                "population": {"path": population_tif, "sha256": _sha256(population_path)},
            },
            "outputs": {
                "electricity_mask": str(mask_path),
                "population_access_raster": str(population_output_path),
                "metadata": str(metadata_path),
            },
        }
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return metadata
    except Exception as exc:  # noqa: BLE001
        return {
            "schema": "ntl_gpt.electrified_population_proxy.v1",
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }


electrified_detection_tool = StructuredTool.from_function(
    func=detect_electrified_areas_by_thresholding,
    name="Detect_Electrified_Areas_by_Thresholding",
    description=(
        "Estimate a nighttime-light-detectable electricity-access population proxy. "
        "It derives the Liu et al. (2024) midpoint threshold from a frozen 0/1 calibration-sample raster, "
        "classifies valid NTL pixels, transfers the mask to a LandScan-style population grid with nearest-neighbour "
        "resampling, and writes the mask, population raster, and auditable metadata."
    ),
    args_schema=ElectrifiedDetectionInput,
)


__all__ = [
    "ElectrifiedDetectionInput",
    "detect_electrified_areas_by_thresholding",
    "electrified_detection_tool",
]

