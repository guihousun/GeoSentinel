"""Deterministic 30-night VNP46A2 persistence classification.

The rule is intentionally fixed for benchmark case BV1-098.  It never derives
an adaptive radiance threshold: a QA-qualified finite radiance strictly above
zero is illuminated, and pixels with fewer than 24 valid observations are
classified as uncertain before any lit-count class is considered.
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

from storage_manager import storage_manager


REQUIRED_NIGHTS = 30
MINIMUM_VALID_OBSERVATIONS = 24
OUTPUT_NODATA = 255
CLASS_BACKGROUND = 0
CLASS_TRANSIENT = 1
CLASS_PERSISTENT = 2
CLASS_UNCERTAIN = 3
RULE_ID = "BV1-098-fixed-persistence-v1"

CLASS_NAMES = {
    CLASS_BACKGROUND: "background",
    CLASS_TRANSIENT: "transient",
    CLASS_PERSISTENT: "persistent",
    CLASS_UNCERTAIN: "uncertain",
}


class VNP46A2PersistenceInput(BaseModel):
    radiance_tif: str = Field(
        ...,
        description="Exactly 30 bands/nights of VNP46A2 radiance in workspace inputs/.",
    )
    qa_valid_tif: str = Field(
        ...,
        description=(
            "Exactly 30 bands/nights on the same grid. Finite nonzero values mark "
            "QA-qualified observations; zero or NoData marks invalid observations."
        ),
    )
    lit_count_tif: str = Field(
        "vnp46a2_lit_count.tif",
        description="Per-pixel count of QA-qualified finite radiance values above zero.",
    )
    class_tif: str = Field(
        "vnp46a2_persistence_class.tif",
        description="Class raster: 0 background, 1 transient, 2 persistent, 3 uncertain; NoData 255.",
    )
    uncertainty_tif: str = Field(
        "vnp46a2_uncertainty_mask.tif",
        description="Binary mask: 1 when fewer than 24 valid observations, otherwise 0; NoData 255.",
    )
    summary_json: str = Field(
        "vnp46a2_persistence.summary.json",
        description="Rule, grid, hashes, and class-count summary.",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _same_grid(left: rasterio.io.DatasetReader, right: rasterio.io.DatasetReader) -> bool:
    return (
        left.crs == right.crs
        and left.transform == right.transform
        and left.width == right.width
        and left.height == right.height
    )


def _valid_data(data: np.ndarray, nodata: Optional[float]) -> np.ndarray:
    valid = np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        valid &= data != nodata
    return valid


def _output_profile(source: rasterio.io.DatasetReader) -> dict:
    profile = source.profile.copy()
    if not profile.get("tiled"):
        profile.pop("blockxsize", None)
        profile.pop("blockysize", None)
    profile.update(dtype="uint8", count=1, nodata=OUTPUT_NODATA, compress="lzw")
    return profile


def _write_uint8_raster(
    path: Path,
    data: np.ndarray,
    profile: dict,
    *,
    band_description: str,
    tags: dict[str, str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, "w", **profile) as destination:
        destination.write(data.astype(np.uint8), 1)
        destination.set_band_description(1, band_description)
        destination.update_tags(**tags)


def classify_persistence_files(
    radiance_path: Path,
    qa_valid_path: Path,
    lit_count_path: Path,
    class_path: Path,
    uncertainty_path: Path,
    summary_path: Path,
) -> dict:
    """Apply the frozen BV1-098 rule to already-resolved file paths."""

    paths = {
        "radiance_tif": Path(radiance_path),
        "qa_valid_tif": Path(qa_valid_path),
        "lit_count_tif": Path(lit_count_path),
        "class_tif": Path(class_path),
        "uncertainty_tif": Path(uncertainty_path),
        "summary_json": Path(summary_path),
    }
    for key in ("radiance_tif", "qa_valid_tif"):
        if not paths[key].is_file():
            raise FileNotFoundError(f"{key} not found: {paths[key]}")

    output_paths = [paths[key].resolve() for key in ("lit_count_tif", "class_tif", "uncertainty_tif", "summary_json")]
    if len(set(output_paths)) != len(output_paths):
        raise ValueError("lit_count_tif, class_tif, uncertainty_tif, and summary_json must be distinct files")

    with rasterio.open(paths["radiance_tif"]) as radiance_source, rasterio.open(paths["qa_valid_tif"]) as qa_source:
        if radiance_source.count != REQUIRED_NIGHTS or qa_source.count != REQUIRED_NIGHTS:
            raise ValueError(
                f"radiance_tif and qa_valid_tif must each contain exactly {REQUIRED_NIGHTS} bands/nights"
            )
        if radiance_source.crs is None or qa_source.crs is None:
            raise ValueError("radiance_tif and qa_valid_tif must declare a CRS")
        if not _same_grid(radiance_source, qa_source):
            raise ValueError("qa_valid_tif must have the exact CRS, transform, width, and height of radiance_tif")

        radiance = radiance_source.read().astype(np.float64)
        qa = qa_source.read().astype(np.float64)
        radiance_is_data = _valid_data(radiance, radiance_source.nodata)
        qa_is_data = _valid_data(qa, qa_source.nodata)
        valid = radiance_is_data & qa_is_data & (qa != 0)
        illuminated = valid & (radiance > 0)

        valid_count = np.sum(valid, axis=0, dtype=np.uint8)
        lit_count = np.sum(illuminated, axis=0, dtype=np.uint8)
        uncertain = valid_count < MINIMUM_VALID_OBSERVATIONS

        classes = np.full(valid_count.shape, OUTPUT_NODATA, dtype=np.uint8)
        certain = ~uncertain
        classes[certain & (lit_count == 0)] = CLASS_BACKGROUND
        classes[certain & (lit_count >= 1) & (lit_count <= 23)] = CLASS_TRANSIENT
        classes[certain & (lit_count >= 24)] = CLASS_PERSISTENT
        classes[uncertain] = CLASS_UNCERTAIN
        uncertainty_mask = uncertain.astype(np.uint8)

        if np.any(classes == OUTPUT_NODATA):
            raise RuntimeError("internal classification error left one or more pixels unclassified")

        profile = _output_profile(radiance_source)
        grid = {
            "crs": str(radiance_source.crs),
            "transform": [float(value) for value in list(radiance_source.transform)[:6]],
            "width": int(radiance_source.width),
            "height": int(radiance_source.height),
            "band_count": int(radiance_source.count),
        }

    common_tags = {
        "rule_id": RULE_ID,
        "required_nights": str(REQUIRED_NIGHTS),
        "minimum_valid_observations": str(MINIMUM_VALID_OBSERVATIONS),
        "illuminated_test": "qa_valid_and_finite_radiance_gt_0",
        "adaptive_threshold": "false",
    }
    _write_uint8_raster(
        paths["lit_count_tif"],
        lit_count,
        profile,
        band_description="qa_qualified_illuminated_night_count",
        tags={**common_tags, "value_range": "0-30", "nodata": str(OUTPUT_NODATA)},
    )
    _write_uint8_raster(
        paths["class_tif"],
        classes,
        profile,
        band_description="fixed_30_night_persistence_class",
        tags={
            **common_tags,
            "class_0": "background",
            "class_1": "transient",
            "class_2": "persistent",
            "class_3": "uncertain",
            "nodata_class": str(OUTPUT_NODATA),
        },
    )
    _write_uint8_raster(
        paths["uncertainty_tif"],
        uncertainty_mask,
        profile,
        band_description="insufficient_valid_observations",
        tags={**common_tags, "class_0": "sufficient", "class_1": "uncertain", "nodata_class": str(OUTPUT_NODATA)},
    )

    class_counts = {
        name: int(np.count_nonzero(classes == code))
        for code, name in CLASS_NAMES.items()
    }
    summary = {
        "schema": "ntl_gpt.vnp46a2_persistence.v1",
        "status": "success",
        "rule": {
            "id": RULE_ID,
            "required_nights": REQUIRED_NIGHTS,
            "qa_valid_test": "finite QA value not equal to NoData and not equal to zero",
            "illuminated_test": "QA-qualified finite radiance > 0",
            "minimum_valid_observations": MINIMUM_VALID_OBSERVATIONS,
            "priority": ["uncertain", "persistent", "transient", "background"],
            "classes": {
                "0": "background: 0 illuminated nights and at least 24 valid observations",
                "1": "transient: 1-23 illuminated nights and at least 24 valid observations",
                "2": "persistent: at least 24 illuminated nights and at least 24 valid observations",
                "3": "uncertain: fewer than 24 valid observations",
                str(OUTPUT_NODATA): "output NoData",
            },
            "adaptive_threshold": False,
        },
        "grid": grid,
        "counts": {
            "total_pixels": int(classes.size),
            "class_pixels": class_counts,
            "uncertain_pixels": int(np.count_nonzero(uncertainty_mask == 1)),
            "valid_observation_count_min": int(valid_count.min()),
            "valid_observation_count_max": int(valid_count.max()),
            "illuminated_night_count_min": int(lit_count.min()),
            "illuminated_night_count_max": int(lit_count.max()),
        },
        "inputs": {
            "radiance": {"file": paths["radiance_tif"].name, "sha256": _sha256(paths["radiance_tif"])},
            "qa_valid": {"file": paths["qa_valid_tif"].name, "sha256": _sha256(paths["qa_valid_tif"])},
        },
        "outputs": {
            "lit_count": {"file": paths["lit_count_tif"].name, "sha256": _sha256(paths["lit_count_tif"])},
            "class": {"file": paths["class_tif"].name, "sha256": _sha256(paths["class_tif"])},
            "uncertainty": {"file": paths["uncertainty_tif"].name, "sha256": _sha256(paths["uncertainty_tif"])},
        },
    }
    paths["summary_json"].parent.mkdir(parents=True, exist_ok=True)
    paths["summary_json"].write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def classify_vnp46a2_persistence(
    radiance_tif: str,
    qa_valid_tif: str,
    lit_count_tif: str = "vnp46a2_lit_count.tif",
    class_tif: str = "vnp46a2_persistence_class.tif",
    uncertainty_tif: str = "vnp46a2_uncertainty_mask.tif",
    summary_json: str = "vnp46a2_persistence.summary.json",
    config: Optional[RunnableConfig] = None,
) -> dict:
    """Resolve thread-scoped paths and apply the fixed persistence rule."""

    thread_id = storage_manager.get_thread_id_from_config(config) if config else None
    try:
        resolved = {
            "radiance": Path(storage_manager.resolve_input_path(radiance_tif, thread_id)),
            "qa": Path(storage_manager.resolve_input_path(qa_valid_tif, thread_id)),
            "lit": Path(storage_manager.resolve_output_path(lit_count_tif, thread_id)),
            "class": Path(storage_manager.resolve_output_path(class_tif, thread_id)),
            "uncertainty": Path(storage_manager.resolve_output_path(uncertainty_tif, thread_id)),
            "summary": Path(storage_manager.resolve_output_path(summary_json, thread_id)),
        }
        result = classify_persistence_files(
            resolved["radiance"],
            resolved["qa"],
            resolved["lit"],
            resolved["class"],
            resolved["uncertainty"],
            resolved["summary"],
        )
        return {
            **result,
            "resolved_outputs": {
                "lit_count_tif": str(resolved["lit"]),
                "class_tif": str(resolved["class"]),
                "uncertainty_tif": str(resolved["uncertainty"]),
                "summary_json": str(resolved["summary"]),
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "schema": "ntl_gpt.vnp46a2_persistence.v1",
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }


VNP46A2_persistence_classification_tool = StructuredTool.from_function(
    func=classify_vnp46a2_persistence,
    name="VNP46A2_persistence_classification_tool",
    description=(
        "Classify an exactly 30-night VNP46A2 radiance stack with a matching QA-valid stack. "
        "The frozen rule uses radiance > 0, requires 24 valid observations, assigns uncertain first, "
        "and writes lit-count, class, uncertainty, and auditable summary artifacts without an adaptive threshold."
    ),
    args_schema=VNP46A2PersistenceInput,
)


__all__ = [
    "CLASS_BACKGROUND",
    "CLASS_PERSISTENT",
    "CLASS_TRANSIENT",
    "CLASS_UNCERTAIN",
    "MINIMUM_VALID_OBSERVATIONS",
    "OUTPUT_NODATA",
    "REQUIRED_NIGHTS",
    "RULE_ID",
    "VNP46A2PersistenceInput",
    "VNP46A2_persistence_classification_tool",
    "classify_persistence_files",
    "classify_vnp46a2_persistence",
]

