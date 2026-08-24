"""Compatibility adapters for NTL-GPT's legacy LangChain tools.

The application still exposes human-readable strings and Pydantic v1 schemas to
Deep Agents.  These helpers keep that public contract while routing local GIS
work through the shared, structured ``ntl_toolkit.core`` implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import rasterio

from ntl_toolkit.core import ntl as ntl_core
from ntl_toolkit.core import raster as raster_core
from ntl_toolkit.core import vector as vector_core
from ntl_toolkit.runtime import reserve_output_path
from ntl_toolkit.schemas import ToolResult


def _failure(result: ToolResult) -> str:
    message = result.error.message if result.error is not None else result.summary
    return f"Error: {message}"


def _output_name(path: str | Path) -> str:
    return Path(path).name


def _output_reference(path: str | Path) -> str:
    return f"outputs/{_output_name(path)}"


def _result_output(result: ToolResult, *, role: str = "primary") -> str | None:
    for output in result.outputs:
        if output.role == role:
            return output.path
    return result.outputs[0].path if result.outputs else None


def composite_local(
    raster_paths: Sequence[str | Path],
    output_path: str | Path,
    *,
    fallback_nodata: float | None = None,
) -> str:
    """Create a composite and preserve the legacy chat response format.

    ``fallback_nodata`` is applied only to sources that have no nodata metadata.
    """
    if fallback_nodata is not None:
        try:
            float(fallback_nodata)
        except (TypeError, ValueError):
            return "Error: fallback_nodata must be numeric when provided."

    result = ntl_core.composite_ntl_rasters(
        raster_paths,
        output_path,
        fallback_nodata=fallback_nodata,
    )
    if result.status != "succeeded":
        return _failure(result)

    coverage = float(result.metrics.get("coverage", 0.0))
    actual_output = _result_output(result) or output_path
    return (
        f"Success! Mean composite saved to '{_output_reference(actual_output)}'.\n"
        f"- Input files processed: {int(result.metrics.get('input_count', len(raster_paths)))}\n"
        f"- Effective pixel coverage: {coverage:.2%}"
    )


def zonal_statistics(
    *,
    raster_paths: Sequence[str | Path],
    vector_path: str | Path,
    output_path: str | Path,
    selected_indices: Sequence[str] | None,
    only_global: bool,
) -> str:
    """Run shared zonal statistics and render the previous LangChain output."""
    result = ntl_core.calculate_zonal_statistics(
        raster_paths=raster_paths,
        vector_path=vector_path,
        output_path=output_path,
        selected_indices=selected_indices,
        only_global=only_global,
    )
    if result.status != "succeeded":
        return _failure(result)

    actual_output = _result_output(result)
    if actual_output is None:
        return "Error: Statistics completed without a CSV output."

    frame = pd.read_csv(actual_output)
    global_rows = frame.loc[frame["Region"] == "Global_Summary"]
    metric_columns = [
        column
        for column in frame.columns
        if column not in {"Raster_file", "Year", "Region"}
    ]
    summary_blocks: list[str] = []
    for _, row in global_rows.iterrows():
        values = []
        for column in metric_columns:
            value = row[column]
            values.append(f"- {column}: {float(value):.4f}" if pd.notna(value) else f"- {column}: None")
        summary_blocks.append(f"[{row['Raster_file']}]\n" + "\n".join(values))
    summary = "\n\n".join(summary_blocks)
    total_feature_rows = int((frame["Region"] != "Global_Summary").sum())
    output_ref = _output_reference(actual_output)

    if len(raster_paths) == 1:
        if total_feature_rows <= 0:
            return (
                f"Results saved to: {output_ref}\n\n"
                f"**Global Summary (Total ROI):**\n{summary}\n"
                "Note: Detailed statistics for each sub-region are available in the generated CSV file."
            )
        return (
            f"Success: Analysis completed for {total_feature_rows} region rows.\n"
            f"Results saved to: {output_ref}\n\n"
            f"**Global Summary (Total ROI):**\n{summary}\n"
            "Note: Detailed statistics for each sub-region are available in the generated CSV file."
        )

    return (
        f"Success: Batch analysis completed for {len(raster_paths)} rasters.\n"
        f"Feature rows: {total_feature_rows}\n"
        f"Results saved to: {output_ref}\n\n"
        f"**Global Summary (Per Raster):**\n{summary}"
    )


def trend_analysis(
    raster_paths: Sequence[str | Path],
    vector_path: str | Path,
    output_prefix: str | Path,
) -> str:
    """Run the core trend calculation and retain the legacy output descriptions."""
    if len(raster_paths) < 3:
        return "Error: Trend analysis requires at least 3 time-series rasters."

    result = ntl_core.analyze_ntl_trend(raster_paths, vector_path, output_prefix)
    if result.status != "succeeded":
        return _failure(result)

    slope_output = _result_output(result, role="slope")
    pvalue_output = _result_output(result, role="pvalue")
    if slope_output is None or pvalue_output is None:
        return "Error: Trend analysis completed without both raster outputs."

    plot_output = reserve_output_path(
        Path(output_prefix).with_name(f"{Path(output_prefix).name}_trend_viz.png")
    )
    try:
        with rasterio.open(slope_output) as dataset:
            slope = dataset.read(1, masked=True)
        finite_values = slope.compressed()
        limit = float(pd.Series(finite_values).abs().quantile(0.98)) if finite_values.size else 1.0
        if not limit or not pd.notna(limit):
            limit = 1.0
        figure, axis = plt.subplots(figsize=(10, 7))
        image = axis.imshow(slope, cmap="RdYlBu_r", vmin=-limit, vmax=limit)
        figure.colorbar(image, ax=axis, label="Sen's Slope (Annual Change Rate)")
        axis.set_title(f"NTL Trend Analysis: {Path(output_prefix).name}\n(Mann-Kendall & Sen's Slope)")
        axis.set_xlabel("Pixel X")
        axis.set_ylabel("Pixel Y")
        figure.tight_layout()
        figure.savefig(plot_output, dpi=300, bbox_inches="tight")
        plt.close(figure)
    except (OSError, ValueError, rasterio.errors.RasterioError) as exc:
        return (
            f"Error: Trend raster outputs were created, but visualization generation failed: {exc}"
        )

    return (
        f"Masked trend analysis for '{Path(output_prefix).name}' completed.\n"
        f"- **Slope Map**: `{_output_reference(slope_output)}` (Rate of change)\n"
        f"- **P-Value Map**: `{_output_reference(pvalue_output)}` (Statistical significance)\n"
        f"- **Visualization**: `{_output_reference(plot_output)}` (Map preview)"
    )


def anomaly_detection(
    raster_paths: Sequence[str | Path],
    output_path: str | Path,
    *,
    target_index: int | None,
    k_sigma: float,
) -> str:
    """Run positive-spike detection through the shared NTL core."""
    result = ntl_core.detect_ntl_anomaly(
        raster_paths,
        output_path,
        target_index=target_index,
        k_sigma=k_sigma,
        minimum_baseline_observations=1,
    )
    if result.status != "succeeded":
        return _failure(result)

    output = _result_output(result, role="anomaly")
    resolved_target = int(result.metrics["target_index"])
    return (
        "Anomaly Detection Task Completed.\n"
        f"- **Target Image**: {Path(raster_paths[resolved_target]).name}\n"
        f"- **Method**: Pixel-wise Z-Score Analysis (Threshold: {float(k_sigma)}σ)\n"
        f"- **Result Saved**: `{_output_reference(output or output_path)}`"
    )


def raster_report(path: str | Path, *, mode: str, sample_pixels: int) -> dict[str, Any]:
    """Translate shared raster inspection metrics to the legacy report shape."""
    result = raster_core.inspect_raster(path, mode=mode, sample_pixels=sample_pixels)
    if result.status != "succeeded":
        raise RuntimeError(result.error.message if result.error is not None else result.summary)

    metrics = result.metrics
    bounds = metrics["bounds"]
    report: dict[str, Any] = {
        "path": str(path),
        "exists": True,
        "readable": True,
        "driver": metrics["driver"],
        "crs": metrics["crs"],
        "width": metrics["width"],
        "height": metrics["height"],
        "count_bands": metrics["band_count"],
        "dtype": metrics["dtype"],
        "resolution": tuple(metrics["resolution"]),
        "nodata": metrics["nodata"],
        "bounds": {"left": bounds[0], "bottom": bounds[1], "right": bounds[2], "top": bounds[3]},
    }
    if mode == "full":
        report["band1_stats"] = {
            "count_valid": metrics.get("valid_count", 0),
            "min": metrics.get("min"),
            "max": metrics.get("max"),
            "mean": metrics.get("mean"),
            "std": metrics.get("std"),
        }
        report["hints"] = metrics.get("hints", [])
    return report


def vector_report(path: str | Path, *, mode: str) -> dict[str, Any]:
    """Translate shared vector inspection metrics to the legacy report shape."""
    result = vector_core.inspect_vector(path)
    if result.status != "succeeded":
        raise RuntimeError(result.error.message if result.error is not None else result.summary)

    metrics = result.metrics
    bounds = metrics["bounds"]
    attributes = gpd.read_file(path)
    field_types = {
        column: str(attributes[column].dtype)
        for column in attributes.columns
        if column != attributes.geometry.name
    }
    try:
        import fiona

        with fiona.open(path) as source:
            properties = source.schema.get("properties", {}) if source.schema else {}
            if properties:
                field_types = {
                    str(name): str(field_type)
                    for name, field_type in dict(properties).items()
                }
    except Exception:
        pass
    report: dict[str, Any] = {
        "path": str(path),
        "exists": True,
        "readable": True,
        "crs": metrics["crs"],
        "feature_count": metrics["feature_count"],
        "geometry_types": metrics["geometry_types"],
        "bounds": {"minx": bounds[0], "miny": bounds[1], "maxx": bounds[2], "maxy": bounds[3]},
        "fields": field_types,
    }
    if mode == "full":
        report["sample_records"] = attributes.drop(columns=attributes.geometry.name).head(1).to_dict(orient="records")
    return report
