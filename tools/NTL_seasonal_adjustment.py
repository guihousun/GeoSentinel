"""Deterministic annual seasonal adjustment for daily VNP46A2 ANTL series.

The model follows the single-term harmonic regression used by Li et al.
(2022), with a linear trend and an explicit two-pass MAD outlier rule.  This
module is intentionally local and CSV-based: Earth Engine/download tools
prepare the daily series, while this tool performs the reproducible numerical
adjustment in the current thread workspace.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field

from storage_manager import storage_manager


DEFAULT_PERIOD_DAYS = 365.25
DEFAULT_OUTLIER_THRESHOLD = 3.0
DEFAULT_MIN_VALID_DAYS = 730
FILL_VALUES = {-999.9, -999.0, -9999.0}
REFERENCE_DOI = "https://doi.org/10.1016/j.rse.2022.113269"


class VNP46A2SeasonalAdjustmentInput(BaseModel):
    input_csv_path: str = Field(
        ...,
        description=(
            "Daily VNP46A2 ANTL CSV in the current workspace inputs/. "
            "Required columns are date and antl; optional C2 QA columns are "
            "Mandatory_Quality_Flag, QF_Cloud_Mask, Snow_Flag, and is_valid."
        ),
    )
    output_csv_path: str = Field(
        "vnp46a2_seasonal_adjusted.csv",
        description="CSV filename written under the current workspace outputs/.",
    )
    date_column: str = Field("date", description="Column containing daily dates.")
    value_column: str = Field("antl", description="Column containing daily ANTL values.")
    fit_start_date: Optional[str] = Field(
        None,
        description="Optional inclusive reference-period start date (YYYY-MM-DD).",
    )
    fit_end_date: Optional[str] = Field(
        None,
        description="Optional inclusive reference-period end date (YYYY-MM-DD).",
    )
    output_start_date: Optional[str] = Field(
        None,
        description="Optional inclusive output start date (YYYY-MM-DD).",
    )
    output_end_date: Optional[str] = Field(
        None,
        description="Optional inclusive output end date (YYYY-MM-DD).",
    )
    period_days: float = Field(
        DEFAULT_PERIOD_DAYS,
        description="Annual period in days. The standard method fixes this at 365.25.",
    )
    outlier_threshold: float = Field(
        DEFAULT_OUTLIER_THRESHOLD,
        description="MAD threshold multiplier for the deterministic outlier rule.",
    )
    min_valid_days: int = Field(
        DEFAULT_MIN_VALID_DAYS,
        description="Minimum quality-valid reference observations required for fitting.",
    )
    fill_missing: bool = Field(
        True,
        description=(
            "Fill missing/invalid/outlier output dates with the fitted trend baseline "
            "and mark them with is_filled; set false to retain NaN adjusted values."
        ),
    )


def _parse_optional_date(value: Optional[str], name: str) -> Optional[pd.Timestamp]:
    if value is None or not str(value).strip():
        return None
    try:
        return pd.Timestamp(value).normalize()
    except Exception as exc:  # pragma: no cover - pandas exception text varies
        raise ValueError(f"{name} must be an ISO date (YYYY-MM-DD): {value!r}") from exc


def _boolean_series(series: pd.Series) -> pd.Series:
    """Parse CSV booleans without treating the string 'false' as truthy."""

    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    normalized = series.astype("string").str.strip().str.lower()
    true_values = {"1", "true", "t", "yes", "y", "valid", "clear"}
    return normalized.isin(true_values)


def _initial_quality_mask(frame: pd.DataFrame, value_column: str) -> tuple[pd.Series, list[str]]:
    values = pd.to_numeric(frame[value_column], errors="coerce")
    valid = np.isfinite(values.to_numpy(dtype=float))
    valid &= ~values.isin(FILL_VALUES).to_numpy()
    applied: list[str] = ["finite_value_and_fill_value_filter"]

    for name in ("is_valid", "quality_valid"):
        if name in frame.columns:
            valid &= _boolean_series(frame[name]).to_numpy()
            applied.append(f"{name}=true")
            break

    if "Mandatory_Quality_Flag" in frame.columns:
        quality = pd.to_numeric(frame["Mandatory_Quality_Flag"], errors="coerce")
        valid &= (quality.to_numpy(dtype=float) == 0)
        applied.append("Mandatory_Quality_Flag=0")

    if "QF_Cloud_Mask" in frame.columns:
        cloud_mask = pd.to_numeric(frame["QF_Cloud_Mask"], errors="coerce")
        cloud_values = cloud_mask.fillna(-1).to_numpy(dtype=np.int64)
        cloud_class = (cloud_values >> 6) & 0b11
        valid &= cloud_class == 0
        applied.append("QF_Cloud_Mask_bits_6_7=00")

    if "Snow_Flag" in frame.columns:
        snow = pd.to_numeric(frame["Snow_Flag"], errors="coerce")
        valid &= (snow.to_numpy(dtype=float) == 0)
        applied.append("Snow_Flag=0")

    return pd.Series(valid, index=frame.index, dtype=bool), applied


def _design_matrix(day_index: np.ndarray, period_days: float) -> np.ndarray:
    phase = 2.0 * np.pi * day_index / period_days
    return np.column_stack(
        [
            np.ones(day_index.shape[0], dtype=float),
            day_index.astype(float),
            np.cos(phase),
            np.sin(phase),
        ]
    )


def _fit_ols(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    coefficients, _, rank, _ = np.linalg.lstsq(x, y, rcond=None)
    if rank < x.shape[1]:
        raise ValueError("reference series does not identify trend and annual harmonic terms")
    return coefficients


def _adjust_frame(
    source: pd.DataFrame,
    *,
    date_column: str,
    value_column: str,
    fit_start_date: Optional[str],
    fit_end_date: Optional[str],
    output_start_date: Optional[str],
    output_end_date: Optional[str],
    period_days: float,
    outlier_threshold: float,
    min_valid_days: int,
    fill_missing: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if date_column not in source.columns:
        raise ValueError(f"missing required date column: {date_column}")
    if value_column not in source.columns:
        alternatives = [name for name in ("antl", "DNB_BRDF_Corrected_NTL", "value") if name in source.columns]
        if len(alternatives) == 1:
            value_column = alternatives[0]
        else:
            raise ValueError(f"missing required value column: {value_column}")

    dates = pd.to_datetime(source[date_column], errors="coerce")
    if dates.isna().any():
        raise ValueError(f"{date_column} contains invalid dates")
    if getattr(dates.dt, "tz", None) is not None:
        dates = dates.dt.tz_localize(None)
    dates = dates.dt.normalize()
    if dates.duplicated().any():
        duplicate_dates = dates[dates.duplicated()].dt.strftime("%Y-%m-%d").tolist()
        raise ValueError(f"duplicate daily dates are not allowed: {duplicate_dates[:3]}")

    frame = source.copy()
    frame[date_column] = dates
    frame["_quality_valid"] = _initial_quality_mask(frame, value_column)[0]
    frame["_raw_value"] = pd.to_numeric(frame[value_column], errors="coerce")
    frame = frame.set_index(date_column).sort_index()

    fit_start = _parse_optional_date(fit_start_date, "fit_start_date")
    fit_end = _parse_optional_date(fit_end_date, "fit_end_date")
    output_start = _parse_optional_date(output_start_date, "output_start_date")
    output_end = _parse_optional_date(output_end_date, "output_end_date")
    if fit_start is not None and fit_end is not None and fit_start > fit_end:
        raise ValueError("fit_start_date must not be after fit_end_date")
    if output_start is not None and output_end is not None and output_start > output_end:
        raise ValueError("output_start_date must not be after output_end_date")

    calendar_start = min(frame.index.min(), fit_start or frame.index.min(), output_start or frame.index.min())
    calendar_end = max(frame.index.max(), fit_end or frame.index.max(), output_end or frame.index.max())
    calendar = pd.date_range(calendar_start, calendar_end, freq="D")
    full = frame.reindex(calendar)
    full.index.name = date_column
    full["_raw_value"] = pd.to_numeric(full["_raw_value"], errors="coerce")
    full["_quality_valid"] = full["_quality_valid"].astype("boolean").fillna(False).astype(bool)
    full["is_missing_date"] = ~full.index.isin(frame.index)
    full["is_quality_valid"] = full["_quality_valid"] & full["_raw_value"].notna()
    full["is_outlier"] = False
    full["is_filled"] = False

    origin = calendar[0]
    day_index = (calendar - origin).days.to_numpy(dtype=float)
    fit_mask = pd.Series(True, index=calendar)
    if fit_start is not None:
        fit_mask &= calendar >= fit_start
    if fit_end is not None:
        fit_mask &= calendar <= fit_end
    fit_mask &= full["is_quality_valid"]
    valid_count = int(fit_mask.sum())
    if valid_count < max(int(min_valid_days), 4):
        raise ValueError(
            f"insufficient quality-valid reference observations: {valid_count}; "
            f"minimum is {min_valid_days}"
        )

    fit_positions = np.flatnonzero(fit_mask.to_numpy())
    x_fit = _design_matrix(day_index[fit_positions], period_days)
    y_fit = full.iloc[fit_positions]["_raw_value"].to_numpy(dtype=float)
    first_coefficients = _fit_ols(x_fit, y_fit)
    first_residuals = y_fit - x_fit @ first_coefficients
    residual_median = float(np.median(first_residuals))
    mad = float(np.median(np.abs(first_residuals - residual_median)))
    threshold = float(outlier_threshold) * 1.4826 * mad
    if mad == 0.0:
        reference_inliers = np.ones(first_residuals.shape[0], dtype=bool)
    else:
        reference_inliers = np.abs(first_residuals - residual_median) <= threshold
    if int(reference_inliers.sum()) < 4:
        raise ValueError("MAD rule rejected too many reference observations")
    coefficients = _fit_ols(x_fit[reference_inliers], y_fit[reference_inliers])

    x_all = _design_matrix(day_index, period_days)
    prediction = x_all @ coefficients
    all_residuals = full["_raw_value"].to_numpy(dtype=float) - prediction
    if mad == 0.0:
        outlier_mask = np.zeros(len(full), dtype=bool)
    else:
        outlier_mask = (
            full["is_quality_valid"].to_numpy()
            & np.isfinite(all_residuals)
            & (np.abs(all_residuals - residual_median) > threshold)
        )
    full["is_outlier"] = outlier_mask

    seasonal = coefficients[2] * x_all[:, 2] + coefficients[3] * x_all[:, 3]
    trend = coefficients[0] + coefficients[1] * day_index
    adjusted = full["_raw_value"].to_numpy(dtype=float) - seasonal
    fill_mask = (~full["is_quality_valid"].to_numpy()) | outlier_mask
    if fill_missing:
        adjusted[fill_mask] = trend[fill_mask]
    else:
        adjusted[fill_mask] = np.nan
    full["seasonal_component"] = seasonal
    full["trend"] = trend
    full["adjusted_antl"] = adjusted
    full["is_filled"] = fill_mask
    full["fit_reference"] = fit_mask.to_numpy()

    if output_start is not None or output_end is not None:
        output_mask = pd.Series(True, index=calendar)
        if output_start is not None:
            output_mask &= calendar >= output_start
        if output_end is not None:
            output_mask &= calendar <= output_end
        full = full.loc[output_mask]

    output = full.reset_index()
    output = output.rename(columns={"_raw_value": "raw_antl"})
    output = output.drop(columns=["_quality_valid"], errors="ignore")
    metadata = {
        "method": "single_term_harmonic_regression",
        "reference": "Li et al. (2022), Eq. 1",
        "reference_doi": REFERENCE_DOI,
        "period_days": float(period_days),
        "harmonic_order": 1,
        "trend_order": 1,
        "outlier_rule": "two_pass_ols_mad",
        "outlier_threshold": float(outlier_threshold),
        "residual_mad": mad,
        "residual_threshold": threshold,
        "fit_start_date": fit_start.strftime("%Y-%m-%d") if fit_start is not None else None,
        "fit_end_date": fit_end.strftime("%Y-%m-%d") if fit_end is not None else None,
        "output_start_date": output_start.strftime("%Y-%m-%d") if output_start is not None else None,
        "output_end_date": output_end.strftime("%Y-%m-%d") if output_end is not None else None,
        "reference_valid_count": valid_count,
        "reference_inlier_count": int(reference_inliers.sum()),
        "output_row_count": int(len(output)),
        "output_missing_or_invalid_count": int((~output["is_quality_valid"]).sum()),
        "output_outlier_count": int(output["is_outlier"].sum()),
        "output_filled_count": int(output["is_filled"].sum()),
        "quality_columns_applied": _initial_quality_mask(frame.reset_index(), value_column)[1],
        "fill_missing": bool(fill_missing),
    }
    return output, metadata


def run_vnp46a2_seasonal_adjustment(
    input_csv_path: str,
    output_csv_path: str = "vnp46a2_seasonal_adjusted.csv",
    date_column: str = "date",
    value_column: str = "antl",
    fit_start_date: Optional[str] = None,
    fit_end_date: Optional[str] = None,
    output_start_date: Optional[str] = None,
    output_end_date: Optional[str] = None,
    period_days: float = DEFAULT_PERIOD_DAYS,
    outlier_threshold: float = DEFAULT_OUTLIER_THRESHOLD,
    min_valid_days: int = DEFAULT_MIN_VALID_DAYS,
    fill_missing: bool = True,
) -> dict[str, Any]:
    """Apply deterministic annual seasonal adjustment to a daily ANTL CSV."""

    try:
        if period_days <= 0:
            raise ValueError("period_days must be positive")
        if outlier_threshold <= 0:
            raise ValueError("outlier_threshold must be positive")
        if min_valid_days < 4:
            raise ValueError("min_valid_days must be at least 4")
        input_path = Path(storage_manager.resolve_input_path(input_csv_path))
        output_path = Path(storage_manager.resolve_output_path(output_csv_path))
        if not input_path.is_file():
            raise FileNotFoundError(f"input CSV not found: {input_csv_path}")
        source = pd.read_csv(input_path)
        output, metadata = _adjust_frame(
            source,
            date_column=date_column,
            value_column=value_column,
            fit_start_date=fit_start_date,
            fit_end_date=fit_end_date,
            output_start_date=output_start_date,
            output_end_date=output_end_date,
            period_days=period_days,
            outlier_threshold=outlier_threshold,
            min_valid_days=min_valid_days,
            fill_missing=fill_missing,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output.to_csv(output_path, index=False, date_format="%Y-%m-%d", float_format="%.12g")
        metadata_path = output_path.with_suffix(".json")
        metadata.update(
            {
                "status": "success",
                "input_csv": str(input_csv_path),
                "output_csv": str(output_path),
                "metadata_json": str(metadata_path),
            }
        )
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return metadata
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
            "input_csv": str(input_csv_path),
        }


VNP46A2_seasonal_adjustment_tool = StructuredTool.from_function(
    func=run_vnp46a2_seasonal_adjustment,
    name="VNP46A2_seasonal_adjustment_tool",
    description=(
        "Apply a deterministic annual seasonal adjustment to a daily VNP46A2 ANTL CSV. "
        "The method follows Li et al. (2022), Eq. 1: one annual harmonic (period 365.25) "
        "plus a linear trend, fitted with two-pass OLS/MAD outlier handling. "
        "It creates a complete calendar, applies available VNP46A2 QA columns, preserves raw values "
        "and flags, and writes adjusted_antl plus a metadata JSON under outputs/. "
        "This is seasonal adjustment only; it does not perform VNP46A2 viewing-angle correction, "
        "and it does not relabel gap-filled observations as direct observations."
    ),
    args_schema=VNP46A2SeasonalAdjustmentInput,
)


__all__ = [
    "VNP46A2SeasonalAdjustmentInput",
    "run_vnp46a2_seasonal_adjustment",
    "VNP46A2_seasonal_adjustment_tool",
]

