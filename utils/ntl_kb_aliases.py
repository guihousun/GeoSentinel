"""Shared alias and workflow normalization helpers for NTL KB RAG."""

from __future__ import annotations

import re
from typing import Any


TOOL_ALIAS_MAP: dict[str, str] = {
    # Legacy -> canonical tool names
    "Preprocess_NOAA20_VIIRS": "Noaa20_VIIRS_Preprocess",
    "VNP46A2_Angular_Correction": "VNP46A2_angular_correction_tool",
    "NTL_Zonal_Statistics": "NTL_raster_statistics",
    "Geocode_China": "geocode_tool",
    "Reverse_Geocode": "reverse_geocode_tool",
    "POI_Search": "poi_search_tool",
    "Get_Boundary_AMap_China": "get_administrative_division_data",
    "Get_Boundary_OSM": "get_administrative_division_osm_tool",
    "Tavily_Search": "tavily_search",
    "VNCI_Calculation": "VNCI_Compute",
}


def normalize_tool_name(name: str | None) -> str:
    if not name:
        return ""
    return TOOL_ALIAS_MAP.get(name, name)


def _infer_builtin_tool_name(step: dict[str, Any]) -> str:
    """Infer missing builtin tool names from known input key patterns."""
    text_blob = " ".join(
        str(step.get(k, "")) for k in ("name", "note", "description", "code")
    ).lower()

    # Text-based heuristics first when model omits input fields.
    if ("download" in text_blob or "retrieve" in text_blob) and (
        "npp-viirs" in text_blob
        or "nighttime light" in text_blob
        or "ntl" in text_blob
        or "vnp46" in text_blob
    ):
        return "NTL_download_tool"
    if "boundary" in text_blob or "administrative division" in text_blob:
        return "get_administrative_division_data"
    if "geocode" in text_blob and "reverse" not in text_blob:
        return "geocode_tool"
    if "reverse geocode" in text_blob:
        return "reverse_geocode_tool"
    if re.search(r"\bpois?\b|point of interest", text_blob):
        return "poi_search_tool"
    if "zonal" in text_blob or "statistics" in text_blob:
        return "NTL_raster_statistics"
    if "trend" in text_blob:
        return "Analyze_NTL_trend"
    if "anomaly" in text_blob:
        return "Detect_NTL_anomaly"
    if "vnci" in text_blob:
        return "VNCI_Compute"

    input_data = step.get("input")
    if not isinstance(input_data, dict):
        input_data = {}
    input_files = step.get("input_files")
    if isinstance(input_files, dict):
        keys = set(input_data.keys()) | set(input_files.keys())
    else:
        keys = set(input_data.keys())

    if {"study_area", "scale_level", "temporal_resolution"} <= keys:
        return "NTL_download_tool"
    if {"city_name", "output_name"} <= keys:
        return "get_administrative_division_data"
    if {"address"} <= keys:
        return "geocode_tool"
    if {"latitudes", "longitudes"} <= keys:
        return "reverse_geocode_tool"
    if {"latitude", "longitude"} <= keys:
        return "poi_search_tool"
    if {"ndvi_tif", "ntl_tif", "output_tif"} <= keys:
        return "VNCI_Compute"
    if {"ndvi_tif", "ntl_tif"} <= keys:
        return "VNCI_Compute"
    if {"raster_files", "save_subfolder"} <= keys:
        return "Analyze_NTL_trend"
    if {"raster_files", "k_sigma"} <= keys:
        return "Detect_NTL_anomaly"
    if {"ntl_tif_path", "shapefile_path", "output_csv_path"} <= keys:
        return "NTL_raster_statistics"
    if {"input_tif", "output_tif", "threshold"} <= keys:
        return "Detect_Electrified_Areas_by_Thresholding"
    return ""


def _looks_like_descriptive_step_name(name: str) -> bool:
    """Heuristic: descriptive phrases are workflow text, not executable tool ids."""
    if not name:
        return False
    # Existing tool names are snake/camel-like tokens without long natural-language phrases.
    if " " not in name:
        return False
    lowered = name.lower()
    keywords = (
        "model",
        "regression",
        "analysis",
        "relationship",
        "fit",
        "evaluate",
        "compare",
        "report",
        "workflow",
    )
    return any(k in lowered for k in keywords)


def flatten_records(data: Any) -> list[dict[str, Any]]:
    """Flatten nested list/dict records into a single dict list."""
    out: list[dict[str, Any]] = []
    if isinstance(data, dict):
        out.append(data)
    elif isinstance(data, list):
        for item in data:
            out.extend(flatten_records(item))
    return out


def normalize_workflow_task(task: dict[str, Any]) -> dict[str, Any]:
    """Normalize one workflow task while keeping business semantics unchanged."""
    normalized = dict(task)
    steps_in = normalized.get("steps")
    if not isinstance(steps_in, list):
        steps_in = []

    steps_out: list[dict[str, Any]] = []
    for idx, step in enumerate(steps_in, start=1):
        if not isinstance(step, dict):
            continue
        item = dict(step)
        step_type = str(item.get("type", "")).strip() or "builtin_tool"
        item["type"] = step_type

        # Compatible with model outputs that use tool_name/input_parameters schema.
        if "name" not in item:
            if "tool_name" in item:
                item["name"] = item.get("tool_name")
            elif "tool" in item:
                item["name"] = item.get("tool")
        if not isinstance(item.get("input"), dict) and isinstance(item.get("input_parameters"), dict):
            item["input"] = dict(item.get("input_parameters", {}))
        if not isinstance(item.get("input"), dict) and isinstance(item.get("parameters"), dict):
            item["input"] = dict(item.get("parameters", {}))
        if (
            not isinstance(item.get("description"), str) or not str(item.get("description", "")).strip()
        ) and isinstance(item.get("tool_description"), str):
            item["description"] = str(item.get("tool_description", "")).strip()
        if (
            not isinstance(item.get("description"), str) or not str(item.get("description", "")).strip()
        ) and isinstance(item.get("action"), str):
            item["description"] = str(item.get("action", "")).strip()

        raw_name = item.get("name")
        name = normalize_tool_name(str(raw_name).strip()) if raw_name is not None else ""
        inferred_name = _infer_builtin_tool_name(item) if step_type == "builtin_tool" else ""
        if step_type == "builtin_tool":
            if name.lower() in {"geospatial_code", "python_code", "code_step"}:
                item["type"] = "geospatial_code"
                step_type = "geospatial_code"
                name = ""
            # LLM sometimes emits descriptive phrases instead of tool identifiers.
            if (not name or " " in name) and inferred_name:
                name = inferred_name
            elif _looks_like_descriptive_step_name(name):
                # Preserve business intent but avoid turning narrative text into invalid tool ids.
                item["type"] = "geospatial_code"
                step_type = "geospatial_code"
                if not isinstance(item.get("description"), str) or not item["description"].strip():
                    item["description"] = str(raw_name).strip()
                name = ""
        if not name:
            if step_type == "geospatial_code":
                name = f"geospatial_code_step_{idx}"
            else:
                name = f"builtin_tool_step_{idx}"
        item["name"] = name

        if step_type == "builtin_tool":
            if not isinstance(item.get("input"), dict):
                item["input"] = {}
        elif step_type == "geospatial_code":
            if not isinstance(item.get("description"), str) or not item["description"].strip():
                item["description"] = "Task-specific geospatial code step."

        steps_out.append(item)

    normalized["steps"] = steps_out
    return normalized


def normalize_workflow_payload(
    payload: dict[str, Any], valid_tool_names: set[str] | None = None
) -> tuple[dict[str, Any], list[str]]:
    """
    Normalize workflow payload and return invalid tool names after normalization.
    """
    normalized = normalize_workflow_task(payload)
    invalid_names: list[str] = []
    if valid_tool_names is not None:
        for step in normalized.get("steps", []):
            if step.get("type") == "builtin_tool":
                name = str(step.get("name", ""))
                if name not in valid_tool_names:
                    invalid_names.append(name)
    return normalized, invalid_names
