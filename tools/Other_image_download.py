from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import ee
import geemap
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from storage_manager import current_thread_id, storage_manager

_PROJECT_ID = "empyrean-caster-430308-m2"


def _ensure_ee_initialized() -> None:
    try:
        ee.Initialize(project=_PROJECT_ID)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=_PROJECT_ID)


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = None
    if isinstance(config, dict):
        runtime_config = config
    else:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited

    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _resolve_workspace_export_dir(out_name: str, thread_id: str) -> Path:
    folder = Path(str(out_name or "").strip()).name or "downloads"
    folder_path = Path(storage_manager.resolve_input_path(folder, thread_id=thread_id))
    folder_path.mkdir(parents=True, exist_ok=True)
    return folder_path


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def _guess_is_in_china(study_area: str, is_in_china: Optional[bool]) -> bool:
    if isinstance(is_in_china, bool):
        return is_in_china
    token = (study_area or "").strip().lower()
    if token in {"china", "prc", "people's republic of china", "中国", "中华人民共和国"}:
        return True
    return _contains_cjk(study_area)


def _normalize_study_area(study_area: str, scale_level: str) -> str:
    raw = (study_area or "").strip()
    if scale_level == "country":
        alias = {"中国": "China", "中华人民共和国": "China", "china": "China", "prc": "China"}
        for k, v in alias.items():
            if raw.lower() == k.lower():
                return v
    return raw


def _sample_candidate_names(region, name_property: str, max_items: int = 5) -> list[str]:
    try:
        names = region.aggregate_array(name_property).distinct().getInfo() or []
        return [str(x) for x in names[:max_items]]
    except Exception:
        return []


def _pick_unique_region(region, name_property: str, study_area: str, scale_level: str, country_name: Optional[str]) -> Tuple[Optional[ee.FeatureCollection], Optional[str]]:
    count = int(region.size().getInfo())
    if count == 0:
        return None, None
    if count == 1:
        return region, None

    candidates = _sample_candidate_names(region, name_property=name_property)
    hint = ""
    if scale_level == "province" and not country_name:
        hint = " Add 'country_name' to disambiguate global province names."
    candidate_text = f" Candidates: {', '.join(candidates)}." if candidates else ""
    return None, f"Ambiguous area name '{study_area}' under scale level '{scale_level}' ({count} matches).{hint}{candidate_text}"


def _filter_region_with_fallback(admin_boundary, name_property: str, study_area: str, scale_level: str, country_name: Optional[str]):
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))
    picked, err = _pick_unique_region(region, name_property, study_area, scale_level, country_name)
    if picked is not None or err is not None:
        return picked, err

    title = study_area.title()
    if title != study_area:
        region = admin_boundary.filter(ee.Filter.eq(name_property, title))
        picked, err = _pick_unique_region(region, name_property, study_area, scale_level, country_name)
        if picked is not None or err is not None:
            return picked, err

    token = study_area.split(",")[0].strip()
    token = re.sub(r"\b(region|province|state)\b", "", token, flags=re.IGNORECASE).strip()
    if token:
        region = admin_boundary.filter(ee.Filter.stringContains(name_property, token))
        picked, err = _pick_unique_region(region, name_property, study_area, scale_level, country_name)
        if picked is not None or err is not None:
            return picked, err

    return None, None


def _error_result(message: str) -> dict:
    return {"output_files": [], "error": message}


def _parse_year_range(time_range_input: str) -> tuple[int, int]:
    tr = (time_range_input or "").replace(" ", "")
    if "to" in tr:
        start_year, end_year = map(int, tr.split("to", 1))
    else:
        start_year = end_year = int(tr)
    if start_year > end_year:
        raise ValueError("Start year must not be greater than end year.")
    return start_year, end_year


def _resolve_boundary(scale_level: str, study_area: str, is_in_china: bool, country_name: Optional[str] = None):
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")
    intl_country_collection = ee.FeatureCollection("FAO/GAUL/2015/level0")
    intl_province_collection = ee.FeatureCollection("FAO/GAUL/2015/level1")

    directly_governed = {"北京市", "天津市", "上海市", "重庆市", "Beijing", "Tianjin", "Shanghai", "Chongqing"}

    if is_in_china:
        if scale_level == "country":
            admin_boundary, name_property = national_collection, "NAME"
        elif scale_level == "province" or (scale_level == "city" and study_area in directly_governed):
            admin_boundary, name_property = province_collection, "name"
        elif scale_level == "city":
            admin_boundary, name_property = city_collection, "name"
        elif scale_level == "county":
            admin_boundary, name_property = county_collection, "name"
        else:
            raise ValueError("Unknown scale level. Use country/province/city/county.")
    else:
        if scale_level == "country":
            admin_boundary, name_property = intl_country_collection, "ADM0_NAME"
        elif scale_level == "province":
            admin_boundary, name_property = intl_province_collection, "ADM1_NAME"
            if country_name:
                admin_boundary = admin_boundary.filter(ee.Filter.eq("ADM0_NAME", country_name))
        else:
            raise ValueError("Global mode only supports country/province.")

    region, err = _filter_region_with_fallback(
        admin_boundary=admin_boundary,
        name_property=name_property,
        study_area=study_area,
        scale_level=scale_level,
        country_name=country_name,
    )
    return region, err


class NDVIDataInput(BaseModel):
    study_area: str = Field(..., description="Study area name")
    scale_level: str = Field(..., description="country/province/city/county")
    time_range_input: str = Field(..., description="YYYY or YYYY to YYYY")
    out_name: str = Field(..., description="Output local folder path")
    is_in_China: Optional[bool] = Field(None, description="Whether study area is in China; auto-inferred if omitted.")
    country_name: Optional[str] = Field(None, description="Optional country name for global province disambiguation, e.g. 'Myanmar'.")


def NDVI_download_tool(
    study_area: str,
    scale_level: str,
    time_range_input: str,
    out_name: str,
    is_in_China: Optional[bool] = None,
    country_name: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
    **kwargs,
):
    try:
        thread_id = _resolve_thread_id_from_config(config)
        _ensure_ee_initialized()
        scale_level = (scale_level or "").strip().lower()
        if is_in_China is None and "is_in_china" in kwargs:
            is_in_China = kwargs.get("is_in_china")
        is_in_China = _guess_is_in_china(study_area, is_in_China)
        country_name = (country_name or "").strip() or None
        study_area = _normalize_study_area(study_area, scale_level)

        if scale_level == "province" and study_area.lower() == "china":
            scale_level = "country"

        region, region_error = _resolve_boundary(
            scale_level=scale_level,
            study_area=study_area,
            is_in_china=is_in_China,
            country_name=country_name,
        )
        if region_error:
            return _error_result(region_error)
        if region is None:
            return _error_result(
                f"No area named '{study_area}' found under scale level '{scale_level}'. "
                "Try adding country_name or use a more specific area name."
            )

        start_year, end_year = _parse_year_range(time_range_input)
        export_dir = _resolve_workspace_export_dir(out_name, thread_id=thread_id)
        exported_files = []

        for year in range(start_year, end_year + 1):
            collection = (
                ee.ImageCollection("MODIS/061/MOD13Q1")
                .filterDate(f"{year}-01-01", f"{year}-12-31")
                .select("NDVI")
                .map(lambda img: img.multiply(0.0001).set(img.toDictionary(img.propertyNames())))
            )
            image = collection.filterBounds(region.geometry()).map(lambda img: img.clip(region)).mean()
            export_path = str(export_dir / f"NDVI_{study_area}_{year}.tif")
            geemap.ee_export_image(
                ee_object=image,
                filename=export_path,
                scale=250,
                region=region.geometry(),
                crs="EPSG:4326",
                file_per_band=False,
            )
            exported_files.append(export_path)

        return {"output_files": exported_files}
    except Exception as e:
        return _error_result(f"NDVI_download_tool failed: {e}")


class LandScanDataInput(BaseModel):
    study_area: str = Field(..., description="Study area name")
    scale_level: str = Field(..., description="country/province/city/county")
    time_range_input: str = Field(..., description="YYYY or YYYY to YYYY")
    out_name: str = Field(..., description="Output local folder path")
    is_in_China: Optional[bool] = Field(None, description="Whether study area is in China")
    country_name: Optional[str] = Field(None, description="Optional country name for global province disambiguation, e.g. 'Myanmar'.")


def landscan_download_tool(
    study_area: str,
    scale_level: str,
    time_range_input: str,
    out_name: str,
    is_in_China: Optional[bool] = None,
    country_name: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
    **kwargs,
):
    try:
        thread_id = _resolve_thread_id_from_config(config)
        _ensure_ee_initialized()
        scale_level = (scale_level or "").strip().lower()
        if is_in_China is None and "is_in_china" in kwargs:
            is_in_China = kwargs.get("is_in_china")
        is_in_China = _guess_is_in_china(study_area, is_in_China)
        country_name = (country_name or "").strip() or None
        study_area = _normalize_study_area(study_area, scale_level)

        if scale_level == "province" and study_area.lower() == "china":
            scale_level = "country"

        region, region_error = _resolve_boundary(
            scale_level=scale_level,
            study_area=study_area,
            is_in_china=is_in_China,
            country_name=country_name,
        )
        if region_error:
            return _error_result(region_error)
        if region is None:
            return _error_result(
                f"No area named '{study_area}' found under scale level '{scale_level}'. "
                "Try adding country_name or use a more specific area name."
            )

        start_year, end_year = _parse_year_range(time_range_input)
        export_dir = _resolve_workspace_export_dir(out_name, thread_id=thread_id)
        exported_files = []

        for year in range(start_year, end_year + 1):
            image = (
                ee.ImageCollection("projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL")
                .filterDate(f"{year}-01-01", f"{year+1}-01-01")
                .first()
            )
            image = ee.Image(image).clip(region.geometry())
            export_path = str(export_dir / f"LandScan_{study_area}_{year}.tif")
            geemap.ee_export_image(
                ee_object=image,
                filename=export_path,
                scale=1000,
                region=region.geometry(),
                crs="EPSG:4326",
                file_per_band=False,
            )
            exported_files.append(export_path)

        return {"output_files": exported_files}
    except Exception as e:
        return _error_result(f"LandScan_download_tool failed: {e}")


class WorldPopDataInput(BaseModel):
    study_area: str = Field(..., description="Study area name")
    scale_level: str = Field(..., description="country/province/city/county")
    time_range_input: str = Field(..., description="YYYY or YYYY to YYYY")
    out_name: str = Field(..., description="Output local folder path")
    is_in_China: Optional[bool] = Field(None, description="Whether study area is in China; auto-inferred if omitted.")
    country_name: Optional[str] = Field(None, description="Optional country name for global province disambiguation, e.g. 'Myanmar'.")


def worldpop_download_tool(
    study_area: str,
    scale_level: str,
    time_range_input: str,
    out_name: str,
    is_in_China: Optional[bool] = None,
    country_name: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
    **kwargs,
):
    try:
        thread_id = _resolve_thread_id_from_config(config)
        _ensure_ee_initialized()
        scale_level = (scale_level or "").strip().lower()
        if is_in_China is None and "is_in_china" in kwargs:
            is_in_China = kwargs.get("is_in_china")
        is_in_China = _guess_is_in_china(study_area, is_in_China)
        country_name = (country_name or "").strip() or None
        study_area = _normalize_study_area(study_area, scale_level)

        if scale_level == "province" and study_area.lower() == "china":
            scale_level = "country"

        region, region_error = _resolve_boundary(
            scale_level=scale_level,
            study_area=study_area,
            is_in_china=is_in_China,
            country_name=country_name,
        )
        if region_error:
            return _error_result(region_error)
        if region is None:
            return _error_result(
                f"No area named '{study_area}' found under scale level '{scale_level}'. "
                "Try adding country_name or use a more specific area name."
            )

        start_year, end_year = _parse_year_range(time_range_input)
        export_dir = _resolve_workspace_export_dir(out_name, thread_id=thread_id)
        exported_files = []

        for year in range(start_year, end_year + 1):
            image = ee.Image(f"WorldPop/GP/100m/pop/{year}").clip(region.geometry())
            export_path = str(export_dir / f"WorldPop_{study_area}_{year}.tif")
            geemap.ee_export_image(
                ee_object=image,
                filename=export_path,
                scale=100,
                region=region.geometry(),
                crs="EPSG:4326",
                file_per_band=False,
            )
            exported_files.append(export_path)

        return {"output_files": exported_files}
    except Exception as e:
        return _error_result(f"WorldPop_download_tool failed: {e}")


NDVI_download_tool = StructuredTool.from_function(
    NDVI_download_tool,
    name="NDVI_download_tool",
    description="Download annual NDVI (MODIS MOD13Q1) for a region from GEE.",
    args_schema=NDVIDataInput,
)

LandScan_download_tool = StructuredTool.from_function(
    landscan_download_tool,
    name="LandScan_download_tool",
    description="Download annual LandScan population rasters for a region from GEE.",
    args_schema=LandScanDataInput,
)

WorldPop_download_tool = StructuredTool.from_function(
    worldpop_download_tool,
    name="WorldPop_download_tool",
    description="Download annual WorldPop rasters for a region from GEE.",
    args_schema=WorldPopDataInput,
)
