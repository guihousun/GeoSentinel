from __future__ import annotations

import os
import re
import json
import math
import calendar
import contextlib
import io
from datetime import datetime, timedelta
from typing import Optional, Literal

import ee
import geemap
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager

_PROJECT_ID = (
    os.getenv("GEE_DEFAULT_PROJECT_ID", "").strip()
    or os.getenv("EE_PROJECT_ID", "").strip()
    or "empyrean-caster-430308-m2"
)
BBoxLike = str | list[float] | tuple[float, float, float, float] | dict[str, float]


def _get_streamlit_secret(name: str):
    try:
        st_obj = getattr(__import__("builtins"), "st", None)
        if st_obj is not None and hasattr(st_obj, "secrets"):
            return st_obj.secrets.get(name)
    except Exception:
        return None
    return None


def _ensure_ee_initialized() -> None:
    sa_email = os.getenv("EE_SERVICE_ACCOUNT") or _get_streamlit_secret("EE_SERVICE_ACCOUNT")
    sa_key_json = os.getenv("EE_PRIVATE_KEY_JSON") or _get_streamlit_secret("EE_PRIVATE_KEY_JSON")
    if sa_email and sa_key_json:
        creds = ee.ServiceAccountCredentials(sa_email, key_data=str(sa_key_json))
        ee.Initialize(credentials=creds, project=_PROJECT_ID)
        return

    try:
        if _PROJECT_ID:
            ee.Initialize(project=_PROJECT_ID)
        else:
            ee.Initialize()
    except Exception:
        # For personal local use, fall back to interactive auth if no env-backed creds are available.
        ee.Authenticate()
        if _PROJECT_ID:
            ee.Initialize(project=_PROJECT_ID)
        else:
            ee.Initialize()


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


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def _guess_is_in_china(study_area: str, is_in_china: Optional[bool]) -> bool:
    if isinstance(is_in_china, bool):
        return is_in_china
    token = (study_area or "").strip().lower()
    if token in {"china", "prc", "people's republic of china", "中国", "中华人民共和国"}:
        return True
    return _contains_cjk(study_area)


def _normalize_dataset_name(dataset_name: Optional[str], temporal_resolution: str) -> Optional[str]:
    if not dataset_name:
        return dataset_name
    key = str(dataset_name).strip().lower()
    key = key.replace("_", "-").replace("–", "-").replace("—", "-").replace(" ", "")

    annual_alias = {
        "npp-viirs-like": "NPP-VIIRS-Like",
        "nppviirslike": "NPP-VIIRS-Like",
        "npp-viirs": "NPP-VIIRS",
        "nppviirs": "NPP-VIIRS",
        "dmsp-ols": "DMSP-OLS",
        "dmspols": "DMSP-OLS",
        "dmsp/ols": "DMSP-OLS",
    }
    monthly_alias = {
        "noaa-vcmslcfg": "NOAA_VCMSLCFG",
        "vcmslcfg": "NOAA_VCMSLCFG",
        "noaa_vcmslcfg": "NOAA_VCMSLCFG",
    }
    daily_alias = {
        "vnp46a2": "VNP46A2",
        "vnp46a1": "VNP46A1",
    }

    if temporal_resolution == "annual":
        return annual_alias.get(key, dataset_name)
    if temporal_resolution == "monthly":
        return monthly_alias.get(key, dataset_name)
    if temporal_resolution == "daily":
        return daily_alias.get(key, dataset_name)
    return dataset_name


def _normalize_study_area(study_area: str, scale_level: str) -> str:
    raw = (study_area or "").strip()
    if scale_level == "country":
        m = {
            "中国": "China",
            "中华人民共和国": "China",
            "china": "China",
            "prc": "China",
        }
        for k, v in m.items():
            if raw.lower() == k.lower():
                return v
    return raw


def _split_admin_and_country(study_area: str) -> tuple[str, Optional[str]]:
    raw = (study_area or "").strip()
    if not raw:
        return "", None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return raw, None


def _filter_region_with_fallback(admin_boundary, name_property: str, study_area: str):
    # exact
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))
    if region.size().getInfo() > 0:
        return region

    # title case fallback
    title = study_area.title()
    if title != study_area:
        region = admin_boundary.filter(ee.Filter.eq(name_property, title))
        if region.size().getInfo() > 0:
            return region

    # token contains fallback (e.g. "Sagaing Region, Myanmar" -> "Sagaing")
    token = study_area.split(",")[0].strip()
    token = re.sub(r"\b(region|province|state)\b", "", token, flags=re.IGNORECASE).strip()
    if token:
        region = admin_boundary.filter(ee.Filter.stringContains(name_property, token))
        if region.size().getInfo() > 0:
            return region

    return None


def _resolve_geoboundaries_country_group(country_collection, country_name: str) -> Optional[str]:
    if not country_name:
        return None
    region = _filter_region_with_fallback(country_collection, "shapeName", country_name)
    if region is None:
        return None
    count = int(region.size().getInfo())
    if count <= 0:
        return None
    first_feature = ee.Feature(region.first())
    return str(first_feature.get("shapeGroup").getInfo() or "").strip() or None


def _error_result(message: str, **extra) -> dict:
    payload = {"status": "error", "output_files": [], "error": message}
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def _ensure_tif_suffix(filename: str) -> str:
    safe = (filename or "").strip()
    if not safe:
        return safe
    base, ext = os.path.splitext(safe)
    if ext:
        return safe
    return f"{base}.tif" if base else "output.tif"


def _normalize_batch_base_name(base: str, temporal_resolution: str) -> str:
    """
    Remove trailing range suffix in batch exports to avoid names like:
    shanghai_ntl_2013_2022_2013.tif
    """
    name = (base or "").strip()
    if not name:
        return name

    patterns = []
    if temporal_resolution == "annual":
        patterns = [
            r"[_-](19\d{2}|20\d{2})[_-](19\d{2}|20\d{2})$",
            r"(19\d{2}|20\d{2})to(19\d{2}|20\d{2})$",
        ]
    elif temporal_resolution == "monthly":
        patterns = [
            r"[_-](19\d{2}|20\d{2})-(0[1-9]|1[0-2])[_-](19\d{2}|20\d{2})-(0[1-9]|1[0-2])$",
            r"(19\d{2}|20\d{2})-(0[1-9]|1[0-2])to(19\d{2}|20\d{2})-(0[1-9]|1[0-2])$",
        ]
    elif temporal_resolution == "daily":
        patterns = [
            r"[_-](19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])[_-](19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$",
            r"(19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])to(19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$",
        ]

    trimmed = name
    for pat in patterns:
        trimmed = re.sub(pat, "", trimmed).rstrip("_-")
    return trimmed or name


def _parse_time_range(time_range_input: str, temporal_resolution: str) -> tuple[str, str]:
    tr = (time_range_input or "").replace(" ", "")
    if "to" in tr:
        start_str, end_str = [s.strip() for s in tr.split("to", 1)]
    else:
        start_str = end_str = tr

    if temporal_resolution == "annual":
        if re.fullmatch(r"\d{4}-01-01", start_str) and re.fullmatch(r"\d{4}-12-31", end_str):
            start_str = start_str[:4]
            end_str = end_str[:4]

        if not re.fullmatch(r"\d{4}", start_str) or not re.fullmatch(r"\d{4}", end_str):
            raise ValueError("Annual format must be 'YYYY' or 'YYYY to YYYY'.")
        start_date, end_date = f"{start_str}-01-01", f"{end_str}-12-31"

    elif temporal_resolution == "monthly":
        if not re.fullmatch(r"\d{4}-\d{2}", start_str) or not re.fullmatch(r"\d{4}-\d{2}", end_str):
            raise ValueError("Monthly format must be 'YYYY-MM' or 'YYYY-MM to YYYY-MM'.")
        sy, sm = map(int, start_str.split("-"))
        ey, em = map(int, end_str.split("-"))
        start_date = f"{sy}-{sm:02d}-01"
        end_date = f"{ey}-{em:02d}-{calendar.monthrange(ey, em)[1]}"

    elif temporal_resolution == "daily":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_str):
            raise ValueError("Daily format must be 'YYYY-MM-DD' or 'YYYY-MM-DD to YYYY-MM-DD'.")
        start_date, end_date = start_str, end_str
    else:
        raise ValueError("temporal_resolution must be one of: annual, monthly, daily")

    if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
        raise ValueError("Start date cannot be later than end date.")
    return start_date, end_date


def _parse_bbox_input(raw_bbox: Optional[BBoxLike]) -> Optional[tuple[float, float, float, float]]:
    if raw_bbox is None:
        return None

    values: list[object]
    payload: object = raw_bbox
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return None
        if text.startswith("[") or text.startswith("{"):
            try:
                payload = json.loads(text)
            except Exception as exc:
                raise ValueError("bbox JSON format is invalid. Use minx,miny,maxx,maxy.") from exc
        else:
            payload = [x.strip() for x in text.split(",")]

    if isinstance(payload, dict):
        required_keys = ("minx", "miny", "maxx", "maxy")
        if any(k not in payload for k in required_keys):
            raise ValueError("bbox dict must include keys: minx,miny,maxx,maxy")
        values = [payload["minx"], payload["miny"], payload["maxx"], payload["maxy"]]
    elif isinstance(payload, (list, tuple)):
        if len(payload) != 4:
            raise ValueError("bbox/box must contain 4 numbers: minx,miny,maxx,maxy")
        values = list(payload)
    else:
        raise ValueError("bbox/box must be a CSV string, JSON array, or dict with minx,miny,maxx,maxy")

    try:
        minx, miny, maxx, maxy = [float(v) for v in values]
    except (TypeError, ValueError) as exc:
        raise ValueError("bbox/box values must be numeric.") from exc

    for v in (minx, miny, maxx, maxy):
        if not math.isfinite(v):
            raise ValueError("bbox/box values must be finite numbers.")

    if minx < -180 or maxx > 180:
        raise ValueError("bbox longitude must be within [-180, 180].")
    if miny < -90 or maxy > 90:
        raise ValueError("bbox latitude must be within [-90, 90].")
    if maxx <= minx or maxy <= miny:
        raise ValueError("Invalid bbox/box: require maxx > minx and maxy > miny.")

    return minx, miny, maxx, maxy


def _build_bbox_region(bbox: tuple[float, float, float, float]):
    minx, miny, maxx, maxy = bbox
    geom = ee.Geometry.Rectangle([minx, miny, maxx, maxy], None, False)
    return ee.FeatureCollection([ee.Feature(geom)])


def _coalesce_bbox_input(
    bbox: Optional[BBoxLike],
    box: Optional[BBoxLike],
    kwargs: dict,
) -> Optional[BBoxLike]:
    if bbox not in (None, ""):
        return bbox
    if box not in (None, ""):
        return box

    legacy_bbox = kwargs.get("bbox")
    if legacy_bbox not in (None, ""):
        return legacy_bbox
    legacy_box = kwargs.get("box")
    if legacy_box not in (None, ""):
        return legacy_box
    return None


class NightlightDataInput(BaseModel):
    study_area: Optional[str] = Field(
        None,
        description=(
            "Name of the study area. Required when bbox/box is not provided. China examples: '南京市'. "
            "Outside China, for province/city/county levels use 'admin_name, country' "
            "(e.g., 'Tehran, Iran')."
        ),
    )
    scale_level: Optional[Literal["country", "province", "city", "county", "district"]] = Field(
        None, description="Administrative scale level. Required when bbox/box is not provided."
    )
    temporal_resolution: Literal["annual", "monthly", "daily"] = Field(..., description="Temporal resolution.")
    time_range_input: str = Field(..., description="Annual: YYYY to YYYY; Monthly: YYYY-MM to YYYY-MM; Daily: YYYY-MM-DD to YYYY-MM-DD")
    out_name: str = Field(..., description="Output filename only, e.g. 'ntl_shanghai_2020.tif'")
    dataset_name: Optional[str] = Field(None, description="Annual: NPP-VIIRS-Like/NPP-VIIRS/DMSP-OLS; Monthly fixed; Daily: VNP46A2/VNP46A1")
    collection_name: Optional[str] = Field(None, description="Reserved optional field.")
    is_in_China: Optional[bool] = Field(None, description="Whether study area is in China. If omitted, inferred from study_area.")
    bbox: Optional[BBoxLike] = Field(
        None,
        description="Optional bounding box in lon/lat (WGS84): minx,miny,maxx,maxy. If provided, takes priority over study_area/scale_level.",
    )
    box: Optional[BBoxLike] = Field(
        None,
        description="Alias of bbox. Format: minx,miny,maxx,maxy.",
    )


def ntl_download_tool(
    study_area: Optional[str],
    scale_level: Optional[str],
    temporal_resolution: str,
    time_range_input: str,
    out_name: str,
    dataset_name: Optional[str] = None,
    collection_name: Optional[str] = None,
    is_in_China: Optional[bool] = None,
    bbox: Optional[BBoxLike] = None,
    box: Optional[BBoxLike] = None,
    config: Optional[RunnableConfig] = None,
    **kwargs,
):
    try:
        thread_id = _resolve_thread_id_from_config(config)

        # Backward-compatible aliases from legacy callers.
        if dataset_name is None and collection_name:
            dataset_name = collection_name
        if is_in_China is None and "is_in_china" in kwargs:
            is_in_China = kwargs.get("is_in_china")

        scale_level = str(scale_level or "").strip().lower()
        temporal_resolution = str(temporal_resolution or "").strip().lower()
        dataset_name = _normalize_dataset_name(dataset_name, temporal_resolution)
        out_name = _ensure_tif_suffix(out_name)
        bbox_input = _coalesce_bbox_input(bbox, box, kwargs)
        bbox_values = _parse_bbox_input(bbox_input)

        _ensure_ee_initialized()

        national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
        province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
        city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
        county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")
        # Global administrative boundaries from geoBoundaries (GEE catalog).
        intl_country_collection = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM0")
        intl_province_collection = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")
        intl_city_collection = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM2")
        intl_county_collection = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM3")
        intl_district_collection = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM4")
        resolved_study_area = (study_area or "").strip()

        def get_administrative_boundaries(level: str, in_china: bool):
            directly_governed = {"北京市", "天津市", "上海市", "重庆市", "Beijing", "Tianjin", "Shanghai", "Chongqing"}
            if in_china:
                if level == "country":
                    return national_collection, "NAME", None
                if level == "province" or (level == "city" and resolved_study_area in directly_governed):
                    return province_collection, "name", None
                if level == "city":
                    return city_collection, "name", None
                if level == "county":
                    return county_collection, "name", None
                raise ValueError("Unknown scale level. Options: country/province/city/county")

            # Global mode (geoBoundaries on GEE): ADM0..ADM4.
            if level == "country":
                return intl_country_collection, "shapeName", None
            if level == "province":
                return intl_province_collection, "shapeName", "ADM1"
            if level == "city":
                return intl_city_collection, "shapeName", "ADM2"
            if level == "county":
                return intl_county_collection, "shapeName", "ADM3"
            if level == "district":
                return intl_district_collection, "shapeName", "ADM4"
            raise ValueError("Unknown scale level. Options: country/province/city/county/district")

        if bbox_values is not None:
            region = _build_bbox_region(bbox_values)
        else:
            if not resolved_study_area:
                return _error_result("study_area is required when bbox/box is not provided.")
            if not scale_level:
                return _error_result("scale_level is required when bbox/box is not provided.")

            is_in_China = _guess_is_in_china(resolved_study_area, is_in_China)
            resolved_study_area = _normalize_study_area(resolved_study_area, scale_level)

            # If caller uses province for "China", auto-correct to country.
            if scale_level == "province" and resolved_study_area.lower() == "china":
                scale_level = "country"

            admin_boundary, name_property, _ = get_administrative_boundaries(scale_level, is_in_China)
            query_name = resolved_study_area
            if not is_in_China and scale_level != "country":
                admin_name, country_name = _split_admin_and_country(resolved_study_area)
                query_name = admin_name
                shape_group = _resolve_geoboundaries_country_group(intl_country_collection, country_name or "")
                if shape_group:
                    admin_boundary = admin_boundary.filter(ee.Filter.eq("shapeGroup", shape_group))
                elif country_name:
                    return _error_result(
                        f"Country '{country_name}' not found in GEE geoBoundaries ADM0. "
                        "Use study_area like 'Tehran, Iran' or a valid country alias."
                    )

            region = _filter_region_with_fallback(admin_boundary, name_property, query_name)
            if region is None:
                if not is_in_China and scale_level in {"city", "county", "district"}:
                    tip = (
                        "Global matching uses geoBoundaries ADM2/ADM3/ADM4 for city/county/district. "
                        "Try input format 'city_or_county, country' (e.g., 'Tehran, Iran')."
                    )
                else:
                    tip = "Try an alias, or specify 'admin_name, country' for non-China regions."
                return _error_result(
                    f"No area named '{query_name}' found under scale level '{scale_level}'. {tip}"
                )

        start_date, end_date = _parse_time_range(time_range_input, temporal_resolution)

        # ---------- Dataset routing ----------
        if temporal_resolution == "annual":
            dataset_name = dataset_name or "NPP-VIIRS-Like"
            start_year, end_year = int(start_date[:4]), int(end_date[:4])

            if dataset_name == "NPP-VIIRS-Like":
                if start_year < 2000 or end_year > 2024:
                    return _error_result("NPP-VIIRS-Like valid year range: 2000-2024")
                col_id, band = "projects/sat-io/open-datasets/npp-viirs-ntl", "b1"

                images = []
                for y in range(start_year, end_year + 1):
                    y_start, y_end = f"{y}-01-01", f"{y+1}-01-01"
                    col = ee.ImageCollection(col_id).filterDate(y_start, y_end).select(band).filterBounds(region.geometry())
                    img = col.map(lambda i: i.clip(region)).mean().set("system:time_start", ee.Date(y_start).millis())
                    images.append(img)
                NTL_collection = ee.ImageCollection(images)

            elif dataset_name == "NPP-VIIRS":
                if start_year < 2012 or end_year > 2024:
                    return _error_result("NPP-VIIRS valid year range: 2012-2024")
                v21, v22, band = "NOAA/VIIRS/DNB/ANNUAL_V21", "NOAA/VIIRS/DNB/ANNUAL_V22", "average"
                images = []
                for y in range(start_year, end_year + 1):
                    y_start, y_end = f"{y}-01-01", f"{y+1}-01-01"
                    src_id = v21 if y <= 2021 else v22
                    col = ee.ImageCollection(src_id).filterDate(y_start, y_end).select(band).filterBounds(region.geometry())
                    img = col.map(lambda i: i.clip(region)).mean().set("system:time_start", ee.Date(y_start).millis())
                    images.append(img)
                NTL_collection = ee.ImageCollection(images)

            elif dataset_name == "DMSP-OLS":
                if start_year < 1992 or end_year > 2013:
                    return _error_result("DMSP-OLS valid year range: 1992-2013")
                col_id, band = "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS", "avg_vis"
                images = []
                for y in range(start_year, end_year + 1):
                    y_start, y_end = f"{y}-01-01", f"{y+1}-01-01"
                    col = ee.ImageCollection(col_id).filterDate(y_start, y_end).select(band).filterBounds(region.geometry())
                    img = col.map(lambda i: i.clip(region)).mean().set("system:time_start", ee.Date(y_start).millis())
                    images.append(img)
                NTL_collection = ee.ImageCollection(images)
            else:
                return _error_result("For annual, dataset_name must be one of: NPP-VIIRS-Like, NPP-VIIRS, DMSP-OLS")

        elif temporal_resolution == "monthly":
            sy, sm = map(int, start_date[:7].split("-"))
            ey, em = map(int, end_date[:7].split("-"))
            if sy < 2014:
                return _error_result("Monthly VIIRS is available from 2014-01 onwards.")

            col_id, band = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG", "avg_rad"
            images = []
            for y in range(sy, ey + 1):
                m_start = sm if y == sy else 1
                m_end = em if y == ey else 12
                for m in range(m_start, m_end + 1):
                    s_day = f"{y}-{m:02d}-01"
                    e_day = f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]}"
                    collection = ee.ImageCollection(col_id).filterDate(s_day, e_day).select(band).filterBounds(region.geometry())
                    image = collection.map(lambda i: i.clip(region)).mean().set("system:time_start", ee.Date(s_day).millis())
                    images.append(image)
            NTL_collection = ee.ImageCollection(images)

        elif temporal_resolution == "daily":
            dataset_name = dataset_name or "VNP46A2"
            daily_map = {
                "VNP46A2": {"id": "NASA/VIIRS/002/VNP46A2", "band": "Gap_Filled_DNB_BRDF_Corrected_NTL"},
                "VNP46A1": {"id": "NOAA/VIIRS/001/VNP46A1", "band": "DNB_At_Sensor_Radiance_500m"},
            }
            if dataset_name not in daily_map:
                return _error_result("For daily, dataset_name must be VNP46A2 or VNP46A1")

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days = (end_dt - start_dt).days + 1
            if days > 31:
                return _error_result("For daily requests longer than 31 days, please use server-side GEE script workflow.")
            if int(start_date[:4]) < 2014:
                return _error_result("Daily VIIRS is available from 2014-01 onwards.")

            col_id = daily_map[dataset_name]["id"]
            band = daily_map[dataset_name]["band"]
            NTL_collection = (
                ee.ImageCollection(col_id)
                .filterDate(start_date, (end_dt + timedelta(days=1)).strftime("%Y-%m-%d"))
                .select(band)
                .filterBounds(region.geometry())
                .map(lambda i: i.clip(region))
            )
        else:
            return _error_result("temporal_resolution must be annual/monthly/daily")

        num_images = int(NTL_collection.size().getInfo())
        if num_images <= 0:
            return _error_result("No images found for the specified date range and region.")

        images_list = NTL_collection.toList(num_images)
        exported_files = []
        export_errors = []

        for i in range(num_images):
            image = ee.Image(images_list.get(i))
            if num_images == 1:
                filename = out_name
            else:
                if temporal_resolution == "annual":
                    image_date = image.date().format("YYYY").getInfo()
                elif temporal_resolution == "monthly":
                    image_date = image.date().format("YYYY-MM").getInfo()
                else:
                    image_date = image.date().format("YYYY-MM-dd").getInfo()
                base, ext = os.path.splitext(out_name)
                normalized_base = _normalize_batch_base_name(base, temporal_resolution)
                filename = f"{normalized_base}_{image_date}{ext}"

            abs_input = storage_manager.resolve_input_path(filename, thread_id=thread_id)
            os.makedirs(os.path.dirname(abs_input), exist_ok=True)

            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            try:
                with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
                    geemap.ee_export_image(
                        ee_object=image,
                        filename=abs_input,
                        scale=500,
                        region=region.geometry(),
                        crs="EPSG:4326",
                        file_per_band=False,
                    )

                captured = "\n".join(
                    part.strip()
                    for part in (stdout_buf.getvalue(), stderr_buf.getvalue())
                    if part and part.strip()
                )
                file_ok = os.path.exists(abs_input) and os.path.getsize(abs_input) > 0
                export_failed_text = (
                    "An error occurred while downloading" in captured
                    or "Total request size" in captured
                    or "must be less than or equal to 50331648" in captured
                )
                if export_failed_text or not file_ok:
                    if file_ok and export_failed_text:
                        try:
                            os.remove(abs_input)
                        except OSError:
                            pass
                    export_errors.append(captured or f"Export did not create output file: {filename}")
                    continue

                exported_files.append(filename)
            except Exception as exc:
                captured = "\n".join(
                    part.strip()
                    for part in (str(exc), stdout_buf.getvalue(), stderr_buf.getvalue())
                    if part and part.strip()
                )
                export_errors.append(captured or repr(exc))
                continue

        if not exported_files:
            joined_errors = "\n".join(export_errors)
            last_error = export_errors[-1] if export_errors else "unknown export error"
            size_limit_hit = "Total request size" in joined_errors or "50331648" in joined_errors
            return _error_result(
                f"Export failed for all target images. Last error: {last_error}",
                error_type="gee_download_size_limit" if size_limit_hit else "gee_export_failed",
                recommended_execution_mode="gee_server_side" if size_limit_hit else None,
                recommended_method=(
                    "Use server-side GEE reduction/export table instead of local raster download."
                    if size_limit_hit
                    else None
                ),
            )

        return {"status": "success", "output_files": exported_files}

    except Exception as e:
        return _error_result(f"NTL_download_tool failed: {e}")


NTL_download_tool = StructuredTool.from_function(
    ntl_download_tool,
    name="NTL_download_tool",
    description=(
        "Download nighttime light (NTL) imagery from Google Earth Engine. "
        "Use output filename only (saved to workspace inputs/). "
        "Supports either administrative region matching (study_area + scale_level) or direct bbox/box AOI (minx,miny,maxx,maxy). "
        "Outside China, administrative matching uses GEE geoBoundaries (WM/geoLab/geoBoundaries/600, ADM0-ADM4). "
        "Annual datasets: NPP-VIIRS-Like/NPP-VIIRS/DMSP-OLS; monthly fixed NOAA_VCMSLCFG; daily VNP46A2/VNP46A1. "
        "Country-scale downloads are allowed for file-focused small AOIs, but if GEE reports a request-size/export "
        "limit or no output file is created, treat the download as failed and switch to server-side GEE. "
        "Do not use local raster download as the primary path for national/multi-province statistics; use server-side "
        "reduceRegions/table workflow instead. For long daily ranges, switch to server-side GEE script mode."
    ),
    args_schema=NightlightDataInput,
)
