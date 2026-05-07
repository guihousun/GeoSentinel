from __future__ import annotations

import argparse
import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager


GADM_URL_TEMPLATE = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_{iso3}_shp.zip"
GEOBOUNDARIES_META_URL_TEMPLATE = "https://www.geoboundaries.org/api/current/gbOpen/{iso3}/ADM{adm}/"


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


def _require_geopandas() -> Any:
    try:
        import geopandas as gpd  # type: ignore

        return gpd
    except Exception as exc:
        raise RuntimeError(
            "This script requires geopandas. Install dependencies first, for example: "
            "pip install geopandas fiona pyproj shapely"
        ) from exc


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().casefold())


def _contains_token(value: str, target: str) -> bool:
    value_norm = _normalize(value)
    target_norm = _normalize(target)
    if not value_norm or not target_norm:
        return False
    if value_norm == target_norm:
        return True
    parts = [p.strip() for p in re.split(r"[|;/,]", value_norm) if p.strip()]
    return target_norm in parts or target_norm in value_norm


def _resolve_iso3(country: str) -> str:
    c = (country or "").strip()
    if not c:
        raise ValueError("country is required")
    if re.fullmatch(r"[A-Za-z]{3}", c):
        return c.upper()
    try:
        import pycountry  # type: ignore

        found = pycountry.countries.lookup(c)
        return str(found.alpha_3).upper()
    except Exception as exc:
        raise ValueError(
            "Unable to resolve country name to ISO3. Please pass ISO3 directly (e.g. CHN, USA) "
            "or install pycountry: pip install pycountry"
        ) from exc


def _resolve_adm_level(level: str, adm_level: Optional[int]) -> int:
    if adm_level is not None:
        if adm_level < 0 or adm_level > 5:
            raise ValueError("adm-level must be in [0, 5]")
        return adm_level
    key = (level or "country").strip().lower()
    mapping = {
        "country": 0,
        "province": 1,
        "state": 1,
        "city": 2,
        "county": 3,
        "district": 3,
        "town": 4,
        "local": 4,
    }
    if key not in mapping:
        raise ValueError(f"Unsupported level: {level}")
    return mapping[key]


def _download_file(url: str, dst: Path, timeout: int = 60) -> Path:
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                f.write(chunk)
    return dst


def _pick_gadm_layer_name(members: Iterable[str], iso3: str, adm_level: int) -> Optional[str]:
    base = f"gadm41_{iso3.upper()}_"
    shp_names = [m for m in members if m.lower().endswith(".shp") and Path(m).name.startswith(base)]
    if not shp_names:
        return None

    by_level = {}
    for name in shp_names:
        stem = Path(name).stem
        m = re.match(rf"gadm41_{iso3.upper()}_(\d+)$", stem)
        if m:
            by_level[int(m.group(1))] = name
    if adm_level in by_level:
        return by_level[adm_level]
    lower = [k for k in by_level if k <= adm_level]
    if lower:
        return by_level[max(lower)]
    return by_level[min(by_level.keys())]


def _extract_layer_sidecars(zip_path: Path, layer_shp_member: str, out_dir: Path) -> Path:
    stem = Path(layer_shp_member).stem
    base_dir = str(Path(layer_shp_member).parent).replace("\\", "/")
    wanted_exts = {".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix", ".sbn", ".sbx"}

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            p = Path(name)
            if p.stem != stem:
                continue
            if p.suffix.lower() not in wanted_exts:
                continue
            if str(p.parent).replace("\\", "/") != base_dir:
                continue
            target = out_dir / p.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(name, "r") as src, target.open("wb") as dst:
                dst.write(src.read())

    shp_path = out_dir / f"{stem}.shp"
    if not shp_path.exists():
        raise FileNotFoundError(f"Extracted shapefile not found: {shp_path}")
    return shp_path


def _filter_admin(gdf: Any, *, province: str, city: str, district: str) -> Any:
    filtered = gdf

    filters = [(1, province), (2, city), (3, district)]
    for idx, target in filters:
        target = (target or "").strip()
        if not target:
            continue
        cols = [c for c in filtered.columns if c.upper() in {f"NAME_{idx}", f"VARNAME_{idx}", f"NL_NAME_{idx}"}]
        if not cols:
            continue

        mask = None
        for col in cols:
            hit = filtered[col].astype(str).map(lambda v: _contains_token(v, target))
            mask = hit if mask is None else (mask | hit)

        if mask is None:
            continue
        filtered = filtered[mask]

    return filtered


def _save_with_fallback(gdf: gpd.GeoDataFrame, output_base: Path, prefer: str) -> Path:
    prefer = prefer.lower()
    if prefer not in {"auto", "shp", "geojson"}:
        prefer = "auto"

    def _save_shp(path_no_ext: Path) -> Path:
        shp_path = path_no_ext.with_suffix(".shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile", encoding="utf-8")
        return shp_path

    def _save_geojson(path_no_ext: Path) -> Path:
        geo_path = path_no_ext.with_suffix(".geojson")
        gdf.to_file(geo_path, driver="GeoJSON")
        return geo_path

    if prefer == "shp":
        return _save_shp(output_base)
    if prefer == "geojson":
        return _save_geojson(output_base)

    try:
        return _save_shp(output_base)
    except Exception:
        return _save_geojson(output_base)


def _save_as_geojson_or_shp(gdf: Any, output_base: Path, output_format: str, convert_geojson_to_shp: bool) -> Path:
    fmt = str(output_format or "geojson").strip().lower()
    want_shp = fmt == "shp" or bool(convert_geojson_to_shp)
    if want_shp:
        shp_path = output_base.with_suffix(".shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile", encoding="utf-8")
        return shp_path
    geojson_path = output_base.with_suffix(".geojson")
    gdf.to_file(geojson_path, driver="GeoJSON")
    return geojson_path


def fetch_geoboundaries_boundary(
    *,
    country: str,
    level: str = "province",
    adm_level: Optional[int] = None,
    place_name: str = "",
    output_name: str = "",
    output_dir: str = ".",
    output_format: str = "shp",
    convert_geojson_to_shp: bool = True,
) -> Path:
    gpd = _require_geopandas()
    iso3 = _resolve_iso3(country)
    target_adm = _resolve_adm_level(level, adm_level)
    if target_adm > 4:
        raise ValueError("geoBoundaries current coverage is typically ADM0-ADM4. Please use adm-level <= 4.")

    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_stem = output_name.strip() if output_name.strip() else f"{iso3.lower()}_geoboundaries_adm{target_adm}"
    out_stem = re.sub(r"[^A-Za-z0-9._-]", "_", out_stem)
    out_base = out_dir / out_stem

    meta_url = GEOBOUNDARIES_META_URL_TEMPLATE.format(iso3=iso3, adm=target_adm)
    meta_resp = requests.get(meta_url, timeout=30)
    if meta_resp.status_code != 200:
        raise RuntimeError(
            f"geoBoundaries metadata unavailable for {iso3} ADM{target_adm}. HTTP {meta_resp.status_code}."
        )
    meta = meta_resp.json()
    gj_url = str(meta.get("gjDownloadURL") or "").strip()
    if not gj_url:
        raise RuntimeError(f"geoBoundaries gjDownloadURL missing for {iso3} ADM{target_adm}.")

    with tempfile.TemporaryDirectory(prefix="geoboundaries_dl_") as td:
        temp_geojson = Path(td) / f"geoboundaries_{iso3}_adm{target_adm}.geojson"
        _download_file(gj_url, temp_geojson)
        gdf = gpd.read_file(temp_geojson)

        target = str(place_name or "").strip()
        if target:
            if "shapeName" not in gdf.columns:
                raise RuntimeError("geoBoundaries data missing shapeName column, cannot filter by place_name.")
            mask = gdf["shapeName"].astype(str).map(lambda v: _contains_token(v, target))
            gdf = gdf[mask]
            if gdf.empty:
                raise RuntimeError(
                    f"No matching shapeName found for place_name='{target}' in {iso3} ADM{target_adm}."
                )
        return _save_as_geojson_or_shp(
            gdf=gdf,
            output_base=out_base,
            output_format=output_format,
            convert_geojson_to_shp=convert_geojson_to_shp,
        )


class GetAdministrativeDivisionGeoBoundariesInput(BaseModel):
    country: str = Field(..., description="Country name or ISO3, e.g. 'Iran' or 'IRN'.")
    level: str = Field(
        "province",
        description="Semantic level: country/province/city/county/district/town/local. Ignored if adm_level provided.",
    )
    adm_level: Optional[int] = Field(
        default=None, description="Exact ADM level, 0-4 for geoBoundaries. Overrides level when set."
    )
    place_name: str = Field(
        default="",
        description="Optional shapeName filter at target ADM level (e.g., 'Tehran'). Leave empty for full-country layer.",
    )
    input_name: str = Field(
        ...,
        description="Output filename in current workspace inputs (with or without extension).",
    )
    output_format: str = Field(
        default="shp",
        description="Preferred output format: 'geojson' or 'shp'. Default is 'shp'.",
    )
    convert_geojson_to_shp: bool = Field(
        default=True,
        description="When true, force ESRI Shapefile output even though source is geojson (default true).",
    )


def get_administrative_division_geoboundaries(
    country: str,
    input_name: str,
    level: str = "province",
    adm_level: Optional[int] = None,
    place_name: str = "",
    output_format: str = "shp",
    convert_geojson_to_shp: bool = True,
    config: Optional[RunnableConfig] = None,
) -> str:
    try:
        thread_id = _resolve_thread_id_from_config(config)
        raw_name = str(input_name or "").strip()
        if not raw_name:
            raise ValueError("input_name is required.")

        name_base, ext = Path(raw_name).stem, Path(raw_name).suffix.lower()
        if ext in {".shp", ".geojson"}:
            normalized_name = raw_name
        else:
            normalized_ext = ".shp" if (str(output_format).lower() == "shp" or convert_geojson_to_shp) else ".geojson"
            normalized_name = f"{name_base}{normalized_ext}"

        abs_input_path = Path(storage_manager.resolve_input_path(normalized_name, thread_id=thread_id))
        output_dir = str(abs_input_path.parent)
        output_name = abs_input_path.stem

        saved_path = fetch_geoboundaries_boundary(
            country=country,
            level=level,
            adm_level=adm_level,
            place_name=place_name,
            output_name=output_name,
            output_dir=output_dir,
            output_format=output_format,
            convert_geojson_to_shp=convert_geojson_to_shp,
        )
        payload = {
            "status": "success",
            "source": "geoBoundaries",
            "country": country,
            "level": level,
            "adm_level": _resolve_adm_level(level, adm_level),
            "place_name": place_name,
            "saved_file": saved_path.name,
            "saved_path": str(saved_path),
            "format": saved_path.suffix.lower().lstrip("."),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps(
            {
                "status": "fail",
                "source": "geoBoundaries",
                "error": str(exc),
            },
            ensure_ascii=False,
            indent=2,
        )


get_administrative_division_geoboundaries_tool = StructuredTool.from_function(
    get_administrative_division_geoboundaries,
    name="get_administrative_division_geoboundaries_tool",
    description=(
        "Download global administrative boundaries from geoBoundaries by country and ADM level "
        "(ADM0-ADM4), optionally filter by place_name, and save into current workspace inputs/. "
        "Supports output as GeoJSON, or convert GeoJSON to ESRI Shapefile when requested."
    ),
    args_schema=GetAdministrativeDivisionGeoBoundariesInput,
)


def fetch_admin_boundary(
    *,
    country: str,
    level: str = "country",
    adm_level: Optional[int] = None,
    province: str = "",
    city: str = "",
    district: str = "",
    output_name: str = "",
    output_dir: str = ".",
    format_preference: str = "auto",
) -> Path:
    gpd = _require_geopandas()
    iso3 = _resolve_iso3(country)
    target_adm = _resolve_adm_level(level, adm_level)
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    out_stem = output_name.strip() if output_name.strip() else f"{iso3.lower()}_adm{target_adm}"
    out_stem = re.sub(r"[^A-Za-z0-9._-]", "_", out_stem)
    out_base = out_dir / out_stem

    # Primary path: GADM shapefile
    with tempfile.TemporaryDirectory(prefix="gadm_dl_") as td:
        temp_dir = Path(td)
        zip_path = temp_dir / f"gadm41_{iso3}_shp.zip"
        try:
            _download_file(GADM_URL_TEMPLATE.format(iso3=iso3), zip_path)
            with zipfile.ZipFile(zip_path, "r") as zf:
                layer_member = _pick_gadm_layer_name(zf.namelist(), iso3=iso3, adm_level=target_adm)
            if not layer_member:
                raise RuntimeError(f"No shapefile layers found in GADM package for {iso3}")

            shp_path = _extract_layer_sidecars(zip_path, layer_member, temp_dir / "layer")
            gdf = gpd.read_file(shp_path)
            gdf = _filter_admin(gdf, province=province, city=city, district=district)
            if gdf.empty:
                raise RuntimeError(
                    "No matching features after filtering. "
                    f"country={country}, province={province}, city={city}, district={district}"
                )
            return _save_with_fallback(gdf, out_base, prefer=format_preference)
        except Exception as gadm_err:
            # Fallback path: geoBoundaries GeoJSON
            meta_url = GEOBOUNDARIES_META_URL_TEMPLATE.format(iso3=iso3, adm=target_adm)
            meta = requests.get(meta_url, timeout=30)
            meta.raise_for_status()
            meta_json = meta.json()
            gj_url = str(meta_json.get("gjDownloadURL") or "").strip()
            if not gj_url:
                raise RuntimeError(
                    f"GADM download failed and geoBoundaries has no gjDownloadURL for {iso3} ADM{target_adm}. "
                    f"GADM error: {gadm_err}"
                )
            geo_path = temp_dir / f"geoboundaries_{iso3}_adm{target_adm}.geojson"
            _download_file(gj_url, geo_path)
            gdf = gpd.read_file(geo_path)
            gdf = _filter_admin(gdf, province=province, city=city, district=district)
            if gdf.empty:
                raise RuntimeError(
                    "GeoJSON fallback loaded, but no matching features after filtering. "
                    f"country={country}, province={province}, city={city}, district={district}"
                )
            return _save_with_fallback(gdf, out_base, prefer="geojson")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Fetch global administrative boundaries. Primary source: GADM SHP (ADM0-ADM4), "
            "fallback source: geoBoundaries GeoJSON. Supports filtering by country/province/city/district."
        )
    )
    p.add_argument("--country", required=True, help="Country name or ISO3 (e.g., China or CHN)")
    p.add_argument(
        "--level",
        default="country",
        choices=["country", "province", "state", "city", "county", "district", "town", "local"],
        help="Semantic level selector (ignored when --adm-level is provided).",
    )
    p.add_argument("--adm-level", type=int, default=None, help="Exact admin level ADM0-ADM5.")
    p.add_argument("--province", default="", help="Province/state name filter (NAME_1-like fields).")
    p.add_argument("--city", default="", help="City name filter (NAME_2-like fields).")
    p.add_argument("--district", default="", help="District/county name filter (NAME_3-like fields).")
    p.add_argument("--output-dir", default=".", help="Output directory.")
    p.add_argument("--output-name", default="", help="Output base filename without extension.")
    p.add_argument(
        "--format",
        default="auto",
        choices=["auto", "shp", "geojson"],
        help="Output preference. auto means SHP first, then GeoJSON fallback.",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    try:
        path = fetch_admin_boundary(
            country=args.country,
            level=args.level,
            adm_level=args.adm_level,
            province=args.province,
            city=args.city,
            district=args.district,
            output_name=args.output_name,
            output_dir=args.output_dir,
            format_preference=args.format,
        )
        print(f"[ok] saved: {path}")
        return 0
    except Exception as exc:
        print(f"[error] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
