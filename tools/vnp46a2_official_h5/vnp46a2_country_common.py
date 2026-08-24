from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import osmnx as ox
from dotenv import load_dotenv


DATASET_ID = "NASA/VIIRS/002/VNP46A2"
BAND = "DNB_BRDF_Corrected_NTL"
SIMPLIFY_TOLERANCE_DEG = 0.001
DEFAULT_START = "2026-01-27"
DEFAULT_OUTPUT_ROOT = Path(r"F:\专业数据\夜间灯光")


@dataclass(frozen=True)
class CountrySpec:
    iso3: str
    slug: str
    name: str
    osm_query: str


COUNTRIES: list[CountrySpec] = [
    CountrySpec("IRN", "iran", "Iran", "Iran"),
    CountrySpec("SAU", "saudi_arabia", "Saudi Arabia", "Saudi Arabia"),
    CountrySpec("IRQ", "iraq", "Iraq", "Iraq"),
    CountrySpec("ARE", "united_arab_emirates", "United Arab Emirates", "United Arab Emirates"),
    CountrySpec("KWT", "kuwait", "Kuwait", "Kuwait"),
    CountrySpec("QAT", "qatar", "Qatar", "Qatar"),
    CountrySpec("BHR", "bahrain", "Bahrain", "Bahrain"),
    CountrySpec("SYR", "syria", "Syria", "Syria"),
    CountrySpec("ISR", "israel", "Israel", "Israel"),
    CountrySpec("IND", "india", "India", "India"),
    CountrySpec("PAK", "pakistan", "Pakistan", "Pakistan"),
    CountrySpec("MMR", "myanmar", "Myanmar", "Myanmar"),
    CountrySpec("THA", "thailand", "Thailand", "Thailand"),
    CountrySpec("VNM", "vietnam", "Vietnam", "Vietnam"),
    CountrySpec("MYS", "malaysia", "Malaysia", "Malaysia"),
    CountrySpec("IDN", "indonesia", "Indonesia", "Indonesia"),
    CountrySpec("SGP", "singapore", "Singapore", "Singapore"),
    CountrySpec("PHL", "philippines", "Philippines", "Philippines"),
    CountrySpec("JPN", "japan", "Japan", "Japan"),
    CountrySpec("KOR", "south_korea", "South Korea", "South Korea"),
    CountrySpec("NZL", "new_zealand", "New Zealand", "New Zealand"),
    CountrySpec("AUS", "australia", "Australia", "Australia"),
]


def load_project_env() -> None:
    for candidate in (Path.cwd() / ".env", Path(__file__).resolve().parents[1] / ".env"):
        if candidate.exists():
            load_dotenv(candidate)


def iter_dates(start: str, end: str) -> list[str]:
    current = datetime.strptime(start, "%Y-%m-%d").date()
    end_day = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    while current <= end_day:
        out.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return out


def selected_countries(tokens: Iterable[str] | None) -> list[CountrySpec]:
    if not tokens:
        return COUNTRIES
    wanted = {str(token).strip().lower() for token in tokens if str(token).strip()}
    out = [
        country
        for country in COUNTRIES
        if country.iso3.lower() in wanted
        or country.slug.lower() in wanted
        or country.name.lower() in wanted
    ]
    found = {item.iso3.lower() for item in out} | {item.slug.lower() for item in out} | {item.name.lower() for item in out}
    missing = sorted(wanted - found)
    if missing:
        raise ValueError(f"Unknown country selectors: {', '.join(missing)}")
    return out


def init_ee() -> str:
    import ee

    load_project_env()
    project = os.getenv("GEE_DEFAULT_PROJECT_ID")
    if project:
        ee.Initialize(project=project)
    else:
        ee.Initialize()
    return project or ""


def gee_latest_day() -> str:
    import ee

    millis = ee.ImageCollection(DATASET_ID).aggregate_max("system:time_start").getInfo()
    if millis is None:
        raise RuntimeError(f"No images found in {DATASET_ID}.")
    return datetime.fromtimestamp(float(millis) / 1000, tz=UTC).strftime("%Y-%m-%d")


def clamp_end_day(requested_end: str, latest_day: str) -> str:
    requested = datetime.strptime(requested_end, "%Y-%m-%d").date()
    latest = datetime.strptime(latest_day, "%Y-%m-%d").date()
    return min(requested, latest).strftime("%Y-%m-%d")


def run_dir(output_root: Path, start: str, end: str) -> Path:
    return output_root / f"VNP46A2_unfilled_osm_0p001_{start}_to_{end}"


def fetch_osm_boundary(country: CountrySpec, boundary_dir: Path) -> gpd.GeoDataFrame:
    path = boundary_dir / f"osm_admin0_{country.iso3}_{country.slug}.geojson"
    if path.exists() and path.stat().st_size > 1000:
        return gpd.read_file(path).to_crs("EPSG:4326")

    ox.settings.use_cache = True
    ox.settings.log_console = False
    gdf = ox.geocode_to_gdf(country.osm_query, which_result=1).to_crs("EPSG:4326")
    if gdf.empty:
        raise RuntimeError(f"OSM/Nominatim returned no boundary for {country.name}.")
    gdf = gdf.copy()
    gdf["download_source"] = "OSM Nominatim via osmnx.geocode_to_gdf"
    gdf["requested_place"] = country.osm_query
    gdf["iso3"] = country.iso3
    gdf["slug"] = country.slug
    boundary_dir.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GeoJSON")
    return gdf


def simplified_boundary(gdf: gpd.GeoDataFrame, country: CountrySpec, simplified_dir: Path) -> gpd.GeoDataFrame:
    out = simplified_dir / f"osm_admin0_{country.iso3}_{country.slug}_simplified_0p001.geojson"
    if out.exists() and out.stat().st_size > 1000:
        return gpd.read_file(out).to_crs("EPSG:4326")
    simplified = gdf.copy()
    simplified["geometry"] = simplified.geometry.simplify(SIMPLIFY_TOLERANCE_DEG, preserve_topology=True)
    simplified = simplified[~simplified.geometry.is_empty & simplified.geometry.notna()].copy()
    simplified_dir.mkdir(parents=True, exist_ok=True)
    simplified.to_file(out, driver="GeoJSON")
    return simplified


def area_km2(gdf: gpd.GeoDataFrame) -> float:
    return float(gdf.to_crs("EPSG:6933").geometry.area.sum() / 1_000_000.0)

