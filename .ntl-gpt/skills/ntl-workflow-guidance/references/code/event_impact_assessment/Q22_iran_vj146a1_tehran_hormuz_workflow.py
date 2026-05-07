from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box

from storage_manager import storage_manager
from tools.official_vj_dnb_gif_tool import run_official_vj_dnb_gif
from tools.official_vj_dnb_pipeline_tool import run_official_vj_dnb_fullchain


THREAD_ID = "debug"
REPO_ROOT = Path(__file__).resolve().parents[7]

PIPELINE_RUN_LABEL = "iran_conflict_vj146a1_20260225_20260305_strict"
CASE_ROOT_VIRTUAL = "/data/processed/workflow_cases/q22_iran_vj146a1_tehran_hormuz"
TEHRAN_BOUNDARY_VIRTUAL = f"{CASE_ROOT_VIRTUAL}/map_inputs/tehran_boundary.geojson"
TEHRAN_EVENTS_VIRTUAL = f"{CASE_ROOT_VIRTUAL}/map_inputs/tehran_recent_strikes.geojson"
VJ146A1_INPUT_VIRTUAL = (
    f"/data/processed/official_vj_dnb_pipeline_runs/{PIPELINE_RUN_LABEL}/"
    "gridded_workspace/outputs/VJ146A1"
)

IRAN_BBOX = "44.03,25.08,63.33,39.77"
TEHRAN_VIEW_BBOX = "51.0090099001868,35.4614815995962,51.86879269992319,35.999977199887894"
HORMUZ_VIEW_BBOX = "55.589999999999996,24.77,58.213,27.5333"

ADM2_PATH = REPO_ROOT / "base_data" / "Iran_War" / "data" / "boundaries" / "iran_geoboundaries_all_levels_shp" / "iran_geoboundaries_adm2.shp"
EVENT_PATH = REPO_ROOT / "base_data" / "Iran_War" / "data" / "event_feeds" / "inss_arcgis_strikes_latest" / "inss_arcgis_strikes_merged.geojson"


def _real_output_path(virtual_path: str) -> Path:
    return Path(storage_manager.resolve_deepagents_path(virtual_path, thread_id=THREAD_ID))


def _prepare_tehran_inputs() -> dict[str, str]:
    tehran_boundary_path = _real_output_path(TEHRAN_BOUNDARY_VIRTUAL)
    tehran_events_path = _real_output_path(TEHRAN_EVENTS_VIRTUAL)
    tehran_boundary_path.parent.mkdir(parents=True, exist_ok=True)

    adm2 = gpd.read_file(ADM2_PATH).to_crs(4326)
    tehran = adm2[adm2["shapeName"].astype(str).isin(["Tehran", "City of Tehran"])].copy()
    if tehran.empty:
        raise RuntimeError("Tehran boundary not found in ADM2 boundary dataset")
    tehran.to_file(tehran_boundary_path, driver="GeoJSON")

    events = gpd.read_file(EVENT_PATH).to_crs(4326)
    window = box(*map(float, TEHRAN_VIEW_BBOX.split(",")))
    tehran_recent = events[
        events.geometry.within(window)
        & events["date_iso"].astype(str).between("2026-02-25", "2026-03-05")
    ].copy()
    tehran_recent.to_file(tehran_events_path, driver="GeoJSON")

    return {
        "tehran_boundary": TEHRAN_BOUNDARY_VIRTUAL,
        "tehran_events": TEHRAN_EVENTS_VIRTUAL,
    }


def main() -> None:
    fullchain = run_official_vj_dnb_fullchain(
        start_date="2026-02-25",
        end_date="2026-03-05",
        bbox=IRAN_BBOX,
        output_root="official_vj_dnb_pipeline_runs",
        run_label=PIPELINE_RUN_LABEL,
        sources="VJ146A1",
        qa_mode="strict",
        generate_gif=False,
        token_env="EARTHDATA_TOKEN",
    )

    prepared = _prepare_tehran_inputs()

    tehran = run_official_vj_dnb_gif(
        input_dir=VJ146A1_INPUT_VIRTUAL,
        output_root="official_vj_dnb_gif_runs",
        run_label="tehran_0225_0305_strict_white_viridis",
        style_palette="white_viridis",
        boundary_vector=prepared["tehran_boundary"],
        overlay_vector=prepared["tehran_events"],
        boundary_edge_color="#4b5563",
        boundary_linewidth=1.25,
        boundary_alpha=0.95,
        view_bbox=TEHRAN_VIEW_BBOX,
        title_prefix="Tehran VJ146A1 Strict",
        basemap_style="light",
        basemap_provider="CartoDB.Positron",
        basemap_alpha=1.0,
        ntl_alpha=0.93,
        transparent_below=0.25,
        duration_ms=850,
        cmap="viridis",
    )

    hormuz = run_official_vj_dnb_gif(
        input_dir=VJ146A1_INPUT_VIRTUAL,
        output_root="official_vj_dnb_gif_runs",
        run_label="hormuz_0225_0305_strict_white_viridis",
        style_palette="white_viridis",
        overlay_vector="/shared/Iran_War/data/ports/hormuz/hormuz_ports.geojson",
        overlay_label_field="name",
        point_legend_label="Hormuz Ports",
        view_bbox=HORMUZ_VIEW_BBOX,
        title_prefix="Hormuz VJ146A1 Strict",
        basemap_style="light",
        basemap_provider="CartoDB.Positron",
        basemap_alpha=1.0,
        ntl_alpha=0.93,
        transparent_below=0.25,
        duration_ms=850,
        cmap="viridis",
    )

    print(json.dumps({"fullchain": fullchain, "tehran": tehran, "hormuz": hormuz}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
