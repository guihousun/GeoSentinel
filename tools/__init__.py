from __future__ import annotations

from collections.abc import Iterator, Sequence
from importlib import import_module
from typing import Any


_EXPORTS: dict[str, tuple[str, str]] = {
    "NTL_composite_local_tool": (".NTL_Composite", "NTL_composite_local_tool"),
    "VNP46A2_angular_correction_tool": (".VNP46A2_angular_correction", "VNP46A2_angular_correction_tool"),
    "VNP46A2_seasonal_adjustment_tool": (
        ".NTL_seasonal_adjustment",
        "VNP46A2_seasonal_adjustment_tool",
    ),
    "VNP46A2_persistence_classification_tool": (
        ".VNP46A2_persistence",
        "VNP46A2_persistence_classification_tool",
    ),
    "dmsp_viirs_harmonization_tool": (
        ".NTL_cross_sensor_harmonization",
        "dmsp_viirs_harmonization_tool",
    ),
    "dmsp_evi_preprocess_tool": (".NTL_preprocess", "dmsp_evi_preprocess_tool"),
    "vnci_index_tool": (".NPP_viirs_index_tool", "vnci_index_tool"),
    "urban_extraction_by_thresholding_tool": (".NTL_urban_structure_extract", "urban_extraction_by_thresholding_tool"),
    "svm_urban_extraction_tool": (".NTL_urban_structure_extract", "svm_urban_extraction_tool"),
    "electrified_detection_tool": (".electrified_detection", "electrified_detection_tool"),
    "detect_urban_centres_tool": (".NTL_urban_structure_extract", "detect_urban_centres_tool"),
    "NTL_raster_statistics_tool": (".NTL_raster_stats", "NTL_raster_statistics_tool"),
    "NTL_Daily_ANTL_Statistics": (".NTL_raster_stats_GEE", "NTL_Daily_ANTL_Statistics"),
    "NTL_Trend_Analysis": (".NTL_trend_detection_tool", "NTL_Trend_Analysis"),
    "otsu_road_extraction_tool": (".main_road", "otsu_road_extraction_tool"),
    "road_mask_to_polyline_tool": (".road_vectorization", "road_mask_to_polyline_tool"),
    "detect_ntl_anomaly_tool": (".NTL_anomaly_detection_tool", "detect_ntl_anomaly_tool"),
    "NTL_Knowledge_Base": (".NTL_Knowledge_Base_Searcher", "NTL_Knowledge_Base"),
    "get_administrative_division_tool": (".GaoDe_tool", "get_administrative_division_tool"),
    "poi_search_tool": (".GaoDe_tool", "poi_search_tool"),
    "reverse_geocode_tool": (".GaoDe_tool", "reverse_geocode_tool"),
    "geocode_tool": (".GaoDe_tool", "geocode_tool"),
    "get_administrative_division_osm_tool": (".GaoDe_tool", "get_administrative_division_osm_tool"),
    "NTL_download_tool": (".GEE_download", "NTL_download_tool"),
    "GEE_raster_download_tool": (".GEE_generic_download", "GEE_raster_download_tool"),
    "GEE_batch_export_tool": (".GEE_batch_export", "GEE_batch_export_tool"),
    "GEE_export_status_tool": (".GEE_batch_export", "GEE_export_status_tool"),
    "GEE_export_cancel_tool": (".GEE_batch_export", "GEE_export_cancel_tool"),
    "get_administrative_division_geoboundaries_tool": (
        ".global_admin_boundary_fetch",
        "get_administrative_division_geoboundaries_tool",
    ),
    "NDVI_download_tool": (".Other_image_download", "NDVI_download_tool"),
    "LandScan_download_tool": (".Other_image_download", "LandScan_download_tool"),
    "google_bigquery_search": (".Google_Bigquery", "google_bigquery_search"),
    "Tavily_search": (".TavilySearch", "Tavily_search"),
    "China_Official_Stats_tool": (".China_official_stats", "China_Official_Stats_tool"),
    "China_Official_GDP_tool": (".China_official_stats", "China_Official_GDP_tool"),
    "Country_GDP_Search_tool": (".country_gdp_tool", "Country_GDP_Search_tool"),
    "geodata_inspector_tool": (".geodata_inspector_tool", "geodata_inspector_tool"),
    "geodata_quick_check_tool": (".geodata_inspector_tool", "geodata_quick_check_tool"),
    "GeoCode_COT_Validation_tool": (".NTL_Code_generation", "GeoCode_COT_Validation_tool"),
    "execute_geospatial_script_tool": (".NTL_Code_generation", "execute_geospatial_script_tool"),
    "GeoCode_Knowledge_Recipes_tool": (".geocode_knowledge_tool", "GeoCode_Knowledge_Recipes_tool"),
    "GEE_dataset_router_tool": (".GEE_specialist_toolkit", "GEE_dataset_router_tool"),
    "GEE_request_plan_tool": (".GEE_specialist_toolkit", "GEE_request_plan_tool"),
    "GEE_script_blueprint_tool": (".GEE_specialist_toolkit", "GEE_script_blueprint_tool"),
    "GEE_catalog_discovery_tool": (".GEE_specialist_toolkit", "GEE_catalog_discovery_tool"),
    "GEE_dataset_metadata_tool": (".GEE_specialist_toolkit", "GEE_dataset_metadata_tool"),
    "dataset_latest_availability_tool": (".GEE_specialist_toolkit", "dataset_latest_availability_tool"),
    "NTL_estimate_indicator_provincial_tool": (
        ".NTL_estimate_indicator",
        "NTL_estimate_indicator_provincial_tool",
    ),
    "DEI_estimate_city_tool": (".NTL_estimate_indicator", "DEI_estimate_city_tool"),
    "official_vj_dnb_fullchain_tool": (".official_vj_dnb_pipeline_tool", "official_vj_dnb_fullchain_tool"),
    "official_vj_dnb_preprocess_tool": (".official_vj_dnb_preprocess_tool", "official_vj_dnb_preprocess_tool"),
    "convert_vj102_vj103_precise_to_tif_tool": (
        ".official_vj_dnb_preprocess_tool",
        "convert_vj102_vj103_precise_to_tif_tool",
    ),
    "NTL_preview_tool": (".ntl_preview_tool", "NTL_preview_tool"),
    "official_vj_dnb_gif_tool": (".official_vj_dnb_gif_tool", "official_vj_dnb_gif_tool"),
    "official_ntl_ais_fusion_tool": (".official_ntl_ais_fusion_tool", "official_ntl_ais_fusion_tool"),
    "official_vnp46a2_h5_country_mosaic_tool": (
        ".vnp46a2_official_h5_country_tool",
        "official_vnp46a2_h5_country_mosaic_tool",
    ),
    "official_vnp46a1_h5_tool": (
        ".vnp46a1_official_h5_tool",
        "official_vnp46a1_h5_tool",
    ),
    "uploaded_pdf_understanding_tool": (".uploaded_file_understanding_tool", "uploaded_pdf_understanding_tool"),
    "wrap_tool_json_safe": (".tool_json_safety", "wrap_tool_json_safe"),
    "conflict_ntl_agent_system_tool": (".conflict_ntl", "conflict_ntl_agent_system_tool"),
    "conflict_ntl_screen_events_tool": (".conflict_ntl", "conflict_ntl_screen_events_tool"),
    "conflict_ntl_generate_analysis_units_tool": (
        ".conflict_ntl",
        "conflict_ntl_generate_analysis_units_tool",
    ),
    "conflict_ntl_source_freshness_tool": (".conflict_ntl", "conflict_ntl_source_freshness_tool"),
    "conflict_ntl_fetch_isw_events_tool": (".conflict_ntl", "conflict_ntl_fetch_isw_events_tool"),
    "conflict_ntl_build_case_report_tool": (".conflict_ntl", "conflict_ntl_build_case_report_tool"),
    "conflict_ntl_compare_case_buffers_tool": (".conflict_ntl", "conflict_ntl_compare_case_buffers_tool"),
    "conflict_city_event_ranking_tool": (".conflict_city_events", "conflict_city_event_ranking_tool"),
}

_ROLE_GROUPS: dict[str, list[str]] = {
    "engineer_tools": [
        "geodata_inspector_tool",
        "geodata_quick_check_tool",
        "execute_geospatial_script_tool",
        "uploaded_pdf_understanding_tool",
    ],
    "data_searcher_tools": [
        "reverse_geocode_tool",
        "geocode_tool",
        "NTL_download_tool",
        "GEE_raster_download_tool",
        "GEE_batch_export_tool",
        "GEE_export_status_tool",
        "GEE_export_cancel_tool",
        "get_administrative_division_tool",
        "poi_search_tool",
        "get_administrative_division_geoboundaries_tool",
        "NDVI_download_tool",
        "LandScan_download_tool",
        "official_vj_dnb_fullchain_tool",
        "official_vj_dnb_preprocess_tool",
        "convert_vj102_vj103_precise_to_tif_tool",
        "NTL_preview_tool",
        "official_vj_dnb_gif_tool",
        "official_ntl_ais_fusion_tool",
        "official_vnp46a2_h5_country_mosaic_tool",
        "official_vnp46a1_h5_tool",
        "China_Official_Stats_tool",
        "China_Official_GDP_tool",
        "Country_GDP_Search_tool",
        "Tavily_search",
        "google_bigquery_search",
        "GEE_request_plan_tool",
        "GEE_script_blueprint_tool",
        "GEE_catalog_discovery_tool",
        "GEE_dataset_metadata_tool",
        "dataset_latest_availability_tool",
        "geodata_inspector_tool",
        "geodata_quick_check_tool",
        "VNP46A2_angular_correction_tool",
        "dmsp_evi_preprocess_tool",
        "NTL_composite_local_tool",
        "vnci_index_tool",
    ],
    "analyst_tools": [
        "geodata_inspector_tool",
        "geodata_quick_check_tool",
        "VNP46A2_seasonal_adjustment_tool",
        "VNP46A2_persistence_classification_tool",
        "dmsp_viirs_harmonization_tool",
        "urban_extraction_by_thresholding_tool",
        "svm_urban_extraction_tool",
        "electrified_detection_tool",
        "otsu_road_extraction_tool",
        "road_mask_to_polyline_tool",
        "detect_urban_centres_tool",
        "NTL_Daily_ANTL_Statistics",
        "NTL_Trend_Analysis",
        "detect_ntl_anomaly_tool",
        "NTL_raster_statistics_tool",
        "NTL_estimate_indicator_provincial_tool",
        "DEI_estimate_city_tool",
        "official_vj_dnb_gif_tool",
        "official_ntl_ais_fusion_tool",
        "uploaded_pdf_understanding_tool",
        "execute_geospatial_script_tool",
    ],
    "event_tracker_tools": [
        "conflict_ntl_fetch_isw_events_tool",
        "conflict_ntl_source_freshness_tool",
        "conflict_ntl_screen_events_tool",
        "conflict_city_event_ranking_tool",
        "Tavily_search",
        "google_bigquery_search",
        "geocode_tool",
        "reverse_geocode_tool",
        "get_administrative_division_geoboundaries_tool",
    ],
}


def _strict_union(*groups: list[str]) -> list[str]:
    return list(dict.fromkeys(name for group in groups for name in group))


_GROUPS: dict[str, list[str]] = {
    **_ROLE_GROUPS,
    "Code_tools": [
        "GeoCode_Knowledge_Recipes_tool",
        "execute_geospatial_script_tool",
        "GeoCode_COT_Validation_tool",
    ],
    "single_agent_tools": _strict_union(
        _ROLE_GROUPS["engineer_tools"],
        _ROLE_GROUPS["data_searcher_tools"],
        _ROLE_GROUPS["analyst_tools"],
        _ROLE_GROUPS["event_tracker_tools"],
    ),
    "specialized_tool_catalog": [
        "VNP46A2_angular_correction_tool",
        "VNP46A2_seasonal_adjustment_tool",
        "VNP46A2_persistence_classification_tool",
        "dmsp_viirs_harmonization_tool",
        "dmsp_evi_preprocess_tool",
        "urban_extraction_by_thresholding_tool",
        "svm_urban_extraction_tool",
        "electrified_detection_tool",
        "otsu_road_extraction_tool",
        "road_mask_to_polyline_tool",
        "detect_urban_centres_tool",
        "NTL_composite_local_tool",
        "NTL_estimate_indicator_provincial_tool",
        "DEI_estimate_city_tool",
        "official_vj_dnb_fullchain_tool",
        "official_vj_dnb_preprocess_tool",
        "convert_vj102_vj103_precise_to_tif_tool",
        "NTL_preview_tool",
        "official_vj_dnb_gif_tool",
        "official_ntl_ais_fusion_tool",
        "official_vnp46a2_h5_country_mosaic_tool",
        "official_vnp46a1_h5_tool",
        "vnci_index_tool",
        "conflict_ntl_agent_system_tool",
        "conflict_ntl_fetch_isw_events_tool",
        "conflict_city_event_ranking_tool",
    ],
}


def _load_export(name: str) -> Any:
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return _load_export(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(_EXPORTS.keys()) + list(_GROUPS.keys())))


class LazyToolCollection(Sequence[Any]):
    def __init__(self, export_names: list[str]):
        self._export_names = list(export_names)
        self._cache: list[Any] | None = None

    def _materialize(self) -> list[Any]:
        if self._cache is None:
            wrap = _load_export("wrap_tool_json_safe")
            self._cache = [wrap(_load_export(name)) for name in self._export_names]
        return self._cache

    def __iter__(self) -> Iterator[Any]:
        return iter(self._materialize())

    def __len__(self) -> int:
        return len(self._materialize())

    def __getitem__(self, item: int | slice) -> Any:
        return self._materialize()[item]

    def __add__(self, other: Any) -> list[Any]:
        return list(self._materialize()) + list(other)

    def __radd__(self, other: Any) -> list[Any]:
        return list(other) + list(self._materialize())

    def __repr__(self) -> str:
        status = "loaded" if self._cache is not None else "lazy"
        return f"LazyToolCollection(status={status}, size={len(self._export_names)})"

    @property
    def export_names(self) -> tuple[str, ...]:
        return tuple(self._export_names)


data_searcher_tools = LazyToolCollection(_GROUPS["data_searcher_tools"])
analyst_tools = LazyToolCollection(_GROUPS["analyst_tools"])
event_tracker_tools = LazyToolCollection(_GROUPS["event_tracker_tools"])
engineer_tools = LazyToolCollection(_GROUPS["engineer_tools"])
single_agent_tools = LazyToolCollection(_GROUPS["single_agent_tools"])
Code_tools = LazyToolCollection(_GROUPS["Code_tools"])
Engineer_tools = engineer_tools
specialized_tool_catalog = LazyToolCollection(_GROUPS["specialized_tool_catalog"])


__all__ = sorted(list(_EXPORTS.keys()) + list(_GROUPS.keys()))
