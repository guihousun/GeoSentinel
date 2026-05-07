from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSpec:
    short_name: str
    processing_mode: str
    variable_candidates: tuple[str, ...]
    qa_variable_candidates: dict[str, tuple[str, ...]] | None = None
    default_qa_mode: str = "balanced"
    night_only: bool = False


_SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "VNP46A1": SourceSpec(
        short_name="VNP46A1",
        processing_mode="gridded_tile_clip",
        variable_candidates=(
            "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance",
        ),
        qa_variable_candidates={
            "QF_Cloud_Mask": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_Cloud_Mask",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_Cloud_Mask",
                "QF_Cloud_Mask",
            ),
            "QF_DNB": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_DNB",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_DNB",
                "QF_DNB",
            ),
        },
        night_only=False,
    ),
    "VNP46A2": SourceSpec(
        short_name="VNP46A2",
        processing_mode="gridded_tile_clip",
        variable_candidates=(
            "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Gap_Filled_DNB_BRDF-Corrected_NTL",
            "Gap_Filled_DNB_BRDF-Corrected_NTL",
            "Gap_Filled_DNB_BRDF_Corrected_NTL",
            "DNB_BRDF-Corrected_NTL",
        ),
        qa_variable_candidates={
            "Mandatory_Quality_Flag": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Mandatory_Quality_Flag",
                "Mandatory_Quality_Flag",
            ),
            "QF_Cloud_Mask": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_Cloud_Mask",
                "QF_Cloud_Mask",
            ),
            "Snow_Flag": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Snow_Flag",
                "Snow_Flag",
            ),
        },
        night_only=False,
    ),
    "VNP46A3": SourceSpec(
        short_name="VNP46A3",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=False,
    ),
    "VNP46A4": SourceSpec(
        short_name="VNP46A4",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=False,
    ),
    "VJ146A1_NRT": SourceSpec(
        short_name="VJ146A1_NRT",
        processing_mode="gridded_tile_clip",
        variable_candidates=(
            "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance",
        ),
        qa_variable_candidates={
            "QF_Cloud_Mask": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_Cloud_Mask",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_Cloud_Mask",
                "QF_Cloud_Mask",
            ),
            "QF_DNB": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_DNB",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_DNB",
                "QF_DNB",
            ),
        },
        night_only=False,
    ),
    "VJ146A1G_NRT": SourceSpec(
        short_name="VJ146A1G_NRT",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=False,
    ),
    "VJ146A2": SourceSpec(
        short_name="VJ146A2",
        processing_mode="gridded_tile_clip",
        variable_candidates=(
            "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Gap_Filled_DNB_BRDF-Corrected_NTL",
            "Gap_Filled_DNB_BRDF-Corrected_NTL",
            "Gap_Filled_DNB_BRDF_Corrected_NTL",
            "DNB_BRDF-Corrected_NTL",
        ),
        qa_variable_candidates={
            "Mandatory_Quality_Flag": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Mandatory_Quality_Flag",
                "Mandatory_Quality_Flag",
            ),
            "QF_Cloud_Mask": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_Cloud_Mask",
                "QF_Cloud_Mask",
            ),
            "Snow_Flag": (
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/Snow_Flag",
                "Snow_Flag",
            ),
        },
        night_only=False,
    ),
    "VJ146A1": SourceSpec(
        short_name="VJ146A1",
        processing_mode="gridded_tile_clip",
        variable_candidates=(
            "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance_500m",
            "DNB_At_Sensor_Radiance",
        ),
        qa_variable_candidates={
            "QF_Cloud_Mask": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_Cloud_Mask",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_Cloud_Mask",
                "QF_Cloud_Mask",
            ),
            "QF_DNB": (
                "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/QF_DNB",
                "HDFEOS/GRIDS/VNP_Grid_DNB/Data Fields/QF_DNB",
                "QF_DNB",
            ),
        },
        night_only=False,
    ),
    "VJ102DNB": SourceSpec(
        short_name="VJ102DNB",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=True,
    ),
    "CLDMSK_L2_VIIRS_NOAA20": SourceSpec(
        short_name="CLDMSK_L2_VIIRS_NOAA20",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=True,
    ),
    "VJ102DNB_NRT": SourceSpec(
        short_name="VJ102DNB_NRT",
        processing_mode="feasibility_only",
        variable_candidates=(),
        night_only=True,
    ),
}


def get_default_sources() -> list[str]:
    return ["VNP46A1", "VNP46A2", "VNP46A3", "VNP46A4"]


def get_nrt_priority_sources() -> list[str]:
    return ["VNP46A1", "VNP46A2", "VNP46A3", "VNP46A4"]


def get_source_spec(source: str) -> SourceSpec:
    key = (source or "").strip().upper()
    if key not in _SOURCE_REGISTRY:
        raise ValueError(f"Unsupported source: {source}")
    return _SOURCE_REGISTRY[key]


def parse_sources_arg(raw: str | None) -> list[str]:
    if not raw:
        return get_default_sources()
    key_raw = raw.strip().lower()
    if key_raw in {"nrt_priority", "nrt-priority"}:
        return get_nrt_priority_sources()
    out: list[str] = []
    for part in raw.split(","):
        key = part.strip().upper()
        if not key:
            continue
        _ = get_source_spec(key)
        if key not in out:
            out.append(key)
    return out or get_default_sources()
