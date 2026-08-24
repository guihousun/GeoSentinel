from __future__ import annotations

import importlib
import importlib.util
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from affine import Affine
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import box


def _ntl_module():
    spec = importlib.util.find_spec("ntl_toolkit.core.ntl")
    assert spec is not None, "ntl_toolkit.core.ntl should exist"
    return importlib.import_module("ntl_toolkit.core.ntl")


def _write_raster(
    path: Path,
    values: np.ndarray,
    *,
    transform=None,
    crs: str | CRS | None = "EPSG:4326",
    nodata: float = -9999.0,
) -> Path:
    raster_transform = transform or from_origin(0.0, 2.0, 1.0, 1.0)
    array = np.asarray(values)
    if array.ndim == 2:
        band_count = 1
        height, width = array.shape
        writer = lambda dataset: dataset.write(array, 1)
    elif array.ndim == 3:
        band_count, height, width = array.shape
        writer = lambda dataset: dataset.write(array)
    else:
        raise ValueError("values must be a 2D or 3D array")

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=int(height),
        width=int(width),
        count=int(band_count),
        dtype=str(array.dtype),
        crs=crs,
        transform=raster_transform,
        nodata=nodata,
    ) as dataset:
        writer(dataset)
    return path


def _write_vector(path: Path, gdf: gpd.GeoDataFrame) -> Path:
    gdf.to_file(path, driver="GeoJSON")
    return path


def test_ntl_module_exports_required_public_callables() -> None:
    module = _ntl_module()

    for name in [
        "calculate_ntl_metrics",
        "calculate_ntl_metrics_for_raster",
        "calculate_zonal_statistics",
        "composite_ntl_rasters",
        "analyze_ntl_trend",
        "detect_ntl_anomaly",
    ]:
        assert hasattr(module, name), f"ntl_toolkit.core.ntl missing {name}"


def test_calculate_ntl_metrics_matches_known_values() -> None:
    values = np.array([[0.0, 2.0], [4.0, np.nan]], dtype=float)

    result = _ntl_module().calculate_ntl_metrics(
        values,
        pixel_area=1.0,
        selected=["TNTL", "LArea", "ANTL", "MaxNTL"],
    )

    assert result == {
        "MaxNTL": 4.0,
        "TNTL": 6.0,
        "LArea": 2.0,
        "ANTL": 2.0,
    }


def test_calculate_ntl_metrics_matches_explicit_formula_values() -> None:
    values = np.array(
        [
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 0.0],
            [0.0, 0.0, 2.0],
        ],
        dtype=float,
    )

    result = _ntl_module().calculate_ntl_metrics(values, pixel_area=2.0)

    assert list(result) == [
        "MaxNTL",
        "MinNTL",
        "SDNTL",
        "TNTL",
        "LArea",
        "3DPLand",
        "3DED",
        "3DLPI",
        "ANTL",
    ]
    assert result["MaxNTL"] == 2.0
    assert result["MinNTL"] == 0.0
    assert result["SDNTL"] == pytest.approx(float(np.std(values)))
    assert result["TNTL"] == 3.0
    assert result["LArea"] == 4.0
    assert result["3DPLand"] == pytest.approx(3.0 / (2.0 * 9.0))
    assert result["3DED"] == pytest.approx(4.0 / 3.0)
    assert result["3DLPI"] == pytest.approx(2.0 / 3.0)
    assert result["ANTL"] == pytest.approx(3.0 / 9.0)


def test_calculate_ntl_metrics_handles_all_zero_and_all_nan_inputs() -> None:
    module = _ntl_module()

    zeros = module.calculate_ntl_metrics(np.zeros((2, 2), dtype=float), pixel_area=1.0)
    all_nan = module.calculate_ntl_metrics(
        np.full((2, 2), np.nan, dtype=float),
        pixel_area=1.0,
    )

    assert zeros == {
        "MaxNTL": 0.0,
        "MinNTL": 0.0,
        "SDNTL": 0.0,
        "TNTL": 0.0,
        "LArea": 0.0,
        "3DPLand": None,
        "3DED": None,
        "3DLPI": None,
        "ANTL": 0.0,
    }
    assert all_nan == {
        "MaxNTL": None,
        "MinNTL": None,
        "SDNTL": None,
        "TNTL": 0.0,
        "LArea": 0.0,
        "3DPLand": None,
        "3DED": None,
        "3DLPI": None,
        "ANTL": None,
    }


def test_calculate_ntl_metrics_validates_selection_order_and_does_not_mutate_inputs() -> None:
    values = np.array([[1.0, np.inf], [3.0, -1.0]], dtype=float)
    original = values.copy()
    selected = ["ANTL", "TNTL", "MaxNTL"]

    result = _ntl_module().calculate_ntl_metrics(values, pixel_area=2.0, selected=selected)

    assert list(result) == ["MaxNTL", "TNTL", "ANTL"]
    assert result["MaxNTL"] == 3.0
    assert result["TNTL"] == 3.0
    assert result["ANTL"] == 1.0
    assert selected == ["ANTL", "TNTL", "MaxNTL"]
    np.testing.assert_array_equal(values, original)
    assert _ntl_module().calculate_ntl_metrics(values, pixel_area=1.0, selected=[]) == {}


@pytest.mark.parametrize(
    ("pixel_area", "selected", "match"),
    [
        (0.0, None, "pixel_area"),
        (-1.0, None, "pixel_area"),
        (float("nan"), None, "pixel_area"),
        (1.0, ["TNTL", "UnknownMetric"], "UnknownMetric"),
    ],
)
def test_calculate_ntl_metrics_rejects_invalid_arguments(
    pixel_area: float,
    selected: list[str] | None,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        _ntl_module().calculate_ntl_metrics(np.array([[1.0]], dtype=float), pixel_area=pixel_area, selected=selected)


def test_calculate_ntl_metrics_for_raster_reports_expected_metrics_and_geographic_warning(
    sample_raster_path: Path,
) -> None:
    result = _ntl_module().calculate_ntl_metrics_for_raster(
        sample_raster_path,
        selected=["TNTL", "ANTL", "LArea", "MaxNTL"],
    )

    assert result.status == "succeeded"
    assert result.tool == "calculate_ntl_metrics_for_raster"
    assert result.outputs == []
    assert result.warnings == ["GEOGRAPHIC_PIXEL_AREA"]
    assert result.metrics["MaxNTL"] == 3.0
    assert result.metrics["TNTL"] == 6.0
    assert result.metrics["LArea"] == 3.0
    assert result.metrics["ANTL"] == 2.0
    assert result.metrics["band"] == 1
    assert result.metrics["pixel_area"] == pytest.approx(1.0)


def test_calculate_ntl_metrics_for_raster_uses_projected_pixel_area(
    runtime_workspace: Path,
) -> None:
    raster_path = _write_raster(
        runtime_workspace / "inputs" / "projected_ntl.tif",
        np.array([[0.0, 5.0], [7.0, 0.0]], dtype=np.float32),
        transform=from_origin(0.0, 60.0, 30.0, 30.0),
        crs="EPSG:3857",
    )

    result = _ntl_module().calculate_ntl_metrics_for_raster(
        raster_path,
        selected=["LArea", "TNTL"],
    )

    assert result.status == "succeeded"
    assert result.warnings == []
    assert result.metrics["TNTL"] == 12.0
    assert result.metrics["LArea"] == pytest.approx(1800.0)
    assert result.metrics["pixel_area"] == pytest.approx(900.0)


@pytest.mark.parametrize(
    ("band", "expected_tntl"),
    [
        (1, 10.0),
        (np.int64(2), 100.0),
        (np.uint8(1), 10.0),
    ],
)
def test_calculate_ntl_metrics_for_raster_accepts_integral_band_types(
    multiband_raster_path: Path,
    band: int,
    expected_tntl: float,
) -> None:
    result = _ntl_module().calculate_ntl_metrics_for_raster(
        multiband_raster_path,
        band=band,
        selected=["TNTL"],
    )

    assert result.status == "succeeded"
    assert result.metrics["band"] == int(band)
    assert result.metrics["TNTL"] == expected_tntl


@pytest.mark.parametrize("band", [True, False, 1.0, 1.5, "1", None])
def test_calculate_ntl_metrics_for_raster_rejects_non_integral_band_types(
    sample_raster_path: Path,
    band: object,
) -> None:
    result = _ntl_module().calculate_ntl_metrics_for_raster(
        sample_raster_path,
        band=band,
        selected=["TNTL"],
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_PARAMETER"
    assert result.error.details["parameter"] == "band"
    assert result.error.details["value"] == band
    assert result.error.details["received_type"] == type(band).__name__


@pytest.mark.parametrize(
    ("path_factory", "band", "selected", "error_code"),
    [
        (lambda request: Path("inputs") / "missing.tif", 1, None, "INPUT_NOT_FOUND"),
        (lambda request: request.getfixturevalue("corrupt_raster_path"), 1, None, "RASTER_READ_FAILED"),
        (lambda request: request.getfixturevalue("raster_without_crs_path"), 1, None, "CRS_MISSING"),
        (lambda request: request.getfixturevalue("multiband_raster_path"), 3, None, "INVALID_PARAMETER"),
        (lambda request: request.getfixturevalue("sample_raster_path"), 1, ["BadMetric"], "INVALID_PARAMETER"),
    ],
)
def test_calculate_ntl_metrics_for_raster_reports_stable_failures(
    request: pytest.FixtureRequest,
    path_factory,
    band: int,
    selected: list[str] | None,
    error_code: str,
) -> None:
    raster_path = path_factory(request)

    result = _ntl_module().calculate_ntl_metrics_for_raster(
        raster_path,
        band=band,
        selected=selected,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == error_code


def test_calculate_zonal_statistics_writes_polygon_and_global_rows(
    admin_polygons_path: Path,
    matching_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "zonal_stats.csv"

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=[matching_raster_path],
        vector_path=admin_polygons_path,
        output_path=output_path,
        selected_indices=["TNTL", "LArea", "ANTL"],
    )

    assert result.status == "succeeded"
    assert result.outputs[0].path == str((runtime_workspace / output_path).resolve(strict=False))
    assert result.outputs[0].media_type == "text/csv"
    assert result.metrics["polygon_count"] == 2
    assert result.metrics["raster_count"] == 1
    assert result.metrics["row_count"] == 3
    assert result.metrics["only_global"] is False
    assert result.warnings == ["GEOGRAPHIC_PIXEL_AREA"]

    frame = pd.read_csv(runtime_workspace / output_path)

    assert frame.columns.tolist() == [
        "Raster_file",
        "Year",
        "Region",
        "TNTL",
        "LArea",
        "ANTL",
    ]
    assert frame["Region"].tolist() == ["west", "east", "Global_Summary"]
    assert frame["TNTL"].tolist() == [30.0, 40.0, 70.0]
    assert frame["LArea"].tolist() == [1.0, 1.0, 2.0]
    assert frame["ANTL"].tolist() == [30.0, 40.0, 35.0]


def test_calculate_zonal_statistics_prefers_case_insensitive_name_field(
    matching_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    vector_path = _write_vector(
        runtime_workspace / "inputs" / "amap_regions.geojson",
        gpd.GeoDataFrame(
            {
                "Level": ["district", "district"],
                "Name": ["west-name", "east-name"],
                "geometry": [box(0.0, 1.0, 1.0, 2.0), box(1.0, 0.0, 2.0, 1.0)],
            },
            crs="EPSG:4326",
        ),
    )
    output_path = Path("outputs") / "zonal_amap_names.csv"

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=[matching_raster_path],
        vector_path=vector_path,
        output_path=output_path,
        selected_indices=["ANTL"],
    )

    assert result.status == "succeeded"
    frame = pd.read_csv(runtime_workspace / output_path)
    assert frame["Region"].tolist() == ["west-name", "east-name", "Global_Summary"]


def test_calculate_zonal_statistics_only_global_supports_multi_raster_labels(
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    raster_2020 = _write_raster(
        runtime_workspace / "inputs" / "ntl_2020.tif",
        np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32),
    )
    raster_2021 = _write_raster(
        runtime_workspace / "inputs" / "ntl_2021.tif",
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
    )
    output_path = Path("outputs") / "zonal_global.csv"

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=[raster_2020, raster_2021],
        vector_path=admin_polygons_path,
        output_path=output_path,
        selected_indices=["TNTL"],
        only_global=True,
    )

    assert result.status == "succeeded"
    assert result.metrics["polygon_count"] == 2
    assert result.metrics["raster_count"] == 2
    assert result.metrics["row_count"] == 2
    assert result.metrics["only_global"] is True

    frame = pd.read_csv(runtime_workspace / output_path)

    assert frame["Raster_file"].tolist() == ["ntl_2020.tif", "ntl_2021.tif"]
    assert frame["Year"].tolist() == [2020, 2021]
    assert frame["Region"].tolist() == ["Global_Summary", "Global_Summary"]


def test_calculate_zonal_statistics_reprojects_vectors_and_handles_outside_polygons(
    mercator_overlap_vector_path: Path,
    far_vector_path: Path,
    sample_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    module = _ntl_module()

    reprojected = module.calculate_zonal_statistics(
        raster_paths=[sample_raster_path],
        vector_path=mercator_overlap_vector_path,
        output_path=Path("outputs") / "zonal_reprojected.csv",
        selected_indices=["TNTL", "ANTL"],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        outside = module.calculate_zonal_statistics(
            raster_paths=[sample_raster_path],
            vector_path=far_vector_path,
            output_path=Path("outputs") / "zonal_outside.csv",
            selected_indices=["TNTL", "LArea", "ANTL", "MaxNTL"],
        )

    assert reprojected.status == "succeeded"
    reprojected_frame = pd.read_csv(runtime_workspace / "outputs" / "zonal_reprojected.csv")
    assert reprojected_frame["TNTL"].tolist() == [6.0, 6.0]
    assert reprojected_frame["ANTL"].tolist() == [2.0, 2.0]

    assert outside.status == "succeeded"
    outside_frame = pd.read_csv(runtime_workspace / "outputs" / "zonal_outside.csv")
    assert outside_frame["Region"].tolist() == ["far", "Global_Summary"]
    assert outside_frame["TNTL"].tolist() == [0.0, 0.0]
    assert outside_frame["LArea"].tolist() == [0.0, 0.0]
    assert outside_frame["ANTL"].isna().tolist() == [True, True]
    assert outside_frame["MaxNTL"].isna().tolist() == [True, True]
    assert caught == []


@pytest.mark.parametrize(
    ("vector_fixture", "expected_code"),
    [
        ("invalid_vector_path", "INVALID_GEOMETRY"),
        ("empty_vector_path", "EMPTY_DATASET"),
        ("vector_without_crs_path", "CRS_MISSING"),
        ("corrupt_vector_path", "VECTOR_READ_FAILED"),
    ],
)
def test_calculate_zonal_statistics_reports_vector_failures(
    request: pytest.FixtureRequest,
    matching_raster_path: Path,
    vector_fixture: str,
    expected_code: str,
) -> None:
    vector_path = request.getfixturevalue(vector_fixture)

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=[matching_raster_path],
        vector_path=vector_path,
        output_path=Path("outputs") / f"{vector_fixture}.csv",
        selected_indices=["TNTL"],
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == expected_code


def test_calculate_ntl_metrics_for_raster_reports_invalid_raster_transform(
    runtime_workspace: Path,
) -> None:
    raster_path = _write_raster(
        runtime_workspace / "inputs" / "zero_det_metrics.tif",
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
        transform=Affine(1.0, 2.0, 0.0, 2.0, 4.0, 0.0),
        crs="EPSG:3857",
    )

    result = _ntl_module().calculate_ntl_metrics_for_raster(
        raster_path,
        selected=["TNTL", "LArea"],
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_RASTER_TRANSFORM"
    assert result.error.details["path"] == str(raster_path)
    assert result.error.details["determinant"] == pytest.approx(0.0)
    assert "Affine" in result.error.details["transform"]
    assert "parameter" not in result.error.details
    assert result.error.suggestion is not None


def test_calculate_ntl_metrics_for_raster_accepts_tiny_nonzero_determinant(
    runtime_workspace: Path,
) -> None:
    raster_path = _write_raster(
        runtime_workspace / "inputs" / "tiny_det_metrics.tif",
        np.array([[0.0, 5.0], [7.0, 0.0]], dtype=np.float32),
        transform=Affine(1.0, 1.0, 0.0, 1.0, 1.000000001, 0.0),
        crs="EPSG:3857",
    )

    result = _ntl_module().calculate_ntl_metrics_for_raster(
        raster_path,
        selected=["LArea", "TNTL"],
    )

    assert result.status == "succeeded"
    assert result.warnings == []
    assert result.metrics["TNTL"] == 12.0
    assert result.metrics["LArea"] == pytest.approx(2e-9)
    assert result.metrics["pixel_area"] == pytest.approx(1e-9)


def test_calculate_zonal_statistics_reserves_output_collision_without_mutating_inputs(
    admin_polygons_path: Path,
    matching_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    requested = runtime_workspace / "outputs" / "zonal_existing.csv"
    requested.write_text("sentinel", encoding="utf-8")
    raster_paths = [matching_raster_path]
    original_paths = list(raster_paths)

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=raster_paths,
        vector_path=admin_polygons_path,
        output_path=Path("outputs") / "zonal_existing.csv",
        selected_indices=["TNTL"],
    )

    assert result.status == "succeeded"
    assert requested.read_text(encoding="utf-8") == "sentinel"
    assert result.outputs[0].path.endswith("zonal_existing_001.csv")
    assert raster_paths == original_paths


def test_calculate_zonal_statistics_cleans_up_partial_output_on_write_failure(
    admin_polygons_path: Path,
    matching_raster_path: Path,
    runtime_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _ntl_module()
    reserved = (runtime_workspace / "outputs" / "zonal_partial.csv").resolve(strict=False)
    real_to_csv = module.pd.DataFrame.to_csv

    def failing_to_csv(self, path_or_buf=None, *args, **kwargs):  # noqa: ANN001
        if path_or_buf is not None:
            Path(path_or_buf).write_text("partial", encoding="utf-8")
        raise OSError("disk full")

    monkeypatch.setattr(module.pd.DataFrame, "to_csv", failing_to_csv)

    result = module.calculate_zonal_statistics(
        raster_paths=[matching_raster_path],
        vector_path=admin_polygons_path,
        output_path=Path("outputs") / "zonal_partial.csv",
        selected_indices=["TNTL"],
    )

    monkeypatch.setattr(module.pd.DataFrame, "to_csv", real_to_csv)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "OUTPUT_WRITE_FAILED"
    assert not reserved.exists()


def test_calculate_zonal_statistics_reports_invalid_raster_transform_without_writing_csv(
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    raster_path = _write_raster(
        runtime_workspace / "inputs" / "zero_det_zonal.tif",
        np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32),
        transform=Affine(1.0, 2.0, 0.0, 2.0, 4.0, 0.0),
        crs="EPSG:3857",
    )
    output_path = Path("outputs") / "zero_det_zonal.csv"

    result = _ntl_module().calculate_zonal_statistics(
        raster_paths=[raster_path],
        vector_path=admin_polygons_path,
        output_path=output_path,
        selected_indices=["TNTL"],
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "INVALID_RASTER_TRANSFORM"
    assert result.error.details["path"] == str(raster_path)
    assert result.error.details["determinant"] == pytest.approx(0.0)
    assert "Affine" in result.error.details["transform"]
    assert result.error.suggestion is not None
    assert not (runtime_workspace / output_path).exists()


def test_composite_ntl_rasters_writes_mean_composite_with_mask_and_metrics(
    composite_series_raster_paths: list[Path],
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "composite_mean.tif"

    result = _ntl_module().composite_ntl_rasters(
        composite_series_raster_paths,
        output_path,
    )

    assert result.status == "succeeded"
    assert result.tool == "composite_ntl_rasters"
    assert len(result.outputs) == 1
    assert result.outputs[0].path == str((runtime_workspace / output_path).resolve(strict=False))
    assert result.outputs[0].media_type == "image/tiff"
    assert result.outputs[0].role == "composite"
    assert result.metrics == {
        "input_count": 2,
        "valid_pixel_count": 3,
        "coverage": pytest.approx(0.75),
        "method": "mean",
    }

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.dtypes == ("float32",)
        assert dataset.nodata == pytest.approx(-9999.0)
        assert dataset.read(1).tolist() == [[2.0, 5.0], [3.0, -9999.0]]
        masked = dataset.read(1, masked=True)
        assert masked.mask.tolist() == [[False, False], [False, True]]
        assert dataset.dataset_mask().tolist() == [[255, 255], [255, 0]]


def test_composite_ntl_rasters_returns_fractional_mean_without_mutating_inputs(
    uint8_mean_left_raster_path: Path,
    uint8_mean_right_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    raster_paths = [uint8_mean_left_raster_path, uint8_mean_right_raster_path]
    original_paths = list(raster_paths)
    output_path = Path("outputs") / "composite_fractional.tif"

    result = _ntl_module().composite_ntl_rasters(
        raster_paths,
        output_path,
    )

    assert result.status == "succeeded"
    assert raster_paths == original_paths

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.read(1).tolist() == [[1.5]]
        assert dataset.nodata == pytest.approx(255.0)


def test_composite_ntl_rasters_accepts_noisy_transform_alignment(
    sample_raster_path: Path,
    noisy_transform_raster_path: Path,
    runtime_workspace: Path,
) -> None:
    result = _ntl_module().composite_ntl_rasters(
        [sample_raster_path, noisy_transform_raster_path],
        Path("outputs") / "composite_noisy_transform.tif",
    )

    assert result.status == "succeeded"
    assert result.metrics["input_count"] == 2
    assert result.metrics["valid_pixel_count"] == 4

    with rasterio.open(runtime_workspace / "outputs" / "composite_noisy_transform.tif") as dataset:
        assert dataset.read(1).tolist() == [[3.0, 4.0], [5.0, 8.0]]


def test_task8_transform_comparator_uses_pixel_scale_tolerance_for_large_origins() -> None:
    module = _ntl_module()
    left = Affine(1.0, 0.0, 1_000_000.0, 0.0, -1.0, 2_000_000.0)
    right = Affine(1.0 + 5e-10, 0.0, 1_000_000.0 + 5e-10, 0.0, -1.0 - 5e-10, 2_000_000.0 + 5e-10)
    shifted = Affine(1.0, 0.0, 1_000_000.5, 0.0, -1.0, 2_000_000.0)

    assert module._transforms_equal(left, right) is True
    assert module._transforms_equal(left, shifted) is False


@pytest.mark.parametrize(
    ("raster_paths", "output_path", "method", "expected_code"),
    [
        ([], Path("outputs") / "empty.tif", "mean", "INVALID_PARAMETER"),
        (["C:partial\\input.tif"], Path("outputs") / "partial_input.tif", "mean", "INVALID_PARAMETER"),
        ([Path("inputs") / "missing.tif"], Path("outputs") / "missing.tif", "mean", "INPUT_NOT_FOUND"),
        (["sample_raster_path", "shifted_raster_path"], Path("outputs") / "grid.tif", "mean", "GRID_MISMATCH"),
        (["sample_raster_path"], "C:partial\\output.tif", "mean", "INVALID_PARAMETER"),
        (["sample_raster_path"], Path("outputs") / "unsupported.tif", "median", "UNSUPPORTED_METHOD"),
    ],
)
def test_composite_ntl_rasters_reports_stable_failures(
    request: pytest.FixtureRequest,
    raster_paths: list[object],
    output_path: object,
    method: str,
    expected_code: str,
) -> None:
    resolved_paths = [
        request.getfixturevalue(path) if isinstance(path, str) and path.endswith(("_path", "_paths")) else path
        for path in raster_paths
    ]

    result = _ntl_module().composite_ntl_rasters(
        resolved_paths,
        output_path,
        method=method,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == expected_code


def test_composite_ntl_rasters_reserves_output_collision_without_overwriting(
    composite_series_raster_paths: list[Path],
    runtime_workspace: Path,
) -> None:
    requested = runtime_workspace / "outputs" / "composite_existing.tif"
    requested.write_bytes(b"sentinel")

    result = _ntl_module().composite_ntl_rasters(
        composite_series_raster_paths,
        Path("outputs") / "composite_existing.tif",
    )

    assert result.status == "succeeded"
    assert requested.read_bytes() == b"sentinel"
    assert result.outputs[0].path.endswith("composite_existing_001.tif")


def test_composite_ntl_rasters_cleans_up_partial_output_on_write_failure(
    composite_series_raster_paths: list[Path],
    runtime_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _ntl_module()
    reserved = (runtime_workspace / "outputs" / "composite_partial.tif").resolve(strict=False)
    real_open = module.rasterio.open

    def failing_open(path, *args, **kwargs):  # noqa: ANN001
        mode = args[0] if args else kwargs.get("mode", "r")
        if mode == "w":
            Path(path).write_bytes(b"partial")
            raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(module.rasterio, "open", failing_open)

    result = module.composite_ntl_rasters(
        composite_series_raster_paths,
        Path("outputs") / "composite_partial.tif",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "OUTPUT_WRITE_FAILED"
    assert not reserved.exists()
    assert not Path(f"{reserved}.aux.xml").exists()


def test_analyze_ntl_trend_writes_slope_and_pvalue_rasters_for_union_roi(
    trend_series_raster_paths: list[Path],
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    output_prefix = Path("outputs") / "trend_roi"

    result = _ntl_module().analyze_ntl_trend(
        trend_series_raster_paths,
        admin_polygons_path,
        output_prefix,
    )

    slope_path = runtime_workspace / "outputs" / "trend_roi_slope_trend.tif"
    pvalue_path = runtime_workspace / "outputs" / "trend_roi_pvalue_map.tif"

    assert result.status == "succeeded"
    assert result.tool == "analyze_ntl_trend"
    assert [artifact.role for artifact in result.outputs] == ["slope", "pvalue"]
    assert [artifact.path for artifact in result.outputs] == [
        str(slope_path.resolve(strict=False)),
        str(pvalue_path.resolve(strict=False)),
    ]
    assert result.metrics["raster_count"] == 3
    assert result.metrics["analyzed_pixel_count"] == 1
    assert result.metrics["minimum_observations"] == 2

    with rasterio.open(slope_path) as slope_dataset:
        masked = slope_dataset.read(1, masked=True)
        assert slope_dataset.height == 1
        assert slope_dataset.width == 2
        assert str(slope_dataset.crs) == "EPSG:4326"
        assert np.isnan(slope_dataset.nodata)
        assert masked.shape == (1, 2)
        assert masked[0, 0] == pytest.approx(10.0)
        assert bool(masked.mask[0, 0]) is False
        assert bool(masked.mask[0, 1]) is True

    with rasterio.open(pvalue_path) as pvalue_dataset:
        masked = pvalue_dataset.read(1, masked=True)
        assert masked.shape == (1, 2)
        assert float(masked[0, 0]) == pytest.approx(1.0 / 3.0)
        assert bool(masked.mask[0, 0]) is False
        assert bool(masked.mask[0, 1]) is True


def test_analyze_ntl_trend_accepts_two_step_series_when_pixel_has_two_observations(
    trend_two_step_raster_paths: list[Path],
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    result = _ntl_module().analyze_ntl_trend(
        trend_two_step_raster_paths,
        admin_polygons_path,
        Path("outputs") / "trend_two_step",
    )

    assert result.status == "succeeded"
    assert result.metrics["raster_count"] == 2
    assert result.metrics["analyzed_pixel_count"] == 1

    with rasterio.open(runtime_workspace / "outputs" / "trend_two_step_slope_trend.tif") as dataset:
        masked = dataset.read(1, masked=True)
        assert masked.shape == (1, 2)
        assert masked[0, 0] == pytest.approx(3.0)
        assert bool(masked.mask[0, 1]) is True


def test_analyze_ntl_trend_reprojects_mercator_roi_and_preserves_expected_slopes(
    trend_series_raster_paths: list[Path],
    mercator_overlap_vector_path: Path,
    runtime_workspace: Path,
) -> None:
    result = _ntl_module().analyze_ntl_trend(
        trend_series_raster_paths,
        mercator_overlap_vector_path,
        Path("outputs") / "trend_mercator",
    )

    assert result.status == "succeeded"
    assert result.metrics["raster_count"] == 3
    assert result.metrics["analyzed_pixel_count"] == 3

    with rasterio.open(runtime_workspace / "outputs" / "trend_mercator_slope_trend.tif") as dataset:
        masked = dataset.read(1, masked=True)
        assert masked.shape == (2, 2)
        assert masked[0, 0] == pytest.approx(10.0)
        assert masked[0, 1] == pytest.approx(20.0)
        assert masked[1, 0] == pytest.approx(10.0)
        assert bool(masked.mask[1, 1]) is True


@pytest.mark.parametrize(
    ("raster_paths", "vector_path", "output_prefix", "expected_code"),
    [
        ([], "admin_polygons_path", Path("outputs") / "trend_empty", "INVALID_PARAMETER"),
        (["sample_raster_path"], "admin_polygons_path", Path("outputs") / "trend_short", "INVALID_PARAMETER"),
        (["trend_series_raster_paths"], "far_vector_path", Path("outputs") / "trend_far", "NO_SPATIAL_OVERLAP"),
        (["trend_series_raster_paths"], "invalid_vector_path", Path("outputs") / "trend_invalid", "INVALID_GEOMETRY"),
        (["matching_raster_path", "crs_mismatch_raster_path"], "admin_polygons_path", Path("outputs") / "trend_grid", "GRID_MISMATCH"),
        (["trend_series_raster_paths"], "admin_polygons_path", "C:partial\\trend", "INVALID_PARAMETER"),
    ],
)
def test_analyze_ntl_trend_reports_stable_failures(
    request: pytest.FixtureRequest,
    raster_paths: list[object],
    vector_path: str,
    output_prefix: object,
    expected_code: str,
) -> None:
    resolved_paths: list[object] = []
    for item in raster_paths:
        value = request.getfixturevalue(item) if isinstance(item, str) and item.endswith(("_path", "_paths")) else item
        if isinstance(value, list):
            resolved_paths.extend(value)
        else:
            resolved_paths.append(value)

    result = _ntl_module().analyze_ntl_trend(
        resolved_paths,
        request.getfixturevalue(vector_path),
        output_prefix,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == expected_code


def test_analyze_ntl_trend_reserves_collisions_independently(
    trend_series_raster_paths: list[Path],
    admin_polygons_path: Path,
    runtime_workspace: Path,
) -> None:
    requested_slope = runtime_workspace / "outputs" / "trend_existing_slope_trend.tif"
    requested_slope.write_bytes(b"sentinel")

    result = _ntl_module().analyze_ntl_trend(
        trend_series_raster_paths,
        admin_polygons_path,
        Path("outputs") / "trend_existing",
    )

    assert result.status == "succeeded"
    assert requested_slope.read_bytes() == b"sentinel"
    assert result.outputs[0].path.endswith("trend_existing_slope_trend_001.tif")
    assert result.outputs[1].path.endswith("trend_existing_pvalue_map.tif")


def test_analyze_ntl_trend_cleans_up_all_new_outputs_on_partial_write_failure(
    trend_series_raster_paths: list[Path],
    admin_polygons_path: Path,
    runtime_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _ntl_module()
    slope_path = (runtime_workspace / "outputs" / "trend_partial_slope_trend.tif").resolve(strict=False)
    pvalue_path = (runtime_workspace / "outputs" / "trend_partial_pvalue_map.tif").resolve(strict=False)
    real_open = module.rasterio.open
    write_calls = 0

    def failing_open(path, *args, **kwargs):  # noqa: ANN001
        nonlocal write_calls
        mode = args[0] if args else kwargs.get("mode", "r")
        if mode == "w":
            write_calls += 1
            if write_calls == 2:
                Path(path).write_bytes(b"partial")
                raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(module.rasterio, "open", failing_open)

    result = module.analyze_ntl_trend(
        trend_series_raster_paths,
        admin_polygons_path,
        Path("outputs") / "trend_partial",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "OUTPUT_WRITE_FAILED"
    assert not slope_path.exists()
    assert not pvalue_path.exists()


def test_detect_ntl_anomaly_marks_only_positive_latest_spike_and_writes_valid_mask(
    anomaly_latest_spike_raster_paths: list[Path],
    runtime_workspace: Path,
) -> None:
    output_path = Path("outputs") / "anomaly_latest.tif"

    result = _ntl_module().detect_ntl_anomaly(
        anomaly_latest_spike_raster_paths,
        output_path,
    )

    assert result.status == "succeeded"
    assert result.tool == "detect_ntl_anomaly"
    assert len(result.outputs) == 1
    assert result.outputs[0].role == "anomaly"
    assert result.metrics == {
        "target_index": 3,
        "k_sigma": 3.0,
        "raster_count": 4,
        "baseline_count": 3,
        "anomaly_pixel_count": 1,
        "valid_pixel_count": 3,
    }

    with rasterio.open(runtime_workspace / output_path) as dataset:
        assert dataset.dtypes == ("uint8",)
        assert dataset.nodata is None
        assert dataset.read(1).tolist() == [[1, 0], [0, 0]]
        assert dataset.dataset_mask().tolist() == [[255, 255], [255, 0]]


def test_detect_ntl_anomaly_supports_explicit_target_index(
    runtime_workspace: Path,
) -> None:
    raster_paths = [
        _write_raster(runtime_workspace / "inputs" / "anomaly_explicit_01.tif", np.array([[1.0, 1.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_explicit_02.tif", np.array([[1.0, 1.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_explicit_03.tif", np.array([[8.0, 1.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_explicit_04.tif", np.array([[1.0, 1.0]], dtype=np.float32)),
    ]

    result = _ntl_module().detect_ntl_anomaly(
        raster_paths,
        Path("outputs") / "anomaly_explicit.tif",
        target_index=2,
    )

    assert result.status == "succeeded"
    assert result.metrics["target_index"] == 2

    with rasterio.open(runtime_workspace / "outputs" / "anomaly_explicit.tif") as dataset:
        assert dataset.read(1).tolist() == [[1, 0]]


def test_detect_ntl_anomaly_masks_pixels_without_three_baseline_observations(
    anomaly_sparse_baseline_raster_paths: list[Path],
    runtime_workspace: Path,
) -> None:
    result = _ntl_module().detect_ntl_anomaly(
        anomaly_sparse_baseline_raster_paths,
        Path("outputs") / "anomaly_sparse.tif",
    )

    assert result.status == "succeeded"
    assert result.metrics["valid_pixel_count"] == 2
    assert result.metrics["anomaly_pixel_count"] == 1

    with rasterio.open(runtime_workspace / "outputs" / "anomaly_sparse.tif") as dataset:
        assert dataset.read(1).tolist() == [[1, 0], [0, 0]]
        assert dataset.dataset_mask().tolist() == [[255, 0], [255, 0]]


def test_detect_ntl_anomaly_keeps_negative_change_as_valid_non_anomaly(
    runtime_workspace: Path,
) -> None:
    raster_paths = [
        _write_raster(runtime_workspace / "inputs" / "anomaly_negative_01.tif", np.array([[5.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_negative_02.tif", np.array([[5.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_negative_03.tif", np.array([[5.0]], dtype=np.float32)),
        _write_raster(runtime_workspace / "inputs" / "anomaly_negative_04.tif", np.array([[0.0]], dtype=np.float32)),
    ]

    result = _ntl_module().detect_ntl_anomaly(
        raster_paths,
        Path("outputs") / "anomaly_negative.tif",
    )

    assert result.status == "succeeded"
    assert result.metrics["anomaly_pixel_count"] == 0
    assert result.metrics["valid_pixel_count"] == 1

    with rasterio.open(runtime_workspace / "outputs" / "anomaly_negative.tif") as dataset:
        assert dataset.read(1).tolist() == [[0]]
        assert dataset.dataset_mask().tolist() == [[255]]


@pytest.mark.parametrize(
    ("raster_paths", "output_path", "target_index", "k_sigma", "expected_code"),
    [
        (["sample_raster_path", "matching_raster_path", "shifted_raster_path"], Path("outputs") / "anomaly_short.tif", None, 3.0, "INVALID_PARAMETER"),
        (["anomaly_latest_spike_raster_paths"], Path("outputs") / "anomaly_bool.tif", True, 3.0, "INVALID_PARAMETER"),
        (["anomaly_latest_spike_raster_paths"], Path("outputs") / "anomaly_range.tif", 4, 3.0, "INVALID_PARAMETER"),
        (["anomaly_latest_spike_raster_paths"], Path("outputs") / "anomaly_sigma.tif", None, 0.0, "INVALID_PARAMETER"),
        (["anomaly_latest_spike_raster_paths"], Path("outputs") / "anomaly_grid.tif", None, 3.0, "GRID_MISMATCH"),
        (["anomaly_latest_spike_raster_paths"], "C:partial\\anomaly.tif", None, 3.0, "INVALID_PARAMETER"),
    ],
)
def test_detect_ntl_anomaly_reports_stable_failures(
    request: pytest.FixtureRequest,
    raster_paths: list[object],
    output_path: object,
    target_index: object,
    k_sigma: float,
    expected_code: str,
) -> None:
    resolved_paths: list[object] = []
    for item in raster_paths:
        value = request.getfixturevalue(item) if isinstance(item, str) and item.endswith(("_path", "_paths")) else item
        if isinstance(value, list):
            resolved_paths.extend(value)
        else:
            resolved_paths.append(value)
    if expected_code == "GRID_MISMATCH":
        resolved_paths[-1] = request.getfixturevalue("shifted_raster_path")

    result = _ntl_module().detect_ntl_anomaly(
        resolved_paths,
        output_path,
        target_index=target_index,
        k_sigma=k_sigma,
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == expected_code


def test_detect_ntl_anomaly_reserves_output_collision_without_overwriting(
    anomaly_latest_spike_raster_paths: list[Path],
    runtime_workspace: Path,
) -> None:
    raster_paths = list(anomaly_latest_spike_raster_paths)
    original_paths = list(raster_paths)
    requested = runtime_workspace / "outputs" / "anomaly_existing.tif"
    requested.write_bytes(b"sentinel")

    result = _ntl_module().detect_ntl_anomaly(
        raster_paths,
        Path("outputs") / "anomaly_existing.tif",
    )

    assert result.status == "succeeded"
    assert requested.read_bytes() == b"sentinel"
    assert result.outputs[0].path.endswith("anomaly_existing_001.tif")
    assert raster_paths == original_paths


def test_detect_ntl_anomaly_cleans_up_partial_output_on_write_failure(
    anomaly_latest_spike_raster_paths: list[Path],
    runtime_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _ntl_module()
    reserved = (runtime_workspace / "outputs" / "anomaly_partial.tif").resolve(strict=False)
    real_open = module.rasterio.open

    def failing_open(path, *args, **kwargs):  # noqa: ANN001
        mode = args[0] if args else kwargs.get("mode", "r")
        if mode == "w":
            Path(path).write_bytes(b"partial")
            raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(module.rasterio, "open", failing_open)

    result = module.detect_ntl_anomaly(
        anomaly_latest_spike_raster_paths,
        Path("outputs") / "anomaly_partial.tif",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "OUTPUT_WRITE_FAILED"
    assert not reserved.exists()
