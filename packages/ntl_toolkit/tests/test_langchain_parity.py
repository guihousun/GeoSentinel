from __future__ import annotations

import importlib
import inspect
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


def _write_raster(
    path: Path,
    values: np.ndarray,
    *,
    nodata: float | None = -9999.0,
    transform=None,
) -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=values.shape[0],
        width=values.shape[1],
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform or from_origin(0.0, 2.0, 1.0, 1.0),
        nodata=nodata,
    ) as dataset:
        dataset.write(values.astype(np.float32), 1)
    return path


def _configure_workspace(tmp_path: Path, monkeypatch):
    storage_module = importlib.import_module("storage_manager")
    data_root = tmp_path / "user_data"
    shared_root = tmp_path / "shared_data"
    monkeypatch.setattr(storage_module.storage_manager, "base_dir", data_root)
    monkeypatch.setattr(storage_module.storage_manager, "shared_dir", shared_root)
    token = storage_module.current_thread_id.set("adapter-parity")
    workspace = storage_module.storage_manager.get_workspace()
    return storage_module, token, workspace


def test_local_composite_uses_shared_core_and_preserves_chat_response(tmp_path: Path, monkeypatch) -> None:
    storage_module, token, workspace = _configure_workspace(tmp_path, monkeypatch)
    try:
        _write_raster(workspace / "inputs" / "first.tif", np.array([[1.0, -9999.0], [3.0, -9999.0]]))
        _write_raster(workspace / "inputs" / "second.tif", np.array([[3.0, 5.0], [-9999.0, -9999.0]]))
        module = importlib.import_module("tools.NTL_Composite")

        response = module.build_ntl_mean_composite_local(["first.tif", "second.tif"], "mean.tif")

        assert "Success! Mean composite saved" in response
        assert (workspace / "outputs" / "mean.tif").exists()
        with rasterio.open(workspace / "outputs" / "mean.tif") as dataset:
            np.testing.assert_allclose(dataset.read(1)[0, :], [2.0, 5.0])
        repeated = module.build_ntl_mean_composite_local(["first.tif", "second.tif"], "mean.tif")
        assert "outputs/mean_001.tif" in repeated
        assert (workspace / "outputs" / "mean_001.tif").exists()
        assert "Success!" in module.build_ntl_mean_composite_local(["first.tif"], "bad.tif", enforce_same_grid=False)
        _write_raster(
            workspace / "inputs" / "shifted.tif",
            np.array([[1.0, 2.0], [3.0, 4.0]]),
            transform=from_origin(0.5, 2.0, 1.0, 1.0),
        )
        unsafe_response = module.build_ntl_mean_composite_local(
            ["first.tif", "shifted.tif"],
            "unsafe.tif",
            enforce_same_grid=False,
        )
        assert "must share the same CRS, dimensions, and affine grid" in unsafe_response

        _write_raster(workspace / "inputs" / "fallback_a.tif", np.array([[-1.0, 2.0], [-1.0, -1.0]]), nodata=None)
        _write_raster(workspace / "inputs" / "fallback_b.tif", np.array([[-1.0, 4.0], [-1.0, -1.0]]), nodata=None)
        module.build_ntl_mean_composite_local(
            ["fallback_a.tif", "fallback_b.tif"],
            "fallback.tif",
            fallback_nodata=-1.0,
        )
        with rasterio.open(workspace / "outputs" / "fallback.tif") as dataset:
            assert dataset.nodata == -1.0
            assert dataset.read(1)[0, 1] == 3.0
    finally:
        storage_module.current_thread_id.reset(token)


def test_statistics_and_inspection_keep_legacy_shapes(tmp_path: Path, monkeypatch) -> None:
    storage_module, token, workspace = _configure_workspace(tmp_path, monkeypatch)
    try:
        _write_raster(workspace / "inputs" / "ntl_2020.tif", np.array([[1.0, 2.0], [3.0, -9999.0]]))
        boundary = gpd.GeoDataFrame(
            {"name": ["roi"]},
            geometry=[box(0.0, 0.0, 2.0, 2.0)],
            crs="EPSG:4326",
        )
        boundary.to_file(workspace / "inputs" / "boundary.geojson", driver="GeoJSON")

        statistics = importlib.import_module("tools.NTL_raster_stats")
        response = statistics.NTL_raster_statistics(
            shapefile_path="boundary.geojson",
            output_csv_path="stats.csv",
            ntl_tif_path="ntl_2020.tif",
            selected_indices=["TNTL", "ANTL"],
        )
        assert "Global Summary" in response
        assert (workspace / "outputs" / "stats.csv").exists()

        inspector = importlib.import_module("tools.geodata_inspector_tool")
        report = json.loads(inspector.inspect_geospatial_assets(
            raster_paths=["ntl_2020.tif"],
            vector_paths=["boundary.geojson"],
            mode="full",
        ))
        assert report["summary"] == {"raster_ok": 1, "raster_fail": 0, "vector_ok": 1, "vector_fail": 0, "gee_ok": 0, "gee_fail": 0}
        assert report["raster_reports"][0]["band1_stats"]["mean"] == 2.0
        assert report["vector_reports"][0]["sample_records"] == [{"name": "roi"}]
        basic_report = json.loads(inspector.inspect_geospatial_assets(vector_paths=["boundary.geojson"], mode="basic"))
        assert basic_report["vector_reports"][0]["fields"]["name"] == "object"
    finally:
        storage_module.current_thread_id.reset(token)


def test_trend_and_anomaly_wrappers_delegate_to_shared_core(tmp_path: Path, monkeypatch) -> None:
    storage_module, token, workspace = _configure_workspace(tmp_path, monkeypatch)
    try:
        boundary = gpd.GeoDataFrame(
            {"name": ["roi"]},
            geometry=[box(0.0, 0.0, 2.0, 2.0)],
            crs="EPSG:4326",
        )
        boundary.to_file(workspace / "inputs" / "boundary.geojson", driver="GeoJSON")
        series = [
            np.array([[1.0, 1.0], [1.0, 1.0]]),
            np.array([[2.0, 1.0], [2.0, 1.0]]),
            np.array([[3.0, 1.0], [3.0, 1.0]]),
            np.array([[9.0, 1.0], [9.0, 1.0]]),
        ]
        names = []
        for index, values in enumerate(series, start=1):
            name = f"series_{index}.tif"
            names.append(name)
            _write_raster(workspace / "inputs" / name, values)

        trend = importlib.import_module("tools.NTL_trend_detection_tool")
        trend_response = trend.analyze_ntl_trend_masked_logic(names[:3], "boundary.geojson", "trend")
        assert "Slope Map" in trend_response
        assert (workspace / "outputs" / "trend_slope_trend.tif").exists()
        assert (workspace / "outputs" / "trend_pvalue_map.tif").exists()
        assert (workspace / "outputs" / "trend_trend_viz.png").exists()
        repeated_trend = trend.analyze_ntl_trend_masked_logic(names[:3], "boundary.geojson", "trend")
        assert "Visualization" in repeated_trend
        assert (workspace / "outputs" / "trend_trend_viz_001.png").exists()

        anomaly = importlib.import_module("tools.NTL_anomaly_detection_tool")
        anomaly_response = anomaly.detect_ntl_anomaly(names, save_filename="anomaly.tif", k_sigma=2.0)
        assert "Anomaly Detection Task Completed" in anomaly_response
        with rasterio.open(workspace / "outputs" / "anomaly.tif") as dataset:
            assert dataset.read(1)[0, 0] == 1
        legacy_response = anomaly.detect_ntl_anomaly(
            [names[0], names[-1]],
            target_index=1,
            save_filename="legacy_anomaly.tif",
            k_sigma=2.0,
        )
        assert "Anomaly Detection Task Completed" in legacy_response
    finally:
        storage_module.current_thread_id.reset(token)


def test_legacy_function_signatures_and_structured_tool_names_remain_stable() -> None:
    composite = importlib.import_module("tools.NTL_Composite")
    statistics = importlib.import_module("tools.NTL_raster_stats")
    trend = importlib.import_module("tools.NTL_trend_detection_tool")
    anomaly = importlib.import_module("tools.NTL_anomaly_detection_tool")

    assert list(inspect.signature(composite.build_ntl_mean_composite_local).parameters) == [
        "file_paths", "out_tif", "enforce_same_grid", "fallback_nodata"
    ]
    assert list(inspect.signature(statistics.NTL_raster_statistics).parameters) == [
        "shapefile_path", "output_csv_path", "ntl_tif_path", "ntl_tif_paths", "selected_indices", "only_global", "config"
    ]
    assert list(inspect.signature(trend.analyze_ntl_trend_masked_logic).parameters) == ["raster_files", "vector_file", "out_prefix"]
    assert list(inspect.signature(anomaly.detect_ntl_anomaly).parameters) == ["raster_files", "target_index", "k_sigma", "save_filename"]
    assert composite.NTL_composite_local_tool.name == "NTL_Mean_Composite"
    assert statistics.NTL_raster_statistics_tool.name == "NTL_raster_statistics"
    assert trend.NTL_Trend_Analysis.name == "Analyze_NTL_trend"
    assert anomaly.detect_ntl_anomaly_tool.name == "Detect_NTL_anomaly"
    def field_names(model) -> set[str]:
        return set(getattr(model, "model_fields", None) or model.__fields__)

    assert field_names(composite.LocalNTLCompositeInput) == {
        "file_paths", "out_tif", "enforce_same_grid", "fallback_nodata"
    }
    assert field_names(statistics.NTL_raster_statistics_input) == {
        "ntl_tif_path", "ntl_tif_paths", "shapefile_path", "output_csv_path", "selected_indices", "only_global"
    }
    assert field_names(trend.MaskedTrendAnalysisInput) == {"raster_files", "vector_file", "out_prefix"}
    assert field_names(anomaly.SimpleAnomalyDetectionInput) == {"raster_files", "target_index", "k_sigma", "save_filename"}
