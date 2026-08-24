from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import Point, Polygon, box


@pytest.fixture
def runtime_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    workspace = tmp_path / "workspace"
    (workspace / "inputs").mkdir(parents=True)
    (workspace / "outputs").mkdir(parents=True)
    monkeypatch.setenv("NTL_MCP_WORKDIR", str(workspace))
    return workspace


def _write_geojson(path: Path, gdf: gpd.GeoDataFrame) -> Path:
    gdf.to_file(path, driver="GeoJSON")
    return path


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


@pytest.fixture
def admin_polygons_path(runtime_workspace: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "shapeName": ["west", "east"],
            "iso3": ["TST", "TST"],
        },
        geometry=[
            box(0.0, 0.0, 1.0, 1.0),
            box(1.0, 0.0, 2.0, 1.0),
        ],
        crs="EPSG:4326",
    )
    return _write_geojson(runtime_workspace / "inputs" / "admin.geojson", gdf)


@pytest.fixture
def point_features_geojson_path(runtime_workspace: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "longitude": [0.5, 1.5, 3.0],
            "latitude": [0.5, 0.5, 3.0],
        },
        geometry=[
            Point(0.5, 0.5),
            Point(1.5, 0.5),
            Point(3.0, 3.0),
        ],
        crs="EPSG:4326",
    )
    return _write_geojson(runtime_workspace / "inputs" / "points.geojson", gdf)


@pytest.fixture
def point_features_csv_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "points.csv"
    pd.DataFrame(
        {
            "id": [1, 2, 3],
            "longitude": [0.5, 1.5, 3.0],
            "latitude": [0.5, 0.5, 3.0],
        }
    ).to_csv(path, index=False, encoding="utf-8")
    return path


@pytest.fixture
def sample_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[1.0, 2.0], [3.0, -9999.0]], dtype=np.float32)
    return _write_raster(runtime_workspace / "inputs" / "sample.tif", values)


@pytest.fixture
def multiband_raster_path(runtime_workspace: Path) -> Path:
    values = np.array(
        [
            [[1.0, 2.0], [3.0, 4.0]],
            [[10.0, 20.0], [30.0, 40.0]],
        ],
        dtype=np.float32,
    )
    return _write_raster(runtime_workspace / "inputs" / "multiband.tif", values)


@pytest.fixture
def raster_without_crs_path(runtime_workspace: Path) -> Path:
    values = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    return _write_raster(
        runtime_workspace / "inputs" / "no_crs.tif",
        values,
        crs=None,
    )


@pytest.fixture
def nodata_less_uint8_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[0, 5], [7, 9]], dtype=np.uint8)
    return _write_raster(
        runtime_workspace / "inputs" / "nodata_less_uint8.tif",
        values,
        nodata=None,
    )


@pytest.fixture
def clip_polygon_path(runtime_workspace: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {"name": ["clip"]},
        geometry=[box(0.0, 1.0, 1.0, 2.0)],
        crs="EPSG:4326",
    )
    return _write_geojson(runtime_workspace / "inputs" / "clip.geojson", gdf)


@pytest.fixture
def triangular_clip_polygon_path(runtime_workspace: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {"name": ["triangle"]},
        geometry=[Polygon([(0.0, 2.0), (1.2, 2.0), (0.0, 0.0)])],
        crs="EPSG:4326",
    )
    return _write_geojson(runtime_workspace / "inputs" / "triangle_clip.geojson", gdf)


@pytest.fixture
def matching_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
    return _write_raster(runtime_workspace / "inputs" / "matching.tif", values)


@pytest.fixture
def adjacent_left_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    return _write_raster(runtime_workspace / "inputs" / "adjacent_left.tif", values)


@pytest.fixture
def adjacent_right_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float32)
    return _write_raster(
        runtime_workspace / "inputs" / "adjacent_right.tif",
        values,
        transform=from_origin(2.0, 2.0, 1.0, 1.0),
    )


@pytest.fixture
def half_pixel_offset_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[9.0, 10.0], [11.0, 12.0]], dtype=np.float32)
    return _write_raster(
        runtime_workspace / "inputs" / "half_pixel_offset.tif",
        values,
        transform=from_origin(0.5, 2.0, 1.0, 1.0),
    )


@pytest.fixture
def overlapping_mean_left_raster_path(runtime_workspace: Path) -> Path:
    values = np.array(
        [
            [[1.0, 2.0], [3.0, -9999.0]],
            [[10.0, 20.0], [30.0, -9999.0]],
        ],
        dtype=np.float32,
    )
    return _write_raster(
        runtime_workspace / "inputs" / "overlap_left.tif",
        values,
    )


@pytest.fixture
def overlapping_mean_right_raster_path(runtime_workspace: Path) -> Path:
    values = np.array(
        [
            [[100.0, 200.0], [300.0, -9999.0]],
            [[1000.0, 2000.0], [3000.0, -9999.0]],
        ],
        dtype=np.float32,
    )
    return _write_raster(
        runtime_workspace / "inputs" / "overlap_right.tif",
        values,
        transform=from_origin(1.0, 2.0, 1.0, 1.0),
    )


@pytest.fixture
def uint8_mean_left_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[1]], dtype=np.uint8)
    return _write_raster(
        runtime_workspace / "inputs" / "uint8_mean_left.tif",
        values,
        nodata=255,
    )


@pytest.fixture
def uint8_mean_right_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[2]], dtype=np.uint8)
    return _write_raster(
        runtime_workspace / "inputs" / "uint8_mean_right.tif",
        values,
        nodata=255,
    )


@pytest.fixture
def shifted_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[1.0, 2.0], [3.0, -9999.0]], dtype=np.float32)
    shifted_transform = from_origin(1.0, 2.0, 1.0, 1.0)
    return _write_raster(
        runtime_workspace / "inputs" / "shifted.tif",
        values,
        transform=shifted_transform,
    )


@pytest.fixture
def noisy_transform_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float32)
    noisy_transform = from_origin(
        0.0,
        np.nextafter(2.0, 3.0),
        np.nextafter(1.0, 2.0),
        np.nextafter(1.0, 2.0),
    )
    return _write_raster(
        runtime_workspace / "inputs" / "noisy_transform.tif",
        values,
        transform=noisy_transform,
        crs=CRS.from_epsg(4326),
    )


@pytest.fixture
def composite_series_raster_paths(runtime_workspace: Path) -> list[Path]:
    first = _write_raster(
        runtime_workspace / "inputs" / "composite_01.tif",
        np.array([[1.0, -9999.0], [3.0, -9999.0]], dtype=np.float32),
    )
    second = _write_raster(
        runtime_workspace / "inputs" / "composite_02.tif",
        np.array([[3.0, 5.0], [-9999.0, -9999.0]], dtype=np.float32),
    )
    return [first, second]


@pytest.fixture
def trend_series_raster_paths(runtime_workspace: Path) -> list[Path]:
    rasters: list[Path] = []
    for index, values in enumerate(
        [
            np.array([[100.0, 200.0], [10.0, 5.0]], dtype=np.float32),
            np.array([[110.0, 220.0], [20.0, 5.0]], dtype=np.float32),
            np.array([[120.0, 240.0], [30.0, 5.0]], dtype=np.float32),
        ],
        start=1,
    ):
        rasters.append(
            _write_raster(
                runtime_workspace / "inputs" / f"trend_{index:02d}.tif",
                values,
            )
        )
    return rasters


@pytest.fixture
def trend_two_step_raster_paths(runtime_workspace: Path) -> list[Path]:
    rasters: list[Path] = []
    for index, values in enumerate(
        [
            np.array([[50.0, 60.0], [2.0, 7.0]], dtype=np.float32),
            np.array([[55.0, 65.0], [5.0, 7.0]], dtype=np.float32),
        ],
        start=1,
    ):
        rasters.append(
            _write_raster(
                runtime_workspace / "inputs" / f"trend_two_step_{index:02d}.tif",
                values,
            )
        )
    return rasters


@pytest.fixture
def anomaly_latest_spike_raster_paths(runtime_workspace: Path) -> list[Path]:
    rasters: list[Path] = []
    for index, values in enumerate(
        [
            np.array([[1.0, 1.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[1.0, 1.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[1.0, 1.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[10.0, 1.0], [0.0, -9999.0]], dtype=np.float32),
        ],
        start=1,
    ):
        rasters.append(
            _write_raster(
                runtime_workspace / "inputs" / f"anomaly_latest_{index:02d}.tif",
                values,
            )
        )
    return rasters


@pytest.fixture
def anomaly_sparse_baseline_raster_paths(runtime_workspace: Path) -> list[Path]:
    rasters: list[Path] = []
    for index, values in enumerate(
        [
            np.array([[1.0, -9999.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[1.0, 1.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[1.0, 1.0], [1.0, -9999.0]], dtype=np.float32),
            np.array([[5.0, 5.0], [1.0, -9999.0]], dtype=np.float32),
        ],
        start=1,
    ):
        rasters.append(
            _write_raster(
                runtime_workspace / "inputs" / f"anomaly_sparse_{index:02d}.tif",
                values,
            )
        )
    return rasters


@pytest.fixture
def corrupt_raster_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "corrupt.tif"
    path.write_text("not-a-raster", encoding="utf-8")
    return path


@pytest.fixture
def band_mismatch_raster_path(runtime_workspace: Path) -> Path:
    values = np.array(
        [
            [[1.0, 2.0], [3.0, 4.0]],
            [[5.0, 6.0], [7.0, 8.0]],
        ],
        dtype=np.float32,
    )
    return _write_raster(runtime_workspace / "inputs" / "band_mismatch.tif", values)


@pytest.fixture
def crs_mismatch_raster_path(runtime_workspace: Path) -> Path:
    values = np.array([[9.0, 10.0], [11.0, 12.0]], dtype=np.float32)
    return _write_raster(
        runtime_workspace / "inputs" / "crs_mismatch.tif",
        values,
        crs="EPSG:3857",
    )


@pytest.fixture
def empty_vector_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "empty.geojson"
    gdf = gpd.GeoDataFrame(
        {"name": pd.Series(dtype="object")},
        geometry=gpd.GeoSeries([], dtype="geometry", crs="EPSG:4326"),
        crs="EPSG:4326",
    )
    return _write_geojson(path, gdf)


@pytest.fixture
def invalid_vector_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "invalid_vector.geojson"
    gdf = gpd.GeoDataFrame(
        {"name": ["invalid"]},
        geometry=[Point()],
        crs="EPSG:4326",
    )
    return _write_geojson(path, gdf)


@pytest.fixture
def vector_without_crs_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "no_crs_vector.shp"
    gdf = gpd.GeoDataFrame(
        {"name": ["clip"]},
        geometry=[box(0.0, 1.0, 1.0, 2.0)],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="ESRI Shapefile")
    prj_path = path.with_suffix(".prj")
    if prj_path.exists():
        prj_path.unlink()
    return path


@pytest.fixture
def mercator_overlap_vector_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "overlap_mercator.geojson"
    gdf = gpd.GeoDataFrame(
        {"name": ["overlap"]},
        geometry=[box(0.0, 0.0, 2.0, 2.0)],
        crs="EPSG:4326",
    ).to_crs("EPSG:3857")
    return _write_geojson(path, gdf)


@pytest.fixture
def far_vector_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "far.geojson"
    gdf = gpd.GeoDataFrame(
        {"name": ["far"]},
        geometry=[box(10.0, 10.0, 11.0, 11.0)],
        crs="EPSG:4326",
    )
    return _write_geojson(path, gdf)


@pytest.fixture
def corrupt_vector_path(runtime_workspace: Path) -> Path:
    path = runtime_workspace / "inputs" / "corrupt.geojson"
    path.write_text("{not-valid-geojson}", encoding="utf-8")
    return path
